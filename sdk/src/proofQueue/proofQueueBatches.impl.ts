import { catchError, concat, EMPTY, expand, forkJoin, last, map, merge, type Observable, of, switchMap } from 'rxjs';
import { type ConnectedReadClients, type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { type ProofRequestHistoryAddresses } from '../proofRequest/fetchProofRequestHistory.js';
import { enqueuedProofRequests$ } from '../rpc/eth/enqueuedProofRequests.js';
import {
    proofRequestsByTarget$,
    type ProofRequestHistoryCursor,
} from '../rpc/eth/proofRequestsByTarget.js';
import { proofQueueBatchSummaries$ } from '../rpc/solana/proofQueueBatchSummaries.js';
import { proofQueueBatchesCovering$ } from '../rpc/solana/proofQueueBatchCovering.js';
import {
    readMachineChanged$,
    readMachineValue$,
    type ReadRetryBackoff,
    startReadThroughConnectionsMachine,
} from '../rpc/connection/readThroughConnections.impl.js';
import { type createBridgeStateMachine } from './bridgeState.impl.js';
import { type createEthereumBlocksMachine } from './ethereumBlocks.impl.js';
import {
    EMPTY_PROOF_QUEUE_BATCHES_VIEW,
    ProofQueueBatchesGraph,
    type ProofQueueBatchesView,
    type ShownProofQueueBatch,
} from './proofQueueBatches.js';

export interface ProofQueueBatchesQuery {
    /** Only this submitting address's requests and the batches holding them; every address when omitted. */
    target?: string;
    /** The lowest block to read proof requests from, e.g. the queue's deployment block. */
    fromBlock: number;
    /** How many batches to keep in view. */
    count: number;
    /** Max blocks per log query, kept under the provider's limits. */
    maxBlockRangePerQuery?: number;
}

/** The running machines the view reads from. */
export interface ProofQueueBatchesSources {
    /** The bridge's state, from `createBridgeStateMachine`. */
    bridgeState: ReturnType<typeof createBridgeStateMachine>;
    /** Ethereum's latest and finalized blocks, from `createEthereumBlocksMachine`. */
    ethereumBlocks: ReturnType<typeof createEthereumBlocksMachine>;
}

/** Requests read per page while finding the batches holding an address's requests. */
const TARGET_PAGE_SIZE = 100;

/**
 * The indices of the newest batches, newest first.
 *
 * @param batchCount How many batches the bridge holds.
 * @param count How many to keep.
 * @returns Up to `count` indices.
 */
const newestIndices = (batchCount: bigint, count: number): bigint[] =>
    Array.from(
        { length: Number(batchCount < BigInt(count) ? batchCount : BigInt(count)) },
        (_, i) => batchCount - 1n - BigInt(i)
    );

/**
 * Starts a live view of the bridge's newest `count` proof queue batches and
 * the requests no batch covers yet, for every submitting address or
 * `query.target`'s, reading through both connections. The bridge's state
 * and Ethereum's blocks come from their machines: the view reads once they
 * are `current`, fails when one fails, and refreshes when one moves past
 * the view (a new batch or queue cursor, a block past the view's latest),
 * waits for a connection or fails, and on every `recheckTrigger$` emission.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param addresses The queue and program addresses.
 * @param query The submitting address, lowest block and view size.
 * @param sources The bridge state and Ethereum blocks machines the view reads from.
 * @param recheckTrigger$ An extra refresh signal, e.g. Nori's stage changing on its websocket.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; its states carry the view. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export function createProofQueueBatchesMachine(
    connections: ProofRequestConnections,
    addresses: ProofRequestHistoryAddresses,
    query: ProofQueueBatchesQuery,
    sources: ProofQueueBatchesSources,
    recheckTrigger$: Observable<unknown> = EMPTY,
    backoff: ReadRetryBackoff = {}
) {
    /**
     * The bridge's queue cursor and batch count, and Ethereum's blocks, from
     * their machines once both are `current`.
     *
     * @returns Them, once; errors with a source's error when it fails.
     */
    const sourcesHeld$ = () =>
        forkJoin([readMachineValue$(sources.bridgeState), readMachineValue$(sources.ethereumBlocks)]).pipe(
            map(([{ bridgeState }, blocks]) => {
                if (bridgeState === undefined) throw new Error('The bridge state machine holds no bridge state.');
                return { queueCursor: bridgeState.queueCursor, batchCount: bridgeState.proofQueueBatchCount, ...blocks };
            })
        );

    /**
     * The batches holding `query.target`'s newest proven requests, newest
     * first, each with its request ids: its requests read from Ethereum
     * newest first, a page at a time, until `count` batches are found or
     * none are left.
     *
     * @param clients The clients to read through.
     * @param target The submitting address.
     * @param queueCursor The first request id no batch covers yet.
     * @param batchCount How many batches the bridge holds.
     * @param latestBlock The highest block to read requests from.
     * @returns Each batch's index and the address's request ids in it.
     */
    const readTargetBatches$ = (
        clients: ConnectedReadClients,
        target: string,
        queueCursor: bigint,
        batchCount: bigint,
        latestBlock: number
    ): Observable<Map<bigint, bigint[]>> => {
        /** One page after `after`, its proven requests added to the batches found so far. */
        const page$ = (byBatch: Map<bigint, bigint[]>, after: ProofRequestHistoryCursor | undefined) =>
            clients
                .ethereum((provider) =>
                    proofRequestsByTarget$(provider, addresses.proofQueueAddress, {
                        target,
                        fromBlock: query.fromBlock,
                        toBlock: latestBlock,
                        order: 'desc',
                        pageSize: TARGET_PAGE_SIZE,
                        after,
                        maxBlockRangePerQuery: query.maxBlockRangePerQuery,
                    })
                )
                .pipe(
                    switchMap((page) => {
                        const proven = page.requests
                            .map(({ requestId }) => requestId)
                            .filter((requestId) => requestId < queueCursor);
                        return clients
                            .solana((rpc) =>
                                proofQueueBatchesCovering$(rpc, proven, batchCount, addresses.programAddress)
                            )
                            .pipe(
                                map((found) => {
                                    const next = new Map(byBatch);
                                    for (const requestId of proven) {
                                        const index = found.get(requestId)?.proofQueueBatchIndex;
                                        if (index === undefined) continue;
                                        const ids = next.get(index);
                                        if (ids) next.set(index, [...ids, requestId]);
                                        else if (next.size < query.count) next.set(index, [requestId]);
                                    }
                                    return { byBatch: next, page };
                                })
                            );
                    })
                );
        return page$(new Map(), undefined).pipe(
            expand(({ byBatch, page }) =>
                byBatch.size >= query.count || page.done ? EMPTY : page$(byBatch, page.cursor)
            ),
            last(),
            map(({ byBatch }) => byBatch)
        );
    };

    /**
     * Reads the view: from the bridge's state and Ethereum's blocks their
     * machines hold, the newest batches (all of them, or those holding
     * `query.target`'s requests) and the requests no batch covers yet.
     *
     * @param clients The clients to read through.
     * @param target The submitting address the view is filtered on, if any.
     * @returns The view, once.
     */
    const readView$ = (clients: ConnectedReadClients, target: string | undefined): Observable<ProofQueueBatchesView> =>
        sourcesHeld$().pipe(
            switchMap(({ queueCursor, batchCount, latestBlock, finalizedBlock }) =>
                (target === undefined
                    ? of(undefined)
                    : readTargetBatches$(clients, target, queueCursor, batchCount, latestBlock)
                ).pipe(
                    switchMap((matching) => {
                        const indices = matching
                            ? [...matching.keys()].sort((a, b) => (a < b ? 1 : -1))
                            : newestIndices(batchCount, query.count);
                        const newest = batchCount > 0n ? [batchCount - 1n] : [];
                        return clients
                            .solana((rpc) =>
                                proofQueueBatchSummaries$(
                                    rpc,
                                    [...new Set([...newest, ...indices])],
                                    addresses.programAddress
                                )
                            )
                            .pipe(
                                switchMap((summaries) => {
                                    const byIndex = new Map(
                                        summaries.map((summary) => [summary.proofQueueBatchIndex, summary])
                                    );
                                    const batches = indices.flatMap((index): ShownProofQueueBatch[] => {
                                        const summary = byIndex.get(index);
                                        return summary ? [{ ...summary, matching: matching?.get(index) }] : [];
                                    });
                                    const newestSummary = newest.length > 0 ? byIndex.get(newest[0]) : undefined;
                                    return clients
                                        .ethereum((provider) =>
                                            enqueuedProofRequests$(provider, addresses.proofQueueAddress, {
                                                fromBlock: newestSummary
                                                    ? Number(newestSummary.outputBlockNumber) + 1
                                                    : query.fromBlock,
                                                toBlock: latestBlock,
                                                fromRequestId: queueCursor,
                                                target,
                                                maxBlockRangePerQuery: query.maxBlockRangePerQuery,
                                            })
                                        )
                                        .pipe(
                                            map((waiting) => ({
                                                batchCount,
                                                queueCursor,
                                                batches,
                                                waiting,
                                                finalizedBlock,
                                                latestBlock,
                                            }))
                                        );
                                })
                            );
                    })
                )
            )
        );

    return startReadThroughConnectionsMachine(ProofQueueBatchesGraph, {
        connections,
        read: (clients, { target }) => readView$(clients, target).pipe(map((view) => ({ target, view }))),
        // Following once both sources hold a value (or one failed, which fails the read): the view
        // waits in `loading` until then. A refresh is due when a source moves past the view after it.
        refreshOn: ({ view }) =>
            concat(
                sourcesHeld$().pipe(
                    map((): void => undefined),
                    catchError(() => of(undefined))
                ),
                merge(
                    readMachineChanged$(
                        sources.bridgeState,
                        ({ bridgeState }) =>
                            bridgeState?.proofQueueBatchCount !== view.batchCount ||
                            bridgeState.queueCursor !== view.queueCursor
                    ),
                    readMachineChanged$(sources.ethereumBlocks, ({ latestBlock }) => latestBlock > view.latestBlock),
                    recheckTrigger$
                )
            ),
        kind: 'logs',
        backoff,
        // The submitting address it is filtered on is its starting data.
        start: { node: 'loading', data: { target: query.target, view: EMPTY_PROOF_QUEUE_BATCHES_VIEW, failedReads: 0 } },
    });
}
