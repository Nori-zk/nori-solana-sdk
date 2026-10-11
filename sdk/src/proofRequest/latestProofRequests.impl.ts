import { type ResolveNodeData } from '@yaw-rx/ystate';
import { EMPTY, filter, map, merge, type Observable, switchMap, timer } from 'rxjs';
import { type ConnectedReadClients, type ProofRequestConnections } from './connectedRead.js';
import { fetchProofRequestHistoryPage$, type ProofRequestHistoryAddresses } from './fetchProofRequestHistory.js';
import { blockNumber$ } from '../rpc/eth/blockNumber.js';
import { type ProofRequestsByTargetQuery } from '../rpc/eth/proofRequestsByTarget.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine, dueOn } from '../rpc/connection/readThroughConnections.impl.js';
import { withOutcome } from '../utils/machines.js';
import { LatestProofRequestsGraph } from './latestProofRequests.js';

export interface LatestProofRequestsQuery extends Pick<
    ProofRequestsByTargetQuery,
    'target' | 'fromBlock' | 'maxBlockRangePerQuery'
> {
    /** How many of the newest requests to keep in view. */
    count: number;
}

/** The newest requests, and the block of the oldest one (the latest block when there are none). */
type NewestRequests = ResolveNodeData<typeof LatestProofRequestsGraph.nodes, 'current'>;

/**
 * Starts a live view of a submitting address's newest `count` proof
 * requests, newest first, reading through both connections. It refreshes
 * every `pollIntervalMs` and on every `recheckTrigger$` emission.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param addresses The queue and program addresses.
 * @param query The submitting address, lowest block and view size.
 * @param pollIntervalMs The delay between refreshes in ms (default: 15000).
 * @param recheckTrigger$ An extra refresh signal, e.g. bridge state changes from the websocket.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; its states carry the view. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export function createLatestProofRequestsMachine(
    connections: ProofRequestConnections,
    addresses: ProofRequestHistoryAddresses,
    query: LatestProofRequestsQuery,
    pollIntervalMs = 15_000,
    recheckTrigger$: Observable<unknown> = EMPTY,
    backoff: ReadRetryBackoff = {}
) {
    /**
     * Reads the newest `count` requests from `fromBlock` up to the latest block.
     *
     * @param clients The clients to read through.
     * @param target The submitting address.
     * @param fromBlock The lowest block to read from.
     * @returns The newest requests and the block of the oldest one, once.
     */
    const readNewest$ = (clients: ConnectedReadClients, target: string, fromBlock: number): Observable<NewestRequests> =>
        clients
            .ethereum((provider) => blockNumber$(provider, 'latest'))
            .pipe(
                switchMap((toBlock) =>
                    fetchProofRequestHistoryPage$(clients, addresses, {
                        target,
                        fromBlock,
                        toBlock,
                        order: 'desc',
                        pageSize: query.count,
                        maxBlockRangePerQuery: query.maxBlockRangePerQuery,
                    }).pipe(
                        map(({ entries }) => ({
                            target,
                            view: entries,
                            oldestBlock: entries.length > 0 ? entries[entries.length - 1].blockNumber : toBlock,
                        }))
                    )
                )
            );

    /**
     * A request the view showed is gone, so a reorg removed it: one not read
     * again that the new view still reaches (it is not full, or the request
     * is newer than its oldest). One pushed out by newer requests is not gone.
     */
    const requestsMissing = (value: NewestRequests, previous: NewestRequests, node: 'loading' | 'refreshing') => {
        if (node !== 'refreshing') return false;
        const read = new Set(value.view.map(({ requestId }) => requestId));
        // The view is newest first: its last request is its oldest.
        const oldestRead = value.view.length < query.count ? undefined : value.view[value.view.length - 1].requestId;
        return previous.view.some(
            ({ requestId }) => !read.has(requestId) && (oldestRead === undefined || requestId > oldestRead)
        );
    };

    return startReadThroughConnectionsMachine(LatestProofRequestsGraph, {
        connections,
        // A load reads from `fromBlock`; a refresh from the oldest block of the view it holds.
        read: (clients, previous, node) =>
            readNewest$(clients, previous.target, node === 'loading' ? query.fromBlock : previous.oldestBlock),
        refreshOn: () => dueOn(merge(timer(pollIntervalMs), recheckTrigger$)),
        arrivedElsewhere: requestsMissing,
        kind: 'logs',
        backoff,
        ownTransitions: (read$) => ({
            // A request in the view has gone: load everything again from `fromBlock`, keeping the view on screen.
            requestsMissing: {
                $: () =>
                    withOutcome(read$, 'succeeded').pipe(
                        filter(({ value, previous, node }) => requestsMissing(value, previous, node))
                    ),
                next: (
                    _missing: unknown,
                    _dest: unknown,
                    { target, view, oldestBlock, failedReads }: NewestRequests & { failedReads: number }
                ) => ({ target, view, oldestBlock, failedReads }),
            },
        }),
        // The submitting address is its starting data.
        start: { node: 'loading', data: { target: query.target, view: [], oldestBlock: 0, failedReads: 0 } },
    });
}
