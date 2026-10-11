import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { concatMap, defer, from, map, type Observable, reduce } from 'rxjs';
import { blockRanges, MAX_BLOCK_RANGE_PER_QUERY } from './blockRanges.js';
import { type ProofRequest } from './proofRequest.js';
import { ethRpcRead$ } from './ethRpcRead.js';

/** A proof request as its `ProofRequested` log records it, with its collection keys. */
export interface EnqueuedProofRequest extends ProofRequest {
    /** The keys relayed into the request's leaf, as 0x-prefixed words. */
    collectionKeys: string[];
}

export interface EnqueuedProofRequestsQuery {
    /** Lowest block searched. */
    fromBlock: number;
    /** Highest block searched. */
    toBlock: number;
    /** Only requests this submitting address enqueued; every request when omitted. */
    target?: string;
    /** Lowest request id kept. */
    fromRequestId?: bigint;
    /** Request ids from here on are left out. */
    toRequestId?: bigint;
    /** Max blocks per log query, kept under the provider's limits. */
    maxBlockRangePerQuery?: number;
}

/**
 * The proof requests enqueued over a block range, from the queue's
 * `ProofRequested` logs, in queue order, with their transaction hashes and
 * collection keys. Logs are queried in block ranges of at most
 * `maxBlockRangePerQuery`, filtered on the indexed `target` when one is
 * given. Reads nothing but logs, so any provider serves it, archive or not.
 *
 * @param provider The Ethereum provider used for every read.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param query The block range, and optionally the submitting address and request id range.
 * @returns The requests, in ascending request id, once.
 */
export const enqueuedProofRequests$ = (
    provider: EthereumProvider,
    proofQueueAddress: string,
    query: EnqueuedProofRequestsQuery
): Observable<EnqueuedProofRequest[]> =>
    defer(() => {
        const queue = NoriProofRequestQueue__factory.connect(proofQueueAddress, provider);
        const inIdRange = (requestId: bigint) =>
            (query.fromRequestId === undefined || requestId >= query.fromRequestId) &&
            (query.toRequestId === undefined || requestId < query.toRequestId);
        return from(
            blockRanges(
                query.fromBlock,
                query.toBlock,
                query.maxBlockRangePerQuery ?? MAX_BLOCK_RANGE_PER_QUERY,
                'asc'
            )
        ).pipe(
            concatMap(([rangeFrom, rangeTo]) =>
                ethRpcRead$(
                    () => queue.queryFilter(queue.filters.ProofRequested(undefined, query.target), rangeFrom, rangeTo),
                    `ProofRequested log query over blocks ${rangeFrom}-${rangeTo} failed.`
                )
            ),
            map((logs) =>
                logs
                    .filter((log) => inIdRange(log.args.requestId))
                    .map(
                        (log): EnqueuedProofRequest => ({
                            requestId: log.args.requestId,
                            target: log.args.target,
                            slotKey: log.args.slotKey,
                            collectionKeys: [...log.args.collectionKeys],
                            blockNumber: log.blockNumber,
                            transactionHash: log.transactionHash,
                        })
                    )
            ),
            reduce((requests, chunk) => [...requests, ...chunk], [] as EnqueuedProofRequest[]),
            map((requests) => requests.sort((a, b) => (a.requestId < b.requestId ? -1 : a.requestId > b.requestId ? 1 : 0)))
        );
    });
