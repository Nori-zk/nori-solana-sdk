import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { concatMap, defer, from, last, map, type Observable, scan, startWith, takeWhile } from 'rxjs';
import { blockRanges, MAX_BLOCK_RANGE_PER_QUERY } from './blockRanges.js';
import { type ProofRequest } from './proofRequest.js';
import { ethRpcRead$ } from './ethRpcRead.js';

/** `asc` pages from oldest to newest request, `desc` from newest to oldest. */
export type ProofRequestHistoryOrder = 'asc' | 'desc';

/** Where a page ended: its last request. The next page continues past it. */
export interface ProofRequestHistoryCursor {
    requestId: bigint;
    blockNumber: number;
}

export interface ProofRequestsByTargetQuery {
    /** The submitting address: the `msg.sender` that enqueued the requests. */
    target: string;
    /** Lowest block searched, e.g. the queue's deployment block. */
    fromBlock: number;
    /** Highest block searched; the latest block when omitted. */
    toBlock?: number;
    order: ProofRequestHistoryOrder;
    /** Requests per page. */
    pageSize: number;
    /** The previous page's `cursor`; the first page when omitted. */
    after?: ProofRequestHistoryCursor;
    /** Max blocks per log query, kept under the provider's limits. */
    maxBlockRangePerQuery?: number;
}

export interface ProofRequestsByTargetPage {
    /** Up to `pageSize` requests, in `order`. */
    requests: ProofRequest[];
    /** Pass as `after` for the next page: the last request returned, or `after` when none were. */
    cursor?: ProofRequestHistoryCursor;
    /** Whether the scan reached the end of the block range. */
    done: boolean;
}

/**
 * Orders two request ids for `Array.prototype.sort`.
 *
 * @param a A request id.
 * @param b Another request id.
 * @returns Negative when `a` comes first, positive when `b` does, 0 when equal.
 */
function compareRequestIds(a: bigint, b: bigint): number {
    return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Rejects a query setting that is not a positive whole number.
 *
 * @param name The setting's name, for the error message.
 * @param value The setting's value.
 * @throws RangeError When `value` is not a positive integer.
 */
function assertPositiveInteger(name: string, value: number) {
    if (!Number.isInteger(value) || value <= 0) {
        throw new RangeError(`${name} must be a positive integer, got ${value}.`);
    }
}

/**
 * One page of the proof requests a submitting address enqueued, from
 * `ProofRequested` logs filtered on their indexed `target`.
 *
 * Logs are queried in block ranges of at most `maxBlockRangePerQuery`,
 * walking up from `fromBlock` (`asc`) or down from `toBlock` (`desc`) until
 * `pageSize` requests are found or the range is exhausted. Request ids grow
 * with block number, so a page continues exactly after its cursor's request,
 * including the rest of the cursor's own block.
 *
 * @param provider The Ethereum provider used for every read.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param query The submitting address, block range (its highest block given), order, page size and cursor.
 * @returns The page, its continuation cursor, and whether the range is exhausted, once.
 */
export const proofRequestsByTarget$ = (
    provider: EthereumProvider,
    proofQueueAddress: string,
    query: ProofRequestsByTargetQuery & { toBlock: number }
): Observable<ProofRequestsByTargetPage> =>
    defer(() => {
        const maxBlockRange = query.maxBlockRangePerQuery ?? MAX_BLOCK_RANGE_PER_QUERY;
        assertPositiveInteger('pageSize', query.pageSize);
        assertPositiveInteger('maxBlockRangePerQuery', maxBlockRange);

        const queue = NoriProofRequestQueue__factory.connect(proofQueueAddress, provider);
        const { after, order, toBlock } = query;
        const [start, end] =
            order === 'asc'
                ? [after?.blockNumber ?? query.fromBlock, toBlock]
                : [after?.blockNumber ?? toBlock, query.fromBlock];
        const isPastCursor = (requestId: bigint) =>
            after === undefined || (order === 'asc' ? requestId > after.requestId : requestId < after.requestId);
        const full = (requests: ProofRequest[]) => requests.length >= query.pageSize;

        return from(blockRanges(start, end, maxBlockRange, order)).pipe(
            concatMap(([rangeFrom, rangeTo]) =>
                ethRpcRead$(
                    () => queue.queryFilter(queue.filters.ProofRequested(undefined, query.target), rangeFrom, rangeTo),
                    `ProofRequested log query over blocks ${rangeFrom}-${rangeTo} failed.`
                )
            ),
            map((logs) =>
                logs
                    .map(
                        (log): ProofRequest => ({
                            requestId: log.args.requestId,
                            target: log.args.target,
                            slotKey: log.args.slotKey,
                            blockNumber: log.blockNumber,
                            transactionHash: log.transactionHash,
                        })
                    )
                    .filter((request) => isPastCursor(request.requestId))
                    .sort((a, b) =>
                        order === 'asc'
                            ? compareRequestIds(a.requestId, b.requestId)
                            : compareRequestIds(b.requestId, a.requestId)
                    )
            ),
            scan((requests, inRange) => [...requests, ...inRange], [] as ProofRequest[]),
            startWith([] as ProofRequest[]),
            // Reads range after range until a page is full or none are left.
            takeWhile((requests) => !full(requests), true),
            last(),
            map((requests): ProofRequestsByTargetPage => {
                if (full(requests)) {
                    const page = requests.slice(0, query.pageSize);
                    const lastRequest = page[page.length - 1];
                    return {
                        requests: page,
                        cursor: { requestId: lastRequest.requestId, blockNumber: lastRequest.blockNumber },
                        done: false,
                    };
                }
                const lastRequest = requests[requests.length - 1];
                return {
                    requests,
                    cursor: lastRequest
                        ? { requestId: lastRequest.requestId, blockNumber: lastRequest.blockNumber }
                        : after,
                    done: true,
                };
            })
        );
    });
