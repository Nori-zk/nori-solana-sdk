import { EMPTY, expand, last, map, type Observable, of, switchMap } from 'rxjs';
import { proofRequestSnapshots$ } from './proofRequestSnapshots.js';
import { type ConnectedReadClients } from './connectedRead.js';
import {
    type ProofRequestStateSnapshot,
    type ProofRequestStateSnapshotRequest,
} from './getProofRequestStateSnapshot.js';
import { blockNumber$ } from '../rpc/eth/blockNumber.js';
import { type ProofRequest } from '../rpc/eth/proofRequest.js';
import {
    proofRequestsByTarget$,
    type ProofRequestHistoryCursor,
    type ProofRequestsByTargetQuery,
} from '../rpc/eth/proofRequestsByTarget.js';
import { bridgeState$ } from '../rpc/solana/bridgeState.js';

/** The addresses a history read needs: no single enqueuing transaction. */
export type ProofRequestHistoryRequest = Omit<
    ProofRequestStateSnapshotRequest,
    'proofRequestTxHash'
>;

/** The queue and program addresses a history read needs besides its clients. */
export type ProofRequestHistoryAddresses = Pick<
    ProofRequestHistoryRequest,
    'proofQueueAddress' | 'programAddress'
>;

/** A request a submitting address enqueued, with where it is now. */
export interface ProofRequestHistoryEntry extends ProofRequest {
    /** `unprocessed`, or `proofAvailable` with the committed batch covering it (what `createProofRequestWitnessMachine` takes). */
    snapshot: ProofRequestStateSnapshot;
}

export interface ProofRequestHistoryPage {
    entries: ProofRequestHistoryEntry[];
    /** Pass as `after` for the next page. */
    cursor?: ProofRequestHistoryCursor;
    /** Whether the scan reached the end of the block range. */
    done: boolean;
}

export interface ProofRequestCounts {
    total: number;
    proofAvailable: number;
    unprocessed: number;
}

/** Requests read per log page while counting; counting keeps no entries, only ids. */
const COUNTING_PAGE_SIZE = 1000;

/**
 * The highest block a read searches: `toBlock`, or Ethereum's latest block
 * when it is omitted.
 *
 * @param clients The runner per chain.
 * @param toBlock The highest block, if given.
 * @returns The block's number, once.
 */
const toBlockOf$ = (clients: ConnectedReadClients, toBlock: number | undefined): Observable<number> =>
    toBlock !== undefined ? of(toBlock) : clients.ethereum((provider) => blockNumber$(provider, 'latest'));

/**
 * Reads one page of a submitting address's proof requests from Ethereum and
 * places them against one read of the bridge state on Solana.
 *
 * @param clients The runner per chain: Ethereum reads the page, Solana places it.
 * @param request The queue and program addresses.
 * @param query The submitting address, block range, order, page size and cursor.
 * @returns The page's entries, its continuation cursor, and whether the range is exhausted, once.
 *   Errors with `ConnectionNotReadyError` when a chain could not serve its part.
 */
export function fetchProofRequestHistoryPage$(
    clients: ConnectedReadClients,
    request: ProofRequestHistoryRequest,
    query: ProofRequestsByTargetQuery
): Observable<ProofRequestHistoryPage> {
    return toBlockOf$(clients, query.toBlock).pipe(
        switchMap((toBlock) =>
            clients.ethereum((provider) =>
                proofRequestsByTarget$(provider, request.proofQueueAddress, { ...query, toBlock })
            )
        ),
        switchMap((page) =>
            page.requests.length === 0
                ? of({ entries: [], cursor: page.cursor, done: page.done })
                : clients
                      .solana((rpc) =>
                          proofRequestSnapshots$(
                              rpc,
                              page.requests.map((proofRequest) => ({
                                  requestId: proofRequest.requestId,
                                  requestBlockNumber: BigInt(proofRequest.blockNumber),
                              })),
                              request.programAddress
                          )
                      )
                      .pipe(
                          map((snapshots) => ({
                              entries: page.requests.map((proofRequest, i) => ({
                                  ...proofRequest,
                                  snapshot: snapshots[i],
                              })),
                              cursor: page.cursor,
                              done: page.done,
                          }))
                      )
        )
    );
}

/**
 * Counts a submitting address's proof requests over a block range, and how
 * many have a proof available: the queue drains in order, so every id below
 * the bridge's queue cursor is proven.
 *
 * @param clients The runner per chain: Ethereum reads the requests, Solana the bridge's queue cursor.
 * @param request The queue and program addresses.
 * @param query The submitting address and block range.
 * @returns The total, proven and unprocessed counts, once.
 *   Errors with `ConnectionNotReadyError` when a chain could not serve its part.
 */
export function fetchProofRequestCountsByTarget$(
    clients: ConnectedReadClients,
    request: ProofRequestHistoryRequest,
    query: Pick<ProofRequestsByTargetQuery, 'target' | 'fromBlock' | 'toBlock' | 'maxBlockRangePerQuery'>
): Observable<ProofRequestCounts> {
    /** One page after `after`, its ids added to those read so far. */
    const page$ = (toBlock: number, requestIds: bigint[], after: ProofRequestHistoryCursor | undefined) =>
        clients
            .ethereum((provider) =>
                proofRequestsByTarget$(provider, request.proofQueueAddress, {
                    ...query,
                    toBlock,
                    order: 'asc',
                    pageSize: COUNTING_PAGE_SIZE,
                    after,
                })
            )
            .pipe(
                map((page) => ({
                    page,
                    requestIds: [...requestIds, ...page.requests.map((proofRequest) => proofRequest.requestId)],
                }))
            );

    // The block range is fixed up front, so every page reads the same range.
    return toBlockOf$(clients, query.toBlock).pipe(
        switchMap((toBlock) =>
            page$(toBlock, [], undefined).pipe(
                expand(({ page, requestIds }) => (page.done ? EMPTY : page$(toBlock, requestIds, page.cursor))),
                last()
            )
        ),
        switchMap(({ requestIds }) =>
            clients
                .solana((rpc) => bridgeState$(rpc, request.programAddress))
                .pipe(
                    map(({ queueCursor }) => {
                        const proofAvailable = requestIds.filter((requestId) => requestId < queueCursor).length;
                        return {
                            total: requestIds.length,
                            proofAvailable,
                            unprocessed: requestIds.length - proofAvailable,
                        };
                    })
                )
        )
    );
}
