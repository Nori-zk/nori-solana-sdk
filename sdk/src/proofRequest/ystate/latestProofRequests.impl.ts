import {
    EMPTY,
    filter,
    map,
    merge,
    type Observable,
    ReplaySubject,
    share,
    Subject,
    switchMap,
    take,
    timer,
} from 'rxjs';
import {
    bothReady$,
    type ConnectedRead,
    type ConnectedReadClients,
    type ProofRequestConnections,
    readThroughConnections$,
    waitingOnChanged$,
} from '../connectedRead.js';
import {
    fetchProofRequestHistoryPage,
    type ProofRequestHistoryAddresses,
    type ProofRequestHistoryEntry,
} from '../proofRequestHistory.js';
import { EthRpcTransportError } from '../rpc/eth/errors.js';
import { type ProofRequestsByTargetQuery } from '../rpc/eth/fetchProofRequestsByTarget.js';
import {
    resolveHealthCheckTimings,
    retryDelayMs,
} from '../rpc/healthCheckTimings.js';
import { withBackoff } from '../rpc/withBackoff.js';
import { dataOnEntry$, stateOf$, type StartedMachine } from './dataOnEntry.js';
import {
    LatestProofRequestsGraph,
    type LatestProofRequestsState,
} from './latestProofRequests.js';
import { type ReadRetryBackoff } from './proofRequestHistory.impl.js';

export interface LatestProofRequestsQuery extends Pick<
    ProofRequestsByTargetQuery,
    'target' | 'fromBlock' | 'maxBlockRangePerQuery'
> {
    /** How many of the newest requests to keep in view. */
    count: number;
}

/** The newest requests read from a block up to the latest one. */
interface NewestRequests {
    view: ProofRequestHistoryEntry[];
    /** The block of the oldest request in `view`, or the latest block when `view` is empty. */
    oldestBlock: number;
}

/** A refresh's read, and whether a request the view showed has gone. */
interface Refresh extends NewestRequests {
    requestsMissing: boolean;
}

/**
 * Keeps only the reads with one outcome, narrowed to it.
 *
 * @param read$ Reads.
 * @param outcome The outcome to keep.
 * @returns The reads with that outcome.
 */
function withOutcome<T, TOutcome extends ConnectedRead<T>['outcome']>(
    read$: Observable<ConnectedRead<T>>,
    outcome: TOutcome
) {
    return read$.pipe(
        filter(
            (read): read is Extract<ConnectedRead<T>, { outcome: TOutcome }> =>
                read.outcome === outcome
        )
    );
}

/**
 * Starts a live view of a submitting address's newest `count` proof
 * requests, newest first, reading through both connections. It refreshes
 * every `pollIntervalMs` and on every `recheckTrigger$` emission.
 *
 * @param connections The Ethereum provider and Solana RPC, with their running connectivity machines.
 * @param addresses The queue and program addresses.
 * @param query The submitting address, lowest block and view size.
 * @param pollIntervalMs The delay between refreshes in ms (default: 15000).
 * @param recheckTrigger$ An extra refresh signal, e.g. bridge state changes from the websocket.
 * @param backoff How long a failed read waits before reading again.
 * @returns
 *   - `latestProofRequests`: the running machine; its states carry the view.
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
    const timings = resolveHealthCheckTimings(backoff);
    const retry$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<StartedMachine<LatestProofRequestsState>>(1);
    const state$ = stateOf$(started$);

    /**
     * Reads the newest `count` requests from `fromBlock` up to the latest block.
     *
     * @param clients The clients to read through.
     * @param fromBlock The lowest block to read from.
     * @returns The newest requests and the block of the oldest one.
     */
    const readNewest = async (
        { provider, rpc }: ConnectedReadClients,
        fromBlock: number
    ): Promise<NewestRequests> => {
        const toBlock = await withBackoff(() =>
            provider.getBlockNumber()
        ).catch((error: unknown) => {
            throw new EthRpcTransportError(
                'Failed to read the latest block number.',
                error
            );
        });
        const { entries } = await fetchProofRequestHistoryPage(
            { ...addresses, provider, rpc },
            {
                target: query.target,
                fromBlock,
                toBlock,
                order: 'desc',
                pageSize: query.count,
                maxBlockRangePerQuery: query.maxBlockRangePerQuery,
            }
        );
        return {
            view: entries,
            oldestBlock:
                entries.length > 0
                    ? entries[entries.length - 1].blockNumber
                    : toBlock,
        };
    };

    // The outcome edges of `loading` share one read per entry into it.
    const load$ = dataOnEntry$<LatestProofRequestsState, 'loading'>(
        state$,
        'loading'
    ).pipe(
        switchMap(() =>
            readThroughConnections$(connections, (clients) =>
                readNewest(clients, query.fromBlock)
            )
        ),
        share()
    );
    // The outcome edges of `refreshing` share one read per entry into it,
    // from the oldest block of the view it carries. Fewer requests than the
    // view showed (up to `count`) means a reorg removed one.
    const refresh$ = dataOnEntry$<LatestProofRequestsState, 'refreshing'>(
        state$,
        'refreshing'
    ).pipe(
        switchMap(({ view, oldestBlock }) =>
            readThroughConnections$(
                connections,
                async (clients): Promise<Refresh> => {
                    const newest = await readNewest(clients, oldestBlock);
                    return {
                        ...newest,
                        requestsMissing:
                            newest.view.length <
                            Math.min(query.count, view.length),
                    };
                }
            )
        ),
        share()
    );
    const refreshSucceeded$ = withOutcome(refresh$, 'succeeded');
    // A failed read waits before reading again, doubling with each one in a row.
    const retryDue$ = merge(
        dataOnEntry$<LatestProofRequestsState, 'failedBeforeFirstView'>(
            state$,
            'failedBeforeFirstView'
        ),
        dataOnEntry$<LatestProofRequestsState, 'failed'>(state$, 'failed')
    ).pipe(
        take(1),
        map(({ failedReads }) => failedReads),
        switchMap((failedReads) => timer(retryDelayMs(failedReads, timings)))
    );

    const machine = LatestProofRequestsGraph.implement({
        viewArrived: {
            $: () => withOutcome(load$, 'succeeded'),
            next: ({ value }) => ({
                view: value.view,
                oldestBlock: value.oldestBlock,
            }),
        },
        refreshDue: {
            $: () =>
                merge(timer(pollIntervalMs), recheckTrigger$).pipe(take(1)),
            next: (_due, _dest, source) => ({
                view: source.view,
                oldestBlock: source.oldestBlock,
                failedReads: 0,
            }),
        },
        refreshArrived: {
            $: () =>
                refreshSucceeded$.pipe(
                    filter(({ value }) => !value.requestsMissing)
                ),
            next: ({ value }) => ({
                view: value.view,
                oldestBlock: value.oldestBlock,
            }),
        },
        requestsMissing: {
            $: () =>
                refreshSucceeded$.pipe(
                    filter(({ value }) => value.requestsMissing)
                ),
            next: (_missing, _dest, source) => ({
                view: source.view,
                failedReads: source.failedReads,
            }),
        },
        connectionLost: {
            $: () =>
                merge(
                    withOutcome(load$, 'connectionLost'),
                    withOutcome(refresh$, 'connectionLost')
                ),
            next: ({ waitingOn }, _dest, source) =>
                'oldestBlock' in source
                    ? {
                          view: source.view,
                          oldestBlock: source.oldestBlock,
                          failedReads: source.failedReads,
                          waitingOn,
                      }
                    : {
                          view: source.view,
                          failedReads: source.failedReads,
                          waitingOn,
                      },
        },
        readFailedOnHealthyConnection: {
            $: () =>
                merge(
                    withOutcome(load$, 'failedOnHealthyConnection'),
                    withOutcome(refresh$, 'failedOnHealthyConnection')
                ),
            next: ({ error }, _dest, source) =>
                'oldestBlock' in source
                    ? {
                          view: source.view,
                          oldestBlock: source.oldestBlock,
                          failedReads: source.failedReads + 1,
                          error,
                      }
                    : {
                          view: source.view,
                          failedReads: source.failedReads + 1,
                          error,
                      },
        },
        readFailed: {
            $: () =>
                merge(
                    withOutcome(load$, 'failed'),
                    withOutcome(refresh$, 'failed')
                ),
            next: ({ error }, _dest, source) =>
                'oldestBlock' in source
                    ? {
                          view: source.view,
                          oldestBlock: source.oldestBlock,
                          failedReads: source.failedReads + 1,
                          error,
                      }
                    : {
                          view: source.view,
                          failedReads: source.failedReads + 1,
                          error,
                      },
        },
        connectionsChanged: {
            $: () => waitingOnChanged$(connections),
            next: (waitingOn, _dest, source) =>
                'oldestBlock' in source
                    ? {
                          view: source.view,
                          oldestBlock: source.oldestBlock,
                          failedReads: source.failedReads,
                          waitingOn,
                      }
                    : {
                          view: source.view,
                          failedReads: source.failedReads,
                          waitingOn,
                      },
        },
        connectionRestored: {
            $: () => bothReady$(connections),
            next: (_restored, _dest, source) =>
                'oldestBlock' in source
                    ? {
                          view: source.view,
                          oldestBlock: source.oldestBlock,
                          failedReads: source.failedReads,
                      }
                    : { view: source.view, failedReads: source.failedReads },
        },
        retryDue: {
            $: () => retryDue$,
            next: (_due, _dest, source) =>
                'oldestBlock' in source
                    ? {
                          view: source.view,
                          oldestBlock: source.oldestBlock,
                          failedReads: source.failedReads,
                      }
                    : { view: source.view, failedReads: source.failedReads },
        },
        retry: {
            $: () => retry$,
            next: (_retry, _dest, source) =>
                'oldestBlock' in source
                    ? {
                          view: source.view,
                          oldestBlock: source.oldestBlock,
                          failedReads: source.failedReads,
                      }
                    : { view: source.view, failedReads: source.failedReads },
        },
        close: {
            $: () => close$,
            next: () => ({}),
        },
    });

    const latestProofRequests = machine.close().start('loading');
    started$.next(latestProofRequests);

    return {
        latestProofRequests,
        retry: () => retry$.next(),
        close: () => close$.next(),
    };
}
