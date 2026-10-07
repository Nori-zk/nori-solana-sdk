import {
    filter,
    type Observable,
    ReplaySubject,
    share,
    Subject,
    switchMap,
    timer,
} from 'rxjs';
import {
    bothReady$,
    type ConnectedRead,
    type ProofRequestConnections,
    readThroughConnections$,
    waitingOnChanged$,
} from '../connectedRead.js';
import {
    fetchProofRequestHistoryPage,
    type ProofRequestHistoryAddresses,
    type ProofRequestHistoryPage,
} from '../proofRequestHistory.js';
import { type ProofRequestsByTargetQuery } from '../rpc/eth/fetchProofRequestsByTarget.js';
import {
    type HealthCheckTimings,
    resolveHealthCheckTimings,
    retryDelayMs,
} from '../rpc/healthCheckTimings.js';
import { dataOnEntry$, stateOf$, type StartedMachine } from './dataOnEntry.js';
import {
    ProofRequestHistoryGraph,
    type ProofRequestHistoryState,
} from './proofRequestHistory.js';

/** The submitting address, block range, order and page size of a paged history. */
export type ProofRequestHistoryQuery = Omit<
    ProofRequestsByTargetQuery,
    'after'
>;

/** How long a failed read waits before reading again. */
export type ReadRetryBackoff = Pick<HealthCheckTimings, 'retryBackoff'>;

/** How one page read ended. */
type PageRead = ConnectedRead<ProofRequestHistoryPage>;

/**
 * Keeps only the page reads with one outcome, narrowed to it.
 *
 * @param read$ Page reads.
 * @param outcome The outcome to keep.
 * @returns The reads with that outcome.
 */
function withOutcome<TOutcome extends PageRead['outcome']>(
    read$: Observable<PageRead>,
    outcome: TOutcome
) {
    return read$.pipe(
        filter(
            (read): read is Extract<PageRead, { outcome: TOutcome }> =>
                read.outcome === outcome
        )
    );
}

/**
 * Starts a paged history of a submitting address's proof requests, reading
 * through both connections. The first page loads at once; each `loadMore()`
 * loads the next.
 *
 * @param connections The Ethereum provider and Solana RPC, with their running connectivity machines.
 * @param addresses The queue and program addresses.
 * @param query The submitting address, block range, order and page size.
 * @param backoff How long a failed read waits before reading again.
 * @returns
 *   - `proofRequestHistory`: the running machine; its states carry every entry loaded so far.
 *   - `loadMore()`: loads the next page, while waiting for more.
 *   - `retry()`: reads the failed page again now, without waiting.
 *   - `close()`: moves the machine to `closed`.
 */
export function createProofRequestHistoryMachine(
    connections: ProofRequestConnections,
    addresses: ProofRequestHistoryAddresses,
    query: ProofRequestHistoryQuery,
    backoff: ReadRetryBackoff = {}
) {
    const timings = resolveHealthCheckTimings(backoff);
    const loadMore$ = new Subject<void>();
    const retry$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<StartedMachine<ProofRequestHistoryState>>(1);
    const state$ = stateOf$(started$);

    // The outcome edges of `loadingPage` share one read per entry into it,
    // from the cursor the machine carries into it.
    const pageRead$ = dataOnEntry$<ProofRequestHistoryState, 'loadingPage'>(
        state$,
        'loadingPage'
    ).pipe(
        switchMap(({ cursor }) =>
            readThroughConnections$(connections, ({ provider, rpc }) =>
                fetchProofRequestHistoryPage(
                    { ...addresses, provider, rpc },
                    { ...query, after: cursor }
                )
            )
        ),
        share()
    );
    const pageReadSucceeded$ = withOutcome(pageRead$, 'succeeded');

    const machine = ProofRequestHistoryGraph.implement({
        pageArrived: {
            $: () =>
                pageReadSucceeded$.pipe(filter(({ value }) => !value.done)),
            next: ({ value }, _dest, source) => ({
                loaded: [...source.loaded, ...value.entries],
                cursor: value.cursor,
            }),
        },
        lastPageArrived: {
            $: () => pageReadSucceeded$.pipe(filter(({ value }) => value.done)),
            next: ({ value }, _dest, source) => ({
                loaded: [...source.loaded, ...value.entries],
            }),
        },
        connectionLost: {
            $: () => withOutcome(pageRead$, 'connectionLost'),
            next: ({ waitingOn }, _dest, source) => ({
                loaded: source.loaded,
                cursor: source.cursor,
                failedReads: source.failedReads,
                waitingOn,
            }),
        },
        readFailedOnHealthyConnection: {
            $: () => withOutcome(pageRead$, 'failedOnHealthyConnection'),
            next: ({ error }, _dest, source) => ({
                loaded: source.loaded,
                cursor: source.cursor,
                failedReads: source.failedReads + 1,
                error,
            }),
        },
        pageFailed: {
            $: () => withOutcome(pageRead$, 'failed'),
            next: ({ error }, _dest, source) => ({
                loaded: source.loaded,
                cursor: source.cursor,
                failedReads: source.failedReads + 1,
                error,
            }),
        },
        loadMore: {
            $: () => loadMore$,
            next: (_more, _dest, source) => ({
                loaded: source.loaded,
                cursor: source.cursor,
                failedReads: 0,
            }),
        },
        connectionsChanged: {
            $: () => waitingOnChanged$(connections),
            next: (waitingOn, _dest, source) => ({
                loaded: source.loaded,
                cursor: source.cursor,
                failedReads: source.failedReads,
                waitingOn,
            }),
        },
        connectionRestored: {
            $: () => bothReady$(connections),
            next: (_restored, _dest, source) => ({
                loaded: source.loaded,
                cursor: source.cursor,
                failedReads: source.failedReads,
            }),
        },
        retryDue: {
            $: () =>
                dataOnEntry$<ProofRequestHistoryState, 'failed'>(
                    state$,
                    'failed'
                ).pipe(
                    switchMap(({ failedReads }) =>
                        timer(retryDelayMs(failedReads, timings))
                    )
                ),
            next: (_due, _dest, source) => ({
                loaded: source.loaded,
                cursor: source.cursor,
                failedReads: source.failedReads,
            }),
        },
        retry: {
            $: () => retry$,
            next: (_retry, _dest, source) => ({
                loaded: source.loaded,
                cursor: source.cursor,
                failedReads: source.failedReads,
            }),
        },
        close: {
            $: () => close$,
            next: () => ({}),
        },
    });

    const proofRequestHistory = machine.close().start('loadingPage');
    started$.next(proofRequestHistory);

    return {
        proofRequestHistory,
        loadMore: () => loadMore$.next(),
        retry: () => retry$.next(),
        close: () => close$.next(),
    };
}
