import {
    catchError,
    defer,
    filter,
    from,
    type Observable,
    of,
    ReplaySubject,
    share,
    switchMap,
    timeout,
    timer,
} from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import { dataOnEntry$, stateOf$, type StartedMachine } from '../../utils/machines.js';
import {
    type HealthCheckTimings,
    resolveHealthCheckTimings,
    retryDelayMs,
} from './healthCheckTimings.js';
import {
    HttpConnectionGraph,
    type HttpConnectionState,
} from './httpConnection.js';

/** What one health check of an HTTP endpoint found. */
export type HttpHealthCheck<THealth> =
    | {
          outcome: 'onExpectedNetwork';
          url: string;
          health: THealth;
          checkedAt: number;
      }
    | { outcome: 'onOtherNetwork'; url: string; found: string; expected: string }
    | { outcome: 'failed'; url: string; error: string };

/** Everything outside the graph that an HTTP connection reacts to. */
export interface HttpConnectionEnvironment<THealth> extends HealthCheckTimings {
    /** The endpoint's URLs, in the order they are tried. */
    urls: string[];
    /**
     * Checks one URL once. Resolves with what it found on a network it
     * reached; rejects when it cannot be reached.
     */
    checkHealth: (
        url: string
    ) => Promise<Exclude<HttpHealthCheck<THealth>, { outcome: 'failed' }>>;
    /** The network machine went offline. */
    networkWentOffline$: Observable<unknown>;
    /** The network machine came back online. */
    networkCameOnline$: Observable<unknown>;
    /** A read against the endpoint failed to reach it. */
    readFailed$: Observable<unknown>;
    /** The owner is closing the connection. */
    close$: Observable<unknown>;
    /** The running machine's states, from `stateOf$`. */
    connection$: Observable<HttpConnectionState<THealth>>;
}

/**
 * Keeps only the health check results with one outcome, narrowed to it.
 *
 * @param check$ Health check results.
 * @param outcome The outcome to keep.
 * @returns The results with that outcome.
 */
function withOutcome<
    THealth,
    TOutcome extends HttpHealthCheck<THealth>['outcome'],
>(check$: Observable<HttpHealthCheck<THealth>>, outcome: TOutcome) {
    return check$.pipe(
        filter(
            (
                check
            ): check is Extract<HttpHealthCheck<THealth>, { outcome: TOutcome }> =>
                check.outcome === outcome
        )
    );
}

/**
 * Implements the HTTP connection graph over its environment: health checks
 * with a timeout, moving on to the next URL after each failed check,
 * background checks while ready, retries with backoff while unreachable or
 * on the wrong network, going offline and back, read failures and closing.
 *
 * @param environment The URLs, the health check, the network, read and
 *   close signals, the machine's own states and the timings.
 * @returns The implemented YState machine; `.close().start('checking')` runs it.
 */
export function createHttpConnectionMachine<THealth>(
    environment: HttpConnectionEnvironment<THealth>
) {
    const { urls } = environment;
    const timings = resolveHealthCheckTimings(environment);

    /**
     * One health check of `url`, timed out, with a failure as a result
     * rather than an error.
     *
     * @param url The URL to check.
     * @returns The check's result, once.
     */
    const runHealthCheck$ = (url: string): Observable<HttpHealthCheck<THealth>> =>
        defer(() => from(environment.checkHealth(url))).pipe(
            timeout(timings.healthCheckTimeoutMs),
            catchError((error: unknown) =>
                of({ outcome: 'failed' as const, url, error: messageOf(error) })
            )
        );
    // The three outcome edges of `checking` share one check per entry into
    // it, of the URL chosen by how many checks have failed in a row: each
    // failure moves on to the next URL.
    const check$ = dataOnEntry$<HttpConnectionState<THealth>, 'checking'>(
        environment.connection$,
        'checking'
    ).pipe(
        switchMap(({ failedChecks }) =>
            runHealthCheck$(urls[failedChecks % urls.length])
        ),
        share()
    );
    // Background checks while ready stay on the URL that passed. `ready` has
    // the `stillReady` self-loop, where the state replayed on re-entry is
    // the previous `ready` rather than the new one; it carries the same URL,
    // because a passing background check stays on it, so the check goes to
    // the right URL either way.
    const backgroundCheck$ = dataOnEntry$<HttpConnectionState<THealth>, 'ready'>(
        environment.connection$,
        'ready'
    ).pipe(
        switchMap(({ url }) =>
            timer(timings.healthCheckIntervalMs).pipe(
                switchMap(() => runHealthCheck$(url))
            )
        ),
        share()
    );

    return HttpConnectionGraph.implement({
        checkFoundExpectedNetwork: {
            $: () => withOutcome(check$, 'onExpectedNetwork'),
            next: ({ url, health, checkedAt }) => ({ url, health, checkedAt }),
        },
        checkFoundOtherNetwork: {
            $: () => withOutcome(check$, 'onOtherNetwork'),
            next: ({ url, found, expected }, _dest, source) => ({
                url,
                found,
                expected,
                failedChecks: source.failedChecks + 1,
            }),
        },
        checkFailed: {
            $: () => withOutcome(check$, 'failed'),
            next: ({ url, error }, _dest, source) => ({
                url,
                failedChecks: source.failedChecks + 1,
                error,
            }),
        },
        backgroundCheckPassed: {
            $: () => withOutcome(backgroundCheck$, 'onExpectedNetwork'),
            next: ({ url, health, checkedAt }) => ({ url, health, checkedAt }),
        },
        backgroundCheckFoundOtherNetwork: {
            $: () => withOutcome(backgroundCheck$, 'onOtherNetwork'),
            next: ({ url, found, expected }) => ({
                url,
                found,
                expected,
                failedChecks: 1,
            }),
        },
        backgroundCheckFailed: {
            $: () => withOutcome(backgroundCheck$, 'failed'),
            next: ({ url, error }) => ({ url, failedChecks: 1, error }),
        },
        readFailed: {
            $: () => environment.readFailed$,
            next: () => ({ failedChecks: 0 }),
        },
        retryDue: {
            $: () =>
                dataOnEntry$<HttpConnectionState<THealth>, 'unreachable'>(
                    environment.connection$,
                    'unreachable'
                ).pipe(
                    switchMap(({ failedChecks }) =>
                        timer(retryDelayMs(failedChecks, timings))
                    )
                ),
            next: (_due, _dest, source) => ({
                failedChecks: source.failedChecks,
            }),
        },
        recheckDue: {
            $: () =>
                dataOnEntry$<HttpConnectionState<THealth>, 'wrongNetwork'>(
                    environment.connection$,
                    'wrongNetwork'
                ).pipe(
                    switchMap(({ failedChecks }) =>
                        timer(retryDelayMs(failedChecks, timings))
                    )
                ),
            next: (_due, _dest, source) => ({
                failedChecks: source.failedChecks,
            }),
        },
        networkWentOffline: {
            $: () => environment.networkWentOffline$,
            next: () => ({}),
        },
        networkCameOnline: {
            $: () => environment.networkCameOnline$,
            next: () => ({ failedChecks: 0 }),
        },
        close: {
            $: () => environment.close$,
            next: () => ({}),
        },
    });
}

/**
 * Starts an HTTP connection machine at `checking`, its states typed with
 * the use's health: the graph declares `ready.health` as `unknown`, and
 * every health this machine carries comes from `checkHealth`, which returns
 * `THealth`.
 *
 * @param environment Everything but the machine's own states, which it is given here.
 * @returns The running machine.
 */
export function httpConnection<THealth>(
    environment: Omit<HttpConnectionEnvironment<THealth>, 'connection$'>
) {
    const started$ = new ReplaySubject<StartedMachine<HttpConnectionState<THealth>>>(1);
    const running = createHttpConnectionMachine<THealth>({
        ...environment,
        connection$: stateOf$(started$),
    })
        .close()
        .start('checking');
    const connection = running as Omit<typeof running, 'state$'> & {
        state$: Observable<HttpConnectionState<THealth>>;
    };
    started$.next(connection);
    return connection;
}

/** A running HTTP connection machine, its states typed with the use's health. */
export type HttpConnection<THealth> = ReturnType<typeof httpConnection<THealth>>;
