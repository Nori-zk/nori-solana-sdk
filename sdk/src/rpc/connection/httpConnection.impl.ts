import {
    catchError,
    defer,
    type Observable,
    of,
    ReplaySubject,
    switchMap,
    take,
    timeout,
    timer,
} from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import {
    dataOnEntry$,
    type GraphState,
    requestOnEntry$,
    stateOf$,
    type StartedMachine,
    withOutcome,
} from '../../utils/machines.js';
import {
    type HealthCheckTimings,
    type ResolvedHealthCheckTimings,
    resolveHealthCheckTimings,
    retryDelayMs,
} from './healthCheckTimings.js';
import { HttpConnectionGraph, type HttpConnectionState } from './httpConnection.js';

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
     * Checks one URL once. Emits what it found on a network it reached;
     * errors when it cannot be reached.
     */
    checkHealth: (url: string) => Observable<Exclude<HttpHealthCheck<THealth>, { outcome: 'failed' }>>;
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
 * One health check, timed out, with a failure as a result rather than an
 * error.
 *
 * @param check$ The check: what it found on a network it reached; errors when it cannot be reached.
 * @param url What was checked, for a failure.
 * @param timeoutMs How long the check may take.
 * @returns The check's result, once.
 */
export const settledHealthCheck$ = <THealth>(
    check$: Observable<Exclude<HttpHealthCheck<THealth>, { outcome: 'failed' }>>,
    url: string,
    timeoutMs: number
): Observable<HttpHealthCheck<THealth>> =>
    check$.pipe(
        take(1),
        timeout(timeoutMs),
        catchError((error: unknown) => of({ outcome: 'failed' as const, url, error: messageOf(error) }))
    );

/** What the transitions of a graph built with `healthCheckedOf` react to. */
export interface HealthCheckedTransitionsOptions<THealth, TCarried extends object> {
    /** One check per entry into `checking`, shared by its outcome edges. */
    check$: Observable<HttpHealthCheck<THealth>>;
    /** One check an interval after each entry into `ready`, shared by its outcome edges. */
    backgroundCheck$: Observable<HttpHealthCheck<THealth>>;
    /** A read failed to reach it. */
    readFailed$: Observable<unknown>;
    /** The network went offline. */
    networkWentOffline$: Observable<unknown>;
    /** The network came back online. */
    networkCameOnline$: Observable<unknown>;
    /** The owner is closing it. */
    close$: Observable<unknown>;
    /** The machine's states, from `stateOf$`. */
    state$: Observable<GraphState>;
    /** How long `unreachable` waits before checking again. */
    timings: ResolvedHealthCheckTimings;
    /** The data every live node carries besides its own, from the node it leaves. */
    carry: (source: TCarried) => TCarried;
}

/**
 * The transitions of a graph built with `healthCheckedOf`: the check's and
 * the background check's outcomes, a failed read, the doubling wait while
 * unreachable, going offline and back, and closing; each carries the data
 * every live node carries (`carry`). Spread into the graph's `implement`,
 * next to its own transitions.
 *
 * @param options The checks, the signals, the machine's states and the timings.
 * @returns The transitions, by name.
 */
export function healthCheckedTransitions<THealth, TCarried extends object>(
    options: HealthCheckedTransitionsOptions<THealth, TCarried>
) {
    const { check$, backgroundCheck$, carry, timings } = options;
    type Source = TCarried & { failedChecks: number };
    return {
        checkFoundExpectedNetwork: {
            $: () => withOutcome(check$, 'onExpectedNetwork'),
            next: (
                { url, health, checkedAt }: Extract<HttpHealthCheck<THealth>, { outcome: 'onExpectedNetwork' }>,
                _dest: unknown,
                source: TCarried
            ) => ({ ...carry(source), url, health, checkedAt }),
        },
        checkFoundOtherNetwork: {
            $: () => withOutcome(check$, 'onOtherNetwork'),
            next: (
                { url, found, expected }: Extract<HttpHealthCheck<THealth>, { outcome: 'onOtherNetwork' }>,
                _dest: unknown,
                source: Source
            ) => ({ ...carry(source), url, found, expected, failedChecks: source.failedChecks + 1 }),
        },
        checkFailed: {
            $: () => withOutcome(check$, 'failed'),
            next: (
                { url, error }: Extract<HttpHealthCheck<THealth>, { outcome: 'failed' }>,
                _dest: unknown,
                source: Source
            ) => ({ ...carry(source), url, failedChecks: source.failedChecks + 1, error }),
        },
        backgroundCheckPassed: {
            $: () => withOutcome(backgroundCheck$, 'onExpectedNetwork'),
            next: (
                { url, health, checkedAt }: Extract<HttpHealthCheck<THealth>, { outcome: 'onExpectedNetwork' }>,
                _dest: unknown,
                source: TCarried
            ) => ({ ...carry(source), url, health, checkedAt }),
        },
        backgroundCheckFoundOtherNetwork: {
            $: () => withOutcome(backgroundCheck$, 'onOtherNetwork'),
            next: (
                { url, found, expected }: Extract<HttpHealthCheck<THealth>, { outcome: 'onOtherNetwork' }>,
                _dest: unknown,
                source: TCarried
            ) => ({ ...carry(source), url, found, expected, failedChecks: 1 }),
        },
        backgroundCheckFailed: {
            $: () => withOutcome(backgroundCheck$, 'failed'),
            next: (
                { url, error }: Extract<HttpHealthCheck<THealth>, { outcome: 'failed' }>,
                _dest: unknown,
                source: TCarried
            ) => ({ ...carry(source), url, failedChecks: 1, error }),
        },
        readFailed: {
            $: () => options.readFailed$,
            next: (_failed: unknown, _dest: unknown, source: TCarried) => ({ ...carry(source), failedChecks: 0 }),
        },
        retryDue: {
            $: () =>
                dataOnEntry$(options.state$, 'unreachable').pipe(
                    switchMap((data) => timer(retryDelayMs((data as { failedChecks: number }).failedChecks, timings)))
                ),
            next: (_due: unknown, _dest: unknown, source: Source) => ({
                ...carry(source),
                failedChecks: source.failedChecks,
            }),
        },
        networkWentOffline: {
            $: () => options.networkWentOffline$,
            next: (_offline: unknown, _dest: unknown, source: TCarried) => ({ ...carry(source) }),
        },
        networkCameOnline: {
            $: () => options.networkCameOnline$,
            next: (_online: unknown, _dest: unknown, source: TCarried) => ({ ...carry(source), failedChecks: 0 }),
        },
        close: {
            $: () => options.close$,
            next: () => ({}),
        },
    };
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
export function createHttpConnectionMachine<THealth>(environment: HttpConnectionEnvironment<THealth>) {
    const { urls } = environment;
    const timings = resolveHealthCheckTimings(environment);
    const check$ = (url: string) =>
        settledHealthCheck$(defer(() => environment.checkHealth(url)), url, timings.healthCheckTimeoutMs);

    return HttpConnectionGraph.implement({
        ...healthCheckedTransitions<THealth, object>({
            // The URL checked is chosen by how many checks have failed in a row: each failure moves on to the next.
            check$: requestOnEntry$(environment.connection$, 'checking', ({ failedChecks }) =>
                check$(urls[failedChecks % urls.length])
            ),
            // Background checks while ready stay on the URL that passed.
            backgroundCheck$: requestOnEntry$(environment.connection$, 'ready', ({ url }) =>
                timer(timings.healthCheckIntervalMs).pipe(switchMap(() => check$(url)))
            ),
            readFailed$: environment.readFailed$,
            networkWentOffline$: environment.networkWentOffline$,
            networkCameOnline$: environment.networkCameOnline$,
            close$: environment.close$,
            state$: environment.connection$,
            timings,
            carry: () => ({}),
        }),
        recheckDue: {
            $: () =>
                dataOnEntry$<HttpConnectionState<THealth>, 'wrongNetwork'>(environment.connection$, 'wrongNetwork').pipe(
                    switchMap(({ failedChecks }) => timer(retryDelayMs(failedChecks, timings)))
                ),
            next: (_due, _dest, source) => ({ failedChecks: source.failedChecks }),
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
export function httpConnection<THealth>(environment: Omit<HttpConnectionEnvironment<THealth>, 'connection$'>) {
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
