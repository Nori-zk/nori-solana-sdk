import { catchError, type Observable, of, switchMap, take, timer } from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import { dataOnEntry$, type GraphState, withOutcome } from '../../utils/machines.js';
import { type ResolvedHealthCheckTimings, retryDelayMs } from './healthCheckTimings.js';

/** What one check of an endpoint found: the network it serves, or why it could not tell. */
export type HealthCheck<THealth> =
    | {
          outcome: 'onExpectedNetwork';
          url: string;
          health: THealth;
          checkedAt: number;
      }
    | { outcome: 'onOtherNetwork'; url: string; found: string; expected: string }
    | { outcome: 'failed'; url: string; error: string };

/**
 * One health check, with a failure as a result rather than an error. Its
 * requests time out themselves, each from when it is sent
 * (`clientOf(url, machine, timeoutMs)`), so the wait for the connection's
 * turn never counts against the endpoint.
 *
 * @param check$ The check: what it found on a network it reached; errors when it cannot be reached.
 * @param url What was checked, for a failure.
 * @returns The check's result, once.
 */
export const settledHealthCheck$ = <THealth>(
    check$: Observable<Exclude<HealthCheck<THealth>, { outcome: 'failed' }>>,
    url: string
): Observable<HealthCheck<THealth>> =>
    check$.pipe(
        take(1),
        catchError((error: unknown) => of({ outcome: 'failed' as const, url, error: messageOf(error) }))
    );

/**
 * The transition of a graph spreading `wrongNetworkOf`: the check finding
 * another network, carrying what it found and what was expected, and the
 * data every live node carries (`carry`).
 *
 * @param options The check the graph's `connectionTransitions` share, and what every live node carries.
 * @returns The transitions, by name.
 */
export function wrongNetworkTransitions<THealth, TCarried extends object>(options: {
    check$: Observable<HealthCheck<THealth>>;
    carry: (source: TCarried) => TCarried;
}) {
    const { check$, carry } = options;
    return {
        checkFoundOtherNetwork: {
            $: () => withOutcome(check$, 'onOtherNetwork'),
            next: (
                { url, found, expected }: Extract<HealthCheck<THealth>, { outcome: 'onOtherNetwork' }>,
                _dest: unknown,
                source: TCarried & { failedChecks: number }
            ) => ({ ...carry(source), url, found, expected, failedChecks: source.failedChecks + 1 }),
        },
    };
}

/** What the transitions of a graph built with `connectionOf` react to. */
export interface ConnectionTransitionsOptions<THealth, TCarried extends object> {
    /** One check per entry into `connecting`, shared by its outcome edges. */
    check$: Observable<HealthCheck<THealth>>;
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
    /** How long `waitingToRetry` waits before connecting again. */
    timings: ResolvedHealthCheckTimings;
    /** The data every live node carries besides its own, from the node it leaves. */
    carry: (source: TCarried) => TCarried;
}

/**
 * The transitions of a graph built with `connectionOf`: the check's
 * outcomes, a failed read, the doubling wait in `waitingToRetry`, going
 * offline and back, and closing; each carries the data every live node
 * carries (`carry`). Spread into the graph's `implement`, next to its own
 * transitions.
 *
 * @param options The check, the signals, the machine's states and the timings.
 * @returns The transitions, by name.
 */
export function connectionTransitions<THealth, TCarried extends object>(
    options: ConnectionTransitionsOptions<THealth, TCarried>
) {
    const { check$, carry, timings } = options;
    type Source = TCarried & { failedChecks: number };
    return {
        checkFoundExpectedNetwork: {
            $: () => withOutcome(check$, 'onExpectedNetwork'),
            next: (
                { url, health, checkedAt }: Extract<HealthCheck<THealth>, { outcome: 'onExpectedNetwork' }>,
                _dest: unknown,
                source: TCarried
            ) => ({ ...carry(source), url, health, checkedAt }),
        },
        checkFailed: {
            $: () => withOutcome(check$, 'failed'),
            next: (
                { url, error }: Extract<HealthCheck<THealth>, { outcome: 'failed' }>,
                _dest: unknown,
                source: Source
            ) => ({ ...carry(source), url, failedChecks: source.failedChecks + 1, error }),
        },
        readFailed: {
            $: () => options.readFailed$,
            next: (_failed: unknown, _dest: unknown, source: TCarried) => ({ ...carry(source), failedChecks: 0 }),
        },
        retryDue: {
            $: () =>
                dataOnEntry$(options.state$, 'waitingToRetry').pipe(
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
