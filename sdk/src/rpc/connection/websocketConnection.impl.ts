import { type RunningMachine } from '@yaw-rx/ystate';
import { filter, map, type Observable, switchMap, take, timer } from 'rxjs';
import { dataOnEntry$, requestOnEntry$ } from '../../utils/machines.js';
import {
    connectionTransitions,
    type HealthCheck,
    wrongNetworkTransitions,
    settledHealthCheck$,
} from './connection.impl.js';
import { type GrantedClients } from './connectionGrants.js';
import { type HealthCheckTimings, resolveHealthCheckTimings, retryDelayMs } from './healthCheckTimings.js';
import { pacedTransitions } from './pacedConnection.impl.js';
import {
    RateLimitedWebSocketWithWrongNetworkGraph,
    type RateLimitedWebSocketWithWrongNetworkState,
    RateLimitedWebSocketGraph,
    type RateLimitedWebSocketState,
    WebSocketConnectionGraph,
    type WebSocketConnectionState,
} from './websocketConnection.js';

/** Everything outside the graph that a websocket connection reacts to. */
export interface WebSocketConnectionEnvironment<THealth>
    extends Pick<HealthCheckTimings, 'retryBackoff' | 'healthCheckTimeoutMs'> {
    /** How many attempts may fail in a row before giving up (default: no limit). */
    maxFailedAttempts?: number;
    /** The URL the socket connects to. */
    url: () => string;
    /** The held socket, once open: at once if it is, when it opens if not; errors if it fails to open. */
    socketOpen$: () => Observable<void>;
    /**
     * Checks the network the open socket serves, once, its requests the
     * machine's own. Emits what it found; errors when it gets no answer.
     */
    checkHealth: (machine: RunningMachine, url: string) => Observable<Exclude<HealthCheck<THealth>, { outcome: 'failed' }>>;
    /** The held socket closed, errored or missed its heartbeat. */
    dropped$: Observable<{ error: string }>;
    /** The owner asks to connect again after giving up. */
    retry$: Observable<unknown>;
    /** A request over the socket got no answer. */
    readFailed$: Observable<unknown>;
    /** The network machine went offline. */
    networkWentOffline$: Observable<unknown>;
    /** The network machine came back online. */
    networkCameOnline$: Observable<unknown>;
    /** The owner is closing the connection. */
    close$: Observable<unknown>;
    /** The running machine's states, from `stateOf$`. */
    connection$: Observable<
        WebSocketConnectionState | RateLimitedWebSocketState | RateLimitedWebSocketWithWrongNetworkState
    >;
    /** The running machine, once started: the subscriber its own checks take their turns as. */
    started$: Observable<RunningMachine>;
}

/**
 * The transitions every websocket connection has: `connectionOf`'s, with
 * one attempt per entry into `connecting` (open the socket, then check it),
 * the last one allowed going to `gaveUp`; the socket dropping; and retrying
 * after giving up.
 *
 * @param environment The socket's signals, the check, the network, retry and
 *   close signals, the machine's own states, the backoff and the attempts allowed.
 * @returns
 *   - `transitions`: the transitions, by name.
 *   - `check$`: each attempt's check but the last one allowed, which a network-checked graph's `wrongNetwork` reads too.
 */
function websocketTransitions<THealth>(environment: WebSocketConnectionEnvironment<THealth>) {
    const timings = resolveHealthCheckTimings(environment);
    const maxFailedAttempts = environment.maxFailedAttempts ?? Infinity;

    // One attempt per entry into `connecting`, shared by its outcome edges,
    // with whether it was the last one allowed. Opening is bounded by the
    // socket, and each check request by its own timeout from when it is sent.
    const attempt$ = requestOnEntry$(environment.connection$, 'connecting', ({ failedChecks }) => {
        const url = environment.url();
        return settledHealthCheck$(
            environment.started$.pipe(
                take(1),
                switchMap((machine) =>
                    environment.socketOpen$().pipe(switchMap(() => environment.checkHealth(machine, url)))
                )
            ),
            url
        ).pipe(
            map((check) => ({ check, ranOut: check.outcome === 'failed' && failedChecks + 1 >= maxFailedAttempts }))
        );
    });

    const check$ = attempt$.pipe(
        filter(({ ranOut }) => !ranOut),
        map(({ check }) => check)
    );

    const transitions = {
        ...connectionTransitions<THealth, object>({
            check$,
            readFailed$: environment.readFailed$,
            networkWentOffline$: environment.networkWentOffline$,
            networkCameOnline$: environment.networkCameOnline$,
            close$: environment.close$,
            state$: environment.connection$,
            timings,
            carry: () => ({}),
        }),
        attemptsRanOut: {
            $: () =>
                attempt$.pipe(
                    filter(({ ranOut }) => ranOut),
                    map(({ check }) => (check.outcome === 'failed' ? check.error : ''))
                ),
            next: (error: string) => ({ error }),
        },
        dropped: {
            $: () => environment.dropped$,
            // From `connecting`, the attempt counts; from a node that served, it is the first failure.
            next: ({ error }: { error: string }, _dest: unknown, source: object) => ({
                url: environment.url(),
                failedChecks: ('failedChecks' in source && typeof source.failedChecks === 'number' ? source.failedChecks : 0) + 1,
                error,
            }),
        },
        retry: {
            $: () => environment.retry$,
            next: () => ({ failedChecks: 0 }),
        },
    };
    return { transitions, check$, timings };
}

/**
 * Implements the websocket connection graph over its environment.
 *
 * @param environment The socket's signals, the check, the network, retry and
 *   close signals, the machine's own states, the backoff and the attempts allowed.
 * @returns The implemented YState machine; `.close().start('connecting')` runs it.
 */
export function createWebSocketConnectionMachine<THealth>(environment: WebSocketConnectionEnvironment<THealth>) {
    return WebSocketConnectionGraph.implement(websocketTransitions(environment).transitions);
}

/**
 * Implements the paced websocket connection graph over its environment: the
 * websocket's transitions, and the rate its grants let requests through.
 *
 * @param environment As `createWebSocketConnectionMachine`'s, with the clients over the socket's grants.
 * @returns The implemented YState machine; `.close().start('connecting')` runs it.
 */
export function createRateLimitedWebSocketMachine<THealth, TClient>(
    environment: WebSocketConnectionEnvironment<THealth> & { granted: GrantedClients<TClient> }
) {
    return RateLimitedWebSocketGraph.implement({
        ...pacedTransitions<{ url: string; checkedAt: number; health: unknown }>({
            granted: environment.granted,
            state$: environment.connection$,
        }),
        ...websocketTransitions(environment).transitions,
    });
}

/**
 * Implements the network-checked websocket connection graph over its
 * environment: the paced websocket's transitions, the check finding another
 * network, and checking again from `wrongNetwork` on a fresh socket after a
 * wait that doubles with each failed check.
 *
 * @param environment As `createRateLimitedWebSocketMachine`'s.
 * @returns The implemented YState machine; `.close().start('connecting')` runs it.
 */
export function createRateLimitedWebSocketWithWrongNetworkMachine<THealth, TClient>(
    environment: WebSocketConnectionEnvironment<THealth> & { granted: GrantedClients<TClient> }
) {
    const { transitions, check$, timings } = websocketTransitions(environment);
    return RateLimitedWebSocketWithWrongNetworkGraph.implement({
        ...pacedTransitions<{ url: string; checkedAt: number; health: unknown }>({
            granted: environment.granted,
            state$: environment.connection$,
        }),
        ...transitions,
        ...wrongNetworkTransitions<THealth, object>({ check$, carry: () => ({}) }),
        recheckDue: {
            $: () =>
                dataOnEntry$(environment.connection$, 'wrongNetwork').pipe(
                    switchMap(({ failedChecks }) => timer(retryDelayMs(failedChecks, timings)))
                ),
            next: (_due: unknown, _dest: unknown, source: { failedChecks: number }) => ({
                failedChecks: source.failedChecks,
            }),
        },
    });
}
