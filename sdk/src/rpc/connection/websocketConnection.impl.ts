import { filter, map, type Observable, switchMap, take, timer } from 'rxjs';
import { dataOnEntry$, requestOnEntry$ } from '../../utils/machines.js';
import {
    type HealthCheckTimings,
    resolveHealthCheckTimings,
    retryDelayMs,
} from './healthCheckTimings.js';
import {
    WebSocketConnectionGraph,
    type WebSocketConnectionState,
} from './websocketConnection.js';

/** Everything outside the graph that a WebSocket connection reacts to. */
export interface WebSocketConnectionEnvironment
    extends Pick<HealthCheckTimings, 'retryBackoff'> {
    /** How many attempts may fail in a row before giving up (default: no limit). */
    maxFailedAttempts?: number;
    /** The socket opened and the owner's open messages were sent. */
    opened$: Observable<{ openedAt: number }>;
    /** The socket failed, or did not open in time, while connecting. */
    failed$: Observable<{ error: string }>;
    /** The open socket closed, errored or missed its heartbeat. */
    dropped$: Observable<{ error: string }>;
    /** The owner asks to connect again after giving up. */
    retry$: Observable<unknown>;
    /** The network machine went offline. */
    networkWentOffline$: Observable<unknown>;
    /** The network machine came back online. */
    networkCameOnline$: Observable<unknown>;
    /** The owner is closing the connection. */
    close$: Observable<unknown>;
    /** The running machine's states, from `stateOf$`. */
    connection$: Observable<WebSocketConnectionState>;
}

/**
 * Implements the WebSocket connection graph over its environment: opening,
 * failing to open, dropping, reconnecting with backoff, giving up after the
 * attempts allowed, going offline and back, retrying and closing. The socket
 * itself is the owner's; the machine only follows what it reports.
 *
 * @param environment The socket's signals, the network, retry and close
 *   signals, the machine's own states, the backoff and the attempts allowed.
 * @returns The implemented YState machine; `.close().start('connecting')` runs it.
 */
export function createWebSocketConnectionMachine(
    environment: WebSocketConnectionEnvironment
) {
    const timings = resolveHealthCheckTimings(environment);
    const maxFailedAttempts = environment.maxFailedAttempts ?? Infinity;

    // A failure while connecting, with the attempts that have failed in a
    // row including it; both outcome edges of `connecting` share it.
    const connectFailure$ = requestOnEntry$(environment.connection$, 'connecting', ({ failedAttempts }) =>
        environment.failed$.pipe(
            take(1),
            map(({ error }) => ({
                error,
                failedAttempts: failedAttempts + 1,
            }))
        )
    );

    return WebSocketConnectionGraph.implement({
        opened: {
            $: () => environment.opened$,
            next: ({ openedAt }) => ({ openedAt }),
        },
        connectFailed: {
            $: () =>
                connectFailure$.pipe(
                    filter(({ failedAttempts }) => failedAttempts < maxFailedAttempts)
                ),
            next: ({ error, failedAttempts }) => ({ failedAttempts, error }),
        },
        attemptsRanOut: {
            $: () =>
                connectFailure$.pipe(
                    filter(({ failedAttempts }) => failedAttempts >= maxFailedAttempts)
                ),
            next: ({ error }) => ({ error }),
        },
        dropped: {
            $: () => environment.dropped$,
            next: ({ error }) => ({ failedAttempts: 1, error }),
        },
        reconnectDue: {
            $: () =>
                dataOnEntry$<WebSocketConnectionState, 'reconnecting'>(
                    environment.connection$,
                    'reconnecting'
                ).pipe(
                    switchMap(({ failedAttempts }) =>
                        timer(retryDelayMs(failedAttempts, timings))
                    )
                ),
            next: (_due, _dest, source) => ({
                failedAttempts: source.failedAttempts,
            }),
        },
        retry: {
            $: () => environment.retry$,
            next: () => ({ failedAttempts: 0 }),
        },
        networkWentOffline: {
            $: () => environment.networkWentOffline$,
            next: () => ({}),
        },
        networkCameOnline: {
            $: () => environment.networkCameOnline$,
            next: () => ({ failedAttempts: 0 }),
        },
        close: {
            $: () => environment.close$,
            next: () => ({}),
        },
    });
}
