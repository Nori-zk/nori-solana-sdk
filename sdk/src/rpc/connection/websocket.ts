import {
    distinctUntilChanged,
    EMPTY,
    filter,
    map,
    Observable,
    ReplaySubject,
    Subject,
    Subscription,
    switchMap,
} from 'rxjs';
import {
    webSocket,
    type WebSocketSubject,
    type WebSocketSubjectConfig,
} from 'rxjs/webSocket';
import { type HealthCheckTimings, resolveHealthCheckTimings } from './healthCheckTimings.js';
import { type NetworkMachine } from './network.impl.js';
import { atNode, stateOf$, type StartedMachine } from '../../utils/machines.js';
import { type WebSocketConnectionState } from './websocketConnection.js';
import { createWebSocketConnectionMachine } from './websocketConnection.impl.js';

/** A heartbeat: a message sent every interval, and the reply that proves the socket is alive. */
export interface WebSocketHeartbeat<T> {
    /** The message sent every `intervalMs`. */
    ping: T;
    /** Whether a received message is the reply to `ping`; replies are not passed on. */
    isPong: (message: T) => boolean;
    /** How often to send `ping`, in ms. */
    intervalMs: number;
    /** How long without a reply before the socket counts as dropped, in ms (default: twice `intervalMs`). */
    timeoutMs?: number;
}

/**
 * Extension of WebSocketSubjectConfig with the connection machine's
 * settings: the heartbeat, the attempts allowed and the timings
 * (`healthCheckTimeoutMs` bounds opening).
 */
export interface ReconnectingWebSocketConfig<T>
    extends WebSocketSubjectConfig<T>,
        Pick<HealthCheckTimings, 'retryBackoff' | 'healthCheckTimeoutMs'> {
    /** Keeps the socket honest: one that stops replying counts as dropped. */
    heartbeat?: WebSocketHeartbeat<T>;
    /** How many attempts may fail in a row before giving up (default: no limit). */
    maxFailedAttempts?: number;
}

/** A running WebSocket connection machine. */
export type WebSocketConnectionMachine = ReturnType<
    ReturnType<ReturnType<typeof createWebSocketConnectionMachine>['close']>['start']
>;

/** The node the machine is in while the socket it holds stays as it is. */
const KEEP_SOCKET = Symbol('keep socket');

/**
 * A Subject wrapper over `WebSocketSubject` whose connection is a WebSocket
 * connection machine: a new `WebSocketSubject` on every entry into
 * `connecting`, kept while `open`, and closed in every other node. The
 * machine reconnects with backoff, gives up after the attempts allowed, and
 * follows the network.
 *
 * The class exposes:
 * - `websocketConnection`: the running connection machine,
 * - an outgoing message buffer: `WebSocketSubject` holds messages sent while
 *   connecting until it opens,
 * - a subscription proxy for incoming messages, which outlives each socket,
 * - `multiplex`, whose subscribe message is sent again every time the socket opens.
 *
 * @template T Type of message payload sent/received over the socket.
 */
export class ReconnectingWebSocketSubject<T> extends Subject<T> {
    readonly websocketConnection: WebSocketConnectionMachine;
    private outgoingBuffer = new Subject<T>();
    private incomingSubject = new Subject<T>();
    private opened$ = new Subject<{ openedAt: number }>();
    private failed$ = new Subject<{ error: string }>();
    private dropped$ = new Subject<{ error: string }>();
    private retry$ = new Subject<void>();
    private close$ = new Subject<void>();
    private subscriptions = new Subscription();
    private socket: WebSocketSubject<T> | null = null;
    private config: ReconnectingWebSocketConfig<T>;

    constructor(
        config: ReconnectingWebSocketConfig<T>,
        network: NetworkMachine['network']
    ) {
        super();
        this.config = config;
        const started$ = new ReplaySubject<StartedMachine<WebSocketConnectionState>>(1);
        this.websocketConnection = createWebSocketConnectionMachine({
            ...config,
            opened$: this.opened$,
            failed$: this.failed$,
            dropped$: this.dropped$,
            retry$: this.retry$,
            networkWentOffline$: network.state$.pipe(
                filter(atNode('offline'))
            ),
            networkCameOnline$: network.state$.pipe(
                filter(atNode('online'))
            ),
            close$: this.close$,
            connection$: stateOf$(started$),
        })
            .close()
            .start('connecting');
        started$.next(this.websocketConnection);

        // A new socket on every entry into `connecting`, kept while `open`,
        // and closed in every other node.
        this.subscriptions.add(
            this.websocketConnection.state$
                .pipe(
                    map((state) =>
                        state.node === 'connecting'
                            ? state
                            : state.node === 'open'
                              ? KEEP_SOCKET
                              : undefined
                    ),
                    filter(
                        (target): target is Exclude<typeof target, typeof KEEP_SOCKET> =>
                            target !== KEEP_SOCKET
                    ),
                    distinctUntilChanged(),
                    switchMap((target) => (target === undefined ? EMPTY : this._connect()))
                )
                .subscribe()
        );
        this.subscriptions.add(
            this.websocketConnection.status$
                .pipe(filter((status) => status !== 'running'))
                .subscribe(() => {
                    this.subscriptions.unsubscribe();
                    this.outgoingBuffer.complete();
                    this.incomingSubject.complete();
                })
        );
    }

    /** Drops the open socket and reconnects. */
    public forceReconnect(): void {
        if (this.socket !== null)
            this.dropped$.next({ error: `Reconnect requested for ${this.config.url}.` });
    }

    /** Connects again after giving up. */
    public retry(): void {
        this.retry$.next();
    }

    /**
     * Holds one `WebSocketSubject` for as long as it is subscribed,
     * reporting its open, its failure to open or its drop.
     *
     * @returns A stream that never emits; its subscription holds the socket.
     */
    private _connect() {
        return new Observable<never>(() => {
            const { heartbeat, retryBackoff, healthCheckTimeoutMs, maxFailedAttempts, ...wsConfig } =
                this.config;
            void retryBackoff;
            void maxFailedAttempts;
            const timings = resolveHealthCheckTimings({ healthCheckTimeoutMs });
            let isOpen = false;
            let settled = false;
            let lastPongAt = 0;
            let heartbeatTimer: ReturnType<typeof setInterval> | undefined;

            const settle = (error: string) => {
                if (settled) return;
                settled = true;
                if (isOpen) this.dropped$.next({ error });
                else this.failed$.next({ error });
            };
            const openTimer = setTimeout(
                () =>
                    settle(
                        `${wsConfig.url} did not open within ${timings.healthCheckTimeoutMs}ms.`
                    ),
                timings.healthCheckTimeoutMs
            );

            const fullConfig: WebSocketSubjectConfig<T> = {
                ...wsConfig,
                openObserver: {
                    next: (evt) => {
                        clearTimeout(openTimer);
                        isOpen = true;
                        wsConfig.openObserver?.next?.(evt);
                        if (heartbeat) {
                            lastPongAt = Date.now();
                            const timeoutMs = heartbeat.timeoutMs ?? heartbeat.intervalMs * 2;
                            heartbeatTimer = setInterval(() => {
                                if (Date.now() - lastPongAt > timeoutMs) {
                                    settle(`${wsConfig.url} stopped replying to its heartbeat.`);
                                    return;
                                }
                                socket.next(heartbeat.ping);
                            }, heartbeat.intervalMs);
                        }
                        this.opened$.next({ openedAt: Date.now() });
                    },
                },
                closeObserver: {
                    next: (evt) => {
                        settle(
                            `The websocket to ${wsConfig.url} closed (${evt.code}${evt.reason ? `: ${evt.reason}` : ''}).`
                        );
                        wsConfig.closeObserver?.next?.(evt);
                    },
                },
            };

            const socket = webSocket<T>(fullConfig);
            this.socket = socket;
            const socketSub = new Subscription();
            socketSub.add(
                socket.subscribe({
                    next: (msg) => {
                        if (heartbeat?.isPong(msg)) {
                            lastPongAt = Date.now();
                            return;
                        }
                        this.incomingSubject.next(msg);
                    },
                    error: () => settle(`The websocket to ${wsConfig.url} failed.`),
                    complete: () => settle(`The websocket to ${wsConfig.url} completed.`),
                })
            );
            socketSub.add(this.outgoingBuffer.subscribe((msg) => socket.next(msg)));

            return () => {
                clearTimeout(openTimer);
                if (heartbeatTimer !== undefined) clearInterval(heartbeatTimer);
                settled = true;
                socketSub.unsubscribe();
                if (this.socket === socket) this.socket = null;
            };
        });
    }

    override next(value: T): void {
        this.outgoingBuffer.next(value);
    }
    /** Errors every subscriber, closes the socket and moves the machine to `closed`. */
    override error(err: unknown): void {
        this.incomingSubject.error(err);
        this.close$.next();
    }
    /** Completes every subscriber, closes the socket and moves the machine to `closed`. */
    override complete(): void {
        this.close$.next();
    }
    subscribe(...args: unknown[]): Subscription {
        return this.incomingSubject.subscribe(...(args as Parameters<Subject<T>['subscribe']>));
    }
    /**
     * Messages for one stream on the shared socket: `subMsg` is sent now if
     * the socket is open and again every time it opens; `unsubMsg` is sent
     * when the stream is unsubscribed.
     */
    multiplex<R>(
        subMsg: () => T,
        unsubMsg: () => T,
        messageFilter: (value: T) => boolean
    ): Observable<R> {
        return new Observable<R>((observer) => {
            const inner = this.subscribe({
                next: (msg: T) => {
                    if (messageFilter(msg)) observer.next(msg as unknown as R);
                },
                error: (e: Error) => observer.error(e),
                complete: () => observer.complete(),
            });
            inner.add(
                this.websocketConnection.state$
                    .pipe(
                        map((state) => (state.node === 'open' ? state.data.openedAt : undefined)),
                        distinctUntilChanged(),
                        filter((openedAt) => openedAt !== undefined)
                    )
                    .subscribe(() => this.next(subMsg()))
            );
            return () => {
                this.next(unsubMsg());
                inner.unsubscribe();
            };
        });
    }
}

/**
 * Factory for creating a reconnecting WebSocket and its running connection machine.
 *
 * The returned socket behaves like a regular `WebSocketSubject`, with its
 * connection run by a WebSocket connection machine.
 *
 * @param config Configuration for WebSocketSubject, the heartbeat, the attempts allowed and the timings.
 * @param network The running network machine the connection follows.
 * @returns Object containing:
 *   - `socket`: the ReconnectingWebSocketSubject instance.
 *   - `connection`: its running connection machine.
 */
export function websocketConnection<T>(
    config: ReconnectingWebSocketConfig<T>,
    network: NetworkMachine['network']
) {
    const socket = new ReconnectingWebSocketSubject<T>(config, network);
    return {
        socket,
        connection: socket.websocketConnection,
    };
}

/** A reconnecting WebSocket and its running connection machine. */
export type WebSocketConnection<T> = ReturnType<typeof websocketConnection<T>>;
