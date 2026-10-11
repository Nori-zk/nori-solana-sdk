import { type RunningMachine } from '@yaw-rx/ystate';
import {
    defer,
    distinctUntilChanged,
    EMPTY,
    filter,
    map,
    merge,
    Observable,
    of,
    ReplaySubject,
    Subject,
    Subscription,
    switchMap,
    take,
    throwError,
} from 'rxjs';
import {
    webSocket,
    type WebSocketSubject,
    type WebSocketSubjectConfig,
} from 'rxjs/webSocket';
import { atNode, type GraphState, stateOf$, type StartedMachine } from '../../utils/machines.js';
import { SERVING_NODES } from './connection.js';
import { type HealthCheck } from './connection.impl.js';
import { grantedClients, type GrantedTransport } from './connectionGrants.js';
import { type HealthCheckTimings, resolveHealthCheckTimings } from './healthCheckTimings.js';
import { type NetworkMachine } from './network.impl.js';
import {
    type RateLimitedWebSocketWithWrongNetworkState,
    type RateLimitedWebSocketState,
    type WebSocketConnectionState,
} from './websocketConnection.js';
import {
    createRateLimitedWebSocketWithWrongNetworkMachine,
    createRateLimitedWebSocketMachine,
    createWebSocketConnectionMachine,
    type WebSocketConnectionEnvironment,
} from './websocketConnection.impl.js';

/**
 * A heartbeat: every message received proves the socket alive, so `ping` is
 * sent only once nothing has been received for `intervalMs`, keeping a quiet
 * socket alive and proving it still answers. A busy socket sends no pings.
 */
export interface WebSocketHeartbeat<T> {
    /** The message sent once nothing has been received for `intervalMs`. */
    ping: T;
    /** Whether a received message is the reply to `ping`; replies are not passed on. */
    isPong: (message: T) => boolean;
    /** How long without receiving anything before sending `ping`, in ms. */
    intervalMs: number;
    /** How long without receiving anything before the socket counts as dropped, in ms (default: twice `intervalMs`). */
    timeoutMs?: number;
}

/**
 * Extension of WebSocketSubjectConfig with the connection machine's
 * settings: the heartbeat, the attempts allowed and the timings
 * (`healthCheckTimeoutMs` bounds opening and the check;
 * `healthCheckIntervalMs` paces an RPC node's heartbeat).
 */
export interface ReconnectingWebSocketConfig<T>
    extends WebSocketSubjectConfig<T>,
        Pick<HealthCheckTimings, 'retryBackoff' | 'healthCheckTimeoutMs' | 'healthCheckIntervalMs' | 'quietSecondsToLift'> {
    /** Keeps the socket honest: one that stops replying counts as dropped. */
    heartbeat?: WebSocketHeartbeat<T>;
    /** How many attempts may fail in a row before giving up (default: no limit). */
    maxFailedAttempts?: number;
    /**
     * The address to connect to, as it changes: each connection takes the
     * latest, and a new one drops a held socket (default: `url`).
     */
    url$?: Observable<string>;
}

/** The nodes the socket is held in: while it is checked, and while it serves. */
const HOLDING_NODES: readonly string[] = ['connecting', ...SERVING_NODES];

/**
 * A Subject wrapper over `WebSocketSubject` whose socket follows its
 * connection machine (`WebSocketConnectionGraph`): a new `WebSocketSubject`
 * when the machine enters `connecting` holding none, kept while it is held
 * (`connecting`, `ready`, `overwhelmed`), and closed in every other node. It
 * tells the machine when the socket opens, fails to open, or drops.
 *
 * The class exposes:
 * - an outgoing message buffer: `WebSocketSubject` holds messages sent while
 *   connecting until it opens,
 * - a subscription proxy for incoming messages, which outlives each socket,
 * - `serving$`: each socket, once the machine serves over it,
 * - `multiplex`, whose subscribe message is sent again on each socket that serves.
 *
 * @template T Type of message payload sent/received over the socket.
 */
export class ReconnectingWebSocketSubject<T> extends Subject<T> {
    /** The held socket closed, errored or missed its heartbeat. */
    readonly dropped$ = new Subject<{ error: string }>();
    /** The owner asks to connect again after giving up. */
    readonly retry$ = new Subject<void>();
    /** A request over the socket got no answer. */
    readonly readFailed$ = new Subject<void>();
    /** The owner is closing the connection. */
    readonly close$ = new Subject<void>();
    /**
     * The socket's number each time the machine comes to serve over it: over
     * a new socket, or the same one after a recheck; at once if it serves now.
     */
    readonly serving$: Observable<number>;
    private outgoingBuffer = new Subject<T>();
    private incomingSubject = new Subject<T>();
    private opened$ = new Subject<void>();
    private failed$ = new Subject<{ error: string }>();
    private subscriptions = new Subscription();
    private socket: WebSocketSubject<T> | null = null;
    private isOpen = false;
    /** Ends the held socket with an error, as a failure to open or a drop. */
    private settleHeld: ((error: string) => void) | null = null;
    /** How many sockets have opened. */
    private opened = 0;
    private config: ReconnectingWebSocketConfig<T>;
    /** The address the next connection opens: `url`, then the latest from `url$`. */
    private url: string;

    /**
     * @param config The socket's configuration, the heartbeat and the timings.
     * @param connection$ The connection machine's states, from `stateOf$`.
     */
    constructor(config: ReconnectingWebSocketConfig<T>, connection$: Observable<GraphState>) {
        super();
        this.config = config;
        this.url = config.url;
        if (config.url$)
            this.subscriptions.add(
                config.url$.pipe(distinctUntilChanged()).subscribe((url) => {
                    if (url === this.url) return;
                    this.url = url;
                    this.forceReconnect();
                })
            );
        // Each time the connection comes to serve: over a new socket, or the same one after a recheck.
        this.serving$ = connection$.pipe(
            map((state) => SERVING_NODES.includes(state.node)),
            distinctUntilChanged(),
            filter((serves) => serves),
            map(() => this.opened)
        );

        // A new socket on entry into a node that holds one from one that does not; closed in every other node.
        this.subscriptions.add(
            connection$
                .pipe(
                    map((state) => HOLDING_NODES.includes(state.node)),
                    distinctUntilChanged(),
                    switchMap((holds) => (holds ? this._connect() : EMPTY))
                )
                .subscribe()
        );
    }

    /** The address the socket connects to. */
    currentUrl(): string {
        return this.url;
    }

    /**
     * Whether the socket numbered `socket` (as `serving$` gives it) is the
     * one open now: what was made on it, such as a subscription, still lives there.
     *
     * @param socket The socket's number.
     * @returns `true` while it is the open socket.
     */
    isOpenSocket(socket: number): boolean {
        return this.isOpen && this.opened === socket;
    }

    /**
     * The held socket, once open: at once if it is, when it opens if not.
     *
     * @returns One emission once it is open; errors if it fails to open.
     */
    socketOpen$(): Observable<void> {
        return defer(() =>
            this.isOpen
                ? of(undefined)
                : merge(
                      this.opened$,
                      this.failed$.pipe(switchMap(({ error }) => throwError(() => new Error(error))))
                  ).pipe(take(1))
        );
    }

    /** Ends the held socket: a drop when it is open, a failure to open when it is opening. */
    public forceReconnect(): void {
        this.settleHeld?.(`Reconnect requested for ${this.url}.`);
    }

    /** Connects again after giving up. */
    public retry(): void {
        this.retry$.next();
    }

    /** Closes the socket and completes every stream; once the machine has stopped. */
    end(): void {
        this.subscriptions.unsubscribe();
        this.outgoingBuffer.complete();
        this.incomingSubject.complete();
    }

    /**
     * Holds one `WebSocketSubject` for as long as it is subscribed,
     * reporting its open, its failure to open or its drop.
     *
     * @returns A stream that never emits; its subscription holds the socket.
     */
    private _connect() {
        return new Observable<never>(() => {
            const {
                heartbeat,
                retryBackoff,
                healthCheckTimeoutMs,
                healthCheckIntervalMs,
                quietSecondsToLift,
                maxFailedAttempts,
                url$,
                ...socketConfig
            } = this.config;
            void retryBackoff;
            void healthCheckIntervalMs;
            void quietSecondsToLift;
            void maxFailedAttempts;
            void url$;
            const wsConfig = { ...socketConfig, url: this.url };
            const timings = resolveHealthCheckTimings({ healthCheckTimeoutMs });
            let isOpen = false;
            let settled = false;
            let pingTimer: ReturnType<typeof setTimeout> | undefined;
            let dropTimer: ReturnType<typeof setTimeout> | undefined;

            const settle = (error: string) => {
                if (settled) return;
                settled = true;
                this.isOpen = false;
                if (isOpen) this.dropped$.next({ error });
                else this.failed$.next({ error });
            };
            /**
             * Something was received, or the socket opened: it is alive. With a
             * heartbeat, the ping goes once nothing more is received for its
             * interval, and the socket counts as dropped once nothing is
             * received for its timeout.
             */
            const heard = () => {
                if (!heartbeat) return;
                clearTimeout(pingTimer);
                clearTimeout(dropTimer);
                pingTimer = setTimeout(() => socket.next(heartbeat.ping), heartbeat.intervalMs);
                dropTimer = setTimeout(
                    () => settle(`${wsConfig.url} stopped replying to its heartbeat.`),
                    heartbeat.timeoutMs ?? heartbeat.intervalMs * 2
                );
            };
            this.settleHeld = settle;
            const openTimer = setTimeout(
                () => settle(`${wsConfig.url} did not open within ${timings.healthCheckTimeoutMs}ms.`),
                timings.healthCheckTimeoutMs
            );

            const fullConfig: WebSocketSubjectConfig<T> = {
                ...wsConfig,
                openObserver: {
                    next: (evt) => {
                        clearTimeout(openTimer);
                        isOpen = true;
                        wsConfig.openObserver?.next?.(evt);
                        heard();
                        this.opened++;
                        this.isOpen = true;
                        this.opened$.next();
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
                        // Anything received proves the socket alive.
                        heard();
                        if (heartbeat?.isPong(msg)) return;
                        this.incomingSubject.next(msg);
                    },
                    error: () => settle(`The websocket to ${wsConfig.url} failed.`),
                    complete: () => settle(`The websocket to ${wsConfig.url} completed.`),
                })
            );
            socketSub.add(this.outgoingBuffer.subscribe((msg) => socket.next(msg)));

            return () => {
                clearTimeout(openTimer);
                clearTimeout(pingTimer);
                clearTimeout(dropTimer);
                settled = true;
                socketSub.unsubscribe();
                if (this.socket === socket) {
                    this.socket = null;
                    this.isOpen = false;
                    this.settleHeld = null;
                }
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
     * the machine serves over the socket and again on each socket it comes
     * to serve over; `unsubMsg` is sent when the stream is unsubscribed.
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
            inner.add(this.serving$.subscribe(() => this.next(subMsg())));
            return () => {
                this.next(unsubMsg());
                inner.unsubscribe();
            };
        });
    }
}

/**
 * The environment of a websocket's connection machine: the socket's
 * signals, the network, and the settings.
 *
 * @param socket The reconnecting websocket.
 * @param config Its settings.
 * @param network The running network machine the connection follows.
 * @param connection$ The machine's states, from `stateOf$`.
 * @param started$ The running machine, once started.
 * @returns Everything but the check.
 */
function environmentOf<T>(
    socket: ReconnectingWebSocketSubject<T>,
    config: ReconnectingWebSocketConfig<T>,
    network: NetworkMachine,
    connection$: Observable<
        WebSocketConnectionState | RateLimitedWebSocketState | RateLimitedWebSocketWithWrongNetworkState
    >,
    started$: Observable<RunningMachine>
): Omit<WebSocketConnectionEnvironment<never>, 'checkHealth'> {
    return {
        retryBackoff: config.retryBackoff,
        healthCheckTimeoutMs: config.healthCheckTimeoutMs,
        maxFailedAttempts: config.maxFailedAttempts,
        url: () => socket.currentUrl(),
        socketOpen$: () => socket.socketOpen$(),
        dropped$: socket.dropped$,
        retry$: socket.retry$,
        readFailed$: socket.readFailed$,
        networkWentOffline$: network.state$.pipe(filter(atNode('offline'))),
        networkCameOnline$: network.state$.pipe(filter(atNode('online'))),
        close$: socket.close$,
        connection$,
        started$,
    };
}

/**
 * Opens a reconnecting websocket to a server that serves no network, such
 * as Nori's, with its running connection machine (`WebSocketConnectionGraph`):
 * a socket that opens is `ready`.
 *
 * @param config Configuration for WebSocketSubject, the heartbeat, the attempts allowed and the timings.
 * @param network The running network machine the connection follows.
 * @returns
 *   - `socket`: the reconnecting websocket.
 *   - `connection`: its running connection machine.
 */
export function websocketConnection<T>(config: ReconnectingWebSocketConfig<T>, network: NetworkMachine) {
    const started$ = new ReplaySubject<StartedMachine<WebSocketConnectionState>>(1);
    const running$ = new ReplaySubject<RunningMachine>(1);
    const connection$ = stateOf$(started$);
    const socket = new ReconnectingWebSocketSubject<T>(config, connection$);
    const connection = createWebSocketConnectionMachine<undefined>({
        ...environmentOf(socket, config, network, connection$, running$),
        checkHealth: (_machine, url) =>
            of({ outcome: 'onExpectedNetwork' as const, url, health: undefined, checkedAt: Date.now() }),
    })
        .close()
        .start('connecting');
    started$.next(connection);
    running$.next(connection);
    connection.status$
        .pipe(
            filter((status) => status !== 'running'),
            take(1)
        )
        .subscribe(() => socket.end());
    return { socket, connection };
}

/** A reconnecting websocket and its running connection machine. */
export type WebSocketConnection<T> = ReturnType<typeof websocketConnection<T>>;

/**
 * What a paced websocket's owner gives: the client its requests and
 * subscriptions go through, made over the socket, and the check of the
 * network the socket serves.
 */
export interface RateLimitedWebSocket<T, THealth, TClient> {
    /** The transport's client over the socket, and what too many requests look like to the node. */
    transport: (socket: ReconnectingWebSocketSubject<T>) => GrantedTransport<TClient>;
    /** Checks the open socket once, through the client given: the network it serves, where it can be asked. */
    checkHealth: (client: TClient, url: string) => Observable<Exclude<HealthCheck<THealth>, { outcome: 'failed' }>>;
    /**
     * Whether the check can find another network, so the connection has
     * `wrongNetwork` (`RateLimitedWebSocketWithWrongNetworkGraph`): an Ethereum
     * node's websocket can be asked, a Solana node's cannot.
     */
    hasWrongNetwork: boolean;
}

/**
 * Opens a reconnecting websocket to an RPC node, with its running
 * connection machine (`RateLimitedWebSocketGraph`): each socket's
 * network is checked once, and every request and subscription over it takes
 * a turn from its grants.
 *
 * @param config Configuration for WebSocketSubject, the heartbeat, the attempts allowed and the timings.
 * @param network The running network machine the connection follows.
 * @param paced The client over the socket and the check.
 * @returns
 *   - `socket`: the reconnecting websocket.
 *   - `connection`: its running connection machine.
 *   - `clientOf(subscriber)`: the client over the socket, every request of which takes the subscriber's turn.
 *   - `load$`: each second's count of the requests over the socket, and the rate after it.
 */
export function rateLimitedWebsocketConnection<T, THealth, TClient>(
    config: ReconnectingWebSocketConfig<T>,
    network: NetworkMachine,
    paced: RateLimitedWebSocket<T, THealth, TClient>
) {
    const started$ = new ReplaySubject<
        StartedMachine<RateLimitedWebSocketState | RateLimitedWebSocketWithWrongNetworkState>
    >(1);
    const running$ = new ReplaySubject<RunningMachine>(1);
    const connection$ = stateOf$(started$);
    const socket = new ReconnectingWebSocketSubject<T>(config, connection$);
    const granted = grantedClients(paced.transport(socket), config.quietSecondsToLift);
    const environment = {
        ...environmentOf(socket, config, network, connection$, running$),
        granted,
        // Each check request times out from when it is sent, never while it waits for its turn.
        checkHealth: (machine: RunningMachine, url: string) =>
            paced.checkHealth(
                granted.clientOf(url, machine, resolveHealthCheckTimings(config).healthCheckTimeoutMs),
                url
            ),
    };
    const connection: StartedMachine<RateLimitedWebSocketState | RateLimitedWebSocketWithWrongNetworkState> &
        Pick<RunningMachine, 'status$' | 'event$'> = paced.hasWrongNetwork
        ? createRateLimitedWebSocketWithWrongNetworkMachine<THealth, TClient>(environment).close().start('connecting')
        : createRateLimitedWebSocketMachine<THealth, TClient>(environment).close().start('connecting');
    started$.next(connection);
    running$.next(connection);
    connection.status$
        .pipe(
            filter((status) => status !== 'running'),
            take(1)
        )
        .subscribe(() => {
            granted.close();
            socket.end();
        });
    return {
        socket,
        connection,
        clientOf: (subscriber: RunningMachine): TClient => granted.clientOf(socket.currentUrl(), subscriber),
        // The count at the URL the socket connects to, following it as it changes.
        load$: connection$.pipe(
            map(() => socket.currentUrl()),
            distinctUntilChanged(),
            switchMap((url) => granted.loadOf(url))
        ),
    };
}
