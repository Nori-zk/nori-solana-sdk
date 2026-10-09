import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { type BrowserProvider, isError } from 'ethers';
import {
    BehaviorSubject,
    catchError,
    combineLatest,
    distinctUntilChanged,
    EMPTY,
    firstValueFrom,
    map,
    type Observable,
    of,
    switchMap,
    throwError,
} from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import { poll$ } from '../../utils/poll.js';
import { requestErrorCode } from '../eth/eip1193.js';
import {
    EthRpcTransportError,
    NoEthereumHttpConfiguredError,
    NoEthereumWebsocketConfiguredError,
    NoWalletConfiguredError,
} from '../eth/errors.js';
import { ethereumHttp, type EthereumHttp } from '../eth/ethereumHttp.js';
import { ethereumWallet, type EthereumWallet } from '../eth/ethereumWallet.impl.js';
import { ethereumWebsocket, type EthereumWebsocket } from '../eth/ethereumWebsocket.js';
import {
    walletSubscription$,
    WalletSubscriptionUnsupportedError,
} from '../eth/walletSubscriptions.js';
import { noriWebsocket } from '../nori/noriWebsocket.js';
import {
    PUBLIC_SOLANA_CLUSTERS,
    type SolanaCluster,
    solanaHttp,
    type SolanaHttp,
} from '../solana/solanaHttp.js';
import {
    solanaWebsocket,
    type SolanaWebsocket,
    solanaWebsocketUrlOf,
} from '../solana/solanaWebsocket.js';
import {
    ConnectionNotReadyError,
    type TransportName,
    type TransportState,
} from './connectionNotReady.js';
import { type HealthCheckTimings, resolveHealthCheckTimings } from './healthCheckTimings.js';
import { getJsonRpcTopic$ } from './jsonRpcTopic.js';
import { createNetworkMachine, type NetworkMachine, type NetworkOptions } from './network.impl.js';

/** A machine's states, as far as being usable is concerned. */
interface MachineStates {
    state$: Observable<{ node: string; data: unknown }>;
}

/** An Ethereum transport that serves requests: http, the websocket or the wallet. */
type RequestTransport = 'http' | 'websocket' | 'wallet';

/** An Ethereum transport that serves subscriptions: the websocket or the wallet. */
type SubscriptionTransport = 'websocket' | 'wallet';

/** The order Ethereum's transports are tried in, per kind of request; the caller's. */
export interface EthereumOrder {
    calls?: RequestTransport[];
    logs?: RequestTransport[];
    subscriptions?: SubscriptionTransport[];
}

/** Something to send `eth_subscribe` through: the websocket, or the wallet. */
export interface EthereumSubscriptionSocket {
    ethSubscribe<T>(params: unknown[]): Observable<T>;
}

/** What makes up the Ethereum chain object: the transports the app configured. */
export interface EthereumTransports {
    http?: Pick<EthereumHttp, 'connection' | 'current' | 'reportReadFailed' | 'close'>;
    websocket?: Pick<EthereumWebsocket, 'connection' | 'socket' | 'current' | 'close'>;
    wallet?: Pick<
        EthereumWallet,
        | 'connection'
        | 'current'
        | 'currentWalletProvider'
        | 'chooseWallet'
        | 'switchToExpectedChain'
        | 'reportReadFailed'
        | 'close'
    >;
}

/** What makes up the Solana chain object. */
export interface SolanaTransports {
    http: Pick<SolanaHttp, 'connection' | 'current' | 'reportReadFailed' | 'close'>;
    websocket: Pick<SolanaWebsocket, 'connection' | 'socket'> & { close(): void };
}

/** The node a transport is usable in: `open` for a websocket, `ready` otherwise. */
const usableNode = (transport: RequestTransport): string =>
    transport === 'websocket' ? 'open' : 'ready';

/**
 * Whether a failure means a request never reached the node, rather than
 * the node answering with an error: our transport errors, ethers' network,
 * timeout and server errors, and EIP-1193's disconnected codes.
 *
 * @param error A failure.
 * @returns `true` when the node was never reached.
 */
function neverReachedNode(error: unknown): boolean {
    if (error instanceof EthRpcTransportError) return true;
    if (
        isError(error, 'NETWORK_ERROR') ||
        isError(error, 'TIMEOUT') ||
        isError(error, 'SERVER_ERROR')
    )
        return true;
    const inner =
        typeof error === 'object' && error !== null && 'error' in error
            ? (error as { error: unknown }).error
            : undefined;
    return [requestErrorCode(error), requestErrorCode(inner)].some(
        (code) => code === 4900 || code === 4901
    );
}

/**
 * Resolves a transport's client when its machine is usable.
 *
 * @param name The transport's name, for the error.
 * @param transport The transport, or `undefined` when not configured.
 * @param node The node it is usable in.
 * @param notConfigured The error for a transport the app did not configure.
 * @returns A function resolving with the client, or rejecting with `ConnectionNotReadyError`
 *   (or `notConfigured`'s error).
 */
function readyOf<TClient>(
    name: TransportName,
    transport: { connection: MachineStates; current(): TClient | undefined } | undefined,
    node: string,
    notConfigured: () => Error = () => new Error(`${name} is not configured.`)
): () => Promise<TClient> {
    return async () => {
        if (transport === undefined) throw notConfigured();
        const state = await firstValueFrom(transport.connection.state$);
        const client = transport.current();
        if (state.node !== node || client === undefined)
            throw new ConnectionNotReadyError([{ transport: name, state }]);
        return client;
    };
}

/**
 * The caller's order for one kind of request, keeping only configured
 * transports. With no order given, the one configured transport that can
 * serve it is used; with several, the caller must give the order.
 *
 * @param kind The kind of request, for the error.
 * @param given The caller's order, if any.
 * @param capable The transports that can serve it.
 * @param configured Which transports are configured.
 * @returns The transports to try, in order.
 */
function orderFor<TTransport extends RequestTransport>(
    kind: keyof EthereumOrder,
    given: TTransport[] | undefined,
    capable: TTransport[],
    configured: (transport: TTransport) => boolean
): TTransport[] {
    if (given !== undefined) return given.filter(configured);
    const available = capable.filter(configured);
    if (available.length <= 1) return available;
    throw new Error(
        `Several Ethereum transports can serve ${kind} (${available.join(', ')}): give ethereum.order.${kind}.`
    );
}

/**
 * The Ethereum chain object over the transports the app configured: each
 * transport with its machine, `ready()` and `close()`, and `forCalls`,
 * `forLogs` and `forSubscriptions`, which go through them in the caller's
 * order.
 *
 * @param transports The configured transports.
 * @param order The caller's order per kind of request.
 * @param pollIntervalMs How often a subscription polls while it polls.
 * @returns The Ethereum chain object.
 */
export function ethereumChain(
    transports: EthereumTransports,
    order: EthereumOrder = {},
    pollIntervalMs = 15_000
) {
    const { http, websocket, wallet } = transports;
    const configured = (transport: RequestTransport) => transports[transport] !== undefined;
    const ready = {
        http: readyOf('ethereum.http', http, 'ready', () => new NoEthereumHttpConfiguredError()),
        websocket: readyOf(
            'ethereum.websocket',
            websocket,
            'open',
            () => new NoEthereumWebsocketConfiguredError()
        ),
        wallet: readyOf<EthereumProvider>(
            'ethereum.wallet',
            wallet,
            'ready',
            () => new NoWalletConfiguredError()
        ),
    };
    const report = {
        http: () => http?.reportReadFailed(),
        websocket: () => websocket?.socket.forceReconnect(),
        wallet: () => wallet?.reportReadFailed(),
    };
    const callsOrder = orderFor('calls', order.calls, ['http', 'websocket', 'wallet'], configured);
    const logsOrder = orderFor('logs', order.logs, ['http', 'websocket', 'wallet'], configured);
    const subscriptionsOrder = orderFor(
        'subscriptions',
        order.subscriptions,
        ['websocket', 'wallet'],
        configured
    );

    /**
     * Runs `fn` on the first ready transport in `transportOrder`; a request
     * in it that never reached the node tells that transport and runs `fn`
     * again on the next.
     */
    const runInOrder = async <T, A extends unknown[]>(
        transportOrder: RequestTransport[],
        fn: (provider: EthereumProvider, ...args: A) => Promise<T>,
        args: A
    ): Promise<T> => {
        const notReady: TransportState[] = [];
        for (const transport of transportOrder) {
            let provider: EthereumProvider;
            try {
                provider = await ready[transport]();
            } catch (error) {
                if (!(error instanceof ConnectionNotReadyError)) throw error;
                notReady.push(...error.notReady);
                continue;
            }
            try {
                return await fn(provider, ...args);
            } catch (error) {
                if (!neverReachedNode(error)) throw error;
                report[transport]();
                notReady.push({
                    transport: `ethereum.${transport}`,
                    state: { node: 'noResponse', data: { error: messageOf(error) } },
                });
            }
        }
        throw new ConnectionNotReadyError(notReady);
    };

    const forCalls = <T, A extends unknown[]>(
        fn: (provider: EthereumProvider, ...args: A) => Promise<T>,
        ...args: A
    ): Promise<T> => runInOrder(callsOrder, fn, args);

    const forLogs = <T, A extends unknown[]>(
        fn: (provider: EthereumProvider, ...args: A) => Promise<T>,
        ...args: A
    ): Promise<T> => runInOrder(logsOrder, fn, args);

    /** Where `eth_subscribe` goes for each subscription transport. */
    const subscriptionSockets: Record<SubscriptionTransport, () => EthereumSubscriptionSocket | undefined> = {
        websocket: () =>
            websocket && {
                ethSubscribe: <T>(params: unknown[]) =>
                    getJsonRpcTopic$<T>(websocket.socket, 'eth_subscribe', params, 'eth_unsubscribe'),
            },
        wallet: () => {
            const provider = wallet?.currentWalletProvider();
            return (
                provider && {
                    ethSubscribe: <T>(params: unknown[]) => walletSubscription$<T>(provider, params),
                }
            );
        },
    };
    const usable$ = (transport: RequestTransport): Observable<boolean> => {
        const machine: MachineStates | undefined = transports[transport]?.connection;
        return machine === undefined
            ? of(false)
            : machine.state$.pipe(
                  map((state) => state.node === usableNode(transport)),
                  distinctUntilChanged()
              );
    };

    /**
     * A subscription on the first ready transport in the caller's
     * subscriptions order; it moves to the next when that one stops being
     * ready, and back when a higher one recovers. A wallet that does not
     * serve `eth_subscribe` is passed over from then on. With none ready it
     * polls `poll` through `forCalls` until one is.
     */
    const forSubscriptions = <T, A extends unknown[]>(
        subscribe: (socket: EthereumSubscriptionSocket, ...args: A) => Observable<T>,
        poll: (provider: EthereumProvider, ...args: A) => Promise<T[]>,
        ...args: A
    ): Observable<T> => {
        const unsupported$ = new BehaviorSubject<ReadonlySet<SubscriptionTransport>>(new Set());
        const chosen$: Observable<SubscriptionTransport | undefined> =
            subscriptionsOrder.length === 0
                ? of(undefined)
                : combineLatest([combineLatest(subscriptionsOrder.map(usable$)), unsupported$]).pipe(
                      map(([usable, unsupported]) =>
                          subscriptionsOrder.find(
                              (transport, i) => usable[i] && !unsupported.has(transport)
                          )
                      ),
                      distinctUntilChanged()
                  );
        return chosen$.pipe(
            switchMap((transport) => {
                const socket = transport && subscriptionSockets[transport]();
                if (transport !== undefined && socket !== undefined)
                    return subscribe(socket, ...args).pipe(
                        catchError((error: unknown) => {
                            if (!(error instanceof WalletSubscriptionUnsupportedError))
                                return throwError(() => error);
                            unsupported$.next(new Set([...unsupported$.value, transport]));
                            return EMPTY;
                        })
                    );
                return poll$(() => forCalls(poll, ...args), pollIntervalMs);
            })
        );
    };

    const chain = {
        http: {
            connection: http?.connection,
            ready: ready.http,
            close: () => http?.close(),
        },
        websocket: {
            connection: websocket?.connection,
            socket: websocket?.socket,
            ready: ready.websocket,
            close: () => websocket?.close(),
        },
        wallet: {
            connection: wallet?.connection,
            ready: readyOf<BrowserProvider>(
                'ethereum.wallet',
                wallet,
                'ready',
                () => new NoWalletConfiguredError()
            ),
            chooseWallet: (uuid: string) => wallet?.chooseWallet(uuid),
            switchToExpectedChain: () => wallet?.switchToExpectedChain(),
            close: () => wallet?.close(),
        },
    };
    const callsUsable$ = (whileChecking: boolean) =>
        (callsOrder.length === 0
            ? of([false])
            : combineLatest(
                  callsOrder.map((transport) => {
                      const machine: MachineStates | undefined = transports[transport]?.connection;
                      return machine === undefined
                          ? of(false)
                          : machine.state$.pipe(
                                map(
                                    (state) =>
                                        state.node === usableNode(transport) ||
                                        (whileChecking && state.node === 'checking')
                                )
                            );
                  })
              )
        ).pipe(
            map((usable) => usable.some(Boolean)),
            distinctUntilChanged()
        );
    internals.set(chain, { forCalls, forLogs, forSubscriptions, callsUsable$ });
    return chain;
}

/** The Ethereum chain: its transports, each with its machine, `ready()` and `close()`. */
export type Ethereum = ReturnType<typeof ethereumChain>;

/** What the sdk's own functions reach an Ethereum chain object through; not on the object. */
interface EthereumInternals {
    forCalls<T, A extends unknown[]>(
        fn: (provider: EthereumProvider, ...args: A) => Promise<T>,
        ...args: A
    ): Promise<T>;
    forLogs<T, A extends unknown[]>(
        fn: (provider: EthereumProvider, ...args: A) => Promise<T>,
        ...args: A
    ): Promise<T>;
    forSubscriptions<T, A extends unknown[]>(
        subscribe: (socket: EthereumSubscriptionSocket, ...args: A) => Observable<T>,
        poll: (provider: EthereumProvider, ...args: A) => Promise<T[]>,
        ...args: A
    ): Observable<T>;
    callsUsable$(whileChecking: boolean): Observable<boolean>;
}

/** Each Ethereum chain object's internals. */
const internals = new WeakMap<object, EthereumInternals>();

/**
 * An Ethereum chain object's internals.
 *
 * @param ethereum An Ethereum chain object made by `ethereumChain`.
 * @returns Its internals.
 */
function internalsOf(ethereum: Ethereum): EthereumInternals {
    const found = internals.get(ethereum);
    if (found === undefined) throw new Error('Not an Ethereum chain made by createConnections.');
    return found;
}

/**
 * Runs `fn(provider, ...args)` on the first ready transport in the chain's
 * calls order; a request in it that never reached the node tells that
 * transport and runs `fn` again from the start on the next ready one. Any
 * other failure is thrown as is; with nothing ready it fails at once with
 * every transport's state (`ConnectionNotReadyError`).
 *
 * @param ethereum The Ethereum chain.
 * @param fn The function to run, given the provider first.
 * @param args Its other arguments.
 * @returns What `fn` returns.
 */
export function forCalls<T, A extends unknown[]>(
    ethereum: Ethereum,
    fn: (provider: EthereumProvider, ...args: A) => Promise<T>,
    ...args: A
): Promise<T> {
    return internalsOf(ethereum).forCalls(fn, ...args);
}

/**
 * As `forCalls`, in the chain's logs order.
 *
 * @param ethereum The Ethereum chain.
 * @param fn The function to run, given the provider first.
 * @param args Its other arguments.
 * @returns What `fn` returns.
 */
export function forLogs<T, A extends unknown[]>(
    ethereum: Ethereum,
    fn: (provider: EthereumProvider, ...args: A) => Promise<T>,
    ...args: A
): Promise<T> {
    return internalsOf(ethereum).forLogs(fn, ...args);
}

/**
 * A subscription on the first ready transport in the chain's subscriptions
 * order, moving to the next when that one stops being ready and back when a
 * higher one recovers; with none ready it polls `poll` through `forCalls`.
 *
 * @param ethereum The Ethereum chain.
 * @param subscribe The subscription, given where `eth_subscribe` goes.
 * @param poll What to poll while nothing can subscribe, given the provider first.
 * @param args Their other arguments.
 * @returns Each value, pushed or polled.
 */
export function forSubscriptions<T, A extends unknown[]>(
    ethereum: Ethereum,
    subscribe: (socket: EthereumSubscriptionSocket, ...args: A) => Observable<T>,
    poll: (provider: EthereumProvider, ...args: A) => Promise<T[]>,
    ...args: A
): Observable<T> {
    return internalsOf(ethereum).forSubscriptions(subscribe, poll, ...args);
}

/**
 * Whether any transport in an Ethereum chain's calls order is usable, as it
 * changes; the reading machines gate on it.
 *
 * @param ethereum An Ethereum chain object.
 * @param whileChecking Count a transport re-checking itself as usable: a
 *   failed request sends it there, and what that failure was is decided
 *   once the check settles.
 * @returns `true` while a call could be made.
 */
export function ethereumCallsUsable$(
    ethereum: Ethereum,
    whileChecking = false
): Observable<boolean> {
    return internals.get(ethereum)?.callsUsable$(whileChecking) ?? of(false);
}

/**
 * The Solana chain object: http (one transport per kind of request) and
 * the websocket.
 *
 * @param transports The transports.
 * @returns The Solana chain object.
 */
export function solanaChain({ http, websocket }: SolanaTransports) {
    return {
        http: {
            connection: http.connection,
            ready: readyOf('solana.http', http, 'ready'),
            close: () => http.close(),
        },
        websocket: {
            connection: websocket.connection,
            socket: websocket.socket,
            close: () => websocket.close(),
        },
    };
}

/** The Solana chain: http and the websocket. */
export type Solana = ReturnType<typeof solanaChain>;

/** Options for `createConnections`: which transports the app has, and settings for all of them. */
export interface ConnectionsOptions {
    ethereum: {
        expectedChainId: bigint;
        http?: { rpcUrl: string };
        websocket?: { url: string };
        wallet?: true;
        order?: EthereumOrder;
    };
    solana:
        | {
              cluster: SolanaCluster;
              http?: { rpcUrls: string[] };
              websocket?: { url: string };
          }
        | {
              expectedGenesisHash: string;
              http: { rpcUrls: string[] };
              websocket?: { url: string };
          };
    nori?: { websocket?: { url: string } };
    healthChecks?: { intervalMs?: number; timeoutMs?: number };
    retries?: { initialDelayMs?: number; maxDelayMs?: number; maxAttempts?: number };
    network?: NetworkOptions;
    WebSocketCtor?: new (url: string, protocols?: string | string[]) => WebSocket;
}

/**
 * Opens every transport the app names, each with its machine, all
 * following one network machine: Ethereum's http, websocket and wallet as
 * configured; Solana's http (the cluster's public URL by default) and
 * websocket (the http URL over wss by default); Nori's websocket. Health
 * checks and retries are set once for all of them.
 *
 * @param options The transports and their settings.
 * @returns `network`, `ethereum`, `solana`, `nori`, and `close()` for everything.
 */
export function createConnections(options: ConnectionsOptions) {
    const { network, close: closeNetwork }: NetworkMachine = createNetworkMachine(options.network);
    const timings: HealthCheckTimings = {
        healthCheckIntervalMs: options.healthChecks?.intervalMs,
        healthCheckTimeoutMs: options.healthChecks?.timeoutMs,
        retryBackoff: {
            initialDelayMs: options.retries?.initialDelayMs,
            maxDelayMs: options.retries?.maxDelayMs,
        },
    };
    const websocketSettings = {
        ...timings,
        maxFailedAttempts: options.retries?.maxAttempts,
        // Left out when not given: rxjs's webSocket only falls back to the global WebSocket for a missing key.
        ...(options.WebSocketCtor && { WebSocketCtor: options.WebSocketCtor }),
    };
    const { expectedChainId } = options.ethereum;

    const ethereum = ethereumChain(
        {
            http:
                options.ethereum.http &&
                ethereumHttp({ expectedChainId, rpcUrl: options.ethereum.http.rpcUrl, ...timings }, network),
            websocket:
                options.ethereum.websocket &&
                ethereumWebsocket(
                    { url: options.ethereum.websocket.url, ...websocketSettings },
                    network,
                    expectedChainId
                ),
            wallet: options.ethereum.wallet && ethereumWallet({ expectedChainId, ...timings }, network),
        },
        options.ethereum.order,
        resolveHealthCheckTimings(timings).healthCheckIntervalMs
    );

    const solanaOptions = options.solana;
    const solanaRpcUrls =
        solanaOptions.http?.rpcUrls ??
        ('cluster' in solanaOptions ? [PUBLIC_SOLANA_CLUSTERS[solanaOptions.cluster].rpcUrl] : []);
    const solanaWs = solanaWebsocket(
        {
            url: solanaOptions.websocket?.url ?? solanaWebsocketUrlOf(solanaRpcUrls[0]),
            ...websocketSettings,
        },
        network
    );
    const solana = solanaChain({
        http: solanaHttp(
            'cluster' in solanaOptions
                ? { cluster: solanaOptions.cluster, rpcUrls: solanaOptions.http?.rpcUrls, ...timings }
                : {
                      expectedGenesisHash: solanaOptions.expectedGenesisHash,
                      rpcUrls: solanaOptions.http.rpcUrls,
                      ...timings,
                  },
            network
        ),
        websocket: { ...solanaWs, close: () => solanaWs.socket.complete() },
    });

    const noriWs = noriWebsocket(
        { ...websocketSettings, url: options.nori?.websocket?.url },
        network
    );
    const nori = {
        websocket: { ...noriWs, close: () => noriWs.socket.complete() },
    };

    return {
        network: { connection: network },
        ethereum,
        solana,
        nori,
        close: () => {
            ethereum.http.close();
            ethereum.websocket.close();
            ethereum.wallet.close();
            solana.http.close();
            solana.websocket.close();
            nori.websocket.close();
            closeNetwork();
        },
    };
}

/** Everything `createConnections` opens. */
export type Connections = ReturnType<typeof createConnections>;

/** Nori: its websocket. */
export type Nori = Connections['nori'];
