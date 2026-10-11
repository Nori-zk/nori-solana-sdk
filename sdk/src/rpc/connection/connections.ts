import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { type BrowserProvider, isError } from 'ethers';
import {
    catchError,
    combineLatest,
    defer,
    distinctUntilChanged,
    filter,
    map,
    NEVER,
    type Observable,
    of,
    startWith,
    switchMap,
    take,
    throwError,
} from 'rxjs';
import { atNode, type GraphState, type StartedMachine } from '../../utils/machines.js';
import { messageOf } from '../../utils/messageOf.js';
import { type Eip1193EventProvider, requestErrorCode } from '../eth/eip1193.js';
import { EthRpcTransportError, NoWalletConfiguredError } from '../eth/errors.js';
import { ethereumHttp, type EthereumHttp } from '../eth/ethereumHttp.js';
import { ethereumWallet, type EthereumWallet } from '../eth/ethereumWallet.impl.js';
import { ethereumWebsocket, type EthereumWebsocket } from '../eth/ethereumWebsocket.js';
import { createWalletAccountMachine } from '../eth/walletAccount.impl.js';
import { walletSubscription$, walletSubscriptionEvents$ } from '../eth/walletSubscriptions.js';
import { startNoriBridgeInfraTransitions } from '../nori/noriBridgeInfraTransitions.impl.js';
import { noriWebsocket } from '../nori/noriWebsocket.js';
import { arrived } from '../nori/state.js';
import { bridgeStateTopic$, ethStateTopic$ } from '../nori/topics.js';
import { type TransactionSendingSigner } from '@solana/kit';
import { NoSolanaWalletConfiguredError, SolanaRpcTransportError } from '../solana/errors.js';
import { solanaWallet, type SolanaWallet } from '../solana/solanaWallet.impl.js';
import {
    PUBLIC_SOLANA_CLUSTERS,
    type SolanaCluster,
    solanaHttp,
    type SolanaHttp,
    type SolanaRpc,
} from '../solana/solanaHttp.js';
import { solanaWebsocket, type SolanaWebsocket, solanaWebsocketUrlOf } from '../solana/solanaWebsocket.js';
import { ConnectionNotReadyError, type TransportName, type TransportState } from './connectionNotReady.js';
import {
    createConnectionStatusMachine,
    httpStatus,
    networkStatus,
    walletStatus,
    websocketStatus,
} from './connectionStatus.impl.js';
import { type HealthCheckTimings, resolveHealthCheckTimings } from './healthCheckTimings.js';
import { jsonRpcSubscription$, jsonRpcTopic$, type SubscriptionEvent } from './jsonRpcTopic.js';
import { createNetworkMachine, type NetworkOptions } from './network.impl.js';

/** A machine's states, as far as being usable is concerned. */
type MachineStates = StartedMachine<GraphState>;

/** An Ethereum transport that serves requests: http, the websocket or the wallet. */
export type RequestTransport = 'http' | 'websocket' | 'wallet';

/** An Ethereum transport that serves subscriptions: the websocket or the wallet. */
export type SubscriptionTransport = 'websocket' | 'wallet';

/** The order Ethereum's transports are tried in, per kind of request; the caller's. */
export interface EthereumOrder {
    calls?: RequestTransport[];
    logs?: RequestTransport[];
    subscriptions?: SubscriptionTransport[];
}

/** Something to send `eth_subscribe` through: the websocket, or the wallet. */
export interface EthereumSubscriptionSocket {
    /** The subscription's results. */
    ethSubscribe<T>(params: unknown[]): Observable<T>;
    /** The node acknowledging the subscription, each time it is made, then each result. */
    ethSubscribeEvents<T>(params: unknown[]): Observable<SubscriptionEvent<T>>;
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
        | 'walletProvider$'
        | 'chooseWallet'
        | 'switchToExpectedChain'
        | 'reportReadFailed'
        | 'close'
    >;
}

/** What makes up the Solana chain object: the RPC endpoints and the websocket. */
export interface SolanaTransports {
    http: Pick<SolanaHttp, 'connection' | 'rpcOf' | 'reportReadFailed' | 'close'>;
    websocket: Pick<SolanaWebsocket, 'connection' | 'socket'> & { close(): void };
    /** The user's Solana wallet, which signs and sends the app's Solana transactions. */
    wallet?: SolanaWallet;
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
 * A transport's client while its machine is usable.
 *
 * @param name The transport's name, for the error.
 * @param transport The transport, or `undefined` when not configured.
 * @param node The node it is usable in.
 * @param notConfigured The error for a transport the app did not configure.
 * @returns A function giving the client once, or erroring with `ConnectionNotReadyError`
 *   (or `notConfigured`'s error).
 */
function readyOf<TClient>(
    name: TransportName,
    transport: { connection: MachineStates; current(): TClient | undefined } | undefined,
    node: string,
    notConfigured: () => Error = () => new Error(`${name} is not configured.`)
): () => Observable<TClient> {
    return () =>
        transport === undefined
            ? throwError(notConfigured)
            : transport.connection.state$.pipe(
                  take(1),
                  map((state) => {
                      const client = transport.current();
                      if (state.node !== node || client === undefined)
                          throw new ConnectionNotReadyError([{ transport: name, state }]);
                      return client;
                  })
              );
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
 * transport with its machine and `close()` (the wallet also `ready$()`, to
 * sign), and `forCalls$`, `forLogs$` and `subscriptionsOf`, which go
 * through them in the caller's order.
 *
 * @param transports The configured transports.
 * @param order The caller's order per kind of request.
 * @param pollIntervalMs How often the chain changes machine polls while no transport can subscribe.
 * @returns The chain object.
 */
export function ethereumChain(
    transports: EthereumTransports,
    order: EthereumOrder = {},
    pollIntervalMs = 5_000
) {
    const { http, websocket, wallet } = transports;
    const configured = (transport: RequestTransport) => transports[transport] !== undefined;
    const transportName = (transport: RequestTransport): TransportName => `ethereum.${transport}`;
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
     * Runs `op` on the first transport in `transportOrder` whose machine is
     * usable; a request that never reached the node tells that transport and
     * runs `op` on the next. With none left it errors with every transport's
     * state (`ConnectionNotReadyError`). Unsubscribing cancels it.
     */
    const runInOrder$ = <T>(
        transportOrder: RequestTransport[],
        op: (provider: EthereumProvider) => Observable<T>
    ): Observable<T> =>
        defer(() => {
            const notReady: TransportState[] = [];
            const attempt = (index: number): Observable<T> => {
                if (index >= transportOrder.length)
                    return throwError(() => new ConnectionNotReadyError(notReady));
                const transport = transportOrder[index];
                const machine: MachineStates | undefined = transports[transport]?.connection;
                if (machine === undefined) return attempt(index + 1);
                return machine.state$.pipe(
                    take(1),
                    switchMap((state) => {
                        const provider = transports[transport]?.current() as EthereumProvider | undefined;
                        if (state.node !== usableNode(transport) || provider === undefined) {
                            notReady.push({ transport: transportName(transport), state });
                            return attempt(index + 1);
                        }
                        return op(provider).pipe(
                            catchError((error: unknown) => {
                                if (!neverReachedNode(error)) return throwError(() => error);
                                report[transport]();
                                notReady.push({
                                    transport: transportName(transport),
                                    state: { node: 'noResponse', data: { error: messageOf(error) } },
                                });
                                return attempt(index + 1);
                            })
                        );
                    })
                );
            };
            return attempt(0);
        });

    const forCalls$ = <T>(op: (provider: EthereumProvider) => Observable<T>): Observable<T> =>
        runInOrder$(callsOrder, op);

    const forLogs$ = <T>(op: (provider: EthereumProvider) => Observable<T>): Observable<T> =>
        runInOrder$(logsOrder, op);

    /** Where `eth_subscribe` goes for each subscription transport. */
    const subscriptionSockets: Record<SubscriptionTransport, () => EthereumSubscriptionSocket | undefined> = {
        websocket: () =>
            websocket && {
                ethSubscribe: <T>(params: unknown[]) =>
                    jsonRpcTopic$<T>(websocket.socket, 'eth_subscribe', params, 'eth_unsubscribe'),
                ethSubscribeEvents: <T>(params: unknown[]) =>
                    jsonRpcSubscription$<T>(websocket.socket, 'eth_subscribe', params, 'eth_unsubscribe'),
            },
        wallet: () => {
            const provider = wallet?.currentWalletProvider();
            return (
                provider && {
                    ethSubscribe: <T>(params: unknown[]) => walletSubscription$<T>(provider, params),
                    ethSubscribeEvents: <T>(params: unknown[]) => walletSubscriptionEvents$<T>(provider, params),
                }
            );
        },
    };
    /**
     * Whether a transport's machine is usable, as it changes.
     *
     * @param transport The transport.
     * @param whileChecking Count it re-checking itself as usable.
     */
    const transportUsable$ = (transport: RequestTransport, whileChecking = false): Observable<boolean> => {
        const machine: MachineStates | undefined = transports[transport]?.connection;
        return machine === undefined
            ? of(false)
            : machine.state$.pipe(
                  map(
                      (state) =>
                          state.node === usableNode(transport) || (whileChecking && state.node === 'checking')
                  ),
                  distinctUntilChanged()
              );
    };

    const forTransport$ = <T>(
        transport: RequestTransport,
        op: (provider: EthereumProvider) => Observable<T>
    ): Observable<T> => runInOrder$([transport], op);

    /** The transports in the caller's subscriptions order whose machine is usable, in that order, as it changes. */
    const subscribable$: Observable<SubscriptionTransport[]> =
        subscriptionsOrder.length === 0
            ? NEVER.pipe(startWith([]))
            : combineLatest(subscriptionsOrder.map((transport) => transportUsable$(transport))).pipe(
                  map((usable) => subscriptionsOrder.filter((_, i) => usable[i]))
              );

    /**
     * `eth_subscribe` on one transport: the node acknowledging it, then each
     * result; errors with its state (`ConnectionNotReadyError`) when it has
     * nothing to send it through.
     */
    const ethSubscribe$ = <T>(transport: SubscriptionTransport, params: unknown[]): Observable<SubscriptionEvent<T>> =>
        defer(() => {
            const socket = subscriptionSockets[transport]();
            if (socket !== undefined) return socket.ethSubscribeEvents<T>(params);
            const machine: MachineStates | undefined = transports[transport]?.connection;
            return (machine?.state$ ?? of({ node: 'notConfigured', data: {} })).pipe(
                take(1),
                switchMap((state) =>
                    throwError(() => new ConnectionNotReadyError([{ transport: transportName(transport), state }]))
                )
            );
        });

    // The chain's status over its calls order: what its reads and views follow.
    const transportStatus$ = {
        http: () => http?.connection.state$.pipe(httpStatus(transportName('http'))),
        websocket: () =>
            websocket?.connection.state$.pipe(websocketStatus(transportName('websocket'))),
        wallet: () => wallet?.connection.state$.pipe(walletStatus(transportName('wallet'))),
    };
    const status = createConnectionStatusMachine(
        callsOrder.flatMap((transport) => {
            const status$ = transportStatus$[transport]();
            return status$ === undefined ? [] : [status$];
        })
    );

    const chain = {
        status: {
            connection: status,
            close: () => status.close(),
        },
        http: {
            connection: http?.connection,
            close: () => http?.close(),
        },
        websocket: {
            connection: websocket?.connection,
            socket: websocket?.socket,
            close: () => websocket?.close(),
        },
        wallet: {
            connection: wallet?.connection,
            ready$: readyOf<BrowserProvider>(
                transportName('wallet'),
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
            : combineLatest(callsOrder.map((transport) => transportUsable$(transport, whileChecking)))
        ).pipe(
            map((usable) => usable.some(Boolean)),
            distinctUntilChanged()
        );
    const walletProvider$: Observable<Eip1193EventProvider> = wallet?.walletProvider$ ?? NEVER;
    internals.set(chain, {
        forCalls$,
        forLogs$,
        forTransport$,
        subscribable$,
        ethSubscribe$,
        pollIntervalMs,
        callsUsable$,
        transportUsable$,
        walletProvider$,
    });
    return chain;
}

/** The Ethereum chain: its transports, each with its machine and `close()`, and the wallet's `ready$()`. */
export type Ethereum = ReturnType<typeof ethereumChain>;

/** What the sdk's own functions reach an Ethereum chain object through; not on the object. */
interface EthereumInternals {
    forCalls$<T>(op: (provider: EthereumProvider) => Observable<T>): Observable<T>;
    forLogs$<T>(op: (provider: EthereumProvider) => Observable<T>): Observable<T>;
    forTransport$<T>(transport: RequestTransport, op: (provider: EthereumProvider) => Observable<T>): Observable<T>;
    subscribable$: Observable<SubscriptionTransport[]>;
    ethSubscribe$<T>(transport: SubscriptionTransport, params: unknown[]): Observable<SubscriptionEvent<T>>;
    pollIntervalMs: number;
    callsUsable$(whileChecking: boolean): Observable<boolean>;
    transportUsable$(transport: RequestTransport, whileChecking?: boolean): Observable<boolean>;
    walletProvider$: Observable<Eip1193EventProvider>;
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
 * Runs `op` on the first usable transport in the chain's calls order; a
 * request in it that never reached the node tells that transport and runs
 * `op` on the next. With none left it errors with every transport's state
 * (`ConnectionNotReadyError`). Unsubscribing cancels it.
 *
 * @param ethereum The chain.
 * @param op The read, given the provider.
 * @returns What `op` emits.
 */
export function forCalls$<T>(
    ethereum: Ethereum,
    op: (provider: EthereumProvider) => Observable<T>
): Observable<T> {
    return internalsOf(ethereum).forCalls$(op);
}

/**
 * As `forCalls$`, in the chain's logs order.
 *
 * @param ethereum The chain.
 * @param op The read, given the provider.
 * @returns What `op` emits.
 */
export function forLogs$<T>(
    ethereum: Ethereum,
    op: (provider: EthereumProvider) => Observable<T>
): Observable<T> {
    return internalsOf(ethereum).forLogs$(op);
}

/**
 * What Ethereum's subscriptions go through: the chain changes machine's
 * transports.
 *
 * @param ethereum The Ethereum chain.
 * @returns
 *   - `subscribable$`: the transports in the chain's subscriptions order whose machine is usable, in that order, as it changes.
 *   - `ethSubscribe$(transport, params)`: `eth_subscribe` on one of them: its acknowledgement, then each result.
 *   - `pollIntervalMs`: how often to poll while none can subscribe.
 */
export function subscriptionsOf(
    ethereum: Ethereum
): Pick<EthereumInternals, 'subscribable$' | 'ethSubscribe$' | 'pollIntervalMs'> {
    const { subscribable$, ethSubscribe$, pollIntervalMs } = internalsOf(ethereum);
    return { subscribable$, ethSubscribe$, pollIntervalMs };
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
 * Runs `op` on one transport of the Ethereum chain, as `forCalls$` does on
 * its calls order: a request that never reached the node tells the
 * transport, and an unusable transport errors with its state
 * (`ConnectionNotReadyError`).
 *
 * @param ethereum The Ethereum chain.
 * @param transport The transport.
 * @param op The read, given the transport's provider.
 * @returns What `op` emits.
 */
export function forTransport$<T>(
    ethereum: Ethereum,
    transport: RequestTransport,
    op: (provider: EthereumProvider) => Observable<T>
): Observable<T> {
    return internalsOf(ethereum).forTransport$(transport, op);
}

/**
 * Whether one transport of the Ethereum chain is usable, as it changes.
 *
 * @param ethereum The Ethereum chain.
 * @param transport The transport.
 * @param whileChecking Count it re-checking itself as usable.
 * @returns `true` while it is usable.
 */
export function transportUsable$(
    ethereum: Ethereum,
    transport: RequestTransport,
    whileChecking = false
): Observable<boolean> {
    return internals.get(ethereum)?.transportUsable$(transport, whileChecking) ?? of(false);
}

/**
 * The Ethereum chain's wallet's EIP-1193 provider, once the user's wallet is chosen.
 *
 * @param ethereum The Ethereum chain.
 * @returns The chosen wallet's provider; nothing without a wallet.
 */
export function walletProviderOf$(ethereum: Ethereum): Observable<Eip1193EventProvider> {
    return internals.get(ethereum)?.walletProvider$ ?? NEVER;
}

/**
 * The Solana chain object: its RPC endpoints (`http`), which serve every
 * read, and its websocket, which serves its subscriptions; each with its
 * machine and `close()`, and `status`, whether its reads can run.
 *
 * @param transports The transports.
 * @param pollIntervalMs How often the Solana changes machine polls while the websocket is not open.
 * @returns The Solana chain object.
 */
export function solanaChain({ http, websocket, wallet }: SolanaTransports, pollIntervalMs = 5_000) {
    // Reads go through http only: the chain's status is its http's.
    const status = createConnectionStatusMachine([http.connection.state$.pipe(httpStatus('solana.http'))]);

    /**
     * Runs `op` on the RPC of the endpoint whose check passed while http is
     * `ready`; a request that failed to reach a healthy node tells http.
     * Otherwise it errors with http's state (`ConnectionNotReadyError`).
     * Unsubscribing cancels it.
     */
    const forSolana$ = <T>(op: (rpc: SolanaRpc) => Observable<T>): Observable<T> =>
        http.connection.state$.pipe(
            take(1),
            switchMap((state) => {
                const rpc = state.node === 'ready' ? http.rpcOf(state.data.url) : undefined;
                if (rpc === undefined)
                    return throwError(() => new ConnectionNotReadyError([{ transport: 'solana.http', state }]));
                return op(rpc).pipe(
                    catchError((error: unknown) => {
                        if (!(error instanceof SolanaRpcTransportError)) return throwError(() => error);
                        http.reportReadFailed();
                        return throwError(
                            () =>
                                new ConnectionNotReadyError([
                                    {
                                        transport: 'solana.http',
                                        state: { node: 'noResponse', data: { error: messageOf(error) } },
                                    },
                                ])
                        );
                    })
                );
            })
        );

    const callsUsable$ = (whileChecking: boolean): Observable<boolean> =>
        http.connection.state$.pipe(
            map(({ node }) => node === 'ready' || (whileChecking && node === 'checking')),
            distinctUntilChanged()
        );

    const subscribable$: Observable<boolean> = websocket.connection.state$.pipe(
        map(({ node }) => node === 'open'),
        distinctUntilChanged()
    );

    const subscribe$ = <T>(
        subscribeMethod: string,
        params: unknown[],
        unsubscribeMethod: string
    ): Observable<SubscriptionEvent<T>> =>
        jsonRpcSubscription$<T>(websocket.socket, subscribeMethod, params, unsubscribeMethod);

    const chain = {
        status: {
            connection: status,
            close: () => status.close(),
        },
        http: {
            connection: http.connection,
            close: () => http.close(),
        },
        websocket: {
            connection: websocket.connection,
            socket: websocket.socket,
            close: () => websocket.close(),
        },
        wallet: {
            connection: wallet?.connection,
            ready$: readyOf<TransactionSendingSigner>(
                'solana.wallet',
                wallet,
                'ready',
                () => new NoSolanaWalletConfiguredError()
            ),
            chooseWallet: (name: string) => wallet?.chooseWallet(name),
            connect: () => wallet?.connect(),
            disconnect: () => wallet?.disconnect(),
            close: () => wallet?.close(),
        },
    };
    const walletUsable$: Observable<boolean> =
        wallet === undefined
            ? of(false)
            : wallet.connection.state$.pipe(
                  map(({ node }) => node === 'ready'),
                  distinctUntilChanged()
              );
    solanaInternals.set(chain, { forSolana$, callsUsable$, walletUsable$, subscribable$, subscribe$, pollIntervalMs });
    return chain;
}

/** The Solana chain: http and the websocket, each with its machine and `close()`, and its status. */
export type Solana = ReturnType<typeof solanaChain>;

/** What the sdk's own functions reach a Solana chain object through; not on the object. */
interface SolanaInternals {
    forSolana$<T>(op: (rpc: SolanaRpc) => Observable<T>): Observable<T>;
    callsUsable$(whileChecking: boolean): Observable<boolean>;
    walletUsable$: Observable<boolean>;
    subscribable$: Observable<boolean>;
    subscribe$<T>(
        subscribeMethod: string,
        params: unknown[],
        unsubscribeMethod: string
    ): Observable<SubscriptionEvent<T>>;
    pollIntervalMs: number;
}

/** Each Solana chain object's internals. */
const solanaInternals = new WeakMap<object, SolanaInternals>();

/**
 * A Solana chain object's internals.
 *
 * @param solana A Solana chain object made by `solanaChain`.
 * @returns Its internals.
 */
function solanaInternalsOf(solana: Solana): SolanaInternals {
    const found = solanaInternals.get(solana);
    if (found === undefined) throw new Error('Not a Solana chain made by createConnections.');
    return found;
}

/**
 * Runs `op` on the Solana RPC while http is `ready`; a request that failed
 * to reach a healthy node tells http, which checks itself again, and errors
 * with `ConnectionNotReadyError` as an unanswered request. Unsubscribing
 * cancels it.
 *
 * @param solana The Solana chain.
 * @param op The read, given the RPC.
 * @returns What `op` emits.
 */
export function forSolana$<T>(solana: Solana, op: (rpc: SolanaRpc) => Observable<T>): Observable<T> {
    return solanaInternalsOf(solana).forSolana$(op);
}

/**
 * Whether Solana's http can serve a read, as it changes; the reading
 * machines gate on it.
 *
 * @param solana The Solana chain.
 * @param whileChecking Count it re-checking itself as usable.
 * @returns `true` while a read could be made.
 */
export function solanaCallsUsable$(solana: Solana, whileChecking = false): Observable<boolean> {
    return solanaInternals.get(solana)?.callsUsable$(whileChecking) ?? of(false);
}

/**
 * Whether the user's Solana wallet can sign and send, as it changes: while
 * it is `ready`; never without a wallet.
 *
 * @param solana The Solana chain.
 * @returns `true` while it can.
 */
export function solanaWalletUsable$(solana: Solana): Observable<boolean> {
    return solanaInternals.get(solana)?.walletUsable$ ?? of(false);
}

/**
 * What Solana's subscriptions go through: the Solana changes machine's
 * websocket.
 *
 * @param solana The Solana chain.
 * @returns
 *   - `subscribable$`: whether the websocket is open, as it changes.
 *   - `subscribe$(subscribeMethod, params, unsubscribeMethod)`: one subscription on it: its acknowledgement, then each notification.
 *   - `pollIntervalMs`: how often to poll while it is not open.
 */
export function solanaSubscriptionsOf(
    solana: Solana
): Pick<SolanaInternals, 'subscribable$' | 'subscribe$' | 'pollIntervalMs'> {
    const { subscribable$, subscribe$, pollIntervalMs } = solanaInternalsOf(solana);
    return { subscribable$, subscribe$, pollIntervalMs };
}

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
              wallet?: true;
          }
        | {
              expectedGenesisHash: string;
              http: { rpcUrls: string[] };
              websocket?: { url: string };
              wallet?: true;
          };
    nori?: { websocket?: { url: string } };
    healthChecks?: { intervalMs?: number; timeoutMs?: number };
    requests?: { timeoutMs?: number };
    retries?: { initialDelayMs?: number; maxDelayMs?: number; maxAttempts?: number };
    network?: NetworkOptions;
    WebSocketCtor?: new (url: string, protocols?: string | string[]) => WebSocket;
    /** Where wallets announce themselves (EIP-6963 and the Wallet Standard); the window by default. */
    walletEvents?: EventTarget;
}

/**
 * Opens every transport the app names, each with its machine, all
 * following one network machine: Ethereum's http, websocket and wallet as
 * configured; Solana's http (the cluster's public URL by default) and
 * websocket (the http URL over wss by default); Nori's websocket. Health
 * checks and retries are set once for all of them. `createConnections`
 * opens one of these per set of options and shares it.
 *
 * @param options The transports and their settings.
 * @returns `network`, `ethereum`, `solana`, `nori`, and `close()` for everything.
 */
function openConnections(options: ConnectionsOptions) {
    const network = createNetworkMachine(options.network);
    const timings: HealthCheckTimings = {
        healthCheckIntervalMs: options.healthChecks?.intervalMs,
        healthCheckTimeoutMs: options.healthChecks?.timeoutMs,
        requestTimeoutMs: options.requests?.timeoutMs,
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
    const { healthCheckIntervalMs } = resolveHealthCheckTimings(timings);

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
            wallet:
                options.ethereum.wallet &&
                ethereumWallet(
                    { expectedChainId, ...timings, ...(options.walletEvents && { walletEvents: options.walletEvents }) },
                    network
                ),
        },
        options.ethereum.order,
        healthCheckIntervalMs
    );

    // Solana's http comes first: it refuses a setup with no RPC URL, which its websocket's default needs.
    const solanaOptions = options.solana;
    const solanaHttpTransport = solanaHttp(
        'cluster' in solanaOptions
            ? { cluster: solanaOptions.cluster, rpcUrls: solanaOptions.http?.rpcUrls, ...timings }
            : {
                  expectedGenesisHash: solanaOptions.expectedGenesisHash,
                  rpcUrls: solanaOptions.http.rpcUrls,
                  ...timings,
              },
        network
    );
    const solanaRpcUrls =
        solanaOptions.http?.rpcUrls ??
        ('cluster' in solanaOptions ? [PUBLIC_SOLANA_CLUSTERS[solanaOptions.cluster].rpcUrl] : []);
    // Without a websocket URL of its own, the websocket follows http: the URL whose check passed, over wss.
    const solanaWs = solanaWebsocket(
        {
            url: solanaOptions.websocket?.url ?? solanaWebsocketUrlOf(solanaRpcUrls[0]),
            ...(solanaOptions.websocket?.url === undefined && {
                url$: solanaHttpTransport.connection.state$.pipe(
                    filter(atNode('ready')),
                    map(({ data }) => solanaWebsocketUrlOf(data.url))
                ),
            }),
            ...websocketSettings,
        },
        network
    );
    // The wallet sends on the cluster's Wallet Standard chain; a cluster by genesis hash is the public one it matches, else local.
    const walletCluster =
        'cluster' in solanaOptions
            ? solanaOptions.cluster
            : Object.entries(PUBLIC_SOLANA_CLUSTERS).find(
                  ([, { genesisHash }]) => genesisHash === solanaOptions.expectedGenesisHash
              )?.[0];
    const solanaWalletTransport =
        solanaOptions.wallet &&
        solanaWallet({
            chain: `solana:${walletCluster ?? 'localnet'}`,
            ...(options.walletEvents && { walletEvents: options.walletEvents }),
        });
    const solana = solanaChain(
        {
            http: solanaHttpTransport,
            websocket: { ...solanaWs, close: () => solanaWs.socket.complete() },
            wallet: solanaWalletTransport,
        },
        healthCheckIntervalMs
    );
    // The account the Ethereum wallet shares, kept on the wallet.
    const ethereumAccount = options.ethereum.wallet ? createWalletAccountMachine({ ethereum, solana }) : undefined;

    const noriWs = noriWebsocket(
        { ...websocketSettings, url: options.nori?.websocket?.url },
        network
    );
    // Everything read from Nori's websocket is started once here and shared:
    // one socket, so one server's view, for the whole app.
    const noriStatus = createConnectionStatusMachine([
        noriWs.connection.state$.pipe(websocketStatus('nori.websocket')),
    ]);
    const networkStatusMachine = createConnectionStatusMachine([network.state$.pipe(networkStatus())]);
    const nori = {
        status: { connection: noriStatus, close: () => noriStatus.close() },
        websocket: { ...noriWs, close: () => noriWs.socket.complete() },
        /** The pipeline's transitions and the stage it is at, kept following until `close()`. */
        transitions: startNoriBridgeInfraTransitions(noriWs),
        /** The bridge's state (`state.bridge`), stamped as it arrives. */
        bridgeState$: arrived(bridgeStateTopic$(noriWs.socket)),
        /** Ethereum's finalized block and slot (`state.eth`), stamped as they arrive. */
        finality$: arrived(ethStateTopic$(noriWs.socket)),
    };

    return {
        network: {
            connection: network,
            status: {
                connection: networkStatusMachine,
                close: () => networkStatusMachine.close(),
            },
        },
        ethereum: Object.assign(ethereum, { wallet: Object.assign(ethereum.wallet, { account: ethereumAccount }) }),
        solana,
        nori,
        close: () => {
            ethereumAccount?.close();
            ethereum.status.close();
            solana.status.close();
            nori.status.close();
            networkStatusMachine.close();
            ethereum.http.close();
            ethereum.websocket.close();
            ethereum.wallet.close();
            solana.http.close();
            solana.websocket.close();
            solana.wallet.close();
            nori.transitions.close();
            nori.websocket.close();
            network.close();
        },
    };
}

/** Each open set of connections, by its options' key, and how many callers hold it. */
const openByKey = new Map<string, { connections: ReturnType<typeof openConnections>; holders: number }>();

/** An id per function or event target in the options (the WebSocket constructor, `walletEvents`), so different ones give different keys. */
const objectIds = new WeakMap<object, number>();
let nextObjectId = 0;

/**
 * The key of a set of options: the same options give the same key.
 *
 * @param options The options.
 * @returns The key.
 */
const keyOf = (options: ConnectionsOptions): string =>
    JSON.stringify(options, (_key, value: unknown) => {
        if (typeof value === 'bigint') return `${value.toString()}n`;
        // A function or an event target is told apart by which one it is, not by its JSON.
        if (typeof value === 'function' || value instanceof EventTarget) {
            let id = objectIds.get(value);
            if (id === undefined) {
                id = nextObjectId++;
                objectIds.set(value, id);
            }
            return `object#${id.toString()}`;
        }
        return value;
    });

/**
 * The app's connections for `options`: one set per set of options, shared
 * by every caller. Every call with the same options gets the same
 * transports, machines and Nori websocket, so nothing opens a second
 * socket; each socket can land on a different one of Nori's websocket
 * servers, whose cached state can differ. The set opens on the first call
 * and closes once every caller has called its own `close()`.
 *
 * @param options The transports and their settings.
 * @returns `network`, `ethereum`, `solana`, `nori`, and this caller's `close()`.
 */
export function createConnections(options: ConnectionsOptions) {
    const key = keyOf(options);
    let shared = openByKey.get(key);
    if (shared === undefined) {
        shared = { connections: openConnections(options), holders: 0 };
        openByKey.set(key, shared);
    }
    shared.holders++;
    const held = shared;
    let released = false;
    return {
        ...held.connections,
        close: () => {
            if (released) return;
            released = true;
            held.holders--;
            if (held.holders > 0) return;
            openByKey.delete(key);
            held.connections.close();
        },
    };
}

/** Everything `createConnections` opens. */
export type Connections = ReturnType<typeof createConnections>;

/** Nori: its websocket. */
export type Nori = Connections['nori'];
