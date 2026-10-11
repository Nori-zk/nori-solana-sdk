import {
    combineLatest,
    distinctUntilChanged,
    filter,
    map,
    type Observable,
    of,
    type OperatorFunction,
    ReplaySubject,
    shareReplay,
    skip,
    Subject,
    switchMap,
    take,
} from 'rxjs';
import type { EthereumWalletState } from '../eth/ethereumWallet.js';
import { type StartedMachine } from '../../utils/machines.js';
import { ConnectionStatusGraph, type ConnectionStatusState } from './connectionStatus.js';
import type { HttpConnectionState } from './httpConnection.js';
import type { NetworkState } from './network.js';
import type { WebSocketConnectionState } from './websocketConnection.js';

/** A status a connection, or one of its transports, can be at: a node of the graph but `closed`. */
export type LiveConnectionStatus = Exclude<ConnectionStatusState, { node: 'closed' }>;

/** A transport a connection's status follows: its machine's states, as statuses. */
export type StatusTransport = Observable<LiveConnectionStatus>;

const UNSURE: LiveConnectionStatus = { node: 'unsure', data: {} };

/**
 * An http connection machine's states as statuses: `ready` is connected;
 * checking or unreachable after at most one failure in a row is unsure.
 *
 * @param transport The transport's name.
 * @returns The operator.
 */
export const httpStatus = (
    transport: string
): OperatorFunction<HttpConnectionState<unknown>, LiveConnectionStatus> =>
    map((state) => {
        if (state.node === 'ready') return { node: 'connected', data: { transport } };
        if ((state.node === 'checking' || state.node === 'unreachable') && state.data.failedChecks <= 1)
            return UNSURE;
        return { node: 'down', data: { reason: state.node } };
    });

/**
 * A websocket connection machine's states as statuses: `open` is connected;
 * connecting or reconnecting after at most one failure in a row is unsure.
 *
 * @param transport The transport's name.
 * @returns The operator.
 */
export const websocketStatus = (
    transport: string
): OperatorFunction<WebSocketConnectionState, LiveConnectionStatus> =>
    map((state) => {
        if (state.node === 'open') return { node: 'connected', data: { transport } };
        if (
            (state.node === 'connecting' || state.node === 'reconnecting') &&
            state.data.failedAttempts <= 1
        )
            return UNSURE;
        return { node: 'down', data: { reason: state.node } };
    });

/**
 * The wallet machine's states as statuses: `ready` (on the expected chain)
 * is connected; looking for wallets, asking the user to switch chain, or
 * checking or unreachable after at most one failure in a row is unsure.
 *
 * @param transport The transport's name.
 * @returns The operator.
 */
export const walletStatus = (
    transport: string
): OperatorFunction<EthereumWalletState, LiveConnectionStatus> =>
    map((state) => {
        if (state.node === 'ready') return { node: 'connected', data: { transport } };
        if (state.node === 'lookingForWallets' || state.node === 'askingToSwitchChain') return UNSURE;
        if ((state.node === 'checking' || state.node === 'unreachable') && state.data.failedChecks <= 1)
            return UNSURE;
        return { node: 'down', data: { reason: state.node } };
    });

/**
 * The network machine's states as statuses: `online` is connected;
 * `checking` is unsure.
 *
 * @returns The operator.
 */
export const networkStatus = (): OperatorFunction<NetworkState, LiveConnectionStatus> =>
    map((state) => {
        if (state.node === 'online') return { node: 'connected', data: { transport: 'network' } };
        if (state.node === 'checking') return UNSURE;
        return { node: 'down', data: { reason: state.node } };
    });

/**
 * A connection's status from its transports', in its order: connected
 * through the first one connected, else unsure if one is, else down for
 * the first one's reason.
 *
 * @param statuses The transports' statuses, in the connection's order.
 * @returns The connection's status.
 */
function statusOf(statuses: LiveConnectionStatus[]): LiveConnectionStatus {
    return (
        statuses.find(({ node }) => node === 'connected') ??
        statuses.find(({ node }) => node === 'unsure') ??
        statuses[0] ?? { node: 'down', data: { reason: 'notConfigured' } }
    );
}

/**
 * Whether two statuses are the same node with the same data.
 *
 * @param a A status.
 * @param b Another status.
 * @returns `true` when they are the same.
 */
const sameStatus = (a: LiveConnectionStatus, b: LiveConnectionStatus): boolean =>
    a.node === b.node && JSON.stringify(a.data) === JSON.stringify(b.data);

/**
 * Starts a connection's status machine over its transports, in the
 * connection's order. It starts `unsure` and follows the transports from
 * there.
 *
 * @param transports The transports, each its machine's states as statuses.
 * @returns The running machine. Its control:
 *   - `close()`: moves the machine to `closed`.
 */
export function createConnectionStatusMachine(transports: StatusTransport[]) {
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<StartedMachine<ConnectionStatusState>>(1);

    // The connection's status each time it changes, from once the machine has
    // started. The latest is replayed to each node's edges as they subscribe:
    // it is the status that brought the machine to that node. It follows the
    // transports while a node's edges listen, so until the machine closes.
    const status$ = started$.pipe(
        take(1),
        switchMap(() =>
            transports.length === 0 ? of<LiveConnectionStatus[]>([]) : combineLatest(transports)
        ),
        map(statusOf),
        distinctUntilChanged(sameStatus),
        shareReplay({ bufferSize: 1, refCount: true })
    );
    /**
     * The statuses at one node.
     *
     * @param node The node.
     * @returns Those statuses, narrowed to it.
     */
    const at = <TNode extends LiveConnectionStatus['node']>(node: TNode) =>
        status$.pipe(
            filter(
                (status): status is Extract<LiveConnectionStatus, { node: TNode }> => status.node === node
            )
        );

    const machine = ConnectionStatusGraph.implement({
        connected: {
            $: () => at('connected'),
            next: ({ data }) => data,
        },
        unsure: {
            $: () => at('unsure'),
            next: () => ({}),
        },
        down: {
            $: () => at('down'),
            next: ({ data }) => data,
        },
        // At its own node the replayed status is the one that brought the
        // machine there; only a later one is a change.
        servingTransportChanged: {
            $: () => at('connected').pipe(skip(1)),
            next: ({ data }) => data,
        },
        reasonChanged: {
            $: () => at('down').pipe(skip(1)),
            next: ({ data }) => data,
        },
        close: {
            $: () => close$,
            next: () => ({}),
        },
    });

    const connectionStatus = machine.close().start('unsure');
    started$.next(connectionStatus);

    return Object.assign(connectionStatus, { close: () => close$.next() });
}
