import { define } from '@yaw-rx/ystate';
import { filter, type Observable } from 'rxjs';
import { getReconnectingBridgeSocket$ } from './socket.js';

type BridgeSocketConnectionState =
    | 'connecting'
    | 'open'
    | 'closed'
    | 'reconnecting'
    | 'permanently-closed';

/**
 * The bridge websocket's connection states and the transitions
 * `ReconnectingWebSocketSubject` makes between them:
 *
 * - `connecting` to `open` when the socket opens, or to `closed` when the attempt fails.
 * - `open` to `closed` on close, error, or a missed pong (`forceReconnect`).
 * - `closed` to `reconnecting` while retries remain, or to `permanently-closed` once they run out.
 * - `reconnecting` to `connecting` when the backoff timer fires.
 */
export const BridgeSocketConnectivityGraph = define({
    nodes: {
        connecting: {},
        open: {},
        closed: {},
        reconnecting: {},
        'permanently-closed': {},
    },
    edges: {
        opened: {
            from: 'connecting',
            to: 'open',
            on: 'socketOpened.next',
        },
        connectFailed: {
            from: 'connecting',
            to: 'closed',
            on: 'socketClosed.next',
        },
        dropped: {
            from: 'open',
            to: 'closed',
            on: 'socketClosed.next',
        },
        reconnectScheduled: {
            from: 'closed',
            to: 'reconnecting',
            on: 'socketReconnecting.next',
        },
        retriesExhausted: {
            from: 'closed',
            to: 'permanently-closed',
            on: 'socketPermanentlyClosed.next',
        },
        reconnectStarted: {
            from: 'reconnecting',
            to: 'connecting',
            on: 'socketConnecting.next',
        },
    },
});

function onState(
    connectionState$: Observable<BridgeSocketConnectionState>,
    state: BridgeSocketConnectionState
) {
    return connectionState$.pipe(filter((current) => current === state));
}

/**
 * Creates the connectivity machine driven by a reconnecting socket's
 * connection state stream.
 *
 * @param connectionState$ The `bridgeSocketConnectionState$` of `getReconnectingBridgeSocket$`.
 * @returns An implemented YState machine for the socket's connectivity.
 */
export function createBridgeSocketConnectivityMachine(
    connectionState$: Observable<BridgeSocketConnectionState>
) {
    return BridgeSocketConnectivityGraph.implement({
        socketOpened: {
            $: () => onState(connectionState$, 'open'),
            next: () => ({}),
        },
        socketClosed: {
            $: () => onState(connectionState$, 'closed'),
            next: () => ({}),
        },
        socketReconnecting: {
            $: () => onState(connectionState$, 'reconnecting'),
            next: () => ({}),
        },
        socketPermanentlyClosed: {
            $: () => onState(connectionState$, 'permanently-closed'),
            next: () => ({}),
        },
        socketConnecting: {
            $: () => onState(connectionState$, 'connecting'),
            next: () => ({}),
        },
    });
}

/**
 * Opens the reconnecting bridge websocket and returns its stable message
 * stream together with the connectivity machine that tracks it.
 *
 * @param url WebSocket server URL (default: wss://wss.nori.it.com)
 * @param heartBeatInterval Interval in ms for sending pings (default: 3000)
 * @param pongTimeoutMultiplier Multiplier to determine allowed pong delay before reconnection (default: 2)
 * @returns `bridgeSocket$`, its `bridgeSocketConnectionState$`, and `bridgeSocketConnectivity`.
 */
export function getBridgeSocketWithConnectivity$(
    url?: string,
    heartBeatInterval?: number,
    pongTimeoutMultiplier?: number
) {
    const { bridgeSocket$, bridgeSocketConnectionState$ } = getReconnectingBridgeSocket$(
        url,
        heartBeatInterval,
        pongTimeoutMultiplier
    );
    return {
        bridgeSocket$,
        bridgeSocketConnectionState$,
        bridgeSocketConnectivity: createBridgeSocketConnectivityMachine(
            bridgeSocketConnectionState$
        ),
    };
}
