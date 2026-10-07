import { define } from '@yaw-rx/ystate';

export type BridgeSocketConnectionState =
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
 * - `closed` to `reconnecting` while retries remain, or to `permanentlyClosed` once they run out.
 * - `reconnecting` to `connecting` when the backoff timer fires.
 */
export const BridgeSocketConnectivityGraph = define({
    nodes: {
        connecting: {},
        open: {},
        closed: {},
        reconnecting: {},
        permanentlyClosed: {},
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
            to: 'permanentlyClosed',
            on: 'socketPermanentlyClosed.next',
        },
        reconnectStarted: {
            from: 'reconnecting',
            to: 'connecting',
            on: 'socketConnecting.next',
        },
    },
});
