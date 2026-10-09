import { define, type StateUnion } from '@yaw-rx/ystate';

/**
 * Whether a WebSocket to one URL is open. The same definition serves every
 * server; what is sent on every open and how the heartbeat works is the
 * implementation's. No node is a dead end.
 *
 * - `connecting` opens the socket. Opening it is `open`; a socket that fails
 *   or does not open in time is `reconnecting`, or `gaveUp` when that was
 *   the last attempt allowed.
 * - `open`: the socket is open and what the owner sends on every open has
 *   been sent. A socket that closes, errors or misses its heartbeat is
 *   `reconnecting`.
 * - `reconnecting` opens it again after a wait that doubles with each
 *   failed attempt in a row (`failedAttempts`), carrying the last `error`.
 * - `gaveUp`: the attempts allowed ran out. It holds the last `error` and
 *   connects again on `retry`, or when the network comes back online.
 * - Going offline pauses everything in `offline`; coming back online
 *   connects at once.
 * - `closed` ends the machine from any node and completes its streams.
 */
export const WebSocketConnectionGraph = define({
    nodes: {
        connecting: { failedAttempts: 0 },
        open: { openedAt: 0 },
        reconnecting: { failedAttempts: 0, error: '' },
        gaveUp: { error: '' },
        offline: {},
        closed: {},
    },
    edges: {
        socketOpened: {
            from: 'connecting',
            to: 'open',
            on: 'opened.next',
        },
        connectFailed: {
            from: 'connecting',
            to: 'reconnecting',
            on: 'connectFailed.next',
        },
        attemptsRanOut: {
            from: 'connecting',
            to: 'gaveUp',
            on: 'attemptsRanOut.next',
        },
        socketDropped: {
            from: 'open',
            to: 'reconnecting',
            on: 'dropped.next',
        },
        reconnectStarted: {
            from: 'reconnecting',
            to: 'connecting',
            on: 'reconnectDue.next',
        },
        retryRequested: {
            from: 'gaveUp',
            to: 'connecting',
            on: 'retry.next',
        },

        wentOfflineWhileConnecting: {
            from: 'connecting',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        wentOfflineWhileOpen: {
            from: 'open',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        wentOfflineWhileReconnecting: {
            from: 'reconnecting',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        wentOfflineAfterGivingUp: {
            from: 'gaveUp',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        cameOnline: {
            from: 'offline',
            to: 'connecting',
            on: 'networkCameOnline.next',
        },

        closedWhileConnecting: {
            from: 'connecting',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileOpen: {
            from: 'open',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileReconnecting: {
            from: 'reconnecting',
            to: 'closed',
            on: 'close.next',
        },
        closedAfterGivingUp: {
            from: 'gaveUp',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileOffline: {
            from: 'offline',
            to: 'closed',
            on: 'close.next',
        },
    },
});

/** A WebSocket connection's state: a node of the graph and its data. */
export type WebSocketConnectionState = StateUnion<
    typeof WebSocketConnectionGraph.nodes
>;
