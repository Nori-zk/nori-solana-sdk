import { define, type StateUnion } from '@yaw-rx/ystate';

/**
 * Whether a connection can serve requests: one machine per connection, over
 * its transports in the connection's order, each a running machine (http,
 * websocket, wallet, the network). Every reader and every view of the
 * connection follows this machine. No node is a dead end.
 *
 * - `unsure`: no transport is up and one has no outcome yet, or has failed
 *   once in a row and is checking again.
 * - `connected`: a transport is up; `transport` names the first one up in
 *   the connection's order, followed as another takes over
 *   (`servingTransportChanged`).
 * - `down`: no transport is up or unsure; `reason` is the node the first
 *   transport's machine is at (`unreachable`, `offline`, `wrongNetwork`,
 *   `noWalletFound`, …), followed as it changes (`reasonChanged`).
 * - Any of the three moves to any other as the transports' states change.
 * - `closed` ends the machine from any node.
 */
export const ConnectionStatusGraph = define({
    nodes: {
        unsure: {},
        connected: { transport: '' },
        down: { reason: '' },
        closed: {},
    },
    edges: {
        connectedFromUnsure: { from: 'unsure', to: 'connected', on: 'connected.next' },
        downFromUnsure: { from: 'unsure', to: 'down', on: 'down.next' },

        becameUnsure: { from: 'connected', to: 'unsure', on: 'unsure.next' },
        servingTransportChanged: {
            from: 'connected',
            to: 'connected',
            on: 'servingTransportChanged.next',
        },
        wentDown: { from: 'connected', to: 'down', on: 'down.next' },

        checkingAgain: { from: 'down', to: 'unsure', on: 'unsure.next' },
        reconnected: { from: 'down', to: 'connected', on: 'connected.next' },
        reasonChanged: { from: 'down', to: 'down', on: 'reasonChanged.next' },

        closedWhileUnsure: { from: 'unsure', to: 'closed', on: 'close.next' },
        closedWhileConnected: { from: 'connected', to: 'closed', on: 'close.next' },
        closedWhileDown: { from: 'down', to: 'closed', on: 'close.next' },
    },
});

/** A connection's status: a node of the graph and its data. */
export type ConnectionStatusState = StateUnion<typeof ConnectionStatusGraph.nodes>;
