import { define, type StateUnion } from '@yaw-rx/ystate';
import { connectionOf, wrongNetworkOf } from './connection.js';
import { pacedOf } from './pacedConnection.js';

const { nodes, edges } = connectionOf({});
const wrongNetwork = wrongNetworkOf({});
const paced = pacedOf(nodes.ready);

/** The node a websocket has besides `connectionOf`'s: it stopped trying. */
const socketNodes = { gaveUp: { error: '' } };

/** The edges a websocket has besides `connectionOf`'s: its socket dropping, and giving up. */
const socketEdges = {
    droppedWhileConnecting: { from: 'connecting', to: 'waitingToRetry', on: 'dropped.next' },
    droppedWhileReady: { from: 'ready', to: 'waitingToRetry', on: 'dropped.next' },
    attemptsRanOut: { from: 'connecting', to: 'gaveUp', on: 'attemptsRanOut.next' },
    retryRequested: { from: 'gaveUp', to: 'connecting', on: 'retry.next' },
    wentOfflineAfterGivingUp: { from: 'gaveUp', to: 'offline', on: 'networkWentOffline.next' },
    closedAfterGivingUp: { from: 'gaveUp', to: 'closed', on: 'close.next' },
} as const;

/**
 * Whether requests and subscriptions can run over a websocket to one URL
 * (`connectionOf`). Its socket tells it when it drops (closing, erroring or
 * missing its heartbeat), so nothing asks again on a timer; it is checked
 * once per socket. The socket is held in `connecting` and the nodes that
 * serve, and closed in every other node. No node is a dead end.
 *
 * - `connecting` opens a socket, unless it holds one, then checks it. A
 *   socket that fails, does not open in time or fails its check is
 *   `waitingToRetry`, or `gaveUp` when that was the last attempt allowed.
 * - A held socket that drops goes to `waitingToRetry`.
 * - `gaveUp`: the attempts allowed ran out. It holds the last `error` and
 *   connects again on `retry`, or when the network comes back online.
 *
 * Nori's websocket is this graph: its server pushes topics, serves no
 * network to check, and turns nothing away for too many requests.
 */
export const WebSocketConnectionGraph = define({
    nodes: { ...nodes, ...socketNodes },
    edges: { ...edges, ...socketEdges },
});

/**
 * A websocket to an RPC node that cannot be asked which network it serves:
 * `WebSocketConnectionGraph`, and the rate the node takes requests at
 * (`pacedOf`). Every request and subscription sent over it takes a turn
 * from its grants; the socket is held in `overwhelmed` too, and dropping
 * there goes to `waitingToRetry`. A Solana node's websocket serves
 * subscriptions only, so this is its graph.
 */
export const RateLimitedWebSocketGraph = define({
    nodes: { ...nodes, ...paced.nodes, ...socketNodes },
    edges: {
        ...edges,
        ...paced.edges,
        ...socketEdges,
        droppedWhileOverwhelmed: { from: 'overwhelmed', to: 'waitingToRetry', on: 'dropped.next' },
    },
});

/**
 * A websocket to an RPC node that is asked which network it serves:
 * `RateLimitedWebSocketGraph`, and `wrongNetwork` (`wrongNetworkOf`),
 * which connects again on a fresh socket after a wait that doubles with
 * each failed check (`wrongNetworkRechecked`). An Ethereum node's websocket
 * answers `eth_chainId`, so this is its graph.
 */
export const RateLimitedWebSocketWithWrongNetworkGraph = define({
    nodes: { ...nodes, ...wrongNetwork.nodes, ...paced.nodes, ...socketNodes },
    edges: {
        ...edges,
        ...wrongNetwork.edges,
        ...paced.edges,
        ...socketEdges,
        droppedWhileOverwhelmed: { from: 'overwhelmed', to: 'waitingToRetry', on: 'dropped.next' },
        wrongNetworkRechecked: { from: 'wrongNetwork', to: 'connecting', on: 'recheckDue.next' },
    },
});

/** A websocket connection's state: a node of the graph and its data. */
export type WebSocketConnectionState = StateUnion<typeof WebSocketConnectionGraph.nodes>;

/** A paced websocket connection's state: a node of the graph and its data. */
export type RateLimitedWebSocketState = StateUnion<typeof RateLimitedWebSocketGraph.nodes>;

/** A network-checked websocket connection's state: a node of the graph and its data. */
export type RateLimitedWebSocketWithWrongNetworkState = StateUnion<typeof RateLimitedWebSocketWithWrongNetworkGraph.nodes>;
