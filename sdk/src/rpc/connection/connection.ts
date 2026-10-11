/** The nodes a connection serves requests in: `ready`, and `overwhelmed` where a graph spreads `pacedOf`. */
export const SERVING_NODES: readonly string[] = ['ready', 'overwhelmed'];

/**
 * A connection to an endpoint: whether requests can run against it. Graphs
 * whose machine is this spread its nodes and edges, each live node with the
 * data it carries (`carried`), and add their own: one whose check can find
 * another network adds `wrongNetworkOf`; the HTTP connection and the
 * wallet add a background check while ready (`backgroundCheckedEdges`); the
 * websocket, the socket dropping. No node is a dead end.
 *
 * - `connecting` reaches the endpoint and checks it, once. Its outcome
 *   edges race on that single check, shared per entry into `connecting`.
 * - `ready`: it serves, with what the check found and when.
 * - A read that fails against it sends `readFailed`: `ready` to `connecting`
 *   at once.
 * - `waitingToRetry` connects again after a wait that doubles with each
 *   failed check, read from its own `failedChecks`.
 * - Going offline pauses everything in `offline`; coming back online
 *   connects at once.
 * - `closed` ends the machine from any node and completes its streams.
 *
 * @param carried The data every live node carries besides its own.
 * @returns The nodes and edges.
 */
export const connectionOf = <TCarried extends object>(carried: TCarried) => ({
    nodes: {
        connecting: { ...carried, failedChecks: 0 },
        ready: { ...carried, url: '', checkedAt: 0, health: undefined as unknown },
        waitingToRetry: { ...carried, url: '', failedChecks: 0, error: '' },
        offline: { ...carried },
        closed: {},
    },
    edges: {
        healthCheckPassed: { from: 'connecting', to: 'ready', on: 'checkFoundExpectedNetwork.next' },
        healthCheckFailed: { from: 'connecting', to: 'waitingToRetry', on: 'checkFailed.next' },

        recheckStarted: { from: 'ready', to: 'connecting', on: 'readFailed.next' },
        retryStarted: { from: 'waitingToRetry', to: 'connecting', on: 'retryDue.next' },

        wentOfflineWhileConnecting: { from: 'connecting', to: 'offline', on: 'networkWentOffline.next' },
        wentOfflineWhileReady: { from: 'ready', to: 'offline', on: 'networkWentOffline.next' },
        wentOfflineWhileWaitingToRetry: { from: 'waitingToRetry', to: 'offline', on: 'networkWentOffline.next' },
        cameOnline: { from: 'offline', to: 'connecting', on: 'networkCameOnline.next' },

        closedWhileConnecting: { from: 'connecting', to: 'closed', on: 'close.next' },
        closedWhileReady: { from: 'ready', to: 'closed', on: 'close.next' },
        closedWhileWaitingToRetry: { from: 'waitingToRetry', to: 'closed', on: 'close.next' },
        closedWhileOffline: { from: 'offline', to: 'closed', on: 'close.next' },
    } as const,
});

/**
 * A connection whose check finds which network the endpoint serves:
 * `wrongNetwork`, where it serves another, with what it found and what was
 * expected, for the message. Graphs spread it next to `connectionOf`'s
 * nodes and edges: the HTTP connection, the wallet and the Ethereum
 * websocket. A connection whose check cannot ask the network (Nori's
 * server, a Solana websocket) does not, so the node is never a dead end.
 *
 * @param carried The data every live node carries besides its own.
 * @returns The node and edges.
 */
export const wrongNetworkOf = <TCarried extends object>(carried: TCarried) => ({
    nodes: {
        wrongNetwork: { ...carried, url: '', found: '', expected: '', failedChecks: 0 },
    },
    edges: {
        wrongNetworkFound: { from: 'connecting', to: 'wrongNetwork', on: 'checkFoundOtherNetwork.next' },
        wentOfflineOnWrongNetwork: { from: 'wrongNetwork', to: 'offline', on: 'networkWentOffline.next' },
        closedOnWrongNetwork: { from: 'wrongNetwork', to: 'closed', on: 'close.next' },
    } as const,
});
