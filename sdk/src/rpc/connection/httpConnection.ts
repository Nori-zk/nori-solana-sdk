import { define, type StateUnion } from '@yaw-rx/ystate';

/**
 * A connection checked by its health: whether reads can run against it.
 * Graphs whose machine is this spread its nodes and edges, each live node
 * with the data it carries (`carried`), and add their own; the HTTP
 * connection is it with a recheck on the wrong network, the wallet is it
 * with finding, choosing and switching the wallet. No node is a dead end.
 *
 * - `checking` runs one health check. Its three outcome edges race on that
 *   single check, shared per entry into `checking`.
 * - `ready` runs a background check every interval through its own three
 *   outcome edges rather than going back through `checking`, so reads never
 *   pause for routine checks. A passing one is the `stillReady` self-loop,
 *   emitting once per interval with what the check found and when.
 * - A read that fails against it sends `readFailed`: `ready` to `checking`
 *   at once instead of waiting for the next interval.
 * - `unreachable` checks again after a wait that doubles with each failed
 *   check, read from its own `failedChecks`.
 * - `wrongNetwork`: it serves another network. It carries what it found and
 *   what was expected, for the message.
 * - Going offline pauses everything in `offline`; coming back online checks
 *   at once.
 * - `closed` ends the machine from any node and completes its streams.
 *
 * @param carried The data every live node carries besides its own.
 * @returns The nodes and edges.
 */
export const healthCheckedOf = <TCarried extends object>(carried: TCarried) => ({
    nodes: {
        checking: { ...carried, failedChecks: 0 },
        ready: { ...carried, url: '', checkedAt: 0, health: undefined as unknown },
        wrongNetwork: { ...carried, url: '', found: '', expected: '', failedChecks: 0 },
        unreachable: { ...carried, url: '', failedChecks: 0, error: '' },
        offline: { ...carried },
        closed: {},
    },
    edges: {
        healthCheckPassed: { from: 'checking', to: 'ready', on: 'checkFoundExpectedNetwork.next' },
        wrongNetworkFound: { from: 'checking', to: 'wrongNetwork', on: 'checkFoundOtherNetwork.next' },
        healthCheckFailed: { from: 'checking', to: 'unreachable', on: 'checkFailed.next' },

        stillReady: { from: 'ready', to: 'ready', on: 'backgroundCheckPassed.next' },
        switchedToWrongNetwork: { from: 'ready', to: 'wrongNetwork', on: 'backgroundCheckFoundOtherNetwork.next' },
        becameUnreachable: { from: 'ready', to: 'unreachable', on: 'backgroundCheckFailed.next' },

        recheckStarted: { from: 'ready', to: 'checking', on: 'readFailed.next' },
        retryStarted: { from: 'unreachable', to: 'checking', on: 'retryDue.next' },

        wentOfflineWhileChecking: { from: 'checking', to: 'offline', on: 'networkWentOffline.next' },
        wentOfflineWhileReady: { from: 'ready', to: 'offline', on: 'networkWentOffline.next' },
        wentOfflineOnWrongNetwork: { from: 'wrongNetwork', to: 'offline', on: 'networkWentOffline.next' },
        wentOfflineWhileUnreachable: { from: 'unreachable', to: 'offline', on: 'networkWentOffline.next' },
        cameOnline: { from: 'offline', to: 'checking', on: 'networkCameOnline.next' },

        closedWhileChecking: { from: 'checking', to: 'closed', on: 'close.next' },
        closedWhileReady: { from: 'ready', to: 'closed', on: 'close.next' },
        closedOnWrongNetwork: { from: 'wrongNetwork', to: 'closed', on: 'close.next' },
        closedWhileUnreachable: { from: 'unreachable', to: 'closed', on: 'close.next' },
        closedWhileOffline: { from: 'offline', to: 'closed', on: 'close.next' },
    } as const,
});

const { nodes, edges } = healthCheckedOf({});

/**
 * Whether reads can run against an HTTP endpoint: one of its URLs answers
 * and serves the expected network, or the node says why not
 * (`healthCheckedOf`). The same definition serves every chain; what a health
 * check asks, and what a passing one finds (`ready.health`), is the
 * implementation's.
 *
 * - `checking` checks the current URL; `unreachable` and `wrongNetwork` check
 *   again on the next URL, after a wait that doubles with each failed check.
 * - `wrongNetwork` checks again on that doubling wait (`wrongNetworkRechecked`),
 *   so a corrected endpoint recovers.
 *
 * `ready.health` is typed per use by `HttpConnectionState<THealth>`.
 */
export const HttpConnectionGraph = define({
    nodes,
    edges: {
        ...edges,
        wrongNetworkRechecked: { from: 'wrongNetwork', to: 'checking', on: 'recheckDue.next' },
    },
});

/** An HTTP connection's state: a node of the graph and its data, `ready.health` typed as `THealth`. */
export type HttpConnectionState<THealth> =
    StateUnion<typeof HttpConnectionGraph.nodes> extends infer TState
        ? TState extends { node: 'ready'; data: infer TData }
            ? {
                  node: 'ready';
                  data: Omit<TData, 'health'> & { health: THealth };
              }
            : TState
        : never;
