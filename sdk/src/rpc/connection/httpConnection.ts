import { define, type StateUnion } from '@yaw-rx/ystate';

/**
 * Whether reads can run against an HTTP endpoint: one of its URLs answers
 * and serves the expected network, or the node says why not. No node is a
 * dead end. The same definition serves every chain; what a health check
 * asks, and what a passing one finds (`ready.health`), is the
 * implementation's.
 *
 * - `checking` runs one health check on the current URL. Its three outcome
 *   edges race on that single check, shared per entry into `checking`.
 * - `ready` runs a background check every interval through its own three
 *   outcome edges rather than going back through `checking`, so reads never
 *   pause for routine checks. A passing one is the `stillReady` self-loop,
 *   emitting once per interval with what the check found and when.
 * - A read that fails against the endpoint sends `readFailed`: `ready` to
 *   `checking` at once instead of waiting for the next interval.
 * - `unreachable` checks again, on the next URL, after a wait that doubles
 *   with each failed check, read from its own `failedChecks`.
 * - `wrongNetwork`: the URL serves another network. It carries what it found
 *   and what was expected, for the message, and checks again on the same
 *   doubling wait, on the next URL, so a corrected endpoint recovers.
 * - Going offline pauses everything in `offline`; coming back online checks
 *   at once.
 * - `closed` ends the machine from any node and completes its streams.
 *
 * `ready.health` is typed per use by `HttpConnectionState<THealth>`.
 */
export const HttpConnectionGraph = define({
    nodes: {
        checking: { failedChecks: 0 },
        ready: { url: '', checkedAt: 0, health: undefined as unknown },
        wrongNetwork: {
            url: '',
            found: '',
            expected: '',
            failedChecks: 0,
        },
        unreachable: { url: '', failedChecks: 0, error: '' },
        offline: {},
        closed: {},
    },
    edges: {
        healthCheckPassed: {
            from: 'checking',
            to: 'ready',
            on: 'checkFoundExpectedNetwork.next',
        },
        wrongNetworkFound: {
            from: 'checking',
            to: 'wrongNetwork',
            on: 'checkFoundOtherNetwork.next',
        },
        healthCheckFailed: {
            from: 'checking',
            to: 'unreachable',
            on: 'checkFailed.next',
        },

        stillReady: {
            from: 'ready',
            to: 'ready',
            on: 'backgroundCheckPassed.next',
        },
        switchedToWrongNetwork: {
            from: 'ready',
            to: 'wrongNetwork',
            on: 'backgroundCheckFoundOtherNetwork.next',
        },
        becameUnreachable: {
            from: 'ready',
            to: 'unreachable',
            on: 'backgroundCheckFailed.next',
        },

        recheckStarted: {
            from: 'ready',
            to: 'checking',
            on: 'readFailed.next',
        },

        retryStarted: {
            from: 'unreachable',
            to: 'checking',
            on: 'retryDue.next',
        },
        wrongNetworkRechecked: {
            from: 'wrongNetwork',
            to: 'checking',
            on: 'recheckDue.next',
        },

        wentOfflineWhileChecking: {
            from: 'checking',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        wentOfflineWhileReady: {
            from: 'ready',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        wentOfflineOnWrongNetwork: {
            from: 'wrongNetwork',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        wentOfflineWhileUnreachable: {
            from: 'unreachable',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        cameOnline: {
            from: 'offline',
            to: 'checking',
            on: 'networkCameOnline.next',
        },

        closedWhileChecking: {
            from: 'checking',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileReady: {
            from: 'ready',
            to: 'closed',
            on: 'close.next',
        },
        closedOnWrongNetwork: {
            from: 'wrongNetwork',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileUnreachable: {
            from: 'unreachable',
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
