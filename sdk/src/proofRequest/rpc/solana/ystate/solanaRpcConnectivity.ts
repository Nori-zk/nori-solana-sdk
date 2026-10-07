import { define, type StateUnion } from '@yaw-rx/ystate';

/**
 * Whether reads can run against a Solana RPC: one answers and serves the
 * bridge program's cluster, or the node says why not. No node is a dead end.
 *
 * The endpoints are the app's RPC URLs, or the cluster's public endpoint
 * when it gives none. Solana wallets do not serve reads, so there are no
 * wallet edges. A failed health check moves on to the next endpoint, so
 * with several, one that is down or rate-limited is skipped.
 *
 * - `checking` runs one health check (`getGenesisHash`, then `getSlot`) on
 *   the current endpoint. Its three outcome edges race on that single
 *   check, shared per entry into `checking`.
 * - `ready` runs a background check every interval through its own three
 *   outcome edges, so reads never pause for routine checks. A passing one
 *   is the `stillReady` self-loop, emitting once per interval with the
 *   latest finalized slot and the time it was checked.
 * - A read that fails against the RPC sends `readFailed`: `ready` to `checking`.
 * - `unreachable` checks again, on the next endpoint, after a wait that
 *   doubles with each failed check, read from its own `failedChecks`.
 * - `wrongNetwork`: the endpoint serves another cluster. It carries both
 *   genesis hashes for the message and checks again on the same doubling
 *   wait, on the next endpoint, so a corrected endpoint recovers.
 * - Going offline pauses everything in `offline`; coming back online checks
 *   at once.
 * - `closed` ends the machine from any node and completes its streams.
 */
export const SolanaRpcConnectivityGraph = define({
    nodes: {
        checking: { failedChecks: 0 },
        ready: { rpcUrl: '', slot: 0n as bigint, checkedAt: 0 },
        wrongNetwork: {
            rpcUrl: '',
            genesisHash: '',
            expectedGenesisHash: '',
            failedChecks: 0,
        },
        unreachable: { rpcUrl: '', failedChecks: 0, error: '' },
        offline: {},
        closed: {},
    },
    edges: {
        healthCheckPassed: {
            from: 'checking',
            to: 'ready',
            on: 'checkFoundExpectedCluster.next',
        },
        wrongNetworkFound: {
            from: 'checking',
            to: 'wrongNetwork',
            on: 'checkFoundOtherCluster.next',
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
            on: 'backgroundCheckFoundOtherCluster.next',
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

/** The Solana RPC's connection: a node of the graph and its data. */
export type SolanaRpcConnection = StateUnion<
    typeof SolanaRpcConnectivityGraph.nodes
>;
