import { define, type StateUnion } from '@yaw-rx/ystate';

/**
 * Whether reads can run against the Ethereum provider: the user's wallet
 * (when the app gives no RPC URL) or the app's RPC URL. It answers and is on
 * the proof request queue's chain, or the node says why not. No node is a
 * dead end.
 *
 * - `waitingForWallet`: reading through the wallet, and the wallet machine
 *   is not on the expected chain (no wallet, choosing one, another chain,
 *   asking to switch...). Its own state says which, for the message. When
 *   the wallet reaches the expected chain, the provider is checked. With an
 *   RPC URL the machine never enters it.
 * - `checking` runs one health check (`eth_chainId`, then `eth_blockNumber`).
 *   Its three outcome edges race on that single check: their `$` share one
 *   check per entry into `checking`.
 * - `ready` runs a background check every interval through its own three
 *   outcome edges rather than going back through `checking`: passing
 *   through `checking` would take the provider out of `ready` for the
 *   length of every check and pause all reads. A passing background check
 *   is the `stillReady` self-loop, so `state$` emits once per interval
 *   while ready, carrying the latest block and the time it was checked.
 * - A read that fails against the provider sends `readFailed`, moving
 *   `ready` to `checking` at once instead of waiting for the next interval.
 * - `unreachable` checks again after a wait that doubles with each failed
 *   check, read from its own `failedChecks`.
 * - `wrongNetwork`: the RPC URL answers on another chain. It carries both
 *   chains for the message and checks again on the same doubling wait, so a
 *   corrected endpoint recovers.
 * - The wallet's `disconnect` (it can reach no chain) moves to
 *   `unreachable`; its `connect` checks again at once.
 * - Going offline pauses everything in `offline`; coming back online checks
 *   at once.
 * - `closed` ends the machine from any node and completes its streams.
 */
export const EthereumProviderConnectivityGraph = define({
    nodes: {
        waitingForWallet: {},
        checking: { failedChecks: 0 },
        ready: { chainId: 0n as bigint, blockNumber: 0, checkedAt: 0 },
        wrongNetwork: {
            chainId: 0n as bigint,
            expectedChainId: 0n as bigint,
            failedChecks: 0,
        },
        unreachable: { failedChecks: 0, error: '' },
        offline: {},
        closed: {},
    },
    edges: {
        walletReady: {
            from: 'waitingForWallet',
            to: 'checking',
            on: 'walletOnExpectedChain.next',
        },
        walletLeftExpectedChainWhileChecking: {
            from: 'checking',
            to: 'waitingForWallet',
            on: 'walletNotOnExpectedChain.next',
        },
        walletLeftExpectedChain: {
            from: 'ready',
            to: 'waitingForWallet',
            on: 'walletNotOnExpectedChain.next',
        },
        walletLeftExpectedChainOnWrongNetwork: {
            from: 'wrongNetwork',
            to: 'waitingForWallet',
            on: 'walletNotOnExpectedChain.next',
        },
        walletLeftExpectedChainWhileUnreachable: {
            from: 'unreachable',
            to: 'waitingForWallet',
            on: 'walletNotOnExpectedChain.next',
        },

        healthCheckPassed: {
            from: 'checking',
            to: 'ready',
            on: 'checkFoundExpectedChain.next',
        },
        wrongNetworkFound: {
            from: 'checking',
            to: 'wrongNetwork',
            on: 'checkFoundOtherChain.next',
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
            on: 'backgroundCheckFoundOtherChain.next',
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

        walletDisconnectedWhileChecking: {
            from: 'checking',
            to: 'unreachable',
            on: 'walletDisconnected.next',
        },
        walletDisconnectedWhileReady: {
            from: 'ready',
            to: 'unreachable',
            on: 'walletDisconnected.next',
        },
        walletReconnected: {
            from: 'unreachable',
            to: 'checking',
            on: 'walletConnected.next',
        },

        wentOfflineWhileWaitingForWallet: {
            from: 'waitingForWallet',
            to: 'offline',
            on: 'networkWentOffline.next',
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

        closedWhileWaitingForWallet: {
            from: 'waitingForWallet',
            to: 'closed',
            on: 'close.next',
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

/** The Ethereum provider's connection: a node of the graph and its data. */
export type EthereumProviderConnection = StateUnion<
    typeof EthereumProviderConnectivityGraph.nodes
>;
