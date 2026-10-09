import { define, type StateUnion } from '@yaw-rx/ystate';
import { HttpConnectionGraph } from '../connection/httpConnection.js';

/** A wallet the browser offers, as it announces itself (EIP-6963). */
export interface WalletInfo {
    /** Unique per page load. */
    uuid: string;
    /** The wallet's name, for display. */
    name: string;
    /** The wallet's icon as a data URI, for display. */
    icon: string;
    /** The wallet's reverse-DNS id, e.g. `io.metamask`. */
    rdns: string;
}

const noWallet: WalletInfo = { uuid: '', name: '', icon: '', rdns: '' };

/**
 * The user's Ethereum wallet, when reads go through it (the app gave no
 * Ethereum RPC URL): whether there is one, which one, whether it is on the
 * proof request queue's chain, and whether reads through it work. Every
 * node that keeps reads from running carries what the app needs to tell
 * the user why, and none is a dead end.
 *
 * `checking`, `ready`, `wrongNetwork`, `unreachable`, `offline` and `closed`
 * are the HTTP connection's nodes, with their data and the wallet added, so
 * reads gate on the wallet exactly as on an HTTP connection. For a wallet,
 * `url` is its reverse-DNS id.
 *
 * - `lookingForWallets` asks wallets to announce themselves (EIP-6963
 *   `eip6963:requestProvider`), also accepting a legacy `window.ethereum`,
 *   for a short window. Its three outcome edges race on that one search.
 * - `noWalletFound`: there is no wallet to read through; the app can ask
 *   the user to install one. A wallet announcing itself later (installed or
 *   enabled) moves straight to `checking`.
 * - `choosingWallet`: several wallets are installed. The app lists them
 *   and calls `chooseWallet(uuid)`; wallets announcing themselves meanwhile
 *   join the list. With one wallet there is no choice to make.
 * - `checking` asks the wallet for its chain (`eth_chainId`), then its
 *   latest block (`eth_blockNumber`). A wallet that does not answer goes to
 *   `unreachable`, which asks again after a wait that doubles with each
 *   consecutive failure (`failedChecks`, reset by an answer).
 * - `ready` runs a background check every interval; a passing one is the
 *   `stillReady` self-loop, carrying the latest block. A read that fails
 *   through the wallet sends `readFailed`: `ready` to `checking` at once.
 * - `wrongNetwork`: the wallet is on another chain. The app can tell the
 *   user which chain to switch to, and may ask the wallet to switch, once:
 *   `switchToExpectedChain` moves to `askingToSwitchChain`, whose `$` sends
 *   one `wallet_switchEthereumChain` request. Accepting arrives as a normal
 *   chain change. Declining (the wallet's user-rejected error, code 4001)
 *   moves to `switchDeclined`, which has no `switchToExpectedChain` edge, so
 *   the user is not asked again until they change chain themselves. A
 *   request the wallet fails for any other reason goes back to
 *   `wrongNetwork` with the wallet's error in `lastSwitchError`, and the app
 *   may ask again.
 * - Any chain change, from any node with a wallet, checks again.
 * - The wallet's `disconnect` (it can reach no chain) moves to
 *   `unreachable`; its `connect` checks again at once.
 * - Going offline pauses everything in `offline`; coming back online checks
 *   at once.
 * - `closed` ends the machine from any node and completes its streams.
 */
export const EthereumWalletGraph = define({
    nodes: {
        lookingForWallets: {},
        noWalletFound: {},
        choosingWallet: { wallets: [] as WalletInfo[] },
        checking: { ...HttpConnectionGraph.nodes.checking, wallet: noWallet },
        ready: {
            ...HttpConnectionGraph.nodes.ready,
            health: { blockNumber: 0 },
            wallet: noWallet,
        },
        wrongNetwork: {
            ...HttpConnectionGraph.nodes.wrongNetwork,
            wallet: noWallet,
            lastSwitchError: '',
        },
        askingToSwitchChain: {
            ...HttpConnectionGraph.nodes.wrongNetwork,
            wallet: noWallet,
        },
        switchDeclined: {
            ...HttpConnectionGraph.nodes.wrongNetwork,
            wallet: noWallet,
        },
        unreachable: { ...HttpConnectionGraph.nodes.unreachable, wallet: noWallet },
        offline: { ...HttpConnectionGraph.nodes.offline, wallet: noWallet },
        closed: HttpConnectionGraph.nodes.closed,
    },
    edges: {
        oneWalletFound: {
            from: 'lookingForWallets',
            to: 'checking',
            on: 'foundOneWallet.next',
        },
        severalWalletsFound: {
            from: 'lookingForWallets',
            to: 'choosingWallet',
            on: 'foundSeveralWallets.next',
        },
        noWalletFound: {
            from: 'lookingForWallets',
            to: 'noWalletFound',
            on: 'foundNoWallet.next',
        },
        walletInstalled: {
            from: 'noWalletFound',
            to: 'checking',
            on: 'walletAnnounced.next',
        },
        anotherWalletAnnounced: {
            from: 'choosingWallet',
            to: 'choosingWallet',
            on: 'walletAnnounced.next',
        },
        walletChosen: {
            from: 'choosingWallet',
            to: 'checking',
            on: 'chooseWallet.next',
        },

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

        chainChangedWhileChecking: {
            from: 'checking',
            to: 'checking',
            on: 'walletChangedChain.next',
        },
        chainChangedWhileUnreachable: {
            from: 'unreachable',
            to: 'checking',
            on: 'walletChangedChain.next',
        },
        chainChangedWhileReady: {
            from: 'ready',
            to: 'checking',
            on: 'walletChangedChain.next',
        },
        chainChangedOnWrongNetwork: {
            from: 'wrongNetwork',
            to: 'checking',
            on: 'walletChangedChain.next',
        },
        switchAccepted: {
            from: 'askingToSwitchChain',
            to: 'checking',
            on: 'walletChangedChain.next',
        },
        chainChangedAfterDecline: {
            from: 'switchDeclined',
            to: 'checking',
            on: 'walletChangedChain.next',
        },

        switchRequested: {
            from: 'wrongNetwork',
            to: 'askingToSwitchChain',
            on: 'switchToExpectedChain.next',
        },
        userDeclinedSwitch: {
            from: 'askingToSwitchChain',
            to: 'switchDeclined',
            on: 'switchDeclined.next',
        },
        switchRequestFailed: {
            from: 'askingToSwitchChain',
            to: 'wrongNetwork',
            on: 'switchRequestFailed.next',
        },

        disconnectedWhileChecking: {
            from: 'checking',
            to: 'unreachable',
            on: 'walletDisconnected.next',
        },
        disconnectedWhileReady: {
            from: 'ready',
            to: 'unreachable',
            on: 'walletDisconnected.next',
        },
        reconnected: {
            from: 'unreachable',
            to: 'checking',
            on: 'walletConnected.next',
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

        closedWhileLookingForWallets: {
            from: 'lookingForWallets',
            to: 'closed',
            on: 'close.next',
        },
        closedWithNoWallet: {
            from: 'noWalletFound',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileChoosingWallet: {
            from: 'choosingWallet',
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
        closedWhileAskingToSwitchChain: {
            from: 'askingToSwitchChain',
            to: 'closed',
            on: 'close.next',
        },
        closedAfterSwitchDeclined: {
            from: 'switchDeclined',
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

/** The wallet's state: a node of the graph and its data. */
export type EthereumWalletState = StateUnion<typeof EthereumWalletGraph.nodes>;
