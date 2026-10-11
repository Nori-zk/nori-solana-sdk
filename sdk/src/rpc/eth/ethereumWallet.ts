import { define, type StateUnion } from '@yaw-rx/ystate';
import { backgroundCheckedEdges } from '../connection/backgroundChecked.js';
import { connectionOf, wrongNetworkOf } from '../connection/connection.js';
import { pacedOf } from '../connection/pacedConnection.js';

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

const { nodes, edges } = connectionOf({ wallet: noWallet });
const wrongNetwork = wrongNetworkOf({ wallet: noWallet });
const ready = { ...nodes.ready, health: { blockNumber: 0 } };
const paced = pacedOf(ready);

/**
 * The user's Ethereum wallet, when reads go through it (the app gave no
 * Ethereum RPC URL): whether there is one, which one, whether it is on the
 * proof request queue's chain, and whether reads through it work. Every
 * node that keeps reads from running carries what the app needs to tell
 * the user why, and none is a dead end.
 *
 * `connecting`, `ready`, `wrongNetwork`, `waitingToRetry`, `offline` and `closed`,
 * with their edges, are `connectionOf`'s, each live node carrying the
 * wallet, so reads gate on the wallet exactly as on an HTTP connection; the
 * background check while `ready` is `backgroundCheckedEdges`; `overwhelmed`,
 * with its edges, is `pacedOf`'s: every request through the
 * wallet takes a turn from its grants. For a wallet, `url` is its
 * reverse-DNS id. Unlike an HTTP endpoint, a wallet on another chain is not
 * checked again on a timer: it waits for the user to change chain.
 *
 * - `lookingForWallets` asks wallets to announce themselves (EIP-6963
 *   `eip6963:requestProvider`), also accepting a legacy `window.ethereum`,
 *   for a short window. Its three outcome edges race on that one search.
 * - `noWalletFound`: there is no wallet to read through; the app can ask
 *   the user to install one. A wallet announcing itself later (installed or
 *   enabled) moves straight to `connecting`.
 * - `choosingWallet`: several wallets are installed. The app lists them
 *   and calls `chooseWallet(uuid)`; wallets announcing themselves meanwhile
 *   join the list. With one wallet there is no choice to make.
 * - `connecting` asks the wallet for its chain (`eth_chainId`), then its
 *   latest block (`eth_blockNumber`). A wallet that does not answer goes to
 *   `waitingToRetry`, which asks again after a wait that doubles with each
 *   consecutive failure (`failedChecks`, reset by an answer).
 * - `ready` runs a background check every interval; a passing one is the
 *   `stillReady` self-loop, carrying the latest block. A read that fails
 *   through the wallet sends `readFailed`: `ready` to `connecting` at once.
 * - `wrongNetwork`: the wallet is on another chain. The app can tell the
 *   user which chain to switch to, and may ask the wallet to switch, once:
 *   `switchToExpectedChain` moves to `askingToSwitchChain`, whose `$` sends
 *   one `wallet_switchEthereumChain` request. Accepting arrives as a normal
 *   chain change. Declining (the wallet's user-rejected error, code 4001)
 *   moves to `switchDeclined`, which has no `switchToExpectedChain` edge, so
 *   the user is not asked again until they change chain themselves. A
 *   request the wallet fails for any other reason, or leaves unanswered for
 *   `switchTimeoutMs`, goes back to `wrongNetwork` with the reason in
 *   `lastSwitchError`, and the app may ask again.
 * - Any chain change, from any node with a wallet, connects again.
 * - The wallet's `disconnect` (it can reach no chain) moves to
 *   `waitingToRetry` from every node with a wallet; its `connect` connects
 *   again at once.
 * - Going offline pauses everything in `offline`; coming back online
 *   connects at once.
 * - `closed` ends the machine from any node and completes its streams.
 */
export const EthereumWalletGraph = define({
    nodes: {
        lookingForWallets: {},
        noWalletFound: {},
        choosingWallet: { wallets: [] as WalletInfo[] },
        ...nodes,
        ready,
        ...paced.nodes,
        wrongNetwork: { ...wrongNetwork.nodes.wrongNetwork, lastSwitchError: '' },
        askingToSwitchChain: { ...wrongNetwork.nodes.wrongNetwork },
        switchDeclined: { ...wrongNetwork.nodes.wrongNetwork },
    },
    edges: {
        ...edges,
        ...wrongNetwork.edges,
        ...backgroundCheckedEdges,
        ...paced.edges,
        oneWalletFound: {
            from: 'lookingForWallets',
            to: 'connecting',
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
            to: 'connecting',
            on: 'walletAnnounced.next',
        },
        anotherWalletAnnounced: {
            from: 'choosingWallet',
            to: 'choosingWallet',
            on: 'walletAnnounced.next',
        },
        walletChosen: {
            from: 'choosingWallet',
            to: 'connecting',
            on: 'chooseWallet.next',
        },

        chainChangedWhileConnecting: {
            from: 'connecting',
            to: 'connecting',
            on: 'walletChangedChain.next',
        },
        chainChangedWhileWaitingToRetry: {
            from: 'waitingToRetry',
            to: 'connecting',
            on: 'walletChangedChain.next',
        },
        chainChangedWhileReady: {
            from: 'ready',
            to: 'connecting',
            on: 'walletChangedChain.next',
        },
        chainChangedWhileOverwhelmed: {
            from: 'overwhelmed',
            to: 'connecting',
            on: 'walletChangedChain.next',
        },
        chainChangedOnWrongNetwork: {
            from: 'wrongNetwork',
            to: 'connecting',
            on: 'walletChangedChain.next',
        },
        switchAccepted: {
            from: 'askingToSwitchChain',
            to: 'connecting',
            on: 'walletChangedChain.next',
        },
        chainChangedAfterDecline: {
            from: 'switchDeclined',
            to: 'connecting',
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
        switchRequestTimedOut: {
            from: 'askingToSwitchChain',
            to: 'wrongNetwork',
            on: 'switchTimedOut.next',
        },

        disconnectedWhileConnecting: {
            from: 'connecting',
            to: 'waitingToRetry',
            on: 'walletDisconnected.next',
        },
        disconnectedWhileReady: {
            from: 'ready',
            to: 'waitingToRetry',
            on: 'walletDisconnected.next',
        },
        disconnectedWhileOverwhelmed: {
            from: 'overwhelmed',
            to: 'waitingToRetry',
            on: 'walletDisconnected.next',
        },
        disconnectedOnWrongNetwork: {
            from: 'wrongNetwork',
            to: 'waitingToRetry',
            on: 'walletDisconnected.next',
        },
        disconnectedWhileAskingToSwitchChain: {
            from: 'askingToSwitchChain',
            to: 'waitingToRetry',
            on: 'walletDisconnected.next',
        },
        disconnectedAfterSwitchDeclined: {
            from: 'switchDeclined',
            to: 'waitingToRetry',
            on: 'walletDisconnected.next',
        },

        wentOfflineWhileAskingToSwitchChain: {
            from: 'askingToSwitchChain',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        wentOfflineAfterSwitchDeclined: {
            from: 'switchDeclined',
            to: 'offline',
            on: 'networkWentOffline.next',
        },
        reconnected: {
            from: 'waitingToRetry',
            to: 'connecting',
            on: 'walletConnected.next',
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
    },
});

/** The wallet's state: a node of the graph and its data. */
export type EthereumWalletState = StateUnion<typeof EthereumWalletGraph.nodes>;
