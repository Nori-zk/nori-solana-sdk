import { define, type StateUnion } from '@yaw-rx/ystate';

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
 * Ethereum RPC URL): whether there is one, which one, and whether it is on
 * the proof request queue's chain. Every node that keeps reads from running
 * carries what the app needs to tell the user why, and none is a dead end.
 *
 * - `lookingForWallets` asks wallets to announce themselves (EIP-6963
 *   `eip6963:requestProvider`), also accepting a legacy `window.ethereum`,
 *   for a short window. Its three outcome edges race on that one search.
 * - `noWalletFound`: there is no wallet to read through; the app can ask
 *   the user to install one. A wallet announcing itself later (installed or
 *   enabled) moves straight to `checkingChain`.
 * - `choosingWallet`: several wallets are installed. The app lists them
 *   and calls `chooseWallet(uuid)`; wallets announcing themselves meanwhile
 *   join the list. With one wallet there is no choice to make.
 * - `checkingChain` asks the wallet which chain it is on. A wallet that does
 *   not answer goes to `walletNotResponding`, which asks again after a wait
 *   that doubles with each consecutive failure (`failedChecks`, carried
 *   through `checkingChain` and reset by an answer).
 * - `onOtherChain`: the app can tell the user which chain to switch to, and
 *   may ask the wallet to switch, once: `switchToExpectedChain` moves to
 *   `askingToSwitchChain`, whose `$` sends one `wallet_switchEthereumChain`
 *   request. Accepting arrives as a normal chain change. Declining (the
 *   wallet's user-rejected error, code 4001) moves to `switchDeclined`, which
 *   has no `switchToExpectedChain` edge, so the user is not asked again
 *   until they change chain themselves. A request the wallet fails for any
 *   other reason goes back to `onOtherChain` with the wallet's error in
 *   `lastSwitchError`, and the app may ask again.
 * - Any chain change, from any node with a wallet, checks the chain again.
 * - `closed` ends the machine from any node and completes its streams.
 */
export const EthereumWalletGraph = define({
    nodes: {
        lookingForWallets: {},
        noWalletFound: {},
        choosingWallet: { wallets: [] as WalletInfo[] },
        checkingChain: { wallet: noWallet, failedChecks: 0 },
        walletNotResponding: { wallet: noWallet, failedChecks: 0, error: '' },
        onExpectedChain: { wallet: noWallet, chainId: 0n as bigint },
        onOtherChain: {
            wallet: noWallet,
            chainId: 0n as bigint,
            expectedChainId: 0n as bigint,
            lastSwitchError: '',
        },
        askingToSwitchChain: {
            wallet: noWallet,
            chainId: 0n as bigint,
            expectedChainId: 0n as bigint,
        },
        switchDeclined: {
            wallet: noWallet,
            chainId: 0n as bigint,
            expectedChainId: 0n as bigint,
        },
        closed: {},
    },
    edges: {
        oneWalletFound: {
            from: 'lookingForWallets',
            to: 'checkingChain',
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
            to: 'checkingChain',
            on: 'walletAnnounced.next',
        },
        anotherWalletAnnounced: {
            from: 'choosingWallet',
            to: 'choosingWallet',
            on: 'walletAnnounced.next',
        },
        walletChosen: {
            from: 'choosingWallet',
            to: 'checkingChain',
            on: 'chooseWallet.next',
        },

        chainIsExpected: {
            from: 'checkingChain',
            to: 'onExpectedChain',
            on: 'chainFoundExpected.next',
        },
        chainIsOther: {
            from: 'checkingChain',
            to: 'onOtherChain',
            on: 'chainFoundOther.next',
        },
        walletDidNotAnswer: {
            from: 'checkingChain',
            to: 'walletNotResponding',
            on: 'chainCheckFailed.next',
        },
        chainCheckRetried: {
            from: 'walletNotResponding',
            to: 'checkingChain',
            on: 'retryDue.next',
        },

        chainChangedWhileCheckingChain: {
            from: 'checkingChain',
            to: 'checkingChain',
            on: 'walletChangedChain.next',
        },
        chainChangedWhileNotResponding: {
            from: 'walletNotResponding',
            to: 'checkingChain',
            on: 'walletChangedChain.next',
        },
        chainChangedOnExpectedChain: {
            from: 'onExpectedChain',
            to: 'checkingChain',
            on: 'walletChangedChain.next',
        },
        chainChangedOnOtherChain: {
            from: 'onOtherChain',
            to: 'checkingChain',
            on: 'walletChangedChain.next',
        },
        switchAccepted: {
            from: 'askingToSwitchChain',
            to: 'checkingChain',
            on: 'walletChangedChain.next',
        },
        chainChangedAfterDecline: {
            from: 'switchDeclined',
            to: 'checkingChain',
            on: 'walletChangedChain.next',
        },

        switchRequested: {
            from: 'onOtherChain',
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
            to: 'onOtherChain',
            on: 'switchRequestFailed.next',
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
        closedWhileCheckingChain: {
            from: 'checkingChain',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileWalletNotResponding: {
            from: 'walletNotResponding',
            to: 'closed',
            on: 'close.next',
        },
        closedOnExpectedChain: {
            from: 'onExpectedChain',
            to: 'closed',
            on: 'close.next',
        },
        closedOnOtherChain: {
            from: 'onOtherChain',
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
