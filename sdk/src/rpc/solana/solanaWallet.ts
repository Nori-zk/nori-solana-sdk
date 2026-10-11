import { define, type StateUnion } from '@yaw-rx/ystate';

/** A Solana wallet the browser offers, as it registers itself (Wallet Standard). */
export interface SolanaWalletInfo {
    /** The wallet's name, unique among the registered wallets, for display. */
    name: string;
    /** The wallet's icon as a data URI, for display. */
    icon: string;
}

const noWallet: SolanaWalletInfo = { name: '', icon: '' };

/**
 * The user's Solana wallet, which signs and sends the app's Solana
 * transactions (reads go through Solana's RPC): whether there is one,
 * which one, whether it serves the cluster's chain, and whether the user
 * has shared an account with the app. Every node that keeps it from
 * signing carries what the app needs to tell the user why, and none is a
 * dead end.
 *
 * - `lookingForWallets` asks wallets to register themselves (Wallet
 *   Standard `wallet-standard:app-ready`), for a short window. Its three
 *   outcome edges race on that one search.
 * - `noWalletFound`: there is no Solana wallet; the app can ask the user to
 *   install one. A wallet registering itself later (installed or enabled)
 *   moves straight to `connecting`.
 * - `choosingWallet`: several wallets are installed. The app lists them
 *   and calls `chooseWallet(name)`; wallets registering meanwhile join the
 *   list.
 * - `connecting` asks the wallet, without a prompt, for the accounts it
 *   already shares with the app (`standard:connect`, silent). An account
 *   moves to `ready`; none to `notConnected`. A wallet that does not serve
 *   the cluster's chain, or cannot sign and send a transaction
 *   (`solana:signAndSendTransaction`), goes to `unsupportedWallet`, with
 *   the reason.
 * - `notConnected`: no account is shared. `connect()` moves to
 *   `askingToConnect`, whose `$` sends one `standard:connect`: the user
 *   sharing an account moves to `ready`; saying no (the wallet's
 *   user-rejected error, code 4001) to `connectDeclined`, from where
 *   `connect()` asks again on the user's next click; any other failure back
 *   to `notConnected`, with the reason in `lastConnectError`. The user
 *   connecting from inside the wallet moves to `ready` too.
 * - `ready`: `account` is the shared account, which signs. The user
 *   switching account in the wallet is the `accountChanged` self-loop;
 *   the wallet no longer sharing one, or `disconnect()`, moves to
 *   `notConnected`.
 * - `unsupportedWallet`: the wallet changing what it serves checks again.
 * - `closed` ends the machine from any node and completes its streams.
 */
export const SolanaWalletGraph = define({
    nodes: {
        lookingForWallets: {},
        noWalletFound: {},
        choosingWallet: { wallets: [] as SolanaWalletInfo[] },
        connecting: { wallet: noWallet },
        notConnected: { wallet: noWallet, lastConnectError: '' },
        askingToConnect: { wallet: noWallet },
        connectDeclined: { wallet: noWallet },
        unsupportedWallet: { wallet: noWallet, reason: '' },
        ready: { wallet: noWallet, account: '' },
        closed: {},
    },
    edges: {
        oneWalletFound: { from: 'lookingForWallets', to: 'connecting', on: 'foundOneWallet.next' },
        severalWalletsFound: { from: 'lookingForWallets', to: 'choosingWallet', on: 'foundSeveralWallets.next' },
        noWalletFound: { from: 'lookingForWallets', to: 'noWalletFound', on: 'foundNoWallet.next' },
        walletInstalled: { from: 'noWalletFound', to: 'connecting', on: 'walletRegistered.next' },
        anotherWalletRegistered: { from: 'choosingWallet', to: 'choosingWallet', on: 'walletRegistered.next' },
        walletChosen: { from: 'choosingWallet', to: 'connecting', on: 'chooseWallet.next' },

        alreadyConnected: { from: 'connecting', to: 'ready', on: 'sharedAccountFound.next' },
        notYetConnected: { from: 'connecting', to: 'notConnected', on: 'noSharedAccount.next' },
        walletFoundUnsupported: { from: 'connecting', to: 'unsupportedWallet', on: 'walletUnsupported.next' },
        walletChangedWhileUnsupported: { from: 'unsupportedWallet', to: 'connecting', on: 'walletChanged.next' },

        connectRequested: { from: 'notConnected', to: 'askingToConnect', on: 'connect.next' },
        connectAgainAfterDecline: { from: 'connectDeclined', to: 'askingToConnect', on: 'connect.next' },
        connected: { from: 'askingToConnect', to: 'ready', on: 'accountShared.next' },
        userDeclinedConnect: { from: 'askingToConnect', to: 'connectDeclined', on: 'connectDeclined.next' },
        connectFailed: { from: 'askingToConnect', to: 'notConnected', on: 'connectFailed.next' },
        connectedInWallet: { from: 'notConnected', to: 'ready', on: 'accountsChanged.next' },
        connectedInWalletAfterDecline: { from: 'connectDeclined', to: 'ready', on: 'accountsChanged.next' },

        accountChanged: { from: 'ready', to: 'ready', on: 'accountsChanged.next' },
        accountUnshared: { from: 'ready', to: 'notConnected', on: 'accountsUnshared.next' },
        disconnected: { from: 'ready', to: 'notConnected', on: 'disconnect.next' },

        closedWhileLookingForWallets: { from: 'lookingForWallets', to: 'closed', on: 'close.next' },
        closedWithNoWallet: { from: 'noWalletFound', to: 'closed', on: 'close.next' },
        closedWhileChoosingWallet: { from: 'choosingWallet', to: 'closed', on: 'close.next' },
        closedWhileConnecting: { from: 'connecting', to: 'closed', on: 'close.next' },
        closedWhileNotConnected: { from: 'notConnected', to: 'closed', on: 'close.next' },
        closedWhileAskingToConnect: { from: 'askingToConnect', to: 'closed', on: 'close.next' },
        closedAfterConnectDeclined: { from: 'connectDeclined', to: 'closed', on: 'close.next' },
        closedWhileUnsupported: { from: 'unsupportedWallet', to: 'closed', on: 'close.next' },
        closedWhileReady: { from: 'ready', to: 'closed', on: 'close.next' },
    },
});

/** The Solana wallet's state: a node of the graph and its data. */
export type SolanaWalletState = StateUnion<typeof SolanaWalletGraph.nodes>;
