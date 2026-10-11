import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../connection/readThroughConnections.js';

/** What every node carries: the account the wallet shares, and why the last ask for one failed. */
const shared = readThroughConnectionsOf({
    account: undefined as string | undefined,
    lastShareError: '',
});

/**
 * The account the user's wallet shares with the app, read through the
 * wallet alone (`readThroughConnectionsOf` with `needs` naming the wallet):
 * `eth_accounts`, which never prompts the user, read again on the wallet's
 * `accountsChanged`. `account` is `undefined` while none is shared; a wallet
 * that is not `ready` is `waitingForConnectionWhile…`, waiting on it.
 *
 * - `shareAccount()` asks the user to share one: from `current` while no
 *   account is shared, and from `shareDeclined`, as each click is a fresh
 *   ask. `askingToShareAccount` sends one `eth_requestAccounts`.
 * - Accepting arrives at `current` with the account. Declining (the
 *   wallet's user-rejected error, code 4001) moves to `shareDeclined`. Any
 *   other failure (e.g. -32002, a request already pending) goes back to
 *   `current` with the wallet's error in `lastShareError`, for logs.
 * - `accountsChanged` while asking or after declining reads again, so
 *   connecting from inside the wallet moves it; the wallet leaving `ready`
 *   waits for it.
 * - `closed` is terminal.
 */
export const WalletAccountGraph = define({
    nodes: {
        ...shared.nodes,
        askingToShareAccount: { ...shared.nodes.current },
        shareDeclined: { ...shared.nodes.current },
    },
    edges: {
        ...shared.edges,
        shareRequested: { from: 'current', to: 'askingToShareAccount', on: 'shareAccount.next' },
        askedAgain: { from: 'shareDeclined', to: 'askingToShareAccount', on: 'shareAccount.next' },

        shareAccepted: { from: 'askingToShareAccount', to: 'current', on: 'shareAccepted.next' },
        userDeclinedShare: { from: 'askingToShareAccount', to: 'shareDeclined', on: 'shareDeclined.next' },
        shareRequestFailed: { from: 'askingToShareAccount', to: 'current', on: 'shareRequestFailed.next' },
        accountsChangedWhileAsking: {
            from: 'askingToShareAccount',
            to: 'refreshing',
            on: 'accountsChanged.next',
        },
        walletLeftReadyWhileAsking: {
            from: 'askingToShareAccount',
            to: 'waitingForConnectionWhileRefreshing',
            on: 'walletLeftReady.next',
        },
        closedWhileAsking: { from: 'askingToShareAccount', to: 'closed', on: 'close.next' },

        accountsChangedAfterDecline: { from: 'shareDeclined', to: 'refreshing', on: 'accountsChanged.next' },
        walletLeftReadyAfterDecline: {
            from: 'shareDeclined',
            to: 'waitingForConnectionWhileRefreshing',
            on: 'walletLeftReady.next',
        },
        closedAfterDecline: { from: 'shareDeclined', to: 'closed', on: 'close.next' },
    },
});

/** The wallet account's state: a node of the graph and its data. */
export type WalletAccountState = StateUnion<typeof WalletAccountGraph.nodes>;
