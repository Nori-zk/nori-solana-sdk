import { define, type StateUnion } from '@yaw-rx/ystate';
import { sentSolanaTransactionOf } from './solanaTransaction.js';

const { nodes, edges } = sentSolanaTransactionOf('askingToSign');

/**
 * One Solana transaction sent through the user's Solana wallet, made for
 * its instructions and sent on a send request, then followed on the chain
 * until it ends (`sentSolanaTransactionOf`). The wallet's shared account
 * signs it and pays the fee. A send request is gated on the wallet being
 * ready.
 *
 * - `askingToSign`: reads a recent blockhash, and the wallet shows the
 *   transaction, signs it and sends it; one request per entry. Its
 *   outcomes: `sent` with the transaction's signature and
 *   `lastValidBlockHeight`, to `loading`; the user said no, to `declined`;
 *   the program failed it in preflight, to `refused` with the program's
 *   error; the wallet failing for any other reason, or dropping while
 *   asking, to `sendFailed` with the error.
 * - `declined` accepts a send request again, through the same gate.
 */
export const SolanaWalletTransactionGraph = define({
    nodes: {
        ...nodes,
        askingToSign: { instructions: nodes.ready.instructions },
        declined: { instructions: nodes.ready.instructions },
    },
    edges: {
        ...edges,
        sent: { from: 'askingToSign', to: 'loading', on: 'sent.next' },
        userDeclined: { from: 'askingToSign', to: 'declined', on: 'declined.next' },
        programRefused: { from: 'askingToSign', to: 'refused', on: 'refused.next' },
        walletRefused: { from: 'askingToSign', to: 'sendFailed', on: 'sendFailed.next' },
        walletLostWhileAsking: { from: 'askingToSign', to: 'sendFailed', on: 'walletLost.next' },
        sendAgainAfterDecline: { from: 'declined', to: 'askingToSign', on: 'send.next' },
        notReadyAfterDecline: { from: 'declined', to: 'notReadyToSend', on: 'send.error' },
        closedWhileAskingToSign: { from: 'askingToSign', to: 'closed', on: 'close.next' },
        closedAfterDecline: { from: 'declined', to: 'closed', on: 'close.next' },
    },
});

/** The Solana wallet transaction's state: a node of the graph and its data. */
export type SolanaWalletTransactionState = StateUnion<typeof SolanaWalletTransactionGraph.nodes>;
