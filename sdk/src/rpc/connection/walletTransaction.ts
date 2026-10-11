import { define, type StateUnion } from '@yaw-rx/ystate';
import { sentTransactionOf } from '../../transaction/sentTransaction.js';

const { nodes, edges } = sentTransactionOf('askingToSign');

/**
 * One Ethereum transaction sent through the user's wallet, made for one
 * call and sent on a send request, then followed on the chain until it ends
 * (`sentTransactionOf`). A send request is gated on the wallet being ready.
 *
 * - `askingToSign`: the wallet shows the transaction; one request per
 *   entry. Its outcomes: `sent` with the transaction's hash, sender and
 *   nonce, to `loading`; the user said no, to `declined`; the contract
 *   reverted it at gas estimation, to `refused` with the revert's name; the
 *   wallet failing for any other reason, or dropping while asking, to
 *   `sendFailed` with the error.
 * - `declined` accepts a send request again, through the same gate.
 * - The wallet picks the nonce itself, so a send again after a drop cannot
 *   pin it; only the blocks the node has not known it for tell a drop.
 */
export const WalletTransactionGraph = define({
    nodes: {
        ...nodes,
        askingToSign: { transaction: nodes.ready.transaction },
        declined: { transaction: nodes.ready.transaction },
    },
    edges: {
        ...edges,
        sent: { from: 'askingToSign', to: 'loading', on: 'sent.next' },
        userDeclined: { from: 'askingToSign', to: 'declined', on: 'declined.next' },
        contractRefused: { from: 'askingToSign', to: 'refused', on: 'refused.next' },
        walletRefused: { from: 'askingToSign', to: 'sendFailed', on: 'sendFailed.next' },
        walletLostWhileAsking: { from: 'askingToSign', to: 'sendFailed', on: 'walletLost.next' },
        sendAgainAfterDecline: { from: 'declined', to: 'askingToSign', on: 'send.next' },
        notReadyAfterDecline: { from: 'declined', to: 'notReadyToSend', on: 'send.error' },
        closedWhileAskingToSign: { from: 'askingToSign', to: 'closed', on: 'close.next' },
        closedAfterDecline: { from: 'declined', to: 'closed', on: 'close.next' },
    },
});

/** The wallet transaction's state: a node of the graph and its data. */
export type WalletTransactionState = StateUnion<typeof WalletTransactionGraph.nodes>;
