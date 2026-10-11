import { define, type StateUnion } from '@yaw-rx/ystate';
import { sentTransactionOf } from './sentTransaction.js';

const { nodes, edges } = sentTransactionOf('sending');

/**
 * One Ethereum transaction the app sends with a signer of its own (a key it
 * holds), made for one call and sent on a send request, then followed on the
 * chain until it ends (`sentTransactionOf`). A send request is gated on
 * Ethereum's connection.
 *
 * - `sending`: the signer signs and sends it; one send per entry, with its
 *   pinned nonce after a drop. Its outcomes: `sent` with the transaction's
 *   hash, sender and nonce, to `loading`; the contract reverted it at gas
 *   estimation, to `refused` with the revert's name; any other failure (the
 *   signer's node unreachable, a nonce clash), to `sendFailed` with the
 *   error.
 */
export const SignerTransactionGraph = define({
    nodes: {
        ...nodes,
        sending: { transaction: nodes.ready.transaction },
    },
    edges: {
        ...edges,
        sent: { from: 'sending', to: 'loading', on: 'sent.next' },
        contractRefused: { from: 'sending', to: 'refused', on: 'refused.next' },
        failedToSend: { from: 'sending', to: 'sendFailed', on: 'sendFailed.next' },
        closedWhileSending: { from: 'sending', to: 'closed', on: 'close.next' },
    },
});

/** The signer transaction's state: a node of the graph and its data. */
export type SignerTransactionState = StateUnion<typeof SignerTransactionGraph.nodes>;
