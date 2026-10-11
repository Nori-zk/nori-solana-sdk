import { define, type StateUnion } from '@yaw-rx/ystate';
import { sentSolanaTransactionOf } from './solanaTransaction.js';

const { nodes, edges } = sentSolanaTransactionOf('sending');

/**
 * One Solana transaction the app sends with a signer of its own (a key it
 * holds), made for its instructions and sent on a send request, then
 * followed on the chain until it ends (`sentSolanaTransactionOf`). A send
 * request is gated on Solana's connection.
 *
 * - `sending`: reads a recent blockhash, signs the transaction and sends it;
 *   one send per entry. Its outcomes: `sent` with the transaction's
 *   signature and `lastValidBlockHeight`, to `loading` (a send that got no
 *   answer too, as it may have landed); the program failed it in preflight,
 *   to `refused` with the program's error; any other failure, to
 *   `sendFailed` with the error.
 */
export const SolanaSignerTransactionGraph = define({
    nodes: {
        ...nodes,
        sending: { instructions: nodes.ready.instructions },
    },
    edges: {
        ...edges,
        sent: { from: 'sending', to: 'loading', on: 'sent.next' },
        programRefused: { from: 'sending', to: 'refused', on: 'refused.next' },
        failedToSend: { from: 'sending', to: 'sendFailed', on: 'sendFailed.next' },
        closedWhileSending: { from: 'sending', to: 'closed', on: 'close.next' },
    },
});

/** The Solana signer transaction's state: a node of the graph and its data. */
export type SolanaSignerTransactionState = StateUnion<typeof SolanaSignerTransactionGraph.nodes>;
