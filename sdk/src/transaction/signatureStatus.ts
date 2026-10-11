import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { type SolanaSignatureStatus } from '../rpc/solana/signatureStatus.js';

const { nodes, edges } = readThroughConnectionsOf({
    signature: '',
    lastValidBlockHeight: undefined as number | undefined,
    status: undefined as SolanaSignatureStatus | undefined,
    blockHeight: undefined as number | undefined,
});

/**
 * A Solana transaction's status, read through the Solana connection
 * (`readThroughConnectionsOf`): read at once, then again when the
 * transaction reaches the commitment asked for (`signatureSubscribe`), and,
 * with a `lastValidBlockHeight`, every poll interval meanwhile. `signature`,
 * the transaction it is made for, and `lastValidBlockHeight`, the block
 * height after which its blockhash no longer lands, are its starting data.
 * `status` is `undefined` while the cluster does not know the transaction;
 * a status that reached the commitment is never read again. `blockHeight`
 * is the finalized block height the last read saw, read while the
 * transaction is unknown and has a `lastValidBlockHeight`.
 *
 * - `expired`: the cluster never knew the transaction, and the finalized
 *   block height passed `lastValidBlockHeight`, so it never will. Terminal:
 *   sending it again is a new transaction.
 * - `closed` is terminal too.
 */
export const SignatureStatusGraph = define({
    nodes: {
        ...nodes,
        expired: { signature: '', lastValidBlockHeight: 0, blockHeight: 0 },
    },
    edges: {
        ...edges,
        expiredOnFirstRead: { from: 'loading', to: 'expired', on: 'blockhashExpired.next' },
        expiredOnRefresh: { from: 'refreshing', to: 'expired', on: 'blockhashExpired.next' },
    },
});

/** The signature status's state: a node of the graph and its data. */
export type SignatureStatusState = StateUnion<typeof SignatureStatusGraph.nodes>;
