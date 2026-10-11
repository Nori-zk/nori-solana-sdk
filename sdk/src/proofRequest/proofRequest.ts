import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { transactionFollowOf } from '../transaction/transactionFollow.js';
import type {
    ProofAvailableProofRequestSnapshot,
    UndeterminedProofRequestSnapshot,
    UnprocessedProofRequestSnapshot,
} from './getProofRequestStateSnapshot.js';
import { ProofRequestState } from './types.js';

const { nodes, edges } = readThroughConnectionsOf({
    proofRequestTxHash: '',
    transaction: transactionFollowOf(''),
    snapshot: { state: ProofRequestState.Undetermined } as
        | UndeterminedProofRequestSnapshot
        | UnprocessedProofRequestSnapshot,
});

/**
 * Follows one Ethereum proof request until a committed proof queue batch on
 * Solana covers it: its snapshot, read through the connections
 * (`readThroughConnectionsOf`), until the proof is available.
 * `proofRequestTxHash`, the transaction that enqueued the request, is its
 * starting data, carried by every node.
 *
 * - `loading` looks the request up from the transaction that enqueued it.
 *   A transaction that is not mined yet is not a failure: the snapshot
 *   stays `undetermined`, and `transaction` holds the transaction as the
 *   last read found it (`transactionFollow$`).
 * - `current` holds the snapshot while no batch covers the request. An
 *   `undetermined` one is looked up again, and an `unprocessed` one read
 *   again (`refreshing`), every poll interval or recheck signal.
 * - A read that finds a committed batch covering the request moves to
 *   `proofAvailable` (`discoveredProofAvailable` from `loading`,
 *   `proofRequestCommitted` from `refreshing`), which holds the snapshot
 *   with that batch. It is terminal: batches are append-only, so the request
 *   stays covered.
 * - The enqueuing transaction never mined: `transactionReplaced` once its
 *   sender's mined nonce moved past its own, `transactionDropped` once the
 *   node has not known it for the blocks allowed. Both are terminal: the
 *   request was never enqueued.
 * - `closed` is terminal too.
 */
export const ProofRequestStateGraph = define({
    nodes: {
        ...nodes,
        proofAvailable: {
            proofRequestTxHash: '',
            snapshot: {
                state: ProofRequestState.ProofAvailable,
                requestId: 0n,
                requestBlockNumber: 0n,
                queueCursor: 0n,
                proofQueueBatchIndex: 0n,
                proofQueueBatchAddress: '',
                root: '',
                inputQueueCursor: 0n,
                outputQueueCursor: 0n,
                outputBlockNumber: 0n,
                previousOutputBlockNumber: -1n, // sentinel: no previous proof queue batch (first-ever batch)
                indexInBatch: 0n,
            } as ProofAvailableProofRequestSnapshot,
        },
        transactionReplaced: { proofRequestTxHash: '', transaction: transactionFollowOf('') },
        transactionDropped: { proofRequestTxHash: '', transaction: transactionFollowOf('') },
    },
    edges: {
        ...edges,
        discoveredProofAvailable: { from: 'loading', to: 'proofAvailable', on: 'proven.next' },
        proofRequestCommitted: { from: 'refreshing', to: 'proofAvailable', on: 'proven.next' },
        replacedOnFirstRead: { from: 'loading', to: 'transactionReplaced', on: 'transactionReplaced.next' },
        replacedOnRefresh: { from: 'refreshing', to: 'transactionReplaced', on: 'transactionReplaced.next' },
        droppedOnFirstRead: { from: 'loading', to: 'transactionDropped', on: 'transactionDropped.next' },
        droppedOnRefresh: { from: 'refreshing', to: 'transactionDropped', on: 'transactionDropped.next' },
    },
});

export type ProofRequestStateNodeUnion = StateUnion<typeof ProofRequestStateGraph.nodes>;
