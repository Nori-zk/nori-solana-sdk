import { type Nori } from '../rpc/connection/connections.js';
import {
    type ProofRequestsByTargetPage,
    type ProofRequestsByTargetQuery,
} from '../rpc/eth/proofRequestsByTarget.js';
import { type ProofRequest } from '../rpc/eth/proofRequest.js';
import {
    type ProofRequestHistoryAddresses,
    type ProofRequestHistoryPage,
} from '../proofRequest/fetchProofRequestHistory.js';
import {
    type ProofAvailableProofRequestSnapshot,
    type ProofRequestStateSnapshot,
    type ProofRequestStateSnapshotRequest,
    type ProofRequestWitnesses,
    type UndeterminedProofRequestSnapshot,
    type UnprocessedProofRequestSnapshot,
    type VerifiedRequestWitness,
} from '../proofRequest/getProofRequestStateSnapshot.js';
import { createUnprocessedProofRequestStateMachine as createUnprocessedProofRequestStateMachineFrom } from '../proofRequest/unprocessed.impl.js';
import { type RequestWitness } from '@nori-zk/ethereum-solana-proof-queue-utils-glam';

// The machines.
export {
    ProofRequestStateGraph,
    type ProofRequestStateNodeUnion,
} from '../proofRequest/proofRequest.js';
export {
    createProofRequestStateMachine,
    type FollowedProofRequest,
} from '../proofRequest/proofRequest.impl.js';
export {
    ProofRequestHistoryGraph,
    type ProofRequestHistoryState,
} from '../proofRequest/proofRequestHistory.js';
export {
    createProofRequestHistoryMachine,
    type ProofRequestHistoryQuery,
} from '../proofRequest/proofRequestHistory.impl.js';
export {
    ProofRequestWitnessGraph,
    type ProofRequestWitnessState,
} from '../proofRequest/proofRequestWitness.js';
export { createProofRequestWitnessMachine } from '../proofRequest/proofRequestWitness.impl.js';
export {
    LatestProofRequestsGraph,
    type LatestProofRequestsState,
} from '../proofRequest/latestProofRequests.js';
export {
    createLatestProofRequestsMachine,
    type LatestProofRequestsQuery,
} from '../proofRequest/latestProofRequests.impl.js';
export {
    ProofRequestCountsGraph,
    type ProofRequestCountsState,
} from '../proofRequest/proofRequestCounts.js';
export {
    createProofRequestCountsMachine,
    type ProofRequestCountsQuery,
} from '../proofRequest/proofRequestCounts.impl.js';
export {
    ProofRequestBlockTimeGraph,
    type ProofRequestBlockTimeState,
} from '../proofRequest/proofRequestBlockTime.js';
export { createProofRequestBlockTimeMachine } from '../proofRequest/proofRequestBlockTime.impl.js';
export {
    ProofRequestOfTransactionGraph,
    type ProofRequestOfTransactionState,
} from '../proofRequest/proofRequestOfTransaction.js';
export { createProofRequestOfTransactionMachine } from '../proofRequest/proofRequestOfTransaction.impl.js';
export type { ProofRequestCounts } from '../proofRequest/fetchProofRequestHistory.js';
export {
    UnprocessedProofRequestStateGraph,
    type UnprocessedProofRequestStateNodeUnion,
} from '../proofRequest/unprocessed.js';
export { BridgeProofRequestProcessingStatus } from '../rpc/nori/proofRequest.js';
export {
    sortWaitingProofRequests,
    type NoriJob,
    type WaitingProofRequests,
} from '../proofRequest/waitingProofRequests.js';

// Types and errors.
export { ProofRequestState } from '../proofRequest/types.js';
export { ProofRequestWitnessRootMismatchError } from '../proofRequest/getProofRequestStateSnapshot.js';
export {
    MalformedProofRequestError,
    ProofRequestTransactionNotMinedError,
} from '../rpc/eth/errors.js';
export type {
    ProofRequestHistoryCursor,
    ProofRequestHistoryOrder,
} from '../rpc/eth/proofRequestsByTarget.js';
export type { ProofRequestHistoryEntry } from '../proofRequest/fetchProofRequestHistory.js';
export type { RequestLeaf } from '@nori-zk/ethereum-solana-proof-queue-utils-glam';
export type {
    ProofAvailableProofRequestSnapshot,
    ProofRequest,
    ProofRequestHistoryAddresses,
    ProofRequestHistoryPage,
    ProofRequestsByTargetPage,
    ProofRequestsByTargetQuery,
    ProofRequestStateSnapshot,
    ProofRequestStateSnapshotRequest,
    ProofRequestWitnesses,
    RequestWitness,
    UndeterminedProofRequestSnapshot,
    UnprocessedProofRequestSnapshot,
    VerifiedRequestWitness,
};

/**
 * Follows an unprocessed proof request through the bridge's stages, from Nori.
 *
 * @param proofRequestBlockNumber The block the proof request was enqueued in.
 * @param nori Nori.
 * @returns The implemented YState machine.
 */
export function createUnprocessedProofRequestStateMachine(
    proofRequestBlockNumber: number,
    nori: Nori
) {
    return createUnprocessedProofRequestStateMachineFrom(proofRequestBlockNumber, nori.websocket);
}
