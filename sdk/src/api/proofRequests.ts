import {
    type Ethereum,
    forCalls,
    forLogs,
    type Nori,
    type Solana,
} from '../rpc/connection/connections.js';
import { proofRequestAge as proofRequestAgeFrom } from '../rpc/eth/proofRequestAge.js';
import {
    fetchProofRequestsByTarget as fetchProofRequestsByTargetFrom,
    type ProofRequestsByTargetPage,
    type ProofRequestsByTargetQuery,
} from '../rpc/eth/fetchProofRequestsByTarget.js';
import { findRequestIdByTxHash as findRequestIdByTxHashFrom, type ProofRequest } from '../rpc/eth/fetchProofRequest.js';
import {
    fetchProofRequestCountsByTarget as fetchProofRequestCountsByTargetFrom,
    fetchProofRequestHistoryPage as fetchProofRequestHistoryPageFrom,
    type ProofRequestCounts,
    type ProofRequestHistoryAddresses,
    type ProofRequestHistoryPage,
} from '../proofRequest/fetchProofRequestHistory.js';
import {
    fetchProofRequestWitness as fetchProofRequestWitnessFrom,
    getProofRequestStateSnapshot as getProofRequestStateSnapshotFrom,
    type ProofRequestStateSnapshot,
    type ProofRequestStateSnapshotRequest,
} from '../proofRequest/getProofRequestStateSnapshot.js';
import { type ProofRequestStateGraph } from '../proofRequest/proofRequest.js';
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
    type ReadRetryBackoff,
} from '../proofRequest/proofRequestHistory.impl.js';
export {
    LatestProofRequestsGraph,
    type LatestProofRequestsState,
} from '../proofRequest/latestProofRequests.js';
export {
    createLatestProofRequestsMachine,
    type LatestProofRequestsQuery,
} from '../proofRequest/latestProofRequests.impl.js';
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
} from '../rpc/eth/fetchProofRequestsByTarget.js';
export type { ProofRequestHistoryEntry } from '../proofRequest/fetchProofRequestHistory.js';
export type { RequestLeaf } from '@nori-zk/ethereum-solana-proof-queue-utils-glam';
export type {
    ProofRequest,
    ProofRequestCounts,
    ProofRequestHistoryAddresses,
    ProofRequestHistoryPage,
    ProofRequestsByTargetPage,
    ProofRequestsByTargetQuery,
    ProofRequestStateSnapshot,
    ProofRequestStateSnapshotRequest,
    RequestWitness,
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

/**
 * Where a proof request is, read once from Ethereum and Solana.
 *
 * @param chains The Ethereum and Solana chains.
 * @param request The addresses and the transaction that enqueued the request.
 * @returns The unprocessed or proof available state data.
 */
export async function getProofRequestStateSnapshot(
    { ethereum, solana }: { ethereum: Ethereum; solana: Solana },
    request: ProofRequestStateSnapshotRequest
): Promise<ProofRequestStateSnapshot> {
    const rpc = await solana.http.ready();
    return forCalls(ethereum, getProofRequestStateSnapshotFrom, rpc, request);
}

/**
 * The witness of a proof request whose proof is available.
 *
 * @param ethereum The Ethereum chain.
 * @param proofAvailable The proof available state data.
 * @param proofQueueAddress The Ethereum `NoriProofRequestQueue` address.
 * @returns The request's leaf, its bottom-up path and the batch root.
 */
export function getProofRequestWitness(
    ethereum: Ethereum,
    proofAvailable: (typeof ProofRequestStateGraph.nodes)['proofAvailable'],
    proofQueueAddress: string
): Promise<RequestWitness> {
    return forLogs(ethereum, fetchProofRequestWitnessFrom, proofAvailable, proofQueueAddress);
}

/**
 * One page of a submitting address's proof requests, each with where it is now.
 *
 * @param chains The Ethereum and Solana chains.
 * @param addresses The queue and program addresses.
 * @param query The submitting address, block range, order, page size and cursor.
 * @returns The page's entries, its continuation cursor, and whether the range is exhausted.
 */
export async function getProofRequestHistoryPage(
    { ethereum, solana }: { ethereum: Ethereum; solana: Solana },
    addresses: ProofRequestHistoryAddresses,
    query: ProofRequestsByTargetQuery
): Promise<ProofRequestHistoryPage> {
    const rpc = await solana.http.ready();
    return forLogs(ethereum, fetchProofRequestHistoryPageFrom, rpc, addresses, query);
}

/**
 * How many proof requests a submitting address made over a block range, and
 * how many have a proof available.
 *
 * @param chains The Ethereum and Solana chains.
 * @param addresses The queue and program addresses.
 * @param query The submitting address and block range.
 * @returns The total, proven and unprocessed counts.
 */
export async function getProofRequestCountsByTarget(
    { ethereum, solana }: { ethereum: Ethereum; solana: Solana },
    addresses: ProofRequestHistoryAddresses,
    query: Pick<ProofRequestsByTargetQuery, 'target' | 'fromBlock' | 'toBlock' | 'maxBlockRangePerQuery'>
): Promise<ProofRequestCounts> {
    const rpc = await solana.http.ready();
    return forLogs(ethereum, fetchProofRequestCountsByTargetFrom, rpc, addresses, query);
}

/**
 * One page of a submitting address's proof requests.
 *
 * @param ethereum The Ethereum chain.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param query The submitting address, block range, order, page size and cursor.
 * @returns The page, its continuation cursor, and whether the range is exhausted.
 */
export function getProofRequestsByTarget(
    ethereum: Ethereum,
    proofQueueAddress: string,
    query: ProofRequestsByTargetQuery
): Promise<ProofRequestsByTargetPage> {
    return forLogs(ethereum, fetchProofRequestsByTargetFrom, proofQueueAddress, query);
}

/**
 * The proof request a transaction enqueued.
 *
 * @param ethereum The Ethereum chain.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param proofRequestTxHash The transaction that enqueued it.
 * @returns The request's id and block.
 */
export function getRequestIdByTxHash(
    ethereum: Ethereum,
    proofQueueAddress: string,
    proofRequestTxHash: string
): Promise<ProofRequest> {
    return forCalls(ethereum, findRequestIdByTxHashFrom, proofQueueAddress, proofRequestTxHash);
}

/**
 * How long ago a proof request's block was mined, in seconds.
 *
 * @param ethereum The Ethereum chain.
 * @param blockNumber The block the request was enqueued in.
 * @returns Its age in seconds.
 */
export function getProofRequestAge(ethereum: Ethereum, blockNumber: bigint): Promise<number> {
    return forCalls(ethereum, proofRequestAgeFrom, blockNumber);
}
