import { type Address } from '@solana/kit';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { type Ethereum, forCalls, forLogs, type Solana } from '../rpc/connection/connections.js';
import {
    type EnqueuedProofRequest,
    type EnqueuedProofRequestsQuery,
    fetchEnqueuedProofRequests as fetchEnqueuedProofRequestsFrom,
} from '../rpc/eth/fetchEnqueuedProofRequests.js';
import {
    fetchProofRequestBatch as fetchProofRequestBatchFrom,
    type ProofRequestBatchEntry,
} from '../rpc/eth/fetchProofRequestBatch.js';
import { fetchProofQueueHead as fetchProofQueueHeadFrom } from '../rpc/eth/fetchProofQueueHead.js';
import { fetchBridgeState as fetchBridgeStateFrom } from '../rpc/solana/fetchBridgeState.js';
import { fetchProofQueueBatches as fetchProofQueueBatchesFrom } from '../rpc/solana/fetchProofQueueBatches.js';
import {
    fetchProofQueueBatchSummaries as fetchProofQueueBatchSummariesFrom,
    type ProofQueueBatchSummary,
} from '../rpc/solana/fetchProofQueueBatchSummaries.js';
import {
    findProofQueueBatch as findProofQueueBatchFrom,
    type FoundProofQueueBatch,
} from '../rpc/solana/findProofQueueBatch.js';
import { findProofQueueBatchesForRequests as findProofQueueBatchesForRequestsFrom } from '../rpc/solana/findProofQueueBatchesForRequests.js';

export { findProofQueueBatchPda } from '../rpc/solana/findProofQueueBatchPda.js';
export { ProofQueueBatchSearchError, SolanaAccountMismatchError } from '../rpc/solana/errors.js';
export type {
    EnqueuedProofRequest,
    EnqueuedProofRequestsQuery,
    FoundProofQueueBatch,
    ProofQueueBatchSummary,
    ProofRequestBatchEntry,
};

/**
 * The queue's head: how many proof requests have ever been enqueued.
 *
 * @param ethereum The Ethereum chain.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @returns The queue's head.
 */
export function getProofQueueHead(ethereum: Ethereum, proofQueueAddress: string): Promise<bigint> {
    return forCalls(ethereum, fetchProofQueueHeadFrom, proofQueueAddress);
}

/**
 * The proof requests enqueued over a block range.
 *
 * @param ethereum The Ethereum chain.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param query The block range, and optionally the submitting address and request id range.
 * @returns The requests, oldest first.
 */
export function getEnqueuedProofRequests(
    ethereum: Ethereum,
    proofQueueAddress: string,
    query: EnqueuedProofRequestsQuery
): Promise<EnqueuedProofRequest[]> {
    return forLogs(ethereum, fetchEnqueuedProofRequestsFrom, proofQueueAddress, query);
}

/**
 * Every request in one proof queue batch, as of the batch's output block.
 *
 * @param ethereum The Ethereum chain.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param inputQueueCursor Inclusive lower bound of the batch (queue request id).
 * @param outputQueueCursor Exclusive upper bound of the batch.
 * @param previousOutputBlockNumber The previous batch's output block.
 * @param outputBlockNumber The batch's output block.
 * @returns The batch's requests.
 */
export function getProofRequestBatch(
    ethereum: Ethereum,
    proofQueueAddress: string,
    inputQueueCursor: bigint,
    outputQueueCursor: bigint,
    previousOutputBlockNumber: number,
    outputBlockNumber: number
): Promise<ProofRequestBatchEntry[]> {
    return forLogs(ethereum, 
        fetchProofRequestBatchFrom,
        proofQueueAddress,
        inputQueueCursor,
        outputQueueCursor,
        previousOutputBlockNumber,
        outputBlockNumber
    );
}

/**
 * The Solana program's state account: queue cursor, batch count, latest proven head and root.
 *
 * @param solana The Solana chain.
 * @param programAddress The program (default: the Nori program).
 * @returns The state account.
 */
export async function getBridgeState(solana: Solana, programAddress: Address = TOKEN_PROGRAM_ADDRESS) {
    return fetchBridgeStateFrom(await solana.http.ready(), programAddress);
}

/**
 * Proof queue batches on Solana, by index.
 *
 * @param solana The Solana chain.
 * @param indices The batches' indices.
 * @param programAddress The program (default: the Nori program).
 * @returns The batches, in the order asked.
 */
export async function getProofQueueBatches(
    solana: Solana,
    indices: bigint[],
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
) {
    return fetchProofQueueBatchesFrom(await solana.http.ready(), indices, programAddress);
}

/**
 * Proof queue batches on Solana, by index, each with the output block of the batch before it.
 *
 * @param solana The Solana chain.
 * @param indices The batches' indices.
 * @param programAddress The program (default: the Nori program).
 * @returns The batch summaries, in the order asked.
 */
export async function getProofQueueBatchSummaries(
    solana: Solana,
    indices: bigint[],
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<ProofQueueBatchSummary[]> {
    return fetchProofQueueBatchSummariesFrom(await solana.http.ready(), indices, programAddress);
}

/**
 * The proof queue batch that covers a request.
 *
 * @param solana The Solana chain.
 * @param requestId The request's id.
 * @param batchCount How many batches the program holds.
 * @param programAddress The program (default: the Nori program).
 * @returns The batch and its index.
 */
export async function getProofQueueBatch(
    solana: Solana,
    requestId: bigint,
    batchCount: bigint,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<FoundProofQueueBatch> {
    return findProofQueueBatchFrom(await solana.http.ready(), requestId, batchCount, programAddress);
}

/**
 * The proof queue batches that cover several requests.
 *
 * @param solana The Solana chain.
 * @param requestIds The requests' ids.
 * @param batchCount How many batches the program holds.
 * @param programAddress The program (default: the Nori program).
 * @returns Each request's batch, by request id.
 */
export async function getProofQueueBatchesForRequests(
    solana: Solana,
    requestIds: bigint[],
    batchCount: bigint,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<Map<bigint, FoundProofQueueBatch>> {
    return findProofQueueBatchesForRequestsFrom(
        await solana.http.ready(),
        requestIds,
        batchCount,
        programAddress
    );
}
