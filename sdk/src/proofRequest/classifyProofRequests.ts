import {
    type Address,
    type GetAccountInfoApi,
    type GetMultipleAccountsApi,
    type ReadonlyUint8Array,
    type Rpc,
} from '@solana/kit';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { ProofQueueBatchSearchError } from '../rpc/solana/errors.js';
import { fetchBridgeState } from '../rpc/solana/fetchBridgeState.js';
import { fetchProofQueueBatches } from '../rpc/solana/fetchProofQueueBatches.js';
import { findProofQueueBatchesForRequests } from '../rpc/solana/findProofQueueBatchesForRequests.js';
import { findProofQueueBatchPda } from '../rpc/solana/findProofQueueBatchPda.js';
import { ProofRequestState } from './types.js';
import type { ProofRequestStateSnapshot } from './getProofRequestStateSnapshot.js';

/** A proof request's queue id and the Ethereum block that enqueued it. */
export interface QueuedProofRequest {
    requestId: bigint;
    requestBlockNumber: bigint;
}

function toHex(bytes: ReadonlyUint8Array): string {
    return `0x${Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;
}

/**
 * Decides where each proof request is from one read of the bridge state:
 * ids at or beyond its queue cursor are `unprocessed`, the rest are
 * `proofAvailable` with the committed batch that covers them. Batches for
 * the proven ids are found in one shared search, and each distinct batch's
 * predecessor is read once for its output block.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param queuedRequests The proof requests to classify.
 * @param programAddress The Nori Solana bridge program address.
 * @returns One snapshot per queued request, in the order given.
 */
export async function classifyProofRequests(
    rpc: Rpc<GetAccountInfoApi & GetMultipleAccountsApi>,
    queuedRequests: QueuedProofRequest[],
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<ProofRequestStateSnapshot[]> {
    const { queueCursor, proofQueueBatchCount } = await fetchBridgeState(
        rpc,
        programAddress
    );

    const provenIds = queuedRequests
        .filter(({ requestId }) => requestId < queueCursor)
        .map(({ requestId }) => requestId);
    const found = await findProofQueueBatchesForRequests(
        rpc,
        provenIds,
        proofQueueBatchCount,
        programAddress
    );

    const batchIndices = [
        ...new Set([...found.values()].map((f) => f.proofQueueBatchIndex)),
    ];
    const previousIndices = batchIndices
        .filter((index) => index > 0n)
        .map((index) => index - 1n);
    const previousBatches = await fetchProofQueueBatches(
        rpc,
        previousIndices,
        programAddress
    );
    const previousOutputBlockNumbers = new Map(
        previousIndices.map((index, i) => [
            index + 1n,
            previousBatches[i].outputBlockNumber,
        ])
    );
    const batchAddresses = new Map(
        await Promise.all(
            batchIndices.map(
                async (index) =>
                    [
                        index,
                        (
                            await findProofQueueBatchPda(index, programAddress)
                        )[0],
                    ] as const
            )
        )
    );

    return queuedRequests.map(
        ({ requestId, requestBlockNumber }): ProofRequestStateSnapshot => {
            if (requestId >= queueCursor) {
                return {
                    state: ProofRequestState.Unprocessed,
                    requestId,
                    requestBlockNumber,
                    queueCursor,
                    proofQueueBatchCount,
                };
            }
            const covering = found.get(requestId);
            const proofQueueBatchAddress =
                covering && batchAddresses.get(covering.proofQueueBatchIndex);
            if (!covering || !proofQueueBatchAddress) {
                throw new ProofQueueBatchSearchError(
                    requestId,
                    `No committed proof queue batch covers request ${requestId}.`
                );
            }
            const { proofQueueBatchIndex, proofQueueBatch } = covering;
            return {
                state: ProofRequestState.ProofAvailable,
                requestId,
                requestBlockNumber,
                queueCursor,
                proofQueueBatchIndex,
                proofQueueBatchAddress,
                root: toHex(proofQueueBatch.root),
                inputQueueCursor: proofQueueBatch.inputQueueCursor,
                outputQueueCursor: proofQueueBatch.outputQueueCursor,
                outputBlockNumber: proofQueueBatch.outputBlockNumber,
                previousOutputBlockNumber:
                    previousOutputBlockNumbers.get(proofQueueBatchIndex) ?? -1n, // sentinel: no previous proof queue batch (first-ever batch)
                indexInBatch: requestId - proofQueueBatch.inputQueueCursor,
            };
        }
    );
}
