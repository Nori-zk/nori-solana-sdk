import {
    type Address,
    type GetMultipleAccountsApi,
    type ReadonlyUint8Array,
    type Rpc,
} from '@solana/kit';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';
import { fetchProofQueueBatches } from './fetchProofQueueBatches.js';
import { findProofQueueBatchPda } from './findProofQueueBatchPda.js';

/** A committed proof queue batch, with where it sits among the others. */
export interface ProofQueueBatchSummary {
    proofQueueBatchIndex: bigint;
    /** The batch's PDA, base58. */
    proofQueueBatchAddress: string;
    /** The batch root, 0x-prefixed. */
    root: string;
    /** The first request id the batch holds. */
    inputQueueCursor: bigint;
    /** One past the last request id the batch holds. */
    outputQueueCursor: bigint;
    /** The Ethereum block the batch's proof read the queue at. */
    outputBlockNumber: bigint;
    /** The previous batch's output block; -1 for the first batch. Every request in this batch was enqueued after it. */
    previousOutputBlockNumber: bigint;
}

function toHex(bytes: ReadonlyUint8Array): string {
    return `0x${Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;
}

/**
 * Reads the proof queue batches at `proofQueueBatchIndices`, and the batch
 * before each for its output block, in one batched read.
 *
 * Every index must be below the bridge's `proofQueueBatchCount`.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param proofQueueBatchIndices The proof queue batch indices to read.
 * @param programAddress The Nori Solana bridge program address.
 * @returns One summary per index, in the order given.
 */
export async function fetchProofQueueBatchSummaries(
    rpc: Rpc<GetMultipleAccountsApi>,
    proofQueueBatchIndices: bigint[],
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<ProofQueueBatchSummary[]> {
    const indices = [
        ...new Set(
            proofQueueBatchIndices.flatMap((index) =>
                index > 0n ? [index - 1n, index] : [index]
            )
        ),
    ];
    const batches = await fetchProofQueueBatches(rpc, indices, programAddress);
    const byIndex = new Map(indices.map((index, i) => [index, batches[i]]));

    return Promise.all(
        proofQueueBatchIndices.map(async (index) => {
            const batch = byIndex.get(index);
            if (batch === undefined) {
                throw new RangeError(`Proof queue batch ${index} was not read.`);
            }
            const [address] = await findProofQueueBatchPda(
                index,
                programAddress
            );
            return {
                proofQueueBatchIndex: index,
                proofQueueBatchAddress: address,
                root: toHex(batch.root),
                inputQueueCursor: batch.inputQueueCursor,
                outputQueueCursor: batch.outputQueueCursor,
                outputBlockNumber: batch.outputBlockNumber,
                previousOutputBlockNumber:
                    byIndex.get(index - 1n)?.outputBlockNumber ?? -1n,
            };
        })
    );
}
