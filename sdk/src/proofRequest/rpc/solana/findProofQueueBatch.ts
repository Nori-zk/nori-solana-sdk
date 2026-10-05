import { type Address, type GetMultipleAccountsApi, type Rpc } from '@solana/kit';
import { type ProofRequestRootEntry } from '../../../program/accounts/proofRequestRootEntry.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../../program/programs/token.js';
import { ProofQueueBatchSearchError } from './errors.js';
import { fetchProofQueueBatches, MAX_ACCOUNTS_PER_CALL } from './fetchProofQueueBatches.js';

export interface FoundProofQueueBatch {
    proofQueueBatchIndex: bigint;
    proofQueueBatch: ProofRequestRootEntry;
}

function range(from: bigint, to: bigint, step: bigint): bigint[] {
    const values: bigint[] = [];
    for (let value = from; value < to; value += step) values.push(value);
    return values;
}

/**
 * Finds the committed proof queue batch whose
 * `[inputQueueCursor, outputQueueCursor)` covers `requestId`.
 *
 * Batch indices `0..proofQueueBatchCount` are contiguous and their cursor
 * ranges increase monotonically (each batch resumes at the previous batch's
 * output cursor), so the batch is found by a k-ary search: each round reads
 * up to `MAX_ACCOUNTS_PER_CALL` evenly spaced batches in one call and keeps
 * the interval between the last batch starting at or before `requestId` and
 * the next one, until the interval fits in one call.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param requestId The proof request id, which must be below the bridge's queue cursor.
 * @param proofQueueBatchCount The bridge's `proofQueueBatchCount`.
 * @param programAddress The Nori Solana bridge program address.
 * @returns The covering batch and its index.
 * @throws When no committed batch covers `requestId` (it precedes the bridge's first batch).
 */
export async function findProofQueueBatch(
    rpc: Rpc<GetMultipleAccountsApi>,
    requestId: bigint,
    proofQueueBatchCount: bigint,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<FoundProofQueueBatch> {
    const maxPerCall = BigInt(MAX_ACCOUNTS_PER_CALL);
    let low = 0n;
    let high = proofQueueBatchCount;

    while (high - low > maxPerCall) {
        const step = (high - low + maxPerCall - 1n) / maxPerCall;
        const probes = range(low, high, step);
        const batches = await fetchProofQueueBatches(rpc, probes, programAddress);
        let lastAtOrBefore = -1;
        for (let i = 0; i < batches.length; i++) {
            if (batches[i].inputQueueCursor > requestId) break;
            lastAtOrBefore = i;
        }
        if (lastAtOrBefore === -1) {
            throw new ProofQueueBatchSearchError(
                requestId,
                `Request ${requestId} precedes the first committed proof queue batch.`
            );
        }
        low = probes[lastAtOrBefore];
        high = lastAtOrBefore + 1 < probes.length ? probes[lastAtOrBefore + 1] : high;
    }

    const indices = range(low, high, 1n);
    const batches = await fetchProofQueueBatches(rpc, indices, programAddress);
    const covering = batches.findIndex(
        (batch) => batch.inputQueueCursor <= requestId && requestId < batch.outputQueueCursor
    );
    if (covering === -1) {
        throw new ProofQueueBatchSearchError(
            requestId,
            `No committed proof queue batch covers request ${requestId}.`
        );
    }
    return { proofQueueBatchIndex: indices[covering], proofQueueBatch: batches[covering] };
}
