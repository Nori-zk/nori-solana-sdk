import { type Address } from '@solana/kit';
import { map, type Observable, switchMap } from 'rxjs';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { bridgeState$ } from '../rpc/solana/bridgeState.js';
import { ProofQueueBatchSearchError } from '../rpc/solana/errors.js';
import { proofQueueBatchesCovering$ } from '../rpc/solana/proofQueueBatchCovering.js';
import { proofQueueBatches$ } from '../rpc/solana/proofQueueBatches.js';
import { toHex } from '../rpc/solana/proofQueueBatchSummaries.js';
import { type SolanaRpc } from '../rpc/solana/solanaHttp.js';
import { ProofRequestState } from './types.js';
import type { ProofRequestStateSnapshot } from './getProofRequestStateSnapshot.js';

/** A proof request's queue id and the Ethereum block that enqueued it. */
export interface QueuedProofRequest {
    requestId: bigint;
    requestBlockNumber: bigint;
}

/**
 * Where each proof request is, from one read of the bridge state: ids at or
 * beyond its queue cursor are `unprocessed`, the rest are `proofAvailable`
 * with the committed batch that covers them. Batches for the proven ids are
 * found in one shared search, and each distinct batch's predecessor is read
 * once for its output block.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param queuedRequests The proof requests to place.
 * @param programAddress The Nori Solana bridge program address.
 * @returns One snapshot per queued request, in the order given, once.
 */
export const proofRequestSnapshots$ = (
    rpc: SolanaRpc,
    queuedRequests: QueuedProofRequest[],
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Observable<ProofRequestStateSnapshot[]> =>
    bridgeState$(rpc, programAddress).pipe(
        switchMap(({ queueCursor, proofQueueBatchCount }) => {
            const provenIds = queuedRequests
                .filter(({ requestId }) => requestId < queueCursor)
                .map(({ requestId }) => requestId);
            return proofQueueBatchesCovering$(rpc, provenIds, proofQueueBatchCount, programAddress).pipe(
                switchMap((found) => {
                    const batchIndices = [...new Set([...found.values()].map((f) => f.proofQueueBatchIndex))];
                    const previousIndices = batchIndices.filter((index) => index > 0n).map((index) => index - 1n);
                    return proofQueueBatches$(rpc, previousIndices, programAddress).pipe(
                        map((previousBatches) => {
                            const previousOutputBlockNumbers = new Map(
                                previousIndices.map((index, i) => [
                                    index + 1n,
                                    previousBatches[i].batch.outputBlockNumber,
                                ])
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
                                    if (!covering) {
                                        throw new ProofQueueBatchSearchError(
                                            requestId,
                                            `No committed proof queue batch covers request ${requestId}.`
                                        );
                                    }
                                    const { proofQueueBatchIndex, proofQueueBatchAddress, proofQueueBatch } = covering;
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
                        })
                    );
                })
            );
        })
    );
