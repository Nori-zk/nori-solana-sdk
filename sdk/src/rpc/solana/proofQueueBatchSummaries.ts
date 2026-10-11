import { type Address, type ReadonlyUint8Array } from '@solana/kit';
import { defer, map, type Observable, of } from 'rxjs';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';
import { proofQueueBatchesByIndex$ } from './proofQueueBatches.js';
import { type SolanaRpc } from './solanaHttp.js';

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

/**
 * Bytes as a 0x-prefixed hex string.
 *
 * @param bytes The bytes.
 * @returns Their hex.
 */
export function toHex(bytes: ReadonlyUint8Array): string {
    return `0x${Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;
}

/**
 * The proof queue batches at `proofQueueBatchIndices`, and the batch before
 * each for its output block, in one batched read.
 *
 * Every index must be below the bridge's `proofQueueBatchCount`.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param proofQueueBatchIndices The proof queue batch indices to read.
 * @param programAddress The Nori Solana bridge program address.
 * @returns One summary per index, in the order given, once.
 */
export const proofQueueBatchSummaries$ = (
    rpc: SolanaRpc,
    proofQueueBatchIndices: bigint[],
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Observable<ProofQueueBatchSummary[]> =>
    defer(() =>
        proofQueueBatchIndices.length === 0
            ? of([])
            : proofQueueBatchesByIndex$(
                  rpc,
                  proofQueueBatchIndices.flatMap((index) => (index > 0n ? [index - 1n, index] : [index])),
                  programAddress
              ).pipe(
                  map((byIndex) =>
                      proofQueueBatchIndices.map((index): ProofQueueBatchSummary => {
                          const read = byIndex.get(index);
                          if (read === undefined) throw new RangeError(`Proof queue batch ${index} was not read.`);
                          const { address, batch } = read;
                          return {
                              proofQueueBatchIndex: index,
                              proofQueueBatchAddress: address,
                              root: toHex(batch.root),
                              inputQueueCursor: batch.inputQueueCursor,
                              outputQueueCursor: batch.outputQueueCursor,
                              outputBlockNumber: batch.outputBlockNumber,
                              previousOutputBlockNumber: byIndex.get(index - 1n)?.batch.outputBlockNumber ?? -1n,
                          };
                      })
                  )
              )
    );
