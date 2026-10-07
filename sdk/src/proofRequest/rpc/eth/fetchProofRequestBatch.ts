import {
    getEthereumProvider,
    type EthereumProvider,
} from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { withBackoff } from '../withBackoff.js';
import fetchProofRequestBatchByLogs from './fetchProofRequestBatchByLogs.js';
import fetchProofRequestBatchByCall from './fetchProofRequestBatchByCall.js';

export interface ProofRequestBatchEntry {
    target: string;
    collectionKeysCount: number;
    collectionKeys: string[];
    value: string;
}

export interface ProofRequestRecord {
    id: bigint;
    target: string;
    slotKey: string;
    collectionKeysCount: number;
    collectionKeys: string[];
}

/**
 * Reads every `NoriProofRequestQueue` entry with id in
 * `[inputQueueCursor, outputQueueCursor)`, plus the raw storage word each
 * one's `slotKey` points at on its own `target`, at `outputBlockNumber`.
 *
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param inputQueueCursor Inclusive lower bound of the batch (queue request id).
 * @param outputQueueCursor Exclusive upper bound of the batch.
 * @param previousOutputBlockNumber The output block of the settlement job
 *   immediately before this one, i.e. the point before which no request in
 *   this batch could have been enqueued. -1 is a sentinel: there is no
 *   previous job because this batch is the first the queue ever produced.
 * @param outputBlockNumber The Ethereum block to read every value at.
 * @param provider The Ethereum provider used for every read.
 * @returns One entry per request in the batch, in queue order.
 */
export async function fetchProofRequestBatch(
    proofQueueAddress: string,
    inputQueueCursor: bigint,
    outputQueueCursor: bigint,
    previousOutputBlockNumber: number,
    outputBlockNumber: number,
    provider: EthereumProvider = getEthereumProvider()
): Promise<ProofRequestBatchEntry[]> {
    if (outputQueueCursor - inputQueueCursor <= 0n) return [];

    const records =
        previousOutputBlockNumber !== -1 // sentinel: no previous job (first-ever batch)
            ? await fetchProofRequestBatchByLogs(
                  proofQueueAddress,
                  inputQueueCursor,
                  outputQueueCursor,
                  previousOutputBlockNumber,
                  outputBlockNumber,
                  provider
              )
            : await fetchProofRequestBatchByCall(
                  proofQueueAddress,
                  inputQueueCursor,
                  outputQueueCursor,
                  outputBlockNumber,
                  provider
              );

    const values: string[] = [];
    for (const record of records) {
        values.push(
            await withBackoff(() =>
                provider.getStorage(record.target, record.slotKey, outputBlockNumber)
            )
        );
    }

    return records.map((record, index) => ({
        target: record.target,
        collectionKeysCount: record.collectionKeysCount,
        collectionKeys: record.collectionKeys,
        value: values[index],
    }));
}
