import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import type { EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { withBackoff } from '../../utils/withBackoff.js';
import { blockRanges, MAX_BLOCK_RANGE_PER_QUERY } from './blockRanges.js';
import { type ProofRequestRecord } from './fetchProofRequestBatch.js';

/**
 * Reads every `NoriProofRequestQueue` record with id in
 * `[inputQueueCursor, outputQueueCursor)` from `ProofRequested` logs over
 * `[fromBlock, toBlock]`.
 *
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param inputQueueCursor Inclusive lower bound of the batch (queue request id).
 * @param outputQueueCursor Exclusive upper bound of the batch.
 * @param fromBlock Block to start the log search from.
 * @param toBlock Block to end the log search at.
 * @param provider The Ethereum provider used for every read.
 * @returns One record per request in the batch, in queue order.
 */
export default async function fetchProofRequestBatchByLogs(
    proofQueueAddress: string,
    inputQueueCursor: bigint,
    outputQueueCursor: bigint,
    fromBlock: number,
    toBlock: number,
    provider: EthereumProvider
): Promise<ProofRequestRecord[]> {
    const queue = NoriProofRequestQueue__factory.connect(proofQueueAddress, provider);

    const records: ProofRequestRecord[] = [];
    for (const [chunkFrom, chunkTo] of blockRanges(
        fromBlock,
        toBlock,
        MAX_BLOCK_RANGE_PER_QUERY,
        'asc'
    )) {
        const logs = await withBackoff(() =>
            queue.queryFilter(queue.filters.ProofRequested(), chunkFrom, chunkTo)
        );
        for (const log of logs) {
            const { requestId, target, slotKey, collectionKeys } = log.args;
            if (requestId < inputQueueCursor || requestId >= outputQueueCursor) continue;
            records.push({
                id: requestId,
                target,
                slotKey,
                collectionKeysCount: collectionKeys.length,
                collectionKeys,
            });
        }
    }
    return records;
}
