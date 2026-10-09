import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { withBackoff } from '../../utils/withBackoff.js';
import {
    EthDataNotFoundError,
    EthRpcTransportError,
    ProofRequestTransactionNotMinedError,
} from './errors.js';

export interface ProofRequest {
    requestId: bigint;
    target: string;
    slotKey: string;
    blockNumber: number;
    transactionHash: string;
}

/**
 * Finds the proof request emitted by a transaction. The `ProofRequested`
 * log is emitted in the transaction that enqueued the request, so no range
 * scan or matching heuristic is required when its hash is known.
 *
 * @param provider The Ethereum provider used to retrieve the transaction receipt.
 * @param proofQueueAddress The address of the Nori proof request queue.
 * @param proofRequestTxHash The hash of the Ethereum transaction that enqueued the request.
 * @returns The proof request decoded from the matching `ProofRequested` log.
 * @throws EthRpcTransportError When reading the receipt still fails after its retries.
 * @throws ProofRequestTransactionNotMinedError When the transaction has no receipt yet.
 * @throws EthDataNotFoundError When its receipt contains no `ProofRequested`
 *   log from the queue: the wrong transaction or the wrong queue address.
 */
export async function findRequestIdByTxHash(
    provider: EthereumProvider,
    proofQueueAddress: string,
    proofRequestTxHash: string
): Promise<ProofRequest> {
    const queue = NoriProofRequestQueue__factory.connect(proofQueueAddress, provider);
    const receipt = await withBackoff(() =>
        provider.getTransactionReceipt(proofRequestTxHash)
    ).catch((error: unknown) => {
        throw new EthRpcTransportError(
            `Reading the receipt of ${proofRequestTxHash} failed.`,
            error
        );
    });
    if (!receipt) {
        throw new ProofRequestTransactionNotMinedError(proofRequestTxHash);
    }
    for (const log of receipt.logs) {
        if (log.address.toLowerCase() !== proofQueueAddress.toLowerCase()) continue;
        const parsed = queue.interface.parseLog(log);
        if (!parsed || parsed.name !== 'ProofRequested') continue;
        return {
            requestId: parsed.args.requestId as bigint,
            target: parsed.args.target as string,
            slotKey: parsed.args.slotKey as string,
            blockNumber: log.blockNumber,
            transactionHash: log.transactionHash,
        };
    }
    throw new EthDataNotFoundError(
        `No ProofRequested log found for queue ${proofQueueAddress} in tx ${proofRequestTxHash}.`
    );
}
