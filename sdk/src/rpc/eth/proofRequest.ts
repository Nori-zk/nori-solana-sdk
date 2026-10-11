import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { map, type Observable } from 'rxjs';
import { EthDataNotFoundError, ProofRequestTransactionNotMinedError } from './errors.js';
import { ethRpcRead$ } from './ethRpcRead.js';

export interface ProofRequest {
    requestId: bigint;
    target: string;
    slotKey: string;
    blockNumber: number;
    transactionHash: string;
}

/**
 * The proof request a transaction enqueued. The `ProofRequested` log is
 * emitted in the transaction that enqueued the request, so no range scan or
 * matching heuristic is required when its hash is known.
 *
 * @param provider The Ethereum provider used to read the transaction's receipt.
 * @param proofQueueAddress The address of the Nori proof request queue.
 * @param proofRequestTxHash The hash of the Ethereum transaction that enqueued the request.
 * @returns The proof request decoded from the matching `ProofRequested` log, once.
 *   Errors with `ProofRequestTransactionNotMinedError` when the transaction
 *   has no receipt yet, and `EthDataNotFoundError` when its receipt holds no
 *   `ProofRequested` log from the queue: the wrong transaction or the wrong
 *   queue address.
 */
export const proofRequestOfTransaction$ = (
    provider: EthereumProvider,
    proofQueueAddress: string,
    proofRequestTxHash: string
): Observable<ProofRequest> =>
    ethRpcRead$(
        () => provider.getTransactionReceipt(proofRequestTxHash),
        `Reading the receipt of ${proofRequestTxHash} failed.`
    ).pipe(
        map((receipt) => {
            if (!receipt) throw new ProofRequestTransactionNotMinedError(proofRequestTxHash);
            const queue = NoriProofRequestQueue__factory.createInterface();
            for (const log of receipt.logs) {
                if (log.address.toLowerCase() !== proofQueueAddress.toLowerCase()) continue;
                const parsed = queue.parseLog(log);
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
        })
    );
