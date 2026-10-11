import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { map, type Observable } from 'rxjs';
import { ethRpcRead$ } from './ethRpcRead.js';

/** A transaction's receipt once it is mined. */
export interface EthereumTransactionReceipt {
    transactionHash: string;
    blockNumber: bigint;
    /** 1 for success, 0 for a revert. */
    status: number | null;
}

/**
 * A transaction's receipt, once it is mined.
 *
 * @param provider The provider read through.
 * @param transactionHash The transaction.
 * @returns The receipt once mined, `undefined` before, once.
 */
export const transactionReceipt$ = (
    provider: EthereumProvider,
    transactionHash: string
): Observable<EthereumTransactionReceipt | undefined> =>
    ethRpcRead$(
        () => provider.getTransactionReceipt(transactionHash),
        `Failed to read the receipt of ${transactionHash}.`
    ).pipe(
        map((receipt) =>
            receipt === null
                ? undefined
                : { transactionHash: receipt.hash, blockNumber: BigInt(receipt.blockNumber), status: receipt.status }
        )
    );
