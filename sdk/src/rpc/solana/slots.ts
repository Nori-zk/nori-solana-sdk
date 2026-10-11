import { type Address } from '@solana/kit';
import { map, type Observable } from 'rxjs';
import { type SolanaRpc } from './solanaHttp.js';
import { solanaRpcRead$ } from './solanaRpcRead.js';

/**
 * The latest finalized slot.
 *
 * @param rpc The Solana RPC used for the read.
 * @returns The slot, once.
 */
export const finalizedSlot$ = (rpc: SolanaRpc): Observable<number> =>
    solanaRpcRead$(
        (abortSignal) => rpc.getSlot({ commitment: 'finalized' }).send({ abortSignal }),
        'Failed to read the finalized slot.'
    ).pipe(map(Number));

/**
 * The latest finalized block height: what a transaction's
 * `lastValidBlockHeight` is measured against.
 *
 * @param rpc The Solana RPC used for the read.
 * @returns The block height, once.
 */
export const finalizedBlockHeight$ = (rpc: SolanaRpc): Observable<number> =>
    solanaRpcRead$(
        (abortSignal) => rpc.getBlockHeight({ commitment: 'finalized' }).send({ abortSignal }),
        'Failed to read the finalized block height.'
    ).pipe(map(Number));

/**
 * The slot of the newest finalized transaction mentioning `address`: one
 * that writes or reads the account, or calls the program, at that address.
 *
 * @param rpc The Solana RPC used for the read.
 * @param address The account or program.
 * @returns The slot, or `undefined` when no transaction mentions it, once.
 */
export const newestTransactionSlot$ = (rpc: SolanaRpc, address: Address): Observable<number | undefined> =>
    solanaRpcRead$(
        (abortSignal) =>
            rpc.getSignaturesForAddress(address, { limit: 1, commitment: 'finalized' }).send({ abortSignal }),
        `Failed to read the transactions of ${address}.`
    ).pipe(map(([newest]) => (newest === undefined ? undefined : Number(newest.slot))));
