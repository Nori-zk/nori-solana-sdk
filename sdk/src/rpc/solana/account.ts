import { type Address } from '@solana/kit';
import { map, type Observable } from 'rxjs';
import { type SolanaRpc } from './solanaHttp.js';
import { solanaRpcRead$ } from './solanaRpcRead.js';

/** An account as the cluster holds it, at `finalized` commitment. */
export interface SolanaAccount {
    lamports: bigint;
    /** The program that owns it, base58. */
    owner: string;
    /** Its data, base64. */
    data: string;
    executable: boolean;
    space: bigint;
}

/**
 * An account, at `finalized` commitment.
 *
 * @param rpc The Solana RPC used for the read.
 * @param address The account.
 * @returns The account, or `undefined` when none exists at the address, once.
 */
export const account$ = (rpc: SolanaRpc, address: Address): Observable<SolanaAccount | undefined> =>
    solanaRpcRead$(
        (abortSignal) =>
            rpc.getAccountInfo(address, { encoding: 'base64', commitment: 'finalized' }).send({ abortSignal }),
        `Failed to read the account ${address}.`
    ).pipe(
        map(({ value }) =>
            value === null
                ? undefined
                : {
                      lamports: value.lamports,
                      owner: value.owner,
                      data: value.data[0],
                      executable: value.executable,
                      space: value.space,
                  }
        )
    );
