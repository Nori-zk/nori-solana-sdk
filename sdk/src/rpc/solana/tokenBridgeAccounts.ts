import { type Address, getBase64Encoder, getU64Decoder } from '@solana/kit';
import { map, type Observable } from 'rxjs';
import { account$ } from './account.js';
import { type SolanaRpc } from './solanaHttp.js';
import { SPL_TOKEN_PROGRAM_ADDRESS } from './tokenBridgeInstructions.js';

/**
 * A little-endian u64 in an account's data.
 *
 * @param data The account's data, base64.
 * @param offset Where the u64 starts.
 * @returns The u64.
 */
const u64At = (data: string, offset: number): bigint => getU64Decoder().decode(getBase64Encoder().encode(data), offset);

/**
 * How much a recipient has minted so far: `minted_so_far` in its
 * `token_account_storage` account (after the account's 8-byte
 * discriminator), which `mint` creates on its first mint. An account the
 * program does not own holds no mint.
 *
 * @param rpc The Solana RPC used for the read.
 * @param storage The recipient's `token_account_storage` account.
 * @param programAddress The Nori program.
 * @returns The amount minted so far, once.
 */
export const mintedSoFar$ = (rpc: SolanaRpc, storage: Address, programAddress: Address): Observable<bigint> =>
    account$(rpc, storage).pipe(
        map((account) => (account !== undefined && account.owner === programAddress ? u64At(account.data, 8) : 0n))
    );

/**
 * A token account's balance: its `amount`, after its mint and owner. An
 * account the SPL Token program does not own (none yet) holds nothing.
 *
 * @param rpc The Solana RPC used for the read.
 * @param tokenAccount The token account.
 * @returns The balance, in the token's base units, once.
 */
export const tokenBalance$ = (rpc: SolanaRpc, tokenAccount: Address): Observable<bigint> =>
    account$(rpc, tokenAccount).pipe(
        map((account) =>
            account !== undefined && account.owner === SPL_TOKEN_PROGRAM_ADDRESS ? u64At(account.data, 64) : 0n
        )
    );
