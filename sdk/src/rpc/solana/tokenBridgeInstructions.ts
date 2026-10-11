import {
    address,
    type Address,
    getAddressEncoder,
    getProgramDerivedAddress,
    type Instruction,
    type TransactionSigner,
} from '@solana/kit';
import { getBytes, toBeHex } from 'ethers';
import { defer, map, type Observable, switchMap } from 'rxjs';
import { getMintInstructionAsync } from '../../program/instructions/mint.js';
import { findTokenAccountStoragePda } from '../../program/pdas/tokenAccountStorage.js';
import { findTokenPda } from '../../program/pdas/token.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';
import { type VerifiedRequestWitness } from '../../proofRequest/getProofRequestStateSnapshot.js';

/** The SPL Token program, which holds the bridged token's accounts. */
export const SPL_TOKEN_PROGRAM_ADDRESS = address('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');

/** The Associated Token Account program, which places each owner's token account. */
export const ASSOCIATED_TOKEN_PROGRAM_ADDRESS = address('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL');

/**
 * The bridged token's mint: the Nori program's `token` account.
 *
 * @param programAddress The Nori program (default: its address).
 * @returns The mint's address, once.
 */
export const bridgedTokenAddress$ = (programAddress: Address = TOKEN_PROGRAM_ADDRESS): Observable<Address> =>
    defer(() => findTokenPda({ programAddress })).pipe(map(([mint]) => mint));

/**
 * Where a recipient's minted-so-far is kept: the Nori program's
 * `token_account_storage` account for the recipient.
 *
 * @param recipient The recipient.
 * @param programAddress The Nori program (default: its address).
 * @returns The account's address, once.
 */
export const tokenAccountStorageAddress$ = (
    recipient: Address,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Observable<Address> =>
    defer(() => findTokenAccountStoragePda({ recipient }, { programAddress })).pipe(map(([storage]) => storage));

/**
 * An owner's token account for a mint: its associated token account.
 *
 * @param owner The owner.
 * @param mint The mint.
 * @returns The token account's address, once.
 */
export const associatedTokenAddress$ = (owner: Address, mint: Address): Observable<Address> =>
    defer(() =>
        getProgramDerivedAddress({
            programAddress: ASSOCIATED_TOKEN_PROGRAM_ADDRESS,
            seeds: [
                getAddressEncoder().encode(owner),
                getAddressEncoder().encode(SPL_TOKEN_PROGRAM_ADDRESS),
                getAddressEncoder().encode(mint),
            ],
        })
    ).pipe(map(([tokenAccount]) => tokenAccount));

/** A 32-byte hash, as the program's types take it. */
const bytes32 = (hex: string) => [getBytes(hex)] as const;

/**
 * The program's `mint`: the bridged token to `recipient` against a proven
 * deposit, created in the recipient's associated token account. The
 * recipient signs it and pays for the accounts it creates, so the
 * transaction machine sending it is given the same signer.
 *
 * @param recipient The recipient, who signs.
 * @param depositWitness The deposit's witness (`verifiedWitness`, from `createProofRequestWitnessMachine`).
 * @param proofQueueBatchAddress The committed batch holding the deposit (`proofQueueBatchAddress` of its proof available snapshot).
 * @param programAddress The Nori program (default: its address).
 * @returns The instruction, for `createSolanaSignerTransactionMachine` or `createSolanaWalletTransactionMachine`, once.
 */
export const mintCall$ = (
    recipient: TransactionSigner,
    depositWitness: VerifiedRequestWitness,
    proofQueueBatchAddress: string,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Observable<Instruction> =>
    bridgedTokenAddress$(programAddress).pipe(
        switchMap((mint) => associatedTokenAddress$(recipient.address, mint)),
        switchMap((tokenAccount) =>
            defer(() =>
                getMintInstructionAsync(
                    {
                        payer: recipient,
                        recipient,
                        tokenAccount,
                        proofQueueBatch: address(proofQueueBatchAddress),
                        path: depositWitness.path.map(bytes32),
                        index: depositWitness.index,
                        value: {
                            target: [getBytes(depositWitness.value.target)],
                            collectionKeysCount: depositWitness.value.collectionKeysCount,
                            collectionKeys: depositWitness.value.collectionKeys.map(bytes32),
                            // The program takes the value as a little-endian U256.
                            value: [getBytes(toBeHex(depositWitness.value.value, 32)).reverse()],
                        },
                    },
                    { programAddress }
                )
            )
        )
    );
