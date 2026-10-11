import { type Address, getProgramDerivedAddress, getU64Encoder } from '@solana/kit';
import { defer, map, type Observable } from 'rxjs';
import { NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED } from '../../program/constants/token.js';
import { findStatePda } from '../../program/pdas/state.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';

/**
 * The address of the bridge program's state account.
 *
 * @param programAddress The Nori Solana bridge program address.
 * @returns The state account's address, once.
 */
export const stateAddress$ = (programAddress: Address = TOKEN_PROGRAM_ADDRESS): Observable<Address> =>
    defer(() => findStatePda({ programAddress })).pipe(map(([address]) => address));

/**
 * The address of proof queue batch `proofQueueBatchIndex`, derived as
 * `update` derives it: `[NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED, index as u64 little-endian]`.
 * The IDL carries the seed but not this PDA, since `update` derives the
 * address from state in its handler rather than through a seeds constraint.
 *
 * @param proofQueueBatchIndex The batch's index.
 * @param programAddress The Nori Solana bridge program address.
 * @returns The batch's address, once.
 */
export const proofQueueBatchAddress$ = (
    proofQueueBatchIndex: bigint,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Observable<Address> =>
    defer(() =>
        getProgramDerivedAddress({
            programAddress,
            seeds: [NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED, getU64Encoder().encode(proofQueueBatchIndex)],
        })
    ).pipe(map(([address]) => address));
