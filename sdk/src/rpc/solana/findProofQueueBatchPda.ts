import {
    getProgramDerivedAddress,
    getU64Encoder,
    type Address,
    type ProgramDerivedAddress,
} from '@solana/kit';
import { NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED } from '../../program/constants/token.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';

/**
 * The proof queue batch PDA for `proofQueueBatchIndex`, derived as `update`
 * derives it: `[NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED, index as u64 little-endian]`.
 * The IDL carries the seed but not this PDA, since `update` derives the
 * address from state in its handler rather than through a seeds constraint.
 */
export async function findProofQueueBatchPda(
    proofQueueBatchIndex: bigint,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<ProgramDerivedAddress> {
    return getProgramDerivedAddress({
        programAddress,
        seeds: [
            NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED,
            getU64Encoder().encode(proofQueueBatchIndex),
        ],
    });
}
