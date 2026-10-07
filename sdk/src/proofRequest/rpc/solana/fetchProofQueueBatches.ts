import { type Address, type GetMultipleAccountsApi, type Rpc } from '@solana/kit';
import {
    fetchAllProofRequestRootEntry,
    PROOF_REQUEST_ROOT_ENTRY_DISCRIMINATOR,
    type ProofRequestRootEntry,
} from '../../../program/accounts/proofRequestRootEntry.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../../program/programs/token.js';
import { assertProgramAccount } from './accountChecks.js';
import { withSolanaRpcTransportErrors } from './errors.js';
import { findProofQueueBatchPda } from './findProofQueueBatchPda.js';

/** `getMultipleAccounts` accepts at most 100 addresses per call. */
export const MAX_ACCOUNTS_PER_CALL = 100;

/**
 * Reads the proof queue batches at `proofQueueBatchIndices` at `finalized`
 * commitment, at most `MAX_ACCOUNTS_PER_CALL` per RPC call.
 *
 * Every index must be below the bridge's `proofQueueBatchCount`: batches are
 * append-only and never closed, so a missing account is an error.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param proofQueueBatchIndices The proof queue batch indices to read.
 * @param programAddress The Nori Solana bridge program address.
 * @returns One decoded batch per index, in the order given.
 */
export async function fetchProofQueueBatches(
    rpc: Rpc<GetMultipleAccountsApi>,
    proofQueueBatchIndices: bigint[],
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<ProofRequestRootEntry[]> {
    const batches: ProofRequestRootEntry[] = [];
    for (let start = 0; start < proofQueueBatchIndices.length; start += MAX_ACCOUNTS_PER_CALL) {
        const indices = proofQueueBatchIndices.slice(start, start + MAX_ACCOUNTS_PER_CALL);
        const addresses = await Promise.all(
            indices.map(async (index) => (await findProofQueueBatchPda(index, programAddress))[0])
        );
        const accounts = await withSolanaRpcTransportErrors('Reading proof queue batches', () =>
            fetchAllProofRequestRootEntry(rpc, addresses, {
                commitment: 'finalized',
            })
        );
        for (const account of accounts) {
            assertProgramAccount(account, programAddress, PROOF_REQUEST_ROOT_ENTRY_DISCRIMINATOR);
            batches.push(account.data);
        }
    }
    return batches;
}
