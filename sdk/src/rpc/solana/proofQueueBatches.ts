import { type Address } from '@solana/kit';
import { concatMap, defer, forkJoin, from, map, type Observable, of, reduce, switchMap } from 'rxjs';
import {
    fetchAllProofRequestRootEntry,
    PROOF_REQUEST_ROOT_ENTRY_DISCRIMINATOR,
    type ProofRequestRootEntry,
} from '../../program/accounts/proofRequestRootEntry.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';
import { chunk } from '../../utils/arrays.js';
import { assertProgramAccount } from './accountChecks.js';
import { proofQueueBatchAddress$ } from './programAddresses.js';
import { type SolanaRpc } from './solanaHttp.js';
import { solanaRpcRead$ } from './solanaRpcRead.js';

/** `getMultipleAccounts` accepts at most 100 addresses per call. */
export const MAX_ACCOUNTS_PER_CALL = 100;

/** A committed proof queue batch and the address of its account. */
export interface ProofQueueBatchAccount {
    address: Address;
    batch: ProofRequestRootEntry;
}

/**
 * The proof queue batches at `proofQueueBatchIndices`, each with its
 * account's address, at `finalized` commitment, at most
 * `MAX_ACCOUNTS_PER_CALL` per RPC call.
 *
 * Every index must be below the bridge's `proofQueueBatchCount`: batches are
 * append-only and never closed, so a missing account is an error.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param proofQueueBatchIndices The proof queue batch indices to read.
 * @param programAddress The Nori Solana bridge program address.
 * @returns One decoded batch per index, with its address, in the order given, once.
 */
export const proofQueueBatches$ = (
    rpc: SolanaRpc,
    proofQueueBatchIndices: bigint[],
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Observable<ProofQueueBatchAccount[]> =>
    defer(() =>
        from(chunk(proofQueueBatchIndices, MAX_ACCOUNTS_PER_CALL)).pipe(
            concatMap((indices) =>
                forkJoin(indices.map((index) => proofQueueBatchAddress$(index, programAddress))).pipe(
                    switchMap((addresses) =>
                        solanaRpcRead$(
                            (abortSignal) =>
                                fetchAllProofRequestRootEntry(rpc, addresses, { commitment: 'finalized', abortSignal }),
                            'Failed to read proof queue batches.'
                        )
                    ),
                    map((accounts) =>
                        accounts.map((account): ProofQueueBatchAccount => {
                            assertProgramAccount(account, programAddress, PROOF_REQUEST_ROOT_ENTRY_DISCRIMINATOR);
                            return { address: account.address, batch: account.data };
                        })
                    )
                )
            ),
            reduce((batches, read) => [...batches, ...read], [] as ProofQueueBatchAccount[])
        )
    );

/**
 * The proof queue batches at `indices`, each read once, keyed by index.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param indices The proof queue batch indices to read; repeats are read once.
 * @param programAddress The Nori Solana bridge program address.
 * @returns Each batch with its address, by its index, once.
 */
export const proofQueueBatchesByIndex$ = (
    rpc: SolanaRpc,
    indices: bigint[],
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Observable<Map<bigint, ProofQueueBatchAccount>> =>
    defer(() => {
        const unique = [...new Set(indices)];
        return unique.length === 0
            ? of(new Map<bigint, ProofQueueBatchAccount>())
            : proofQueueBatches$(rpc, unique, programAddress).pipe(
                  map((batches) => new Map(unique.map((index, i) => [index, batches[i]])))
              );
    });
