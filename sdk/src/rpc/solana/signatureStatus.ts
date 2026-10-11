import { type Commitment, type Signature } from '@solana/kit';
import { map, type Observable, of, switchMap } from 'rxjs';
import { finalizedBlockHeight$ } from './slots.js';
import { type SolanaRpc } from './solanaHttp.js';
import { solanaRpcRead$ } from './solanaRpcRead.js';

/** A transaction as the cluster knows it: the slot it landed in, the commitment it reached, and its error if it failed. */
export interface SolanaSignatureStatus {
    slot: number;
    confirmationStatus: Commitment | null;
    /** `null` when the transaction succeeded. */
    err: unknown;
}

/** Commitment levels, weakest first. */
const COMMITMENTS: Commitment[] = ['processed', 'confirmed', 'finalized'];

/**
 * Whether a status has reached a commitment.
 *
 * @param status The transaction's status.
 * @param commitment The commitment.
 * @returns `true` once it has.
 */
export const reached = (status: SolanaSignatureStatus, commitment: Commitment): boolean =>
    status.confirmationStatus !== null &&
    COMMITMENTS.indexOf(status.confirmationStatus) >= COMMITMENTS.indexOf(commitment);

/**
 * A transaction's status, searched for in the cluster's whole history.
 *
 * @param rpc The Solana RPC used for the read.
 * @param signature The transaction's signature.
 * @returns Its status, or `undefined` while the cluster does not know it, once.
 */
export const signatureStatus$ = (rpc: SolanaRpc, signature: Signature): Observable<SolanaSignatureStatus | undefined> =>
    solanaRpcRead$(
        (abortSignal) =>
            rpc.getSignatureStatuses([signature], { searchTransactionHistory: true }).send({ abortSignal }),
        `Failed to read the status of ${signature}.`
    ).pipe(
        map(({ value: [status] }) =>
            status === null
                ? undefined
                : { slot: Number(status.slot), confirmationStatus: status.confirmationStatus, err: status.err }
        )
    );

/**
 * A transaction followed by its signature, as the last read found it: its
 * status (`undefined` while the cluster does not know it), the block height
 * after which its blockhash no longer lands, if known, and the finalized
 * block height the last read saw while the cluster did not know it.
 */
export interface SignatureFollow {
    status: SolanaSignatureStatus | undefined;
    lastValidBlockHeight: number | undefined;
    blockHeight: number | undefined;
}

/**
 * One read of a transaction followed by its signature: its status, then,
 * while the cluster does not know it and it has a `lastValidBlockHeight`,
 * the finalized block height, which tells whether it can still land.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param signature The transaction's signature.
 * @param follow The transaction as the last read found it.
 * @returns The transaction as this read finds it, once.
 */
export const signatureFollow$ = (rpc: SolanaRpc, signature: Signature, follow: SignatureFollow): Observable<SignatureFollow> =>
    signatureStatus$(rpc, signature).pipe(
        switchMap((status) =>
            status !== undefined || follow.lastValidBlockHeight === undefined
                ? of({ ...follow, status })
                : finalizedBlockHeight$(rpc).pipe(map((blockHeight) => ({ ...follow, status, blockHeight })))
        )
    );

/**
 * The cluster never knew the transaction, and the finalized block height
 * passed its `lastValidBlockHeight`: it never will.
 *
 * @param follow The transaction as last read.
 * @returns `true` once it has expired.
 */
export const expired = ({ status, lastValidBlockHeight, blockHeight }: SignatureFollow): boolean =>
    status === undefined &&
    lastValidBlockHeight !== undefined &&
    blockHeight !== undefined &&
    blockHeight > lastValidBlockHeight;
