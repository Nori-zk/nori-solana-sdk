/**
 * Where a proof request is in its lifecycle, decided from the chains alone:
 *
 * - `undetermined`: not yet looked up.
 * - `unprocessed`: the request id is at or beyond the bridge's queue cursor.
 * - `proofAvailable`: a committed proof queue batch covers the request id.
 *   Proof queue batches are append-only, so this state is terminal.
 */
export const ProofRequestState = {
    Undetermined: 'undetermined',
    Unprocessed: 'unprocessed',
    ProofAvailable: 'proofAvailable',
} as const;

export type ProofRequestState = (typeof ProofRequestState)[keyof typeof ProofRequestState];
