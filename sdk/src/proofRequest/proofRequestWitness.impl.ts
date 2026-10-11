import { map, NEVER } from 'rxjs';
import { type ProofRequestConnections } from './connectedRead.js';
import { type ProofAvailableProofRequestSnapshot, proofRequestWitnessesOf } from './getProofRequestStateSnapshot.js';
import {
    type ReadRetryBackoff,
    startReadThroughConnectionsMachine,
    dueOn,
} from '../rpc/connection/readThroughConnections.impl.js';
import { proofRequestBatch$ } from '../rpc/eth/proofRequestBatch.js';
import { ProofRequestWitnessGraph } from './proofRequestWitness.js';

/**
 * Starts reading a proven request's witness through the Ethereum
 * connection: its batch's requests, read once from the queue's logs and
 * storage, rebuilt into the batch's Merkle tree and checked against the
 * committed root (a mismatch is a failed read,
 * `ProofRequestWitnessRootMismatchError`).
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param proofAvailable The request's proof available snapshot.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `witness`. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createProofRequestWitnessMachine = (
    connections: ProofRequestConnections,
    proofQueueAddress: string,
    proofAvailable: ProofAvailableProofRequestSnapshot,
    backoff: ReadRetryBackoff = {}
) =>
    startReadThroughConnectionsMachine(ProofRequestWitnessGraph, {
        connections,
        read: (clients, { proofAvailable: proven }) =>
            clients
                .ethereum((provider) =>
                    proofRequestBatch$(
                        provider,
                        proofQueueAddress,
                        proven.inputQueueCursor,
                        proven.outputQueueCursor,
                        Number(proven.previousOutputBlockNumber),
                        Number(proven.outputBlockNumber)
                    )
                )
                .pipe(map((leaves) => ({ proofAvailable: proven, ...proofRequestWitnessesOf(leaves, proven) }))),
        // A committed batch never changes.
        refreshOn: () => dueOn(NEVER),
        kind: 'logs',
        needs: ['ethereum'],
        backoff,
        // The proven request it is made for is its starting data.
        start: { node: 'loading', data: { proofAvailable, witness: undefined, verifiedWitness: undefined, failedReads: 0 } },
    });
