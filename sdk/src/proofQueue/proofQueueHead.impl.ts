import { map } from 'rxjs';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { changesOf$, createChainChangesMachine } from '../rpc/connection/chainChanges.impl.js';
import {
    type ReadRetryBackoff,
    startReadThroughConnectionsMachine,
} from '../rpc/connection/readThroughConnections.impl.js';
import { proofQueueHead$ } from '../rpc/eth/proofQueueHead.js';
import { PROOF_REQUESTED_TOPIC } from '../rpc/eth/topics.js';
import { ProofQueueHeadGraph } from './proofQueueHead.js';

/**
 * Starts reading the queue's head through the Ethereum connection, reading
 * again on each `ProofRequested` the queue emits.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `head`. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createProofQueueHeadMachine = (
    connections: ProofRequestConnections,
    proofQueueAddress: string,
    backoff: ReadRetryBackoff = {}
) => {
    const proofRequested = createChainChangesMachine(connections.ethereum, {
        address: proofQueueAddress,
        topics: [PROOF_REQUESTED_TOPIC],
    });
    return startReadThroughConnectionsMachine(ProofQueueHeadGraph, {
        connections,
        read: (clients) =>
            clients
                .ethereum((provider) => proofQueueHead$(provider, proofQueueAddress))
                .pipe(map((head) => ({ head }))),
        refreshOn: () => changesOf$(proofRequested),
        needs: ['ethereum'],
        backoff,
        owns: [proofRequested],
    });
};
