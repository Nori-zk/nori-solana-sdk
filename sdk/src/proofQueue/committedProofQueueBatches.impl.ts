import { type Address } from '@solana/kit';
import { map, of, switchMap } from 'rxjs';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { changesOf$ } from '../rpc/connection/chainChanges.impl.js';
import {
    type ReadRetryBackoff,
    startReadThroughConnectionsMachine,
} from '../rpc/connection/readThroughConnections.impl.js';
import { bridgeState$ } from '../rpc/solana/bridgeState.js';
import { stateAddress$ } from '../rpc/solana/programAddresses.js';
import { proofQueueBatchSummaries$ } from '../rpc/solana/proofQueueBatchSummaries.js';
import { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';
import { range } from '../utils/arrays.js';
import { CommittedProofQueueBatchesGraph } from './committedProofQueueBatches.js';

/**
 * Starts reading each proof queue batch the bridge commits through the
 * Solana connection, and reading again each time the bridge's state account
 * changes. The first read covers the batches from `fromIndex` to the
 * bridge's batch count; each later read covers the batches committed after
 * the last one read.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param fromIndex The first batch to read, its starting data (default: none committed so far).
 * @param programAddress The Nori Solana bridge program address.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `committed`, the batches the last read found. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createCommittedProofQueueBatchesMachine = (
    connections: ProofRequestConnections,
    fromIndex?: bigint,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS,
    backoff: ReadRetryBackoff = {}
) => {
    const stateChanged = createSolanaChangesMachine(connections.solana, { account: stateAddress$(programAddress) });
    return startReadThroughConnectionsMachine(CommittedProofQueueBatchesGraph, {
        connections,
        read: (clients, previous) =>
            clients.solana((rpc) =>
                bridgeState$(rpc, programAddress).pipe(
                    switchMap(({ proofQueueBatchCount }) => {
                        const from = previous.batchCount ?? proofQueueBatchCount;
                        if (from >= proofQueueBatchCount)
                            return of({ committed: [], batchCount: proofQueueBatchCount });
                        return proofQueueBatchSummaries$(
                            rpc,
                            range(from, proofQueueBatchCount),
                            programAddress
                        ).pipe(map((committed) => ({ committed, batchCount: proofQueueBatchCount })));
                    })
                )
            ),
        refreshOn: () => changesOf$(stateChanged),
        needs: ['solana'],
        backoff,
        owns: [stateChanged],
        // The batch it starts reading from is its starting data.
        start: { node: 'loading', data: { committed: [], batchCount: fromIndex, failedReads: 0 } },
    });
};
