import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { type ProofQueueBatchSummary } from '../rpc/solana/proofQueueBatchSummaries.js';
import { type AsNodeData } from '../utils/machines.js';

/**
 * Each proof queue batch the bridge commits, read through the Solana
 * connection (`readThroughConnectionsOf`) from the bridge's batch accounts:
 * read at once, then again each time the bridge's state account changes.
 * `committed` holds the batches committed since the read before, oldest
 * first, each with its index, address, root, cursors and output block. `batchCount` is the batch the next read starts from: the one it is
 * made with (its starting data; the bridge's batch count when none), then
 * how many batches the bridge held at the last read, so every batch arrives
 * once.
 */
export const CommittedProofQueueBatchesGraph = define(
    readThroughConnectionsOf({
        committed: [] as AsNodeData<ProofQueueBatchSummary>[],
        batchCount: undefined as bigint | undefined,
    })
);

/** The committed batches' state: a node of the graph and its data. */
export type CommittedProofQueueBatchesState = StateUnion<typeof CommittedProofQueueBatchesGraph.nodes>;
