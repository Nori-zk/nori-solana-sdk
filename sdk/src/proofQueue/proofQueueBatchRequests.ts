import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import type { EnqueuedProofRequest } from '../rpc/eth/enqueuedProofRequests.js';
import type { ProofQueueBatchSummary } from '../rpc/solana/proofQueueBatchSummaries.js';
import { type AsNodeData } from '../utils/machines.js';

/**
 * One committed proof queue batch's requests, for every submitting address
 * or one: read through the Ethereum connection over the batch's own block
 * range and request id range (`readThroughConnectionsOf`). `batch`, the
 * batch it is made for, and `target`, the submitting address it is filtered
 * on (`undefined` for every address), are its starting data. A committed
 * batch never changes, so once `current` its refresh is never due.
 */
export const ProofQueueBatchRequestsGraph = define(
    readThroughConnectionsOf({
        batch: {} as AsNodeData<ProofQueueBatchSummary>,
        target: undefined as string | undefined,
        requests: [] as EnqueuedProofRequest[],
    })
);

/** The batch's requests' state: a node of the graph and its data. */
export type ProofQueueBatchRequestsState = StateUnion<typeof ProofQueueBatchRequestsGraph.nodes>;
