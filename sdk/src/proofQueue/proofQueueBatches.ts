import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import type { EnqueuedProofRequest } from '../rpc/eth/enqueuedProofRequests.js';
import type { ProofQueueBatchSummary } from '../rpc/solana/proofQueueBatchSummaries.js';

/** A committed batch in view, with the filtered contract's request ids in it when there is a filter. */
export interface ShownProofQueueBatch extends ProofQueueBatchSummary {
    /** The filtered contract's request ids in this batch; `undefined` without a filter. */
    matching: bigint[] | undefined;
}

/** The newest committed batches and the requests no batch covers yet. */
export interface ProofQueueBatchesView {
    /** How many batches the bridge holds. */
    batchCount: bigint;
    /** The first request id no batch covers yet. */
    queueCursor: bigint;
    /** The newest batches, newest first; with a filter, only those holding its requests. */
    batches: ShownProofQueueBatch[];
    /** The requests no batch covers yet, oldest first; with a filter, only its own. */
    waiting: EnqueuedProofRequest[];
    /** Ethereum's finalized block, which a waiting request must be at or below to be proven. */
    finalizedBlock: number;
    /** Ethereum's latest block when the view was read. */
    latestBlock: number;
}

/** The view before the first read. */
export const EMPTY_PROOF_QUEUE_BATCHES_VIEW: ProofQueueBatchesView = {
    batchCount: 0n,
    queueCursor: 0n,
    batches: [],
    waiting: [],
    finalizedBlock: 0,
    latestBlock: 0,
};

/**
 * A live view of the bridge's newest proof queue batches and the requests
 * no batch covers yet, for every submitting address or one: the view read
 * through the connections and kept current (`readThroughConnectionsOf`).
 * `loading` and `refreshing` take the bridge's state and Ethereum's latest
 * and finalized blocks from their machines once those are `current`, and
 * read the newest batches (or, with a filter, the batches holding the
 * address's newest proven requests) and the requests no batch covers yet.
 * A source that fails fails the read. A refresh is due when the bridge's
 * batch count or queue cursor changes, on each Ethereum block past the
 * view's latest block, when a source waits for a connection or fails, and
 * on each recheck signal (e.g. Nori's stage changing on its websocket).
 * `target`, the submitting address it is filtered on (`undefined` for every
 * address), is its starting data.
 */
export const ProofQueueBatchesGraph = define(
    readThroughConnectionsOf({ target: undefined as string | undefined, view: EMPTY_PROOF_QUEUE_BATCHES_VIEW })
);

/** The live view's state: a node of the graph and its data. */
export type ProofQueueBatchesState = StateUnion<typeof ProofQueueBatchesGraph.nodes>;
