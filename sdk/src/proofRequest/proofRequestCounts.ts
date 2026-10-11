import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { type ProofRequestCounts } from './fetchProofRequestHistory.js';

/**
 * How many proof requests a submitting address made, and how many have a
 * proof available, read through both connections
 * (`readThroughConnectionsOf`): read at once, then again each time the
 * queue enqueues a request or the bridge's state account changes. `target`,
 * the submitting address, is its starting data; `counts` is `undefined`
 * until read.
 */
export const ProofRequestCountsGraph = define(
    readThroughConnectionsOf({
        target: '',
        counts: undefined as ProofRequestCounts | undefined,
    })
);

/** The counts' state: a node of the graph and its data. */
export type ProofRequestCountsState = StateUnion<typeof ProofRequestCountsGraph.nodes>;
