import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';

/**
 * When the Ethereum block that enqueued a proof request was produced, read
 * through the Ethereum connection (`readThroughConnectionsOf`) once: a
 * produced block's time never changes. `blockNumber` is its starting data;
 * `timestampMs` is `undefined` until read. The request's age is the time
 * since `timestampMs`.
 */
export const ProofRequestBlockTimeGraph = define(
    readThroughConnectionsOf({
        blockNumber: 0,
        timestampMs: undefined as number | undefined,
    })
);

/** The block time's state: a node of the graph and its data. */
export type ProofRequestBlockTimeState = StateUnion<typeof ProofRequestBlockTimeGraph.nodes>;
