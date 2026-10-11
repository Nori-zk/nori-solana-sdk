import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';

/**
 * The queue's head, how many proof requests have ever been enqueued, read
 * through the Ethereum connection (`readThroughConnectionsOf`): read at
 * once, then again on each `ProofRequested`. `head` is `undefined` until
 * read.
 */
export const ProofQueueHeadGraph = define(readThroughConnectionsOf({ head: undefined as bigint | undefined }));

/** The queue head's state: a node of the graph and its data. */
export type ProofQueueHeadState = StateUnion<typeof ProofQueueHeadGraph.nodes>;
