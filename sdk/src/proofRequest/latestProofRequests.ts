import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import type { ProofRequestHistoryEntry } from './fetchProofRequestHistory.js';

/** The value read through the connections and kept current: the submitting address, the view and the block of its oldest request. */
const viewReadThroughConnections = readThroughConnectionsOf({
    target: '',
    view: [] as ProofRequestHistoryEntry[],
    oldestBlock: 0,
});

/**
 * A live view of a submitting address's newest N proof requests, newest
 * first: the view read through the connections and kept current
 * (`readThroughConnectionsOf`), with one edge of its own. `target`, the
 * submitting address, is its starting data.
 *
 * - `loading` reads the newest N from `fromBlock`; a refresh is due every
 *   interval, or on the recheck signal (e.g. bridge state changes from the
 *   websocket).
 * - `refreshing` re-reads from the view's oldest block up to the latest
 *   block. New requests enter at the top and states move from unprocessed
 *   to proof available (`valueArrived`), unless a request in the view has
 *   gone: a reorg removed it, and `requestDroppedByReorg` loads everything
 *   again from `fromBlock`, keeping the last view on screen meanwhile.
 */
export const LatestProofRequestsGraph = define({
    nodes: { ...viewReadThroughConnections.nodes },
    edges: {
        ...viewReadThroughConnections.edges,
        requestDroppedByReorg: { from: 'refreshing', to: 'loading', on: 'requestsMissing.next' },
    },
});

/** The live view's state: a node of the graph and its data. */
export type LatestProofRequestsState = StateUnion<
    typeof LatestProofRequestsGraph.nodes
>;
