import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import type { ProofRequestHistoryEntry } from './fetchProofRequestHistory.js';
import type { ProofRequestHistoryCursor } from '../rpc/eth/proofRequestsByTarget.js';

const { nodes, edges } = readThroughConnectionsOf({
    target: '',
    loaded: [] as ProofRequestHistoryEntry[],
    cursor: undefined as ProofRequestHistoryCursor | undefined,
});

/**
 * Pages through a submitting address's proof requests: the requests loaded
 * so far, read through the connections (`readThroughConnectionsOf`).
 * `target`, the submitting address, is its starting data.
 *
 * - `loading` reads the first page; `current` holds the requests loaded so
 *   far and the cursor after them; `loadMore` refreshes them with the next
 *   page (`refreshing`), appended to `loaded`.
 * - The page that exhausts the block range moves to `allLoaded`
 *   (`lastPageLoaded`), from `loading` or `refreshing`.
 * - Loss of a connection is only noticed while reading: `current` does no
 *   reads, and a `loadMore` while a connection is down moves to
 *   `refreshing`, which reports the loss straight away.
 * - `allLoaded` and `closed` are terminal.
 */
export const ProofRequestHistoryGraph = define({
    nodes: {
        ...nodes,
        allLoaded: { target: '', loaded: [] as ProofRequestHistoryEntry[] },
    },
    edges: {
        ...edges,
        lastPageLoaded: { from: 'loading', to: 'allLoaded', on: 'lastPageArrived.next' },
        lastPageLoadedWhileRefreshing: { from: 'refreshing', to: 'allLoaded', on: 'lastPageArrived.next' },
    },
});

/** The paged history's state: a node of the graph and its data. */
export type ProofRequestHistoryState = StateUnion<
    typeof ProofRequestHistoryGraph.nodes
>;
