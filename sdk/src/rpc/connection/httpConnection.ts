import { define, type StateUnion } from '@yaw-rx/ystate';
import { backgroundCheckedEdges } from './backgroundChecked.js';
import { connectionOf, wrongNetworkOf } from './connection.js';
import { pacedOf } from './pacedConnection.js';

const { nodes, edges } = connectionOf({});
const wrongNetwork = wrongNetworkOf({});
const paced = pacedOf(nodes.ready);

/**
 * Whether reads can run against an HTTP endpoint: one of its URLs answers
 * and serves the expected network, or the node says why not
 * (`connectionOf`), checked again every interval while ready
 * (`backgroundCheckedEdges`), and the rate it takes requests at (`pacedOf`).
 * The same definition serves every chain; what a health check asks, and
 * what a passing one finds (`ready.health`), is the implementation's.
 *
 * - `connecting` checks the current URL; `waitingToRetry` and `wrongNetwork`
 *   connect again on the next URL, after a wait that doubles with each
 *   failed check.
 * - `wrongNetwork` connects again on that doubling wait
 *   (`wrongNetworkRechecked`), so a corrected endpoint recovers.
 * - With several URLs, one that answers nothing for a health check's
 *   timeout while it turns every request away for too many is left:
 *   `overwhelmed` connects again on the next URL (`movedOnWhileTurnedAway`).
 *   With one URL there is nowhere to go, and its requests wait their turns.
 *
 * `ready.health` and `overwhelmed.health` are typed per use by `HttpConnectionState<THealth>`.
 */
export const HttpConnectionGraph = define({
    nodes: { ...nodes, ...wrongNetwork.nodes, ...paced.nodes },
    edges: {
        ...edges,
        ...wrongNetwork.edges,
        ...backgroundCheckedEdges,
        ...paced.edges,
        wrongNetworkRechecked: { from: 'wrongNetwork', to: 'connecting', on: 'recheckDue.next' },
        movedOnWhileTurnedAway: { from: 'overwhelmed', to: 'connecting', on: 'turnedAwayTooLong.next' },
    },
});

/** An HTTP connection's state: a node of the graph and its data, `ready.health` and `overwhelmed.health` typed as `THealth`. */
export type HttpConnectionState<THealth> =
    StateUnion<typeof HttpConnectionGraph.nodes> extends infer TState
        ? TState extends { node: infer TNode extends 'ready' | 'overwhelmed'; data: infer TData }
            ? {
                  node: TNode;
                  data: Omit<TData, 'health'> & { health: THealth };
              }
            : TState
        : never;
