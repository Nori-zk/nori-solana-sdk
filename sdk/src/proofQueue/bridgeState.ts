import { define, type StateUnion } from '@yaw-rx/ystate';
import { type NoriSolTokenBridge } from '../program/accounts/noriSolTokenBridge.js';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';

/**
 * The bridge program's state account, read through the Solana connection
 * (`readThroughConnectionsOf`): read at once, then again each time the
 * account changes. `bridgeState` is `undefined` until the first read
 * arrives.
 */
export const BridgeStateGraph = define(
    readThroughConnectionsOf({ bridgeState: undefined as NoriSolTokenBridge | undefined })
);

/** The bridge state's state: a node of the graph and its data. */
export type BridgeStateState = StateUnion<typeof BridgeStateGraph.nodes>;
