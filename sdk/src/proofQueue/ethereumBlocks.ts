import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';

/**
 * Ethereum's latest and finalized blocks, read through the Ethereum
 * connection (`readThroughConnectionsOf`): read at once, then again on each
 * new block. A proof request is proven only once its block is finalized.
 */
export const EthereumBlocksGraph = define(readThroughConnectionsOf({ latestBlock: 0, finalizedBlock: 0 }));

/** The Ethereum blocks' state: a node of the graph and its data. */
export type EthereumBlocksState = StateUnion<typeof EthereumBlocksGraph.nodes>;
