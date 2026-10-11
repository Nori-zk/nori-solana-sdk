import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';

/**
 * How much a recipient has minted so far, read through the Solana
 * connection (`readThroughConnectionsOf`) from its `token_account_storage`
 * account: read at once, then again each time that account changes, which
 * only a mint to the recipient does. `recipient` is its starting data;
 * `minted` is `undefined` until read, and `0` before the first mint.
 */
export const MintedSoFarGraph = define(
    readThroughConnectionsOf({ recipient: '', minted: undefined as bigint | undefined })
);

/** The minted amount's state: a node of the graph and its data. */
export type MintedSoFarState = StateUnion<typeof MintedSoFarGraph.nodes>;
