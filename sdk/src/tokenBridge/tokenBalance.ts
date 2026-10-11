import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';

/**
 * An owner's balance of an SPL token (by default the bridged token), held
 * in its associated token account, read through the Solana connection
 * (`readThroughConnectionsOf`): read at once, then again each time that
 * account changes, a mint included. `owner` is its starting data;
 * `balance` is `undefined` until read, and `0` while the owner has no
 * token account.
 */
export const TokenBalanceGraph = define(
    readThroughConnectionsOf({ owner: '', balance: undefined as bigint | undefined })
);

/** The balance's state: a node of the graph and its data. */
export type TokenBalanceState = StateUnion<typeof TokenBalanceGraph.nodes>;
