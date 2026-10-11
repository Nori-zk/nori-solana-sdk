import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { type SolanaAccount } from '../rpc/solana/account.js';

/**
 * A Solana account, read through the Solana connection
 * (`readThroughConnectionsOf`): read at once, then again each time it
 * changes. `address`, the account it is made for, is its starting data;
 * `account` is `undefined` until read, and while none exists at the address.
 */
export const SolanaAccountGraph = define(
    readThroughConnectionsOf({
        address: '',
        account: undefined as SolanaAccount | undefined,
    })
);

/** The account's state: a node of the graph and its data. */
export type SolanaAccountState = StateUnion<typeof SolanaAccountGraph.nodes>;
