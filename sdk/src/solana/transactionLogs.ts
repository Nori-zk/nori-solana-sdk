import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { type SolanaTransactionLogs } from '../rpc/solana/transactionLogs.js';

/**
 * The logs of the finalized transactions mentioning an address, such as a
 * program's, read through the Solana connection
 * (`readThroughConnectionsOf`): read at once, then again on each new one.
 * `address` is its starting data. `transactions` holds the transactions the
 * last read found, oldest first, each with its logs; `lastSignature` is the
 * newest one read, and each read starts after it, so every transaction
 * arrives once. The first read only finds where it starts: the transactions
 * from then on arrive.
 */
export const TransactionLogsGraph = define(
    readThroughConnectionsOf({
        address: '',
        transactions: [] as SolanaTransactionLogs[],
        lastSignature: undefined as string | undefined,
    })
);

/** The transaction logs' state: a node of the graph and its data. */
export type TransactionLogsState = StateUnion<typeof TransactionLogsGraph.nodes>;
