import { map, NEVER } from 'rxjs';
import { type ProofRequestConnections } from '../../proofRequest/connectedRead.js';
import { transactionReceipt$ } from '../eth/transactionReceipt.js';
import { changesOf$, createChainChangesMachine } from './chainChanges.impl.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine, dueOn } from './readThroughConnections.impl.js';
import { TransactionReceiptGraph } from './transactionReceipt.js';

/**
 * Starts reading an Ethereum transaction's receipt through the Ethereum
 * connection, and reading it again on each new block until it is mined.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param transactionHash The transaction.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries the receipt once mined. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createTransactionReceiptMachine = (
    connections: ProofRequestConnections,
    transactionHash: string,
    backoff: ReadRetryBackoff = {}
) => {
    const newBlocks = createChainChangesMachine(connections.ethereum);
    return startReadThroughConnectionsMachine(TransactionReceiptGraph, {
        connections,
        read: (clients, followed) =>
            clients
                .ethereum((provider) => transactionReceipt$(provider, followed.transactionHash))
                .pipe(map((receipt) => ({ transactionHash: followed.transactionHash, receipt }))),
        refreshOn: ({ receipt }) => (receipt ? dueOn(NEVER) : changesOf$(newBlocks)),
        needs: ['ethereum'],
        backoff,
        owns: [newBlocks],
        // The transaction it is made for is its initial state.
        start: { node: 'loading', data: { transactionHash, receipt: undefined, failedReads: 0 } },
    });
};
