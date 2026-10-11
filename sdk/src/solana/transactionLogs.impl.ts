import { address as addressOf, type Address, signature as signatureOf } from '@solana/kit';
import { map } from 'rxjs';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { changesOf$ } from '../rpc/connection/chainChanges.impl.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine } from '../rpc/connection/readThroughConnections.impl.js';
import { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';
import { transactionLogsRead$ } from '../rpc/solana/transactionLogs.js';
import { TransactionLogsGraph } from './transactionLogs.js';

/**
 * Starts reading the logs of the finalized transactions mentioning an
 * address through the Solana connection, and reading again on each new one.
 * The first read finds the newest transaction there is; each later read
 * gives the transactions after the last one read.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param address The account or program.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `transactions`, those the last read found. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createTransactionLogsMachine = (
    connections: ProofRequestConnections,
    address: Address,
    backoff: ReadRetryBackoff = {}
) => {
    const mentioned = createSolanaChangesMachine(connections.solana, { mentions: address });
    return startReadThroughConnectionsMachine(TransactionLogsGraph, {
        connections,
        read: (clients, held, node) =>
            clients
                .solana((rpc) =>
                    transactionLogsRead$(
                        rpc,
                        addressOf(held.address),
                        held.lastSignature === undefined ? undefined : signatureOf(held.lastSignature),
                        node === 'loading'
                    )
                )
                .pipe(map(({ transactions, lastSignature }) => ({ address: held.address, transactions, lastSignature }))),
        refreshOn: () => changesOf$(mentioned),
        needs: ['solana'],
        backoff,
        owns: [mentioned],
        // The address it is made for is its starting data.
        start: { node: 'loading', data: { address, transactions: [], lastSignature: undefined, failedReads: 0 } },
    });
};
