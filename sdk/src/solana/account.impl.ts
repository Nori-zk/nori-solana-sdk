import { address as addressOf, type Address } from '@solana/kit';
import { map } from 'rxjs';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { changesOf$ } from '../rpc/connection/chainChanges.impl.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine } from '../rpc/connection/readThroughConnections.impl.js';
import { account$ } from '../rpc/solana/account.js';
import { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';
import { SolanaAccountGraph } from './account.js';

/**
 * Starts reading a Solana account through the Solana connection, and
 * reading it again each time it changes.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param address The account.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries the account. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createSolanaAccountMachine = (
    connections: ProofRequestConnections,
    address: Address,
    backoff: ReadRetryBackoff = {}
) => {
    const accountChanged = createSolanaChangesMachine(connections.solana, { account: address });
    return startReadThroughConnectionsMachine(SolanaAccountGraph, {
        connections,
        read: (clients, held) =>
            clients
                .solana((rpc) => account$(rpc, addressOf(held.address)))
                .pipe(map((account) => ({ address: held.address, account }))),
        refreshOn: () => changesOf$(accountChanged),
        needs: ['solana'],
        backoff,
        owns: [accountChanged],
        // The account it is made for is its starting data.
        start: { node: 'loading', data: { address, account: undefined, failedReads: 0 } },
    });
};
