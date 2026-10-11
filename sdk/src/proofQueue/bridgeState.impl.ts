import { type Address } from '@solana/kit';
import { map } from 'rxjs';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { changesOf$ } from '../rpc/connection/chainChanges.impl.js';
import {
    type ReadRetryBackoff,
    startReadThroughConnectionsMachine,
} from '../rpc/connection/readThroughConnections.impl.js';
import { bridgeState$ } from '../rpc/solana/bridgeState.js';
import { stateAddress$ } from '../rpc/solana/programAddresses.js';
import { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';
import { BridgeStateGraph } from './bridgeState.js';

/**
 * Starts reading the bridge program's state account through the Solana
 * connection, and reading it again each time the account changes.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param programAddress The Nori Solana bridge program address.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries the bridge state. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createBridgeStateMachine = (
    connections: ProofRequestConnections,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS,
    backoff: ReadRetryBackoff = {}
) => {
    const stateChanged = createSolanaChangesMachine(connections.solana, { account: stateAddress$(programAddress) });
    return startReadThroughConnectionsMachine(BridgeStateGraph, {
        connections,
        read: (clients) =>
            clients.solana((rpc) => bridgeState$(rpc, programAddress)).pipe(map((bridgeState) => ({ bridgeState }))),
        refreshOn: () => changesOf$(stateChanged),
        needs: ['solana'],
        backoff,
        owns: [stateChanged],
    });
};
