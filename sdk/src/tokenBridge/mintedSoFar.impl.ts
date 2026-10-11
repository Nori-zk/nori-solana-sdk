import { type Address } from '@solana/kit';
import { map, shareReplay, switchMap } from 'rxjs';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { changesOf$ } from '../rpc/connection/chainChanges.impl.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine } from '../rpc/connection/readThroughConnections.impl.js';
import { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';
import { mintedSoFar$ } from '../rpc/solana/tokenBridgeAccounts.js';
import { tokenAccountStorageAddress$ } from '../rpc/solana/tokenBridgeInstructions.js';
import { MintedSoFarGraph } from './mintedSoFar.js';

/**
 * Starts reading how much a recipient has minted so far through the Solana
 * connection, and reading it again each time its `token_account_storage`
 * account changes.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param recipient The recipient.
 * @param programAddress The Nori program (default: its address).
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `minted`. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createMintedSoFarMachine = (
    connections: ProofRequestConnections,
    recipient: Address,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS,
    backoff: ReadRetryBackoff = {}
) => {
    const storage$ = tokenAccountStorageAddress$(recipient, programAddress).pipe(shareReplay(1));
    const storageChanged = createSolanaChangesMachine(connections.solana, { account: storage$ });
    return startReadThroughConnectionsMachine(MintedSoFarGraph, {
        connections,
        read: (clients, held) =>
            clients
                .solana((rpc) => storage$.pipe(switchMap((storage) => mintedSoFar$(rpc, storage, programAddress))))
                .pipe(map((minted) => ({ recipient: held.recipient, minted }))),
        refreshOn: () => changesOf$(storageChanged),
        needs: ['solana'],
        backoff,
        owns: [storageChanged],
        // The recipient it is made for is its starting data.
        start: { node: 'loading', data: { recipient, minted: undefined, failedReads: 0 } },
    });
};
