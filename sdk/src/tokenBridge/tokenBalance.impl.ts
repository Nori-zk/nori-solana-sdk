import { type Address } from '@solana/kit';
import { map, of, shareReplay, switchMap } from 'rxjs';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { changesOf$ } from '../rpc/connection/chainChanges.impl.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine } from '../rpc/connection/readThroughConnections.impl.js';
import { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';
import { tokenBalance$ } from '../rpc/solana/tokenBridgeAccounts.js';
import { associatedTokenAddress$, bridgedTokenAddress$ } from '../rpc/solana/tokenBridgeInstructions.js';
import { TokenBalanceGraph } from './tokenBalance.js';

/**
 * Starts reading an owner's balance of an SPL token through the Solana
 * connection, from its associated token account, and reading it again each
 * time that account changes.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param owner The owner.
 * @param mint The token's mint (default: the bridged token).
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `balance`. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createTokenBalanceMachine = (
    connections: ProofRequestConnections,
    owner: Address,
    mint?: Address,
    backoff: ReadRetryBackoff = {}
) => {
    const tokenAccount$ = (mint === undefined ? bridgedTokenAddress$() : of(mint)).pipe(
        switchMap((token) => associatedTokenAddress$(owner, token)),
        shareReplay(1)
    );
    const tokenAccountChanged = createSolanaChangesMachine(connections.solana, { account: tokenAccount$ });
    return startReadThroughConnectionsMachine(TokenBalanceGraph, {
        connections,
        read: (clients, held) =>
            clients
                .solana((rpc) => tokenAccount$.pipe(switchMap((tokenAccount) => tokenBalance$(rpc, tokenAccount))))
                .pipe(map((balance) => ({ owner: held.owner, balance }))),
        refreshOn: () => changesOf$(tokenAccountChanged),
        needs: ['solana'],
        backoff,
        owns: [tokenAccountChanged],
        // The owner it is made for is its starting data.
        start: { node: 'loading', data: { owner, balance: undefined, failedReads: 0 } },
    });
};
