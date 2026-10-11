import { combineLatest, map } from 'rxjs';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { type ProofRequestConnections } from './connectedRead.js';
import { fetchProofRequestCountsByTarget$, type ProofRequestHistoryAddresses } from './fetchProofRequestHistory.js';
import { changesOf$, createChainChangesMachine } from '../rpc/connection/chainChanges.impl.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine } from '../rpc/connection/readThroughConnections.impl.js';
import { type ProofRequestsByTargetQuery } from '../rpc/eth/proofRequestsByTarget.js';
import { PROOF_REQUESTED_TOPIC } from '../rpc/eth/topics.js';
import { stateAddress$ } from '../rpc/solana/programAddresses.js';
import { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';
import { ProofRequestCountsGraph } from './proofRequestCounts.js';

/** The submitting address and block range the counts cover. */
export type ProofRequestCountsQuery = Pick<
    ProofRequestsByTargetQuery,
    'target' | 'fromBlock' | 'toBlock' | 'maxBlockRangePerQuery'
>;

/**
 * Starts counting a submitting address's proof requests, and how many have
 * a proof available, reading through both connections: again each time the
 * queue enqueues a request (`ProofRequested`) or the bridge's state account
 * changes.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param addresses The queue and program addresses.
 * @param query The submitting address and block range.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `counts`. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createProofRequestCountsMachine = (
    connections: ProofRequestConnections,
    addresses: ProofRequestHistoryAddresses,
    query: ProofRequestCountsQuery,
    backoff: ReadRetryBackoff = {}
) => {
    const proofRequested = createChainChangesMachine(connections.ethereum, {
        address: addresses.proofQueueAddress,
        topics: [PROOF_REQUESTED_TOPIC],
    });
    const stateChanged = createSolanaChangesMachine(connections.solana, {
        account: stateAddress$(addresses.programAddress ?? TOKEN_PROGRAM_ADDRESS),
    });
    return startReadThroughConnectionsMachine(ProofRequestCountsGraph, {
        connections,
        read: (clients, { target }) =>
            fetchProofRequestCountsByTarget$(clients, addresses, { ...query, target }).pipe(
                map((counts) => ({ target, counts }))
            ),
        // Following once both chains' changes are; a change on either makes a refresh due.
        refreshOn: () => combineLatest([changesOf$(proofRequested), changesOf$(stateChanged)]),
        kind: 'logs',
        backoff,
        owns: [proofRequested, stateChanged],
        // The submitting address is its starting data.
        start: { node: 'loading', data: { target: query.target, counts: undefined, failedReads: 0 } },
    });
};
