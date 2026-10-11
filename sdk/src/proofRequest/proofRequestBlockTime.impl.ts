import { map, NEVER } from 'rxjs';
import { type ProofRequestConnections } from './connectedRead.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine, dueOn } from '../rpc/connection/readThroughConnections.impl.js';
import { blockTimestamp$ } from '../rpc/eth/blockTimestamp.js';
import { ProofRequestBlockTimeGraph } from './proofRequestBlockTime.js';

/**
 * Starts reading when the Ethereum block that enqueued a proof request was
 * produced, through the Ethereum connection, once.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param blockNumber The block the proof request was enqueued in.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `timestampMs`. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createProofRequestBlockTimeMachine = (
    connections: ProofRequestConnections,
    blockNumber: number,
    backoff: ReadRetryBackoff = {}
) =>
    startReadThroughConnectionsMachine(ProofRequestBlockTimeGraph, {
        connections,
        read: (clients, held) =>
            clients
                .ethereum((provider) => blockTimestamp$(provider, held.blockNumber))
                .pipe(map((timestampMs) => ({ blockNumber: held.blockNumber, timestampMs }))),
        // A produced block's time never changes.
        refreshOn: () => dueOn(NEVER),
        needs: ['ethereum'],
        backoff,
        // The block it is made for is its starting data.
        start: { node: 'loading', data: { blockNumber, timestampMs: undefined, failedReads: 0 } },
    });
