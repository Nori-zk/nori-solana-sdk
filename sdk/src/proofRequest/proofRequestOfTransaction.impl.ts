import { catchError, map, NEVER, of, throwError } from 'rxjs';
import { type ProofRequestConnections } from './connectedRead.js';
import { changesOf$, createChainChangesMachine } from '../rpc/connection/chainChanges.impl.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine, dueOn } from '../rpc/connection/readThroughConnections.impl.js';
import { ProofRequestTransactionNotMinedError } from '../rpc/eth/errors.js';
import { proofRequestOfTransaction$ } from '../rpc/eth/proofRequest.js';
import { ProofRequestOfTransactionGraph } from './proofRequestOfTransaction.js';

/**
 * Starts reading the proof request a transaction enqueued through the
 * Ethereum connection, and reading again on each new block until the
 * transaction is mined.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param transactionHash The transaction that enqueued the request.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `proofRequest` once mined. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createProofRequestOfTransactionMachine = (
    connections: ProofRequestConnections,
    proofQueueAddress: string,
    transactionHash: string,
    backoff: ReadRetryBackoff = {}
) => {
    const newBlocks = createChainChangesMachine(connections.ethereum);
    return startReadThroughConnectionsMachine(ProofRequestOfTransactionGraph, {
        connections,
        read: (clients, held) =>
            clients
                .ethereum((provider) =>
                    proofRequestOfTransaction$(provider, proofQueueAddress, held.transactionHash).pipe(
                        // A transaction not mined yet holds no request yet.
                        catchError((error: unknown) =>
                            error instanceof ProofRequestTransactionNotMinedError ? of(undefined) : throwError(() => error)
                        )
                    )
                )
                .pipe(map((proofRequest) => ({ transactionHash: held.transactionHash, proofRequest }))),
        refreshOn: ({ proofRequest }) => (proofRequest ? dueOn(NEVER) : changesOf$(newBlocks)),
        needs: ['ethereum'],
        backoff,
        owns: [newBlocks],
        // The transaction it is made for is its starting data.
        start: { node: 'loading', data: { transactionHash, proofRequest: undefined, failedReads: 0 } },
    });
};
