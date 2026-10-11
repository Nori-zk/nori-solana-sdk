import { forkJoin, map } from 'rxjs';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { changesOf$, createChainChangesMachine } from '../rpc/connection/chainChanges.impl.js';
import {
    type ReadRetryBackoff,
    startReadThroughConnectionsMachine,
} from '../rpc/connection/readThroughConnections.impl.js';
import { blockNumber$ } from '../rpc/eth/blockNumber.js';
import { EthereumBlocksGraph } from './ethereumBlocks.js';

/**
 * Starts reading Ethereum's latest and finalized blocks through the
 * Ethereum connection, and reading them again on each new block.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `latestBlock` and `finalizedBlock`. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createEthereumBlocksMachine = (connections: ProofRequestConnections, backoff: ReadRetryBackoff = {}) => {
    const newBlocks = createChainChangesMachine(connections.ethereum);
    return startReadThroughConnectionsMachine(EthereumBlocksGraph, {
        connections,
        read: (clients) =>
            forkJoin([
                clients.ethereum((provider) => blockNumber$(provider, 'latest')),
                clients.ethereum((provider) => blockNumber$(provider, 'finalized')),
            ]).pipe(map(([latestBlock, finalizedBlock]) => ({ latestBlock, finalizedBlock }))),
        refreshOn: () => changesOf$(newBlocks),
        needs: ['ethereum'],
        backoff,
        owns: [newBlocks],
    });
};
