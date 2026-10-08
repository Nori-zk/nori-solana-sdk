import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { withBackoff } from '../../utils/withBackoff.js';
import { EthRpcTransportError } from './errors.js';

/**
 * Reads the queue's `head`: how many proof requests have ever been enqueued,
 * which is also the id the next request gets.
 *
 * @param provider The Ethereum provider used for the read.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @returns The queue's head.
 * @throws EthRpcTransportError When the read still fails after its retries.
 */
export async function fetchProofQueueHead(
    provider: EthereumProvider,
    proofQueueAddress: string
): Promise<bigint> {
    const queue = NoriProofRequestQueue__factory.connect(
        proofQueueAddress,
        provider
    );
    return withBackoff(() => queue.head()).catch((error: unknown) => {
        throw new EthRpcTransportError(
            'Failed to read the proof request queue head.',
            error
        );
    });
}
