import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { withBackoff } from '../../utils/withBackoff.js';
import { EthDataNotFoundError, EthRpcTransportError } from './errors.js';

/**
 * Reads the number of Ethereum's latest finalized block. The bridge only
 * proves finalized state, so a request in a later block waits for finality
 * before any proof can cover it.
 *
 * @param provider The Ethereum provider used for the read.
 * @returns The latest finalized block number.
 * @throws EthRpcTransportError When the read still fails after its retries.
 * @throws EthDataNotFoundError When the node has no finalized block.
 */
export async function fetchFinalizedBlockNumber(
    provider: EthereumProvider
): Promise<number> {
    const block = await withBackoff(() => provider.getBlock('finalized')).catch(
        (error: unknown) => {
            throw new EthRpcTransportError(
                'Failed to read the latest finalized block.',
                error
            );
        }
    );
    if (!block) {
        throw new EthDataNotFoundError('The node has no finalized block.');
    }
    return block.number;
}
