import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { EthDataNotFoundError } from './errors.js';

/**
 * Estimated age (milliseconds) of a proof request, based on its block number.
 *
 * @param provider The Ethereum provider used to retrieve the block.
 * @param blockNumber The Ethereum block number that contains the proof request.
 * @returns The elapsed time between the block timestamp and the current clock,
 * expressed in milliseconds.
 */
export async function proofRequestAge(
    provider: EthereumProvider,
    blockNumber: bigint
): Promise<number> {

    const block = await provider.getBlock(blockNumber);
    if (!block) {
        throw new EthDataNotFoundError(`Failed to fetch block #${blockNumber}.`);
    }

    const blockTimestampSeconds = block.timestamp;
    const nowSeconds = Math.floor(Date.now() / 1000);
    const secondsAgo = nowSeconds - blockTimestampSeconds;

    return secondsAgo * 1000;
}
