import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { map, type Observable } from 'rxjs';
import { EthDataNotFoundError } from './errors.js';
import { ethRpcRead$ } from './ethRpcRead.js';

/**
 * When an Ethereum block was produced.
 *
 * @param provider The Ethereum provider used to read the block.
 * @param blockNumber The block.
 * @returns Its timestamp, in ms since the epoch, once.
 */
export const blockTimestamp$ = (provider: EthereumProvider, blockNumber: number): Observable<number> =>
    ethRpcRead$(() => provider.getBlock(blockNumber), `Failed to read block #${blockNumber}.`).pipe(
        map((block) => {
            if (!block) throw new EthDataNotFoundError(`The node has no block #${blockNumber}.`);
            return block.timestamp * 1000;
        })
    );
