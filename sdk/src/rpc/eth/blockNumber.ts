import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { map, type Observable } from 'rxjs';
import { EthDataNotFoundError } from './errors.js';
import { ethRpcRead$ } from './ethRpcRead.js';

/**
 * The number of the Ethereum block `tag` names: the latest, or the latest
 * finalized. The bridge proves only finalized state, so a request in a later
 * block waits for finality before any proof can cover it.
 *
 * @param provider The provider read through.
 * @param tag `latest` or `finalized`.
 * @returns The block's number, once; errors with `EthRpcTransportError` when
 *   the read fails, `EthDataNotFoundError` when the node has no such block.
 */
export const blockNumber$ = (provider: EthereumProvider, tag: 'latest' | 'finalized'): Observable<number> =>
    ethRpcRead$(() => provider.getBlock(tag), `Failed to read the ${tag} block.`).pipe(
        map((block) => {
            if (block === null) throw new EthDataNotFoundError(`The node has no ${tag} block.`);
            return block.number;
        })
    );
