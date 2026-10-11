import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { type Observable } from 'rxjs';
import { ethRpcRead$ } from './ethRpcRead.js';

/**
 * The queue's `head`: how many proof requests have ever been enqueued, which
 * is also the id the next request gets.
 *
 * @param provider The Ethereum provider used for the read.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @returns The queue's head, once.
 */
export const proofQueueHead$ = (provider: EthereumProvider, proofQueueAddress: string): Observable<bigint> =>
    ethRpcRead$(
        () => NoriProofRequestQueue__factory.connect(proofQueueAddress, provider).head(),
        'Failed to read the proof request queue head.'
    );
