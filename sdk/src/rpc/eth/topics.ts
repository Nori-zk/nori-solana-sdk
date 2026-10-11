import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';

/** Which logs to receive (`eth_subscribe` `logs`): by emitting contract and by topics. */
export interface EthereumLogsFilter {
    address?: string | string[];
    topics?: (string | string[] | null)[];
}

/** The topic of `ProofRequested`, which the queue emits for each request it enqueues. */
export const PROOF_REQUESTED_TOPIC = NoriProofRequestQueue__factory.createInterface().getEvent('ProofRequested').topicHash;
