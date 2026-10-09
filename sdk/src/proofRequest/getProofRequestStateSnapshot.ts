import {
    type Address,
    type GetAccountInfoApi,
    type GetMultipleAccountsApi,
    type Rpc,
} from '@solana/kit';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { classifyProofRequests } from './classifyProofRequests.js';
import { findRequestIdByTxHash } from '../rpc/eth/fetchProofRequest.js';
import { fetchProofRequestBatch } from '../rpc/eth/fetchProofRequestBatch.js';
import { request_witness, type RequestWitness } from '@nori-zk/ethereum-solana-proof-queue-utils-glam';
import type { ProofRequestStateGraph } from './proofRequest.js';

export interface ProofRequestStateSnapshotRequest {
    /** The Ethereum `NoriProofRequestQueue` address. */
    proofQueueAddress: string;
    /** The Ethereum transaction that enqueued the proof request. */
    proofRequestTxHash: string;
    /** The Nori Solana bridge program address. */
    programAddress?: Address;
}

/**
 * Where a proof request is, read once from the chains: the data of the
 * proof request machine's `unprocessed` or `proofAvailable` node, without
 * the machine's own count of failed reads.
 */
export type ProofRequestStateSnapshot =
    | Omit<(typeof ProofRequestStateGraph.nodes)['unprocessed'], 'failedReads'>
    | (typeof ProofRequestStateGraph.nodes)['proofAvailable'];

/**
 * Discovers where a proof request is, from Ethereum and Solana alone.
 *
 * @param provider The Ethereum provider.
 * @param rpc The Solana RPC.
 * @param request The addresses and the transaction that enqueued the request.
 * @returns The unprocessed or proof available state data.
 * @throws ProofRequestTransactionNotMinedError When the transaction is not mined yet.
 * @throws EthRpcTransportError When an Ethereum read still fails after its retries.
 * @throws SolanaRpcTransportError When a Solana read still fails after its retries.
 */
export async function getProofRequestStateSnapshot(
    provider: EthereumProvider,
    rpc: Rpc<GetAccountInfoApi & GetMultipleAccountsApi>,
    request: ProofRequestStateSnapshotRequest
): Promise<ProofRequestStateSnapshot> {
    const { requestId, blockNumber } = await findRequestIdByTxHash(
        provider,
        request.proofQueueAddress,
        request.proofRequestTxHash
    );
    const [snapshot] = await classifyProofRequests(
        rpc,
        [{ requestId, requestBlockNumber: BigInt(blockNumber) }],
        request.programAddress
    );
    return snapshot;
}

export class ProofRequestWitnessRootMismatchError extends Error {
    constructor(
        readonly rebuiltRoot: string,
        readonly committedRoot: string
    ) {
        super(
            `Rebuilt proof queue batch root ${rebuiltRoot} does not match the committed root ${committedRoot}.`
        );
        this.name = 'ProofRequestWitnessRootMismatchError';
    }
}

/**
 * Fetches every request in the proof request's committed batch from
 * Ethereum and builds its witness with `@nori-zk/ethereum-solana-proof-queue-utils-glam` (the SP1
 * guest's own hashing, compiled to WebAssembly), checked against the batch
 * root committed on Solana.
 *
 * @param provider The Ethereum provider.
 * @param proofAvailable The proof available state data.
 * @param proofQueueAddress The Ethereum `NoriProofRequestQueue` address.
 * @returns The request's leaf, its bottom-up path and the batch root.
 * @throws ProofRequestWitnessRootMismatchError When the rebuilt root differs from the committed root.
 */
export async function fetchProofRequestWitness(
    provider: EthereumProvider,
    proofAvailable: (typeof ProofRequestStateGraph.nodes)['proofAvailable'],
    proofQueueAddress: string
): Promise<RequestWitness> {
    const leaves = await fetchProofRequestBatch(
        provider,
        proofQueueAddress,
        proofAvailable.inputQueueCursor,
        proofAvailable.outputQueueCursor,
        Number(proofAvailable.previousOutputBlockNumber),
        Number(proofAvailable.outputBlockNumber)
    );
    const witness = request_witness({ leaves, index: Number(proofAvailable.indexInBatch) });
    if (witness.root !== proofAvailable.root.toLowerCase()) {
        throw new ProofRequestWitnessRootMismatchError(witness.root, proofAvailable.root);
    }
    return witness;
}
