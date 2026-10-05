import {
    type Address,
    type GetAccountInfoApi,
    type GetMultipleAccountsApi,
    type ReadonlyUint8Array,
    type Rpc,
} from '@solana/kit';
import { EMPTY, exhaustMap, from, merge, Observable, timer } from 'rxjs';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { findRequestIdByTxHash } from './rpc/eth/fetchProofRequest.js';
import { fetchProofRequestBatch } from './rpc/eth/fetchProofRequestBatch.js';
import { fetchBridgeState } from './rpc/solana/fetchBridgeState.js';
import { fetchProofQueueBatches } from './rpc/solana/fetchProofQueueBatches.js';
import { findProofQueueBatch } from './rpc/solana/findProofQueueBatch.js';
import { findProofQueueBatchPda } from './rpc/solana/findProofQueueBatchPda.js';
import { ProofRequestState } from './types.js';
import { request_witness, type RequestWitness } from '@nori-zk/nori-hash-utils';
import {
    ProofRequestStateGraph,
    type ProofRequestStateNodeUnion,
} from './ystate/proofRequest.js';

export interface ProofRequestStateSnapshotRequest {
    /** Solana RPC, e.g. from the user's wallet adapter. */
    rpc: Rpc<GetAccountInfoApi & GetMultipleAccountsApi>;
    /** Ethereum provider, e.g. the user's wallet provider. */
    provider: EthereumProvider;
    /** The Ethereum `NoriProofRequestQueue` address. */
    proofQueueAddress: string;
    /** The Ethereum transaction that enqueued the proof request. */
    proofRequestTxHash: string;
    /** The Nori Solana bridge program address. */
    programAddress?: Address;
}

export type ProofRequestStateSnapshot = (typeof ProofRequestStateGraph.nodes)[
    | 'unprocessed'
    | 'proofAvailable'];

function toHex(bytes: ReadonlyUint8Array): string {
    return `0x${Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;
}

async function classifyProofRequest(
    request: ProofRequestStateSnapshotRequest,
    requestId: bigint,
    requestBlockNumber: bigint
): Promise<ProofRequestStateSnapshot> {
    const programAddress = request.programAddress ?? TOKEN_PROGRAM_ADDRESS;
    const { queueCursor, proofQueueBatchCount } = await fetchBridgeState(
        request.rpc,
        programAddress
    );

    if (requestId >= queueCursor) {
        return {
            state: ProofRequestState.Unprocessed,
            requestId,
            requestBlockNumber,
            queueCursor,
            proofQueueBatchCount,
        };
    }

    const { proofQueueBatchIndex, proofQueueBatch } = await findProofQueueBatch(
        request.rpc,
        requestId,
        proofQueueBatchCount,
        programAddress
    );
    const previousOutputBlockNumber =
        proofQueueBatchIndex === 0n
            ? -1n // sentinel: no previous proof queue batch (first-ever batch)
            : (
                  await fetchProofQueueBatches(
                      request.rpc,
                      [proofQueueBatchIndex - 1n],
                      programAddress
                  )
              )[0].outputBlockNumber;
    const [proofQueueBatchAddress] = await findProofQueueBatchPda(
        proofQueueBatchIndex,
        programAddress
    );

    return {
        state: ProofRequestState.ProofAvailable,
        requestId,
        requestBlockNumber,
        queueCursor,
        proofQueueBatchIndex,
        proofQueueBatchAddress,
        root: toHex(proofQueueBatch.root),
        inputQueueCursor: proofQueueBatch.inputQueueCursor,
        outputQueueCursor: proofQueueBatch.outputQueueCursor,
        outputBlockNumber: proofQueueBatch.outputBlockNumber,
        previousOutputBlockNumber,
        indexInBatch: requestId - proofQueueBatch.inputQueueCursor,
    };
}

/**
 * Discovers where a proof request is, from Ethereum and Solana alone.
 *
 * @param request The connections and the transaction that enqueued the request.
 * @returns The unprocessed or proof available state data.
 */
export async function getProofRequestStateSnapshot(
    request: ProofRequestStateSnapshotRequest
): Promise<ProofRequestStateSnapshot> {
    const { requestId, blockNumber } = await findRequestIdByTxHash(
        request.proofQueueAddress,
        request.proofRequestTxHash,
        request.provider
    );
    return classifyProofRequest(request, requestId, BigInt(blockNumber));
}

/**
 * Refreshes a previously discovered proof request state. `proofAvailable`
 * is terminal (proof queue batches are append-only) and is returned as is.
 *
 * @param current The current node and its state data.
 * @param request The connections and the transaction that enqueued the request.
 * @returns The refreshed unprocessed or proof available state data.
 */
export async function recheckProofRequestStateSnapshot(
    current: ProofRequestStateNodeUnion,
    request: ProofRequestStateSnapshotRequest
): Promise<ProofRequestStateSnapshot> {
    if (current.node === ProofRequestState.Undetermined) {
        return getProofRequestStateSnapshot(request);
    }
    if (current.node === ProofRequestState.ProofAvailable) {
        return current.data;
    }
    return classifyProofRequest(request, current.data.requestId, current.data.requestBlockNumber);
}

function toProofRequestStateNode(snapshot: ProofRequestStateSnapshot): ProofRequestStateNodeUnion {
    return {
        node: snapshot.state,
        data: snapshot,
    } as unknown as ProofRequestStateNodeUnion;
}

/**
 * Re-checks the proof request on every `pollIntervalMs` tick and on every
 * `recheckTrigger$` emission (for example bridge stage changes from the
 * bridge websocket), emitting once and completing when its state changes.
 * Checks never overlap: a trigger arriving while one runs is dropped.
 */
function waitForProofRequestStateChange(
    current: ProofRequestStateNodeUnion,
    request: ProofRequestStateSnapshotRequest,
    setLatest: (snapshot: ProofRequestStateSnapshot) => void,
    pollIntervalMs: number,
    recheckTrigger$: Observable<unknown> = EMPTY
) {
    return new Observable<ProofRequestStateSnapshot>((subscriber) =>
        merge(timer(0, pollIntervalMs), recheckTrigger$)
            .pipe(exhaustMap(() => from(recheckProofRequestStateSnapshot(current, request))))
            .subscribe({
                next: (snapshot) => {
                    setLatest(snapshot);
                    if (current.node !== snapshot.state) {
                        subscriber.next(snapshot);
                        subscriber.complete();
                    }
                },
                error: (error) => subscriber.error(error),
            })
    );
}

/**
 * Creates a cold polling transition factory for one proof request.
 *
 * @param request The connections and the transaction that enqueued the request.
 * @param initial The node from which the first transition begins.
 * @param pollIntervalMs The delay between state checks in milliseconds.
 * @param recheckTrigger$ Optional extra recheck signal, e.g. bridge websocket stage changes.
 * @returns A function that accepts a proof request node and emits its next state change.
 */
export function createProofRequestStateSnapshotTransition$(
    request: ProofRequestStateSnapshotRequest,
    initial: ProofRequestStateNodeUnion = { node: 'undetermined', data: {} },
    pollIntervalMs = 15_000,
    recheckTrigger$?: Observable<unknown>
) {
    let latest = initial;
    const setLatest = (snapshot: ProofRequestStateSnapshot) => {
        latest = toProofRequestStateNode(snapshot);
    };

    return (current: ProofRequestStateNodeUnion = latest) => {
        if (current.node === ProofRequestState.ProofAvailable) {
            return EMPTY;
        }
        return waitForProofRequestStateChange(
            current,
            request,
            setLatest,
            pollIntervalMs,
            recheckTrigger$
        );
    };
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
 * Ethereum and builds its witness with `@nori-zk/nori-hash-utils` (the SP1
 * guest's own hashing, compiled to WebAssembly), checked against the batch
 * root committed on Solana.
 *
 * @param proofAvailable The proof available state data.
 * @param request The connections and the transaction that enqueued the request.
 * @returns The request's leaf, its bottom-up path and the batch root.
 * @throws ProofRequestWitnessRootMismatchError When the rebuilt root differs from the committed root.
 */
export async function fetchProofRequestWitness(
    proofAvailable: (typeof ProofRequestStateGraph.nodes)['proofAvailable'],
    request: Pick<ProofRequestStateSnapshotRequest, 'provider' | 'proofQueueAddress'>
): Promise<RequestWitness> {
    const leaves = await fetchProofRequestBatch(
        request.proofQueueAddress,
        proofAvailable.inputQueueCursor,
        proofAvailable.outputQueueCursor,
        Number(proofAvailable.previousOutputBlockNumber),
        Number(proofAvailable.outputBlockNumber),
        request.provider
    );
    const witness = request_witness({ leaves, index: Number(proofAvailable.indexInBatch) });
    if (witness.root !== proofAvailable.root.toLowerCase()) {
        throw new ProofRequestWitnessRootMismatchError(witness.root, proofAvailable.root);
    }
    return witness;
}
