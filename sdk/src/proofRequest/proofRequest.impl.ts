import { type Address } from '@solana/kit';
import { catchError, EMPTY, filter, map, merge, type Observable, of, switchMap, throwError, timer } from 'rxjs';
import { proofRequestSnapshots$ } from './proofRequestSnapshots.js';
import { type ConnectedReadClients, type ProofRequestConnections } from './connectedRead.js';
import {
    proofRequestStateSnapshot$,
    type ProofRequestStateSnapshot,
    type UndeterminedProofRequestSnapshot,
    type UnprocessedProofRequestSnapshot,
} from './getProofRequestStateSnapshot.js';
import { ProofRequestTransactionNotMinedError } from '../rpc/eth/errors.js';
import {
    type ReadRetryBackoff,
    startReadThroughConnectionsMachine,
    dueOn,
} from '../rpc/connection/readThroughConnections.impl.js';
import {
    DROPPED_AFTER_BLOCKS,
    dropped,
    replaced,
    type TransactionFollow,
    transactionFollow$,
    transactionFollowOf,
} from '../transaction/transactionFollow.js';
import { withOutcome } from '../utils/machines.js';
import { ProofRequestState } from './types.js';
import { ProofRequestStateGraph, type ProofRequestStateNodeUnion } from './proofRequest.js';

/** The proof request to follow: the transaction that enqueued it, and where. */
export interface FollowedProofRequest {
    /** The Ethereum `NoriProofRequestQueue` address. */
    proofQueueAddress: string;
    /** The Ethereum transaction that enqueued the proof request. */
    proofRequestTxHash: string;
    /** The Nori Solana bridge program address. */
    programAddress?: Address;
}

/**
 * A followed proof request: the transaction that enqueued it, that
 * transaction as last read, and where the request is, still followed
 * (`current`) or proven (`proofAvailable`).
 */
type FollowedSnapshot = {
    proofRequestTxHash: string;
    transaction: TransactionFollow;
    snapshot: Extract<ProofRequestStateNodeUnion, { node: 'current' | 'proofAvailable' }>['data']['snapshot'];
};

/** A committed batch covers the request: it is proven, and not read again. */
const proven = ({ snapshot }: FollowedSnapshot) => snapshot.state === ProofRequestState.ProofAvailable;

const UNDETERMINED: UndeterminedProofRequestSnapshot = { state: ProofRequestState.Undetermined };

/**
 * Starts following one Ethereum proof request until a committed proof queue
 * batch on Solana covers it, reading through both connections. It looks the
 * request up at once, then reads it again every `pollIntervalMs` and on
 * every `recheckTrigger$` emission until its proof is available. While the
 * transaction that enqueued it is not mined, each read follows that
 * transaction, and it ends `transactionReplaced` or `transactionDropped`
 * when the transaction never will be.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param request The transaction that enqueued the request (its starting data), and the addresses.
 * @param knownSnapshot Where the request is already known to be, to resume
 *   from there (`current`, or `proofAvailable` for a proven one); it is
 *   looked up from the transaction when omitted.
 * @param pollIntervalMs The delay between reads in ms (default: 15000).
 * @param recheckTrigger$ An extra read signal, e.g. bridge state changes from the websocket.
 * @param backoff How long a failed read waits before reading again.
 * @param droppedAfterBlocks Blocks the node may not know the enqueuing transaction before it counts as dropped (default: 12).
 * @returns The running machine; its states carry the snapshot. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export function createProofRequestStateMachine(
    connections: ProofRequestConnections,
    request: FollowedProofRequest,
    knownSnapshot?: ProofRequestStateSnapshot,
    pollIntervalMs = 15_000,
    recheckTrigger$: Observable<unknown> = EMPTY,
    backoff: ReadRetryBackoff = {},
    droppedAfterBlocks = DROPPED_AFTER_BLOCKS
) {
    const { proofQueueAddress, proofRequestTxHash, programAddress } = request;

    /**
     * Looks the request up from the transaction that enqueued it.
     *
     * @param clients The clients to read through.
     * @param txHash The transaction that enqueued it.
     * @returns Its snapshot, or `undetermined` while the transaction is not mined, once.
     */
    const lookUp$ = (clients: ConnectedReadClients, txHash: string): Observable<FollowedSnapshot['snapshot']> =>
        proofRequestStateSnapshot$(clients, { proofQueueAddress, proofRequestTxHash: txHash, programAddress }).pipe(
            catchError((error: unknown) =>
                error instanceof ProofRequestTransactionNotMinedError ? of(UNDETERMINED) : throwError(() => error)
            )
        );

    /**
     * Reads an unprocessed request again from the bridge.
     *
     * @param clients The clients to read through.
     * @param unprocessed The request as last read.
     * @returns Its snapshot, once.
     */
    const readAgain$ = (
        clients: ConnectedReadClients,
        { requestId, requestBlockNumber }: UnprocessedProofRequestSnapshot
    ): Observable<ProofRequestStateSnapshot> =>
        clients
            .solana((rpc) => proofRequestSnapshots$(rpc, [{ requestId, requestBlockNumber }], programAddress))
            .pipe(map(([snapshot]) => snapshot));

    /**
     * Follows the enqueuing transaction, then looks the request up once it is mined.
     *
     * @param clients The clients to read through.
     * @param followed The request as last read.
     * @returns The request as this read finds it, once.
     */
    const determine$ = (clients: ConnectedReadClients, followed: FollowedSnapshot): Observable<FollowedSnapshot> =>
        clients
            .ethereum((provider) => transactionFollow$(provider, followed.transaction))
            .pipe(
                switchMap((transaction) =>
                    transaction.receipt === undefined
                        ? of({ ...followed, transaction, snapshot: UNDETERMINED })
                        : lookUp$(clients, followed.proofRequestTxHash).pipe(
                              map((snapshot) => ({ ...followed, transaction, snapshot }))
                          )
                )
            );

    /** The enqueuing transaction never mined, and never will. */
    const transactionEnded = ({ snapshot, transaction }: FollowedSnapshot) =>
        snapshot.state === ProofRequestState.Undetermined &&
        (replaced(transaction) || dropped(transaction, droppedAfterBlocks));

    // A read can arrive at `proofAvailable`, so it returns the wider followed snapshot, not `current`'s.
    return startReadThroughConnectionsMachine<
        (typeof ProofRequestStateGraph)['nodes'],
        (typeof ProofRequestStateGraph)['edges'],
        FollowedSnapshot
    >(ProofRequestStateGraph, {
        connections,
        read: (clients, followed: FollowedSnapshot) =>
            followed.snapshot.state === ProofRequestState.Unprocessed
                ? readAgain$(clients, followed.snapshot).pipe(map((snapshot) => ({ ...followed, snapshot })))
                : determine$(clients, followed),
        refreshOn: () => dueOn(merge(timer(pollIntervalMs), recheckTrigger$)),
        // A read that finds the request proven, or its transaction ended, moves there, not to `current`.
        arrivedElsewhere: (followed) => proven(followed) || transactionEnded(followed),
        backoff,
        ownTransitions: (read$) => {
            const read = withOutcome(read$, 'succeeded').pipe(map(({ value }) => value));
            const ended = ({ proofRequestTxHash: txHash, transaction }: FollowedSnapshot) => ({
                proofRequestTxHash: txHash,
                transaction,
            });
            return {
                proven: {
                    $: () => read.pipe(filter(proven)),
                    next: ({ proofRequestTxHash: txHash, snapshot }: FollowedSnapshot) => ({
                        proofRequestTxHash: txHash,
                        snapshot,
                    }),
                },
                transactionReplaced: {
                    $: () =>
                        read.pipe(
                            filter(
                                ({ snapshot, transaction }) =>
                                    snapshot.state === ProofRequestState.Undetermined && replaced(transaction)
                            )
                        ),
                    next: ended,
                },
                transactionDropped: {
                    $: () =>
                        read.pipe(
                            filter(
                                ({ snapshot, transaction }) =>
                                    snapshot.state === ProofRequestState.Undetermined &&
                                    dropped(transaction, droppedAfterBlocks)
                            )
                        ),
                    next: ended,
                },
            };
        },
        // The transaction that enqueued the request is its starting data. A known
        // snapshot resumes from there instead of looking the request up; a proven one is done.
        start:
            knownSnapshot === undefined
                ? {
                      node: 'loading',
                      data: {
                          proofRequestTxHash,
                          transaction: transactionFollowOf(proofRequestTxHash),
                          snapshot: UNDETERMINED,
                          failedReads: 0,
                      },
                  }
                : knownSnapshot.state === ProofRequestState.ProofAvailable
                  ? { node: 'proofAvailable', data: { proofRequestTxHash, snapshot: knownSnapshot } }
                  : {
                        node: 'current',
                        data: {
                            proofRequestTxHash,
                            transaction: transactionFollowOf(proofRequestTxHash),
                            snapshot: knownSnapshot,
                        },
                    },
    });
}
