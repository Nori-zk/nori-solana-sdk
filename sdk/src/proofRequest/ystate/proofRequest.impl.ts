import { filter, type Observable } from 'rxjs';
import {
    createProofRequestStateSnapshotTransition$,
    type ProofRequestStateSnapshotRequest,
} from '../getProofRequestStateSnapshot.js';
import { ProofRequestState } from '../types.js';
import { ProofRequestStateGraph, type ProofRequestStateNodeUnion } from './proofRequest.js';

/**
 * Creates the proof request state machine for one Ethereum proof request.
 *
 * @param request The connections and the transaction that enqueued the request.
 * @param initial The node from which the machine begins.
 * @param pollIntervalMs The delay between state checks in milliseconds.
 * @param recheckTrigger$ Optional extra recheck signal, e.g. bridge websocket stage changes.
 * @returns An implemented YState machine for the proof request lifecycle.
 */
export function createProofRequestStateMachine(
    request: ProofRequestStateSnapshotRequest,
    initial: ProofRequestStateNodeUnion = { node: 'undetermined', data: {} },
    pollIntervalMs = 15_000,
    recheckTrigger$?: Observable<unknown>
) {
    const checkProofRequestState$ = createProofRequestStateSnapshotTransition$(
        request,
        initial,
        pollIntervalMs,
        recheckTrigger$
    );

    return ProofRequestStateGraph.implement({
        checkWhetherProofRequestIsUnprocessed: {
            $: () =>
                checkProofRequestState$().pipe(
                    filter(
                        (
                            snapshot
                        ): snapshot is (typeof ProofRequestStateGraph.nodes)['unprocessed'] =>
                            snapshot.state === ProofRequestState.Unprocessed
                    )
                ),
            next: (snapshot) => snapshot,
        },
        checkWhetherProofIsAvailable: {
            $: () =>
                checkProofRequestState$().pipe(
                    filter(
                        (
                            snapshot
                        ): snapshot is (typeof ProofRequestStateGraph.nodes)['proofAvailable'] =>
                            snapshot.state === ProofRequestState.ProofAvailable
                    )
                ),
            next: (snapshot) => snapshot,
        },
        checkWhetherUnprocessedProofRequestIsCommitted: {
            $: () =>
                checkProofRequestState$().pipe(
                    filter(
                        (
                            snapshot
                        ): snapshot is (typeof ProofRequestStateGraph.nodes)['proofAvailable'] =>
                            snapshot.state === ProofRequestState.ProofAvailable
                    )
                ),
            next: (snapshot) => snapshot,
        },
    });
}
