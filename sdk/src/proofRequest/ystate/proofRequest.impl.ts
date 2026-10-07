import {
    EMPTY,
    exhaustMap,
    filter,
    merge,
    type Observable,
    ReplaySubject,
    share,
    Subject,
    switchMap,
    take,
    timer,
} from 'rxjs';
import { type Address } from '@solana/kit';
import { classifyProofRequests } from '../classifyProofRequests.js';
import {
    bothReady$,
    type ConnectedRead,
    type ProofRequestConnections,
    readThroughConnections$,
    waitingOnChanged$,
} from '../connectedRead.js';
import {
    getProofRequestStateSnapshot,
    type ProofRequestStateSnapshot,
} from '../getProofRequestStateSnapshot.js';
import { ProofRequestTransactionNotMinedError } from '../rpc/eth/errors.js';
import { resolveHealthCheckTimings, retryDelayMs } from '../rpc/healthCheckTimings.js';
import { ProofRequestState } from '../types.js';
import { dataOnEntry$, stateOf$, type StartedMachine } from './dataOnEntry.js';
import { ProofRequestStateGraph, type ProofRequestStateNodeUnion } from './proofRequest.js';
import { type ReadRetryBackoff } from './proofRequestHistory.impl.js';

/** The proof request to follow: the transaction that enqueued it, and where. */
export interface FollowedProofRequest {
    /** The Ethereum `NoriProofRequestQueue` address. */
    proofQueueAddress: string;
    /** The Ethereum transaction that enqueued the proof request. */
    proofRequestTxHash: string;
    /** The Nori Solana bridge program address. */
    programAddress?: Address;
}

/** A proof request's state as read: `undefined` while its transaction is not mined. */
type Lookup = ConnectedRead<ProofRequestStateSnapshot | undefined>;
/** A lookup that found the request, narrowed to the state it found. */
type Found<TState extends ProofRequestStateSnapshot['state']> = {
    outcome: 'succeeded';
    value: Extract<ProofRequestStateSnapshot, { state: TState }>;
};

/**
 * Keeps only the lookups with one outcome, narrowed to it.
 *
 * @param lookup$ Lookups.
 * @param outcome The outcome to keep.
 * @returns The lookups with that outcome.
 */
function withOutcome<TOutcome extends Exclude<Lookup['outcome'], 'succeeded'>>(
    lookup$: Observable<Lookup>,
    outcome: TOutcome
) {
    return lookup$.pipe(
        filter((lookup): lookup is Extract<Lookup, { outcome: TOutcome }> => lookup.outcome === outcome)
    );
}

/**
 * Keeps only the lookups that found the request in one state, narrowed to it.
 *
 * @param lookup$ Lookups.
 * @param state The state to keep.
 * @returns The lookups that found the request in that state.
 */
function found<TState extends ProofRequestStateSnapshot['state']>(
    lookup$: Observable<Lookup>,
    state: TState
) {
    return lookup$.pipe(
        filter(
            (lookup): lookup is Found<TState> =>
                lookup.outcome === 'succeeded' && lookup.value?.state === state
        )
    );
}

/**
 * Whether a lookup ends the wait: anything except finding the request where
 * the machine already is (`keepWaitingIn`), or not finding its transaction
 * mined yet.
 *
 * @param keepWaitingIn The state the machine polls in.
 * @returns A predicate on lookups.
 */
const endsTheWait =
    (keepWaitingIn: ProofRequestStateSnapshot['state'] | undefined) =>
    (lookup: Lookup): boolean =>
        lookup.outcome !== 'succeeded' || (lookup.value !== undefined && lookup.value.state !== keepWaitingIn);

/**
 * Starts following one Ethereum proof request until a committed proof queue
 * batch on Solana covers it, reading through both connections. It checks on
 * entry to `undetermined` and `unprocessed`, then every `pollIntervalMs` and
 * on every `recheckTrigger$` emission.
 *
 * @param connections The Ethereum provider and Solana RPC, with their running connectivity machines.
 * @param request The transaction that enqueued the request, and the addresses.
 * @param knownSnapshot Where the request is already known to be, to resume
 *   from there; it is looked up from the transaction when omitted.
 * @param pollIntervalMs The delay between checks in ms (default: 15000).
 * @param recheckTrigger$ An extra check signal, e.g. bridge state changes from the websocket.
 * @param backoff How long a failed read waits before reading again.
 * @returns
 *   - `proofRequestState`: the running machine.
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export function createProofRequestStateMachine(
    connections: ProofRequestConnections,
    request: FollowedProofRequest,
    knownSnapshot?: ProofRequestStateSnapshot,
    pollIntervalMs = 15_000,
    recheckTrigger$: Observable<unknown> = EMPTY,
    backoff: ReadRetryBackoff = {}
) {
    const timings = resolveHealthCheckTimings(backoff);
    const retry$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<StartedMachine<ProofRequestStateNodeUnion>>(1);
    const state$ = stateOf$(started$);
    const checkDue$ = merge(timer(0, pollIntervalMs), recheckTrigger$);

    // The outcome edges of `undetermined` share one lookup per entry into
    // it, repeated until it finds the request or something goes wrong. A
    // transaction that is not mined yet is waited for, not a failure.
    const lookup$ = checkDue$.pipe(
        exhaustMap(() =>
            readThroughConnections$(connections, async ({ provider, rpc }) => {
                try {
                    return await getProofRequestStateSnapshot({ ...request, provider, rpc });
                } catch (error) {
                    if (error instanceof ProofRequestTransactionNotMinedError) return undefined;
                    throw error;
                }
            })
        ),
        filter(endsTheWait(undefined)),
        take(1),
        share()
    );
    // The outcome edges of `unprocessed` share one check per entry into it,
    // of the request it carries, repeated until a batch covers it or
    // something goes wrong.
    const commitCheck$ = dataOnEntry$<ProofRequestStateNodeUnion, 'unprocessed'>(state$, 'unprocessed').pipe(
        switchMap(({ requestId, requestBlockNumber }) =>
            checkDue$.pipe(
                exhaustMap(() =>
                    readThroughConnections$(
                        connections,
                        async ({ rpc }): Promise<ProofRequestStateSnapshot | undefined> => {
                            const [snapshot] = await classifyProofRequests(
                                rpc,
                                [{ requestId, requestBlockNumber }],
                                request.programAddress
                            );
                            return snapshot;
                        }
                    )
                ),
                filter(endsTheWait(ProofRequestState.Unprocessed)),
                take(1)
            )
        ),
        share()
    );
    const eitherCheck$ = merge(lookup$, commitCheck$);
    // A failed read waits before reading again, doubling with each one in a row.
    const retryDue$ = merge(
        dataOnEntry$<ProofRequestStateNodeUnion, 'failedWhileUndetermined'>(state$, 'failedWhileUndetermined'),
        dataOnEntry$<ProofRequestStateNodeUnion, 'failedWhileUnprocessed'>(state$, 'failedWhileUnprocessed')
    ).pipe(
        take(1),
        switchMap(({ failedReads }) => timer(retryDelayMs(failedReads, timings)))
    );

    const machine = ProofRequestStateGraph.implement({
        checkWhetherProofRequestIsUnprocessed: {
            $: () => found(lookup$, ProofRequestState.Unprocessed),
            next: ({ value }) => ({ ...value, failedReads: 0 }),
        },
        checkWhetherProofIsAvailable: {
            $: () => found(lookup$, ProofRequestState.ProofAvailable),
            next: ({ value }) => value,
        },
        checkWhetherUnprocessedProofRequestIsCommitted: {
            $: () => found(commitCheck$, ProofRequestState.ProofAvailable),
            next: ({ value }) => value,
        },
        connectionLost: {
            $: () => withOutcome(eitherCheck$, 'connectionLost'),
            next: ({ waitingOn }, _dest, source) => ({ ...source, waitingOn }),
        },
        readFailedOnHealthyConnection: {
            $: () => withOutcome(eitherCheck$, 'failedOnHealthyConnection'),
            next: ({ error }, _dest, source) => ({ ...source, failedReads: source.failedReads + 1, error }),
        },
        readFailed: {
            $: () => withOutcome(eitherCheck$, 'failed'),
            next: ({ error }, _dest, source) => ({ ...source, failedReads: source.failedReads + 1, error }),
        },
        connectionsChanged: {
            $: () => waitingOnChanged$(connections),
            next: (waitingOn, _dest, source) => ({ ...source, waitingOn }),
        },
        connectionRestored: {
            $: () => bothReady$(connections),
            next: (_restored, _dest, { waitingOn: _waitingOn, ...resumed }) => resumed,
        },
        retryDue: {
            $: () => retryDue$,
            next: (_due, _dest, { error: _error, ...resumed }) => resumed,
        },
        retry: {
            $: () => retry$,
            next: (_retry, _dest, { error: _error, ...resumed }) => resumed,
        },
        close: {
            $: () => close$,
            next: () => ({}),
        },
    });

    const startingMachine = machine.close();
    const proofRequestState =
        knownSnapshot?.state === ProofRequestState.ProofAvailable
            ? startingMachine.start('proofAvailable', undefined, { proofAvailable: knownSnapshot })
            : knownSnapshot?.state === ProofRequestState.Unprocessed
              ? startingMachine.start('unprocessed', undefined, {
                    unprocessed: { ...knownSnapshot, failedReads: 0 },
                })
              : startingMachine.start('undetermined');
    started$.next(proofRequestState);

    return {
        proofRequestState,
        retry: () => retry$.next(),
        close: () => close$.next(),
    };
}
