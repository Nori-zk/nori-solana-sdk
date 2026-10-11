import {
    catchError,
    distinctUntilChanged,
    EMPTY,
    exhaustMap,
    filter,
    map,
    NEVER,
    type Observable,
    of,
    ReplaySubject,
    scan,
    skipWhile,
    startWith,
    Subject,
    switchMap,
    take,
    throwError,
    timer,
} from 'rxjs';
import { type RunningMachine } from '@yaw-rx/ystate';
import { dataOnEntry$, heldAcrossMoves, stateOf$, stateOnEntry$, type StartedMachine } from '../../utils/machines.js';
import { messageOf } from '../../utils/messageOf.js';
import { MAX_BLOCK_RANGE_PER_QUERY } from '../eth/blockRanges.js';
import { blockNumber$ } from '../eth/blockNumber.js';
import { ethRpcRead$ } from '../eth/ethRpcRead.js';
import { type EthereumLogsFilter } from '../eth/topics.js';
import { ChainChangesGraph, type ChainChangesState } from './chainChanges.js';
import { type SubscriptionEvent, SubscriptionRefusedError } from './jsonRpcTopic.js';
import {
    type Ethereum,
    ethereumCallsUsable$,
    forCalls$,
    forLogs$,
    subscriptionsOf,
    type SubscriptionTransport,
} from './connections.js';

/** A state of the chain changes machine but `closed`. */
type FollowingState = Exclude<ChainChangesState, { node: 'closed' }>;

/** Where a poll got to: the latest block or slot, and whether the ones it covered changed the chain. */
export type Poll = {
    lastPolled: number;
    changed: boolean;
};

/**
 * How one chain's changes machine subscribes and polls: what is the chain's
 * own. `TTransport` is the transports it subscribes on.
 */
export interface ChainChangesOptions<TTransport extends SubscriptionTransport> {
    /** The transports in the chain's subscriptions order that are usable, in that order, as it changes. */
    subscribable$: Observable<TTransport[]>;
    /**
     * The subscription on each transport: the node's acknowledgement, then
     * each push; given the running machine, once started, whose turns its
     * requests take.
     */
    pushesOn: (started$: Observable<RunningMachine>) => Record<TTransport, Observable<SubscriptionEvent<unknown>>>;
    /**
     * One poll from `lastPolled`, its requests on the machine's turns: the
     * latest block or slot, and whether the chain changed since; `undefined`
     * when none arrived. The first poll, with no `lastPolled`, only reads the
     * latest one.
     */
    poll$: (lastPolled: number | undefined, machine: RunningMachine) => Observable<Poll | undefined>;
    /** Whether the chain can serve a poll, as it changes. */
    callsUsable$: Observable<boolean>;
    /** How often it polls while no transport can subscribe. */
    pollIntervalMs: number;
}

/**
 * Whether the machine follows the chain in a state: subscribed and
 * acknowledged, or polled at least once.
 *
 * @param data A state's data.
 * @returns `true` once it has.
 */
const followed = (data: FollowingState['data']): boolean =>
    'acknowledged' in data ? data.acknowledged : data.lastPolled !== undefined;

/**
 * Starts a chain changes machine over one chain's subscriptions and polls:
 * subscribed on the first transport in its subscriptions order that is
 * usable and serves the subscription, else polling.
 *
 * @param options How the chain subscribes and polls.
 * @returns The running machine; `changes` counts the changes. Its control:
 *   - `close()`: moves the machine to `closed`.
 */
export function startChainChangesMachine<TTransport extends SubscriptionTransport>({
    subscribable$,
    pushesOn,
    poll$,
    callsUsable$,
    pollIntervalMs,
}: ChainChangesOptions<TTransport>) {
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<StartedMachine<ChainChangesState>>(1);
    const running$ = new ReplaySubject<RunningMachine>(1);
    const state$ = stateOf$(started$);
    const subscriptions = pushesOn(running$);

    /**
     * The transport to subscribe on: the first usable one not in
     * `unsupported`, as it changes; `undefined` while there is none. The
     * transport a subscription was lost on (`lostOn`) is left out until it
     * has been unusable once.
     */
    const chosen$ = (unsupported: string[], lostOn?: string) =>
        subscribable$.pipe(
            scan(
                ({ waiting }, usable) => ({
                    waiting: waiting && lostOn !== undefined && usable.some((transport) => transport === lostOn),
                    usable,
                }),
                { waiting: true, usable: [] as SubscriptionTransport[] }
            ),
            map(({ waiting, usable }) =>
                usable.find((transport) => !unsupported.includes(transport) && !(waiting && transport === lostOn))
            ),
            distinctUntilChanged()
        );

    const entered$ = stateOnEntry$(state$).pipe(
        filter((state): state is FollowingState => state.node !== 'closed')
    );

    const machine = ChainChangesGraph.implement({
        pushes: {
            $: () =>
                dataOnEntry$(state$, 'subscribed').pipe(
                    // A state's data holds the transport's name as a string: one `subscribable$` gave.
                    switchMap(({ transport }) =>
                        subscriptions[transport as TTransport].pipe(
                            // Only a refusal is an error; a subscription ending any other way is lost.
                            catchError((error: unknown) =>
                                error instanceof SubscriptionRefusedError ? throwError(() => error) : EMPTY
                            )
                        )
                    )
                ),
            next: (push, _dest, source) =>
                push.kind === 'acknowledged'
                    ? { ...source, acknowledged: true }
                    : { ...source, changes: source.changes + 1 },
            error: (_refused, _dest, source) => ({
                changes: source.changes + 1,
                unsupported: [...source.unsupported, source.transport],
                lastPolled: undefined,
                lostOn: undefined,
            }),
            complete: (_lost, _dest, source) => ({
                changes: source.changes + 1,
                unsupported: source.unsupported,
                lastPolled: undefined,
                lostOn: source.transport,
            }),
        },
        subscribable: {
            $: () =>
                entered$.pipe(
                    switchMap((state) =>
                        chosen$(state.data.unsupported, 'lostOn' in state.data ? state.data.lostOn : undefined).pipe(
                            filter(
                                (transport): transport is SubscriptionTransport =>
                                    transport !== undefined &&
                                    (state.node !== 'subscribed' || transport !== state.data.transport)
                            )
                        )
                    )
                ),
            next: (transport, _dest, source) => ({
                changes: source.changes + (followed(source) ? 1 : 0),
                unsupported: source.unsupported,
                transport,
                acknowledged: false,
            }),
        },
        unsubscribable: {
            $: () =>
                dataOnEntry$(state$, 'subscribed').pipe(
                    switchMap(({ unsupported }) => chosen$(unsupported)),
                    filter((transport) => transport === undefined)
                ),
            next: (_none, _dest, source) => ({
                changes: source.changes + 1,
                unsupported: source.unsupported,
                lastPolled: undefined,
                lostOn: undefined,
            }),
        },
        polls: {
            $: () =>
                dataOnEntry$(state$, 'polling').pipe(
                    switchMap(({ lastPolled }) =>
                        // It polls only while the chain can serve a poll, and waits in `polling` otherwise.
                        callsUsable$.pipe(
                            switchMap((usable) =>
                                !usable
                                    ? NEVER
                                    : // The first poll goes out at once: it is where the machine starts following from.
                                      (lastPolled === undefined
                                          ? timer(pollIntervalMs, pollIntervalMs).pipe(startWith(0))
                                          : timer(pollIntervalMs, pollIntervalMs)
                                      ).pipe(
                                          exhaustMap(() =>
                                              running$.pipe(
                                                  take(1),
                                                  switchMap((machine) => poll$(lastPolled, machine))
                                              )
                                          ),
                                          filter((poll): poll is Poll => poll !== undefined)
                                      )
                            )
                        )
                    )
                ),
            next: ({ lastPolled, changed }, _dest, source) => ({
                ...source,
                lastPolled,
                changes: source.changes + (changed ? 1 : 0),
            }),
            error: (failure: unknown, _dest, source) => ({ ...source, error: messageOf(failure) }),
        },
        pollDue: {
            $: () => timer(pollIntervalMs),
            next: (_due, _dest, source) => ({
                changes: source.changes,
                unsupported: source.unsupported,
                lastPolled: source.lastPolled,
                lostOn: source.lostOn,
            }),
        },
        close: {
            $: () => close$,
            next: () => ({}),
        },
    });

    const running = machine.close().start('polling');
    started$.next(running);
    running$.next(running);

    return Object.assign(running, { close: () => close$.next() });
}

/**
 * Starts following whether Ethereum changes: subscribed on the first
 * transport in its subscriptions order that is usable and serves the
 * subscription, else polling through its calls order.
 *
 * @param chain The Ethereum chain.
 * @param logsFilter The logs that count as a change; every new block when omitted.
 * @returns The running machine; `changes` counts the changes. Its control:
 *   - `close()`: moves the machine to `closed`.
 */
export function createChainChangesMachine(chain: Ethereum, logsFilter?: EthereumLogsFilter) {
    const { subscribable$, ethSubscribe$, pollIntervalMs } = subscriptionsOf(chain);
    const params = logsFilter === undefined ? ['newHeads'] : ['logs', logsFilter];
    return startChainChangesMachine({
        subscribable$,
        // One subscription per transport, kept while a push loops back to `subscribed`.
        pushesOn: (started$) => ({
            websocket: started$
                .pipe(
                    take(1),
                    switchMap((machine) => ethSubscribe$('websocket', params, machine))
                )
                .pipe(heldAcrossMoves()),
            wallet: started$
                .pipe(
                    take(1),
                    switchMap((machine) => ethSubscribe$('wallet', params, machine))
                )
                .pipe(heldAcrossMoves()),
        }),
        // A gap longer than one log query counts as one change.
        poll$: (lastBlock, machine) =>
            forCalls$(chain, machine, (provider) => blockNumber$(provider, 'latest')).pipe(
                switchMap((latest) => {
                    if (lastBlock === undefined) return of({ lastPolled: latest, changed: false });
                    if (latest <= lastBlock) return of(undefined);
                    if (logsFilter === undefined || latest - lastBlock > MAX_BLOCK_RANGE_PER_QUERY)
                        return of({ lastPolled: latest, changed: true });
                    return forLogs$(chain, machine, (provider) =>
                        ethRpcRead$(
                            () => provider.getLogs({ ...logsFilter, fromBlock: lastBlock + 1, toBlock: latest }),
                            `Log query over blocks ${lastBlock + 1}-${latest} failed.`
                        )
                    ).pipe(map((logs) => ({ lastPolled: latest, changed: logs.length > 0 })));
                })
            ),
        callsUsable$: ethereumCallsUsable$(chain),
        pollIntervalMs,
    });
}

/** A running chain changes machine, with `close()`: Ethereum's, or Solana's (`createSolanaChangesMachine`). */
export type ChainChanges = ReturnType<typeof startChainChangesMachine>;

/**
 * A running chain changes machine's changes, for a reading machine's
 * `refreshOn` to gate on: the count of changes once the machine follows the
 * chain (subscribed and acknowledged, or its first poll has read the latest
 * block), then at each change. It starts nothing: the machine is started
 * and closed by its owner.
 *
 * @param changes The running chain changes machine.
 * @returns The count of changes: first where it follows from, then at each change.
 */
export const changesOf$ = (changes: ChainChanges): Observable<number> =>
    changes.state$.pipe(
        filter((state): state is FollowingState => state.node !== 'closed'),
        skipWhile(({ data }) => !followed(data)),
        map(({ data }) => data.changes),
        distinctUntilChanged()
    );
