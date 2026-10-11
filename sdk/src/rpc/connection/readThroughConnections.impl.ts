import {
    filter,
    map,
    merge,
    type Observable,
    ReplaySubject,
    scan,
    share,
    startWith,
    Subject,
    switchMap,
    take,
    takeUntil,
    timer,
} from 'rxjs';
import {
    bothReady$,
    type ConnectedRead,
    type ConnectedReadClients,
    needsNotReady$,
    type ProofRequestConnections,
    type ReadNeed,
    readThroughConnections$,
    waitingOnChanged$,
} from '../../proofRequest/connectedRead.js';
import {
    type EdgeDef,
    type IncidenceGraphSetMixin,
    type NodeData,
    type StateUnion,
    type TransitionDef,
    type TransitionNames,
    type Widen,
} from '@yaw-rx/ystate';
import {
    dataOnEntry$,
    type GraphState,
    heldAcrossMoves,
    stateOf$,
    type StartedMachine,
    withOutcome,
} from '../../utils/machines.js';
import { type HealthCheckTimings, resolveHealthCheckTimings, retryDelayMs } from './healthCheckTimings.js';
import { type ReadThroughConnectionsState } from './readThroughConnections.js';

/** How long a failed read waits before reading again. */
export type ReadRetryBackoff = Pick<HealthCheckTimings, 'retryBackoff'>;

/** What the read machine's states carry beyond the reading graph's own data. */
const READ_FIELDS = ['failedReads', 'waitingOn', 'error'] as const;

/**
 * A state's data without what the read machine adds to it: the reading
 * graph's own data.
 *
 * @param data A state's data.
 * @returns The data, without `failedReads`, `waitingOn` and `error`.
 */
function ownDataOf<TData extends object>(data: unknown): TData {
    const own = { ...(data as Record<string, unknown>) };
    for (const field of READ_FIELDS) delete own[field];
    return own as TData;
}

/**
 * `refreshOn` for a trigger with no starting point to wait for (a timer, a
 * signal, a wallet event, or `NEVER`): following at once, a refresh due at
 * each emission.
 *
 * @param trigger$ What makes a refresh due.
 * @returns The trigger, as `refreshOn` takes it.
 */
export const dueOn = (trigger$: Observable<unknown>): Observable<unknown> => trigger$.pipe(startWith(undefined));

/** How one read ended, with the data it was read from and the node that read it. */
export type ReadThroughConnectionsRead<TData, TPrevious = TData> = ConnectedRead<TData> & {
    previous: TPrevious;
    node: 'loading' | 'refreshing';
};

/**
 * What a graph built with `readThroughConnectionsOf` reads, and when it
 * reads again: `TData` is what a read returns, `TPrevious` the data it reads
 * from, the shape of the graph's `current` node.
 */
export interface ReadThroughConnectionsOptions<TData extends object, TPrevious extends object = TData> {
    /** The two chains, with their running connectivity machines. */
    connections: ProofRequestConnections;
    /** The machine's states, from `stateOf$`. */
    state$: Observable<GraphState>;
    /**
     * The read, given the data the state holds (the previous value; the
     * defaults on a first read) and whether it is `loading` or `refreshing`.
     */
    read: (clients: ConnectedReadClients, previous: TPrevious, node: 'loading' | 'refreshing') => Observable<TData>;
    /**
     * When the value is due to be read again, given the data a read reads
     * from; followed from before each read starts. Its first emission says
     * it is following (a chain trigger once its starting block is known), and
     * the read waits for it; each later one says a refresh is due. One that
     * emits once and never again keeps the value for good.
     */
    refreshOn: (value: TData) => Observable<unknown>;
    /**
     * A read whose value goes to another of the graph's own edges rather
     * than to `current`, e.g. reloading after a reorg removed a request
     * (default: none).
     */
    arrivedElsewhere?: (value: TData, previous: TPrevious, node: 'loading' | 'refreshing') => boolean;
    /** Whether Ethereum's calls or logs order applies (default: calls). */
    kind?: 'calls' | 'logs';
    /** The chains the read goes through, or the one transport it needs (default: both chains). */
    needs?: ReadNeed[];
    /** How long a failed read waits before reading again. */
    backoff?: ReadRetryBackoff;
    /** Reads again now, while failed. */
    retry$: Subject<void>;
    /** Moves the machine to `closed`. */
    close$: Subject<void>;
}

/**
 * The transitions of a graph built with `readThroughConnectionsOf`: one
 * shared read per entry into `loading` or `refreshing`, through the chains
 * it needs; waiting for a connection that is lost; reading again after a
 * wait that doubles with each failure in a row; a refresh when `refreshOn`
 * emits; and closing. Spread into the graph's `implement`, next to its own
 * transitions.
 *
 * @param options What the graph reads, through which chains, and when it reads again.
 * @returns
 *   - `transitions`: the transitions, by name.
 *   - `read$`: the shared read's outcomes, for a graph's own transitions on them.
 */
export function readThroughConnectionsTransitions<TData extends object, TPrevious extends object = TData>(
    options: ReadThroughConnectionsOptions<TData, TPrevious>
) {
    const {
        connections,
        state$,
        read,
        refreshOn,
        arrivedElsewhere = () => false,
        kind = 'calls',
        needs,
        retry$,
        close$,
    } = options;
    const timings = resolveHealthCheckTimings(options.backoff);

    // `refreshOn`, followed from before each read starts, given the data the
    // read reads from: its first emission says it is following, and any later
    // one that a refresh is due. It is followed through the read and the
    // `current` after it, until the next read, so a change while reading is
    // not missed. The read and `current` hold it, and a move between them in
    // one tick keeps it. A machine started in `current`, with a value it was
    // given, follows it from the start.
    const sinceRead$ = state$.pipe(
        filter(({ node }, index) => node === 'loading' || node === 'refreshing' || (index === 0 && node === 'current')),
        switchMap(({ data }) =>
            refreshOn(ownDataOf<TData>(data)).pipe(
                scan((emitted) => emitted + 1, 0),
                map((emitted) => ({ following: true, due: emitted > 1 })),
                startWith({ following: false, due: false })
            )
        ),
        heldAcrossMoves({ replayLatest: true })
    );

    // The outcome edges of `loading` and `refreshing` share one read per entry
    // into either, started once `refreshOn` is following. Until then, needs
    // that cannot serve the read move it to wait for them.
    const read$: Observable<ReadThroughConnectionsRead<TData, TPrevious>> = state$.pipe(
        filter(({ node }) => node === 'loading' || node === 'refreshing'),
        switchMap(({ node, data }) => {
            const previous = ownDataOf<TPrevious>(data);
            const at = node as 'loading' | 'refreshing';
            const following$ = sinceRead$.pipe(
                filter(({ following }) => following),
                take(1)
            );
            return merge(
                needsNotReady$(connections, needs).pipe(
                    takeUntil(following$),
                    map((waitingOn) => ({ outcome: 'connectionLost' as const, waitingOn }))
                ),
                following$.pipe(
                    switchMap(() =>
                        readThroughConnections$(connections, (clients) => read(clients, previous, at), kind, needs)
                    )
                )
            ).pipe(
                take(1),
                map((outcome) => ({ ...outcome, previous, node: at }))
            );
        }),
        share()
    );
    // A failed read waits before reading again, doubling with each one in a row.
    const retryDue$ = merge(
        dataOnEntry$(state$, 'failedWhileLoading'),
        dataOnEntry$(state$, 'failedWhileRefreshing')
    ).pipe(
        take(1),
        switchMap((data) => timer(retryDelayMs((data as { failedReads: number }).failedReads, timings)))
    );
    // A refresh is due once one came due since the read that brought the value.
    const refreshDue$ = dataOnEntry$(state$, 'current').pipe(
        switchMap(() => sinceRead$.pipe(filter(({ due }) => due))),
        take(1)
    );

    type Source = { failedReads: number };
    const transitions = {
        valueArrived: {
            $: () =>
                withOutcome(read$, 'succeeded').pipe(
                    filter(({ value, previous, node }) => !arrivedElsewhere(value, previous, node))
                ),
            next: ({ value }: { value: TData }) => value,
        },
        refreshDue: {
            $: () => refreshDue$,
            next: (_due: unknown, _dest: unknown, source: object) => ({ ...ownDataOf<TData>(source), failedReads: 0 }),
        },
        connectionLost: {
            $: () => withOutcome(read$, 'connectionLost'),
            next: ({ waitingOn }: { waitingOn: ReadNeed[] }, _dest: unknown, source: Source) => ({
                ...ownDataOf<TData>(source),
                failedReads: source.failedReads,
                waitingOn,
            }),
        },
        readFailedOnHealthyConnection: {
            $: () => withOutcome(read$, 'failedOnHealthyConnection'),
            next: ({ error }: { error: string }, _dest: unknown, source: Source) => ({
                ...ownDataOf<TData>(source),
                failedReads: source.failedReads + 1,
                error,
            }),
        },
        readFailed: {
            $: () => withOutcome(read$, 'failed'),
            next: ({ error }: { error: string }, _dest: unknown, source: Source) => ({
                ...ownDataOf<TData>(source),
                failedReads: source.failedReads + 1,
                error,
            }),
        },
        connectionsChanged: {
            $: () => waitingOnChanged$(connections, needs),
            next: (waitingOn: ReadNeed[], _dest: unknown, source: Source) => ({
                ...ownDataOf<TData>(source),
                failedReads: source.failedReads,
                waitingOn,
            }),
        },
        connectionRestored: {
            $: () => bothReady$(connections, needs),
            next: (_restored: unknown, _dest: unknown, source: Source) => ({
                ...ownDataOf<TData>(source),
                failedReads: source.failedReads,
            }),
        },
        retryDue: {
            $: () => retryDue$,
            next: (_due: unknown, _dest: unknown, source: Source) => ({
                ...ownDataOf<TData>(source),
                failedReads: source.failedReads,
            }),
        },
        retry: {
            $: () => retry$,
            next: (_retry: unknown, _dest: unknown, source: Source) => ({
                ...ownDataOf<TData>(source),
                failedReads: source.failedReads,
            }),
        },
        close: {
            $: () => close$,
            next: () => ({}),
        },
    };
    return { transitions, read$ };
}

/** The nodes of a graph built with `readThroughConnectionsOf` that have failed. */
const FAILED = new Set<string>([
    'failedWhileLoading',
    'failedWhileRefreshing',
] satisfies ReadThroughConnectionsState['node'][]);

/** The nodes of a graph built with `readThroughConnectionsOf` that wait for a connection or have failed. */
const WAITING_OR_FAILED = new Set<string>([
    ...FAILED,
    ...([
        'waitingForConnectionWhileLoading',
        'waitingForConnectionWhileRefreshing',
    ] satisfies ReadThroughConnectionsState['node'][]),
]);

/**
 * The value a machine built with `readThroughConnectionsOf` holds, for
 * another machine's read: its data once it is `current`, or its error once
 * it fails. While it loads, refreshes or waits for a connection, nothing
 * yet: the reading machine's own wait for its connections takes over.
 *
 * @param machine The running machine read from.
 * @returns Its `current` data, once; errors with its error when it fails.
 */
export function readMachineValue$<TState extends GraphState>(
    machine: StartedMachine<TState>
): Observable<Extract<TState, { node: 'current' }>['data']> {
    return machine.state$.pipe(
        filter((state) => state.node === 'current' || FAILED.has(state.node)),
        take(1),
        map((state) => {
            if (state.node !== 'current') throw new Error((state.data as { error: string }).error);
            return state.data as Extract<TState, { node: 'current' }>['data'];
        })
    );
}

/**
 * The states of a machine built with `readThroughConnectionsOf` that
 * another machine reading its value refreshes on: each `current` value
 * `changed` says differs from what it read, each wait for a connection and
 * each failure.
 *
 * @param machine The running machine read from.
 * @param changed Whether a `current` value differs from the one read.
 * @returns Those states.
 */
export function readMachineChanged$<TState extends GraphState>(
    machine: StartedMachine<TState>,
    changed: (data: Extract<TState, { node: 'current' }>['data']) => boolean
): Observable<TState> {
    return machine.state$.pipe(
        filter((state) =>
            state.node === 'current'
                ? changed(state.data as Extract<TState, { node: 'current' }>['data'])
                : WAITING_OR_FAILED.has(state.node)
        )
    );
}

/** What a machine on `readThroughConnectionsOf` reads, through which chains, and when it reads again. */
export type ReadThroughConnectionsMachineOptions<
    TData extends object,
    TNodes extends Record<string, NodeData> = Record<string, NodeData>,
    TEdges extends Record<string, EdgeDef<TNodes>> = Record<string, EdgeDef<TNodes>>,
> = Omit<ReadThroughConnectionsOptions<TData, TNodes['current']>, 'state$' | 'retry$' | 'close$'> & {
    /**
     * The graph's own transitions beside the shared ones, given the shared
     * read's outcomes and the machine's states, typed by the graph; each
     * named by a transition the graph's edges move on.
     */
    ownTransitions?: (
        read$: Observable<ReadThroughConnectionsRead<TData, TNodes['current']>>,
        state$: Observable<StateUnion<TNodes>>
    ) => Partial<Record<TransitionNames<TEdges>, TransitionDef>>;
    /** Running machines this one owns, e.g. the chain changes machine its `refreshOn` gates on; its `close()` closes them too. */
    owns?: { close(): void }[];
    /** The node of the graph it starts in, with that node's data (default: `loading`, reading at once). */
    start?: { [TNode in keyof TNodes]: { node: TNode; data: TNodes[TNode] } }[keyof TNodes];
};

/**
 * Starts a machine on a graph built with `readThroughConnectionsOf`:
 * implemented with `readThroughConnectionsTransitions` (and the graph's own
 * transitions), closed and started in `loading`.
 *
 * A read returns the graph's `current` data (`TData`'s default), checked
 * against the graph: it is never inferred from the read. A graph whose read
 * can arrive at another of its nodes (`arrivedElsewhere`) names the wider
 * type it returns as `TData`.
 *
 * @param graph The graph.
 * @param options What it reads, through which chains, and when it reads again.
 * @returns The running machine, whose `current` carries the value, with
 *   `retry()`, which reads again now, without waiting, while failed, and
 *   `close()`, which moves it to `closed`.
 */
export function startReadThroughConnectionsMachine<
    TNodes extends Record<string, NodeData> & { current: NodeData },
    TEdges extends Record<string, EdgeDef<TNodes>>,
    TData extends object = TNodes['current'],
>(
    graph: IncidenceGraphSetMixin<TNodes, TEdges>,
    options: ReadThroughConnectionsMachineOptions<NoInfer<TData>, TNodes, TEdges>
) {
    const retry$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<StartedMachine<GraphState>>(1);
    const state$ = stateOf$(started$);
    const { transitions, read$ } = readThroughConnectionsTransitions<TData, TNodes['current']>({ ...options, state$, retry$, close$ });
    const { node, data } = options.start ?? { node: 'loading', data: undefined };
    const entry = node as Extract<keyof TNodes, string>;

    // The shared transitions and the graph's own, composed here; ystate checks each against the graph's edges.
    const machine = graph
        .implement({
            ...transitions,
            // The machine's states are the graph's.
            ...options.ownTransitions?.(read$, state$ as Observable<StateUnion<TNodes>>),
        } as Parameters<typeof graph.implement>[0])
        .close()
        .start(entry, undefined, data === undefined ? undefined : ({ [entry]: data } as { [K in typeof entry]?: Widen<TNodes[K]> }));
    // Its states are the graph's (`StateUnion<TNodes>`), whose node names are strings.
    started$.next(machine as StartedMachine<GraphState>);

    return Object.assign(machine, {
        retry: () => retry$.next(),
        close: () => {
            close$.next();
            for (const owned of options.owns ?? []) owned.close();
        },
    });
}
