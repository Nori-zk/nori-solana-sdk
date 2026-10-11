import {
    type EdgeDef,
    type NodeData,
    type ResolveNodeData,
    type RunningMachine,
    type RunningNamespace,
    type TransitionNames,
} from '@yaw-rx/ystate';
import { filter, map, Observable, type ObservedValueOf, ReplaySubject, share, Subject, switchMap, take, timer } from 'rxjs';

/** A graph state, as a running machine's `state$` emits it: one of its nodes and that node's data. */
export type GraphState = ObservedValueOf<RunningMachine['state$']>;

/**
 * Whether a running machine's state is at `node`.
 *
 * @param node The node.
 * @returns A predicate on states.
 */
export const atNode =
    <TNode extends string>(node: TNode) =>
    <TState extends { node: string }>(
        state: TState
    ): state is TState & { node: TNode } =>
        state.node === node;

/**
 * Keeps only the results with one outcome, narrowed to it: one shared check
 * or read feeding each of a node's outcome edges.
 *
 * @param result$ Results, each with an `outcome`.
 * @param outcome The outcome to keep.
 * @returns The results with that outcome.
 */
export function withOutcome<TResult extends { outcome: string }, TOutcome extends TResult['outcome']>(
    result$: Observable<TResult>,
    outcome: TOutcome
): Observable<Extract<TResult, { outcome: TOutcome }>> {
    return result$.pipe(
        filter((result): result is Extract<TResult, { outcome: TOutcome }> => result.outcome === outcome)
    );
}

/**
 * An interface's shape as a graph node's data: the same fields as a type
 * alias, which has the implicit index signature node data needs and an
 * interface lacks.
 */
export type AsNodeData<T> = { [K in keyof T]: T[K] };

/**
 * A contract struct's named fields as a graph node's data: ethers decodes a
 * struct as an array that also carries its fields by name, and a node's data
 * widens an array to its elements, losing the names.
 */
export type StructFields<T> = { [K in keyof T as K extends keyof unknown[] | `${number}` ? never : K]: T[K] };

/** A started machine, as far as its states go: a running namespace's `state$`. */
export type StartedMachine<TState extends GraphState> = Pick<RunningNamespace<TState>, 'state$'>;

/**
 * The running machine's states, once it has started.
 *
 * A machine's `$` are written before the machine is started, and ystate
 * subscribes the starting node's `$` inside `.start()`, before `.start()`
 * returns the running machine. Passing the running machine through a
 * `ReplaySubject` lets a `$` wait for it, during `.start()` or after.
 *
 * @param started$ Emits the running machine once `.start()` returns it.
 * @returns The running machine's `state$`.
 */
export function stateOf$<TState extends GraphState>(
    started$: Observable<StartedMachine<TState>>
): Observable<TState> {
    return started$.pipe(
        take(1),
        switchMap((machine) => machine.state$)
    );
}

/**
 * Emits the state a machine enters, once: for a node's `$` to read the
 * data it carries.
 *
 * A transition's `$` receives only the running dependency machines, never
 * the data of the node it leaves from. This is how a `$` reads that data
 * from its own running machine, which is the source of truth for it.
 *
 * ystate subscribes a node's outgoing `$` *before* it emits the new state,
 * so while a `$` subscribes, `state$` replays the state being left (or
 * nothing yet, while the machine starts). The first state after that is the
 * one entered, a self-loop's included.
 *
 * @param state$ The machine's states, from `stateOf$`.
 * @returns The state entered, once.
 */
export function stateOnEntry$<TState extends GraphState>(
    state$: Observable<TState>
): Observable<TState> {
    return new Observable<TState>((subscriber) => {
        let subscribing = true;
        const subscription = state$
            .pipe(
                filter(() => !subscribing),
                take(1)
            )
            .subscribe(subscriber);
        subscribing = false;
        return subscription;
    });
}

/**
 * Shares a stream among a machine's `$` and keeps it while they move
 * between nodes: ystate unsubscribes the left node's `$` and subscribes the
 * entered node's in one tick, so the stream ends only when no `$` holds it
 * after that tick.
 *
 * @param options `replayLatest`: a new subscriber gets the latest value
 *   (default: no; a stream of events must not replay one into the `$` its
 *   own transition just resubscribed).
 * @returns The operator.
 */
export const heldAcrossMoves = <T>({ replayLatest = false }: { replayLatest?: boolean } = {}) =>
    share<T>({
        connector: () => (replayLatest ? new ReplaySubject<T>(1) : new Subject<T>()),
        resetOnRefCountZero: () => timer(0),
    });

/**
 * One request per entry into `node`, given the data the machine carries
 * into it, shared by the node's outcome edges so they race on that one
 * request.
 *
 * @param state$ The machine's states, from `stateOf$`.
 * @param node The node that makes the request.
 * @param request The request, given the node's data.
 * @returns What the request emits, per entry into `node`.
 */
export function requestOnEntry$<TState extends GraphState, TNode extends TState['node'], TResult>(
    state$: Observable<TState>,
    node: TNode,
    request: (data: Extract<TState, { node: TNode }>['data']) => Observable<TResult>
): Observable<TResult> {
    return dataOnEntry$(state$, node).pipe(switchMap(request), share());
}

/**
 * How a request a node makes ended, one per edge out of the node in
 * `TEdge`: that edge's transition, with the data of the node it leads to.
 */
export type RequestOutcome<
    TGraph extends { nodes: Record<string, NodeData>; edges: Record<string, EdgeDef> },
    TEdge extends keyof TGraph['edges'],
> = {
    [E in TEdge]: {
        transition: TransitionNames<Pick<TGraph['edges'], E>>;
        data: ResolveNodeData<TGraph['nodes'], TGraph['edges'][E]['to']>;
    };
}[TEdge];

/**
 * One request per entry into `node`, given the data the machine carries
 * into it, shared by the node's outcome edges.
 *
 * @param state$ The machine's states, from `stateOf$`.
 * @param node The node that makes the request.
 * @param request The request, given the node's data; it ends as the transition it takes and that transition's data.
 * @returns For a transition, the requests that take it, as the data they carry.
 */
export function requestOutcomes<
    TState extends GraphState,
    TNode extends TState['node'],
    TOutcome extends { transition: string; data: unknown },
>(
    state$: Observable<TState>,
    node: TNode,
    request: (data: Extract<TState, { node: TNode }>['data']) => Observable<TOutcome>
): <TTransition extends TOutcome['transition']>(
    transition: TTransition
) => Observable<Extract<TOutcome, { transition: TTransition }>['data']> {
    const request$ = requestOnEntry$(state$, node, request);
    return <TTransition extends TOutcome['transition']>(transition: TTransition) =>
        request$.pipe(
            filter((outcome): outcome is Extract<TOutcome, { transition: TTransition }> => outcome.transition === transition),
            map(({ data }) => data)
        );
}

/**
 * Emits the data a machine carries into `node`, as it enters it.
 *
 * @param state$ The machine's states, from `stateOf$`.
 * @param node The node whose data to read.
 * @returns The node's data, once, as the machine enters it; nothing when it enters another node.
 */
export function dataOnEntry$<
    TState extends GraphState,
    TNode extends TState['node'],
>(
    state$: Observable<TState>,
    node: TNode
): Observable<Extract<TState, { node: TNode }>['data']> {
    return stateOnEntry$(state$).pipe(
        filter(
            (state): state is Extract<TState, { node: TNode }> =>
                state.node === node
        ),
        map((state) => state.data)
    );
}
