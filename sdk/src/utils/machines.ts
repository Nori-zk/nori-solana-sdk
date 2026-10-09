import { filter, map, type Observable, switchMap, take } from 'rxjs';

/** A graph state: one of its nodes and that node's data. */
type GraphState = { node: string; data: unknown };

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
 * An interface's shape as a graph node's data: the same fields as a type
 * alias, which has the implicit index signature node data needs and an
 * interface lacks.
 */
export type AsNodeData<T> = { [K in keyof T]: T[K] };

/** A started machine, as far as its states go: `.start()`'s running machine. */
export interface StartedMachine<TState extends GraphState> {
    state$: Observable<TState>;
}

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
 * Emits the data a machine carries into `node`, the next time it enters it.
 *
 * A transition's `$` receives only the running dependency machines, never
 * the data of the node it leaves from. This is how a `$` reads that data
 * from its own running machine, which is the source of truth for it.
 *
 * ystate subscribes a node's outgoing `$` *before* it emits the new state,
 * so at the moment a `$` subscribes, `state$` still replays the previous
 * state (or nothing yet, while the machine starts). Filtering for `node`
 * and taking the first match therefore yields the state being entered, not
 * the one being left. That holds for every edge except a self-loop on
 * `node`, where the previous state is also at `node`: do not use this for
 * a node with a self-loop.
 *
 * @param state$ The machine's states, from `stateOf$`.
 * @param node The node whose data to read.
 * @returns The node's data, once, as the machine enters it.
 */
export function dataOnEntry$<
    TState extends GraphState,
    TNode extends TState['node'],
>(
    state$: Observable<TState>,
    node: TNode
): Observable<Extract<TState, { node: TNode }>['data']> {
    return state$.pipe(
        filter(
            (state): state is Extract<TState, { node: TNode }> =>
                state.node === node
        ),
        take(1),
        map((state) => state.data)
    );
}
