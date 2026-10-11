import { define, type StateUnion } from '@yaw-rx/ystate';

/**
 * A value read through the connections and kept current: read for the
 * first time, held, read again when a refresh is due. Graphs whose machine
 * is this, spread its nodes and edges, each with the data it holds
 * (`data`), and add their own; `ReadThroughConnectionsGraph` is it alone.
 * Nothing is a dead end.
 *
 * - `loading` reads the value. It carries the data already on screen:
 *   empty before the first read, the last value when reading from scratch
 *   again. So do `waitingForConnectionWhileLoading` and
 *   `failedWhileLoading`.
 * - `current` holds the value until a refresh is due.
 * - `refreshing` reads again while holding the value. So do
 *   `waitingForConnectionWhileRefreshing` and `failedWhileRefreshing`.
 * - A read that fails to reach a connection reports it to that connection,
 *   which re-checks itself, and the machine stays where it is until the
 *   check settles: a connection that turns out to be down is
 *   `connectionLost`; one that passes straight back to `ready` means the
 *   read itself failed: `readFailedOnHealthyConnection`.
 * - `waitingForConnectionWhile…` names the connections it waits on in
 *   `waitingOn`, updated as they change, and reads again once they are
 *   `ready`.
 * - `failedWhile…` reads again by itself after a wait that doubles with
 *   each consecutive failure (`failedReads`), or at once on `retry`. A read
 *   that arrives resets `failedReads`.
 * - `closed` is terminal.
 *
 * @param data The data the reading graph's nodes hold, as its defaults.
 * @returns The nodes and edges.
 */
export const readThroughConnectionsOf = <TData extends object>(data: TData) => ({
    nodes: {
        loading: { ...data, failedReads: 0 },
        current: { ...data },
        refreshing: { ...data, failedReads: 0 },
        waitingForConnectionWhileLoading: { ...data, failedReads: 0, waitingOn: [] as string[] },
        waitingForConnectionWhileRefreshing: { ...data, failedReads: 0, waitingOn: [] as string[] },
        failedWhileLoading: { ...data, failedReads: 0, error: '' },
        failedWhileRefreshing: { ...data, failedReads: 0, error: '' },
        closed: {},
    },
    edges: {
        valueLoaded: { from: 'loading', to: 'current', on: 'valueArrived.next' },
        connectionLostWhileLoading: {
            from: 'loading',
            to: 'waitingForConnectionWhileLoading',
            on: 'connectionLost.next',
        },
        connectionsChangedWhileLoading: {
            from: 'waitingForConnectionWhileLoading',
            to: 'waitingForConnectionWhileLoading',
            on: 'connectionsChanged.next',
        },
        connectionRestoredWhileLoading: {
            from: 'waitingForConnectionWhileLoading',
            to: 'loading',
            on: 'connectionRestored.next',
        },
        readFailedOnHealthyConnectionWhileLoading: {
            from: 'loading',
            to: 'failedWhileLoading',
            on: 'readFailedOnHealthyConnection.next',
        },
        readFailedWhileLoading: { from: 'loading', to: 'failedWhileLoading', on: 'readFailed.next' },
        retryStartedWhileLoading: { from: 'failedWhileLoading', to: 'loading', on: 'retryDue.next' },
        retryWhileLoading: { from: 'failedWhileLoading', to: 'loading', on: 'retry.next' },

        refreshDue: { from: 'current', to: 'refreshing', on: 'refreshDue.next' },
        valueRefreshed: { from: 'refreshing', to: 'current', on: 'valueArrived.next' },
        connectionLostWhileRefreshing: {
            from: 'refreshing',
            to: 'waitingForConnectionWhileRefreshing',
            on: 'connectionLost.next',
        },
        connectionsChangedWhileRefreshing: {
            from: 'waitingForConnectionWhileRefreshing',
            to: 'waitingForConnectionWhileRefreshing',
            on: 'connectionsChanged.next',
        },
        connectionRestoredWhileRefreshing: {
            from: 'waitingForConnectionWhileRefreshing',
            to: 'refreshing',
            on: 'connectionRestored.next',
        },
        readFailedOnHealthyConnectionWhileRefreshing: {
            from: 'refreshing',
            to: 'failedWhileRefreshing',
            on: 'readFailedOnHealthyConnection.next',
        },
        readFailedWhileRefreshing: { from: 'refreshing', to: 'failedWhileRefreshing', on: 'readFailed.next' },
        retryStartedWhileRefreshing: { from: 'failedWhileRefreshing', to: 'refreshing', on: 'retryDue.next' },
        retryWhileRefreshing: { from: 'failedWhileRefreshing', to: 'refreshing', on: 'retry.next' },

        closedWhileLoading: { from: 'loading', to: 'closed', on: 'close.next' },
        closedWhileCurrent: { from: 'current', to: 'closed', on: 'close.next' },
        closedWhileRefreshing: { from: 'refreshing', to: 'closed', on: 'close.next' },
        closedWhileWaitingForConnectionWhileLoading: {
            from: 'waitingForConnectionWhileLoading',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileWaitingForConnectionWhileRefreshing: {
            from: 'waitingForConnectionWhileRefreshing',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileFailedWhileLoading: { from: 'failedWhileLoading', to: 'closed', on: 'close.next' },
        closedWhileFailedWhileRefreshing: { from: 'failedWhileRefreshing', to: 'closed', on: 'close.next' },
    } as const,
});

/** The value read through the connections and kept current, alone: its data is `value`. */
export const ReadThroughConnectionsGraph = define(readThroughConnectionsOf({ value: undefined as unknown }));

/** The graph's states: a node and its data. */
export type ReadThroughConnectionsState = StateUnion<typeof ReadThroughConnectionsGraph.nodes>;
