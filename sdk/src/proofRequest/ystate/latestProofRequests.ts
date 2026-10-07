import { define, type StateUnion } from '@yaw-rx/ystate';
import type { ConnectionName } from '../connectedRead.js';
import type { ProofRequestHistoryEntry } from '../proofRequestHistory.js';

/**
 * A live view of a submitting address's newest N proof requests, newest
 * first, reading through the Ethereum provider and Solana RPC connectivity
 * machines. It never ends on its own and nothing is a dead end: lost
 * connections are waited for and failures retry themselves.
 *
 * - `loading` reads the newest N from `fromBlock`. It carries the view
 *   already on screen: empty before the first view, the last view while
 *   reloading after a reorg. So do the other `…BeforeFirstView` nodes.
 * - `watching` shows the view until a refresh is due: every interval, or on
 *   the recheck signal (e.g. bridge state changes from the websocket).
 * - `refreshing` re-reads from the view's oldest block up to the latest
 *   block. Its outcomes race on one shared read: `refreshArrived` (new
 *   requests enter at the top and states move from unprocessed to proof
 *   available; every refresh emits the fresh view), `requestsMissing` (a
 *   reorg removed a request in the view: load everything again from
 *   `fromBlock`), `connectionLost`, `readFailedOnHealthyConnection` or
 *   `readFailed`.
 * - A read that fails to reach a connection reports it to that connection,
 *   which re-checks itself, and the machine stays where it is until the
 *   check settles: a connection that turns out to be down is
 *   `connectionLost`; one that passes straight back to `ready` means the
 *   read itself failed: `readFailedOnHealthyConnection`.
 * - `waitingForConnection…` names the connections it waits on in
 *   `waitingOn`, updated as they change, and resumes once both are `ready`.
 * - `failed…` reads again by itself after a wait that doubles with each
 *   consecutive failure (`failedReads`), or at once on `retry`. A read that
 *   arrives resets `failedReads`.
 * - `closed` is terminal.
 */
export const LatestProofRequestsGraph = define({
    nodes: {
        loading: { view: [] as ProofRequestHistoryEntry[], failedReads: 0 },
        watching: { view: [] as ProofRequestHistoryEntry[], oldestBlock: 0 },
        refreshing: {
            view: [] as ProofRequestHistoryEntry[],
            oldestBlock: 0,
            failedReads: 0,
        },
        waitingForConnectionBeforeFirstView: {
            view: [] as ProofRequestHistoryEntry[],
            failedReads: 0,
            waitingOn: [] as ConnectionName[],
        },
        waitingForConnection: {
            view: [] as ProofRequestHistoryEntry[],
            oldestBlock: 0,
            failedReads: 0,
            waitingOn: [] as ConnectionName[],
        },
        failedBeforeFirstView: {
            view: [] as ProofRequestHistoryEntry[],
            failedReads: 0,
            error: '',
        },
        failed: {
            view: [] as ProofRequestHistoryEntry[],
            oldestBlock: 0,
            failedReads: 0,
            error: '',
        },
        closed: {},
    },
    edges: {
        viewLoaded: {
            from: 'loading',
            to: 'watching',
            on: 'viewArrived.next',
        },
        connectionLostWhileLoading: {
            from: 'loading',
            to: 'waitingForConnectionBeforeFirstView',
            on: 'connectionLost.next',
        },
        readFailedOnHealthyConnectionBeforeFirstView: {
            from: 'loading',
            to: 'failedBeforeFirstView',
            on: 'readFailedOnHealthyConnection.next',
        },
        loadFailed: {
            from: 'loading',
            to: 'failedBeforeFirstView',
            on: 'readFailed.next',
        },
        connectionsChangedBeforeFirstView: {
            from: 'waitingForConnectionBeforeFirstView',
            to: 'waitingForConnectionBeforeFirstView',
            on: 'connectionsChanged.next',
        },
        connectionRestoredBeforeFirstView: {
            from: 'waitingForConnectionBeforeFirstView',
            to: 'loading',
            on: 'connectionRestored.next',
        },
        loadRetryStarted: {
            from: 'failedBeforeFirstView',
            to: 'loading',
            on: 'retryDue.next',
        },
        loadAgain: {
            from: 'failedBeforeFirstView',
            to: 'loading',
            on: 'retry.next',
        },

        refreshDue: {
            from: 'watching',
            to: 'refreshing',
            on: 'refreshDue.next',
        },
        viewRefreshed: {
            from: 'refreshing',
            to: 'watching',
            on: 'refreshArrived.next',
        },
        requestDroppedByReorg: {
            from: 'refreshing',
            to: 'loading',
            on: 'requestsMissing.next',
        },
        connectionLostWhileRefreshing: {
            from: 'refreshing',
            to: 'waitingForConnection',
            on: 'connectionLost.next',
        },
        readFailedOnHealthyConnection: {
            from: 'refreshing',
            to: 'failed',
            on: 'readFailedOnHealthyConnection.next',
        },
        refreshFailed: {
            from: 'refreshing',
            to: 'failed',
            on: 'readFailed.next',
        },
        connectionsChanged: {
            from: 'waitingForConnection',
            to: 'waitingForConnection',
            on: 'connectionsChanged.next',
        },
        connectionRestored: {
            from: 'waitingForConnection',
            to: 'refreshing',
            on: 'connectionRestored.next',
        },
        refreshRetryStarted: {
            from: 'failed',
            to: 'refreshing',
            on: 'retryDue.next',
        },
        retryRequested: {
            from: 'failed',
            to: 'refreshing',
            on: 'retry.next',
        },

        closedWhileLoading: {
            from: 'loading',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileWaitingForConnectionBeforeFirstView: {
            from: 'waitingForConnectionBeforeFirstView',
            to: 'closed',
            on: 'close.next',
        },
        closedBeforeFirstView: {
            from: 'failedBeforeFirstView',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileWatching: {
            from: 'watching',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileRefreshing: {
            from: 'refreshing',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileWaitingForConnection: {
            from: 'waitingForConnection',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileFailed: {
            from: 'failed',
            to: 'closed',
            on: 'close.next',
        },
    },
});

/** The live view's state: a node of the graph and its data. */
export type LatestProofRequestsState = StateUnion<
    typeof LatestProofRequestsGraph.nodes
>;
