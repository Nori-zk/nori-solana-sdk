import { define, type StateUnion } from '@yaw-rx/ystate';
import type { ConnectionName } from './connectedRead.js';
import type { ProofRequestHistoryEntry } from './fetchProofRequestHistory.js';
import type { ProofRequestHistoryCursor } from '../rpc/eth/fetchProofRequestsByTarget.js';

/**
 * Pages through a submitting address's proof requests, reading through the
 * Ethereum provider and Solana RPC connectivity machines. Nothing is a dead
 * end: lost connections are waited for and failures retry themselves.
 *
 * - `loadingPage` reads one page from `cursor`. The read's outcomes race on
 *   one shared read: `pageArrived`, `lastPageArrived` (the block range is
 *   exhausted), `connectionLost`, `readFailedOnHealthyConnection` or
 *   `pageFailed`.
 * - A read that fails to reach a connection reports it to that connection,
 *   which re-checks itself, and the machine stays in `loadingPage` until the
 *   check settles: a connection that turns out to be down is
 *   `connectionLost`; one that passes straight back to `ready` means the read
 *   itself failed: `readFailedOnHealthyConnection`.
 * - A connection that is not `ready` when a page starts, or leaves `ready`
 *   during it, is `connectionLost` too.
 * - `waitingForConnection` names the connections it waits on in `waitingOn`,
 *   updated as they change (the `connectionsChanged` self-loop), so the app
 *   can show the right message alongside those connections' own states. It
 *   resumes from the saved cursor once both are `ready`.
 * - `failed` reads again by itself after a wait that doubles with each
 *   consecutive failure (`failedReads`), or at once on `retry`. A page that
 *   arrives resets `failedReads`.
 * - Loss of a connection is only noticed while loading: `waitingForMore`
 *   does no reads, and a `loadMore` while a connection is down moves to
 *   `loadingPage`, which reports the loss straight away.
 * - `allLoaded` and `closed` are terminal.
 */
export const ProofRequestHistoryGraph = define({
    nodes: {
        loadingPage: {
            loaded: [] as ProofRequestHistoryEntry[],
            cursor: undefined as ProofRequestHistoryCursor | undefined,
            failedReads: 0,
        },
        waitingForMore: {
            loaded: [] as ProofRequestHistoryEntry[],
            cursor: undefined as ProofRequestHistoryCursor | undefined,
        },
        waitingForConnection: {
            loaded: [] as ProofRequestHistoryEntry[],
            cursor: undefined as ProofRequestHistoryCursor | undefined,
            failedReads: 0,
            waitingOn: [] as ConnectionName[],
        },
        failed: {
            loaded: [] as ProofRequestHistoryEntry[],
            cursor: undefined as ProofRequestHistoryCursor | undefined,
            failedReads: 0,
            error: '',
        },
        allLoaded: { loaded: [] as ProofRequestHistoryEntry[] },
        closed: {},
    },
    edges: {
        pageLoaded: {
            from: 'loadingPage',
            to: 'waitingForMore',
            on: 'pageArrived.next',
        },
        lastPageLoaded: {
            from: 'loadingPage',
            to: 'allLoaded',
            on: 'lastPageArrived.next',
        },
        connectionLost: {
            from: 'loadingPage',
            to: 'waitingForConnection',
            on: 'connectionLost.next',
        },
        readFailedOnHealthyConnection: {
            from: 'loadingPage',
            to: 'failed',
            on: 'readFailedOnHealthyConnection.next',
        },
        pageFailed: {
            from: 'loadingPage',
            to: 'failed',
            on: 'pageFailed.next',
        },

        moreRequested: {
            from: 'waitingForMore',
            to: 'loadingPage',
            on: 'loadMore.next',
        },
        connectionsChanged: {
            from: 'waitingForConnection',
            to: 'waitingForConnection',
            on: 'connectionsChanged.next',
        },
        connectionRestored: {
            from: 'waitingForConnection',
            to: 'loadingPage',
            on: 'connectionRestored.next',
        },
        retryStarted: {
            from: 'failed',
            to: 'loadingPage',
            on: 'retryDue.next',
        },
        retryRequested: {
            from: 'failed',
            to: 'loadingPage',
            on: 'retry.next',
        },

        closedWhileLoading: {
            from: 'loadingPage',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileWaitingForMore: {
            from: 'waitingForMore',
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

/** The paged history's state: a node of the graph and its data. */
export type ProofRequestHistoryState = StateUnion<
    typeof ProofRequestHistoryGraph.nodes
>;
