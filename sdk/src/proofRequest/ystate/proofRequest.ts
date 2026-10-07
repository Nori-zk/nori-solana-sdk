import { define } from '@yaw-rx/ystate';
import type { ConnectionName } from '../connectedRead.js';
import { ProofRequestState } from '../types.js';

/** What is known of a request the bridge has not proven yet. */
const unprocessedRequest = {
    state: ProofRequestState.Unprocessed as typeof ProofRequestState.Unprocessed,
    requestId: 0n as bigint,
    requestBlockNumber: 0n as bigint,
    queueCursor: 0n as bigint,
    proofQueueBatchCount: 0n as bigint,
};

/**
 * Follows one Ethereum proof request until a committed proof queue batch on
 * Solana covers it, reading through the Ethereum provider and Solana RPC
 * connectivity machines. Nothing is a dead end: lost connections are waited
 * for and failures retry themselves.
 *
 * - `undetermined` looks the request up from the transaction that enqueued
 *   it, on entry and then every poll interval or recheck signal. A
 *   transaction that is not mined yet is not a failure: it keeps looking.
 *   Its outcomes race on one shared lookup: `unprocessed`,
 *   `proofAvailable`, a lost connection, or a failure.
 * - `unprocessed` checks the bridge on entry and then every poll interval or
 *   recheck signal, until a committed batch covers the request.
 * - A read that fails to reach a connection reports it to that connection,
 *   which re-checks itself, and the machine stays where it is until the
 *   check settles: a connection that turns out to be down is
 *   `connectionLost`; one that passes straight back to `ready` means the read
 *   itself failed: `readFailedOnHealthyConnection`.
 * - `waitingForConnection…` names the connections it waits on in
 *   `waitingOn`, updated as they change, and resumes where it was once both
 *   are `ready`. The lost and failed nodes are split by where they resume,
 *   because an edge's target is fixed.
 * - `failed…` reads again by itself after a wait that doubles with each
 *   consecutive failure (`failedReads`), or at once on `retry`. A read that
 *   succeeds resets `failedReads`.
 * - `proofAvailable` is terminal: once a batch covers the request it stays
 *   covered (batches are append-only). `closed` is terminal too.
 */
export const ProofRequestStateGraph = define({
    nodes: {
        undetermined: { failedReads: 0 },
        unprocessed: { ...unprocessedRequest, failedReads: 0 },
        proofAvailable: {
            state: ProofRequestState.ProofAvailable as typeof ProofRequestState.ProofAvailable,
            requestId: 0n as bigint,
            requestBlockNumber: 0n as bigint,
            queueCursor: 0n as bigint,
            proofQueueBatchIndex: 0n as bigint,
            proofQueueBatchAddress: '' as string, // base58 proof queue batch PDA
            root: '' as string, // 0x-prefixed batch root
            inputQueueCursor: 0n as bigint,
            outputQueueCursor: 0n as bigint,
            outputBlockNumber: 0n as bigint,
            previousOutputBlockNumber: -1n as bigint, // sentinel: no previous proof queue batch (first-ever batch)
            indexInBatch: 0n as bigint,
        },
        waitingForConnectionWhileUndetermined: {
            failedReads: 0,
            waitingOn: [] as ConnectionName[],
        },
        waitingForConnectionWhileUnprocessed: {
            ...unprocessedRequest,
            failedReads: 0,
            waitingOn: [] as ConnectionName[],
        },
        failedWhileUndetermined: { failedReads: 0, error: '' },
        failedWhileUnprocessed: { ...unprocessedRequest, failedReads: 0, error: '' },
        closed: {},
    },
    edges: {
        discoveredUnprocessed: {
            from: 'undetermined',
            to: 'unprocessed',
            on: 'checkWhetherProofRequestIsUnprocessed.next',
        },
        discoveredProofAvailable: {
            from: 'undetermined',
            to: 'proofAvailable',
            on: 'checkWhetherProofIsAvailable.next',
        },
        proofRequestCommitted: {
            from: 'unprocessed',
            to: 'proofAvailable',
            on: 'checkWhetherUnprocessedProofRequestIsCommitted.next',
        },

        connectionLostWhileUndetermined: {
            from: 'undetermined',
            to: 'waitingForConnectionWhileUndetermined',
            on: 'connectionLost.next',
        },
        connectionsChangedWhileUndetermined: {
            from: 'waitingForConnectionWhileUndetermined',
            to: 'waitingForConnectionWhileUndetermined',
            on: 'connectionsChanged.next',
        },
        connectionRestoredWhileUndetermined: {
            from: 'waitingForConnectionWhileUndetermined',
            to: 'undetermined',
            on: 'connectionRestored.next',
        },
        readFailedOnHealthyConnectionWhileUndetermined: {
            from: 'undetermined',
            to: 'failedWhileUndetermined',
            on: 'readFailedOnHealthyConnection.next',
        },
        readFailedWhileUndetermined: {
            from: 'undetermined',
            to: 'failedWhileUndetermined',
            on: 'readFailed.next',
        },
        retryStartedWhileUndetermined: {
            from: 'failedWhileUndetermined',
            to: 'undetermined',
            on: 'retryDue.next',
        },
        retryWhileUndetermined: {
            from: 'failedWhileUndetermined',
            to: 'undetermined',
            on: 'retry.next',
        },

        connectionLostWhileUnprocessed: {
            from: 'unprocessed',
            to: 'waitingForConnectionWhileUnprocessed',
            on: 'connectionLost.next',
        },
        connectionsChangedWhileUnprocessed: {
            from: 'waitingForConnectionWhileUnprocessed',
            to: 'waitingForConnectionWhileUnprocessed',
            on: 'connectionsChanged.next',
        },
        connectionRestoredWhileUnprocessed: {
            from: 'waitingForConnectionWhileUnprocessed',
            to: 'unprocessed',
            on: 'connectionRestored.next',
        },
        readFailedOnHealthyConnectionWhileUnprocessed: {
            from: 'unprocessed',
            to: 'failedWhileUnprocessed',
            on: 'readFailedOnHealthyConnection.next',
        },
        readFailedWhileUnprocessed: {
            from: 'unprocessed',
            to: 'failedWhileUnprocessed',
            on: 'readFailed.next',
        },
        retryStartedWhileUnprocessed: {
            from: 'failedWhileUnprocessed',
            to: 'unprocessed',
            on: 'retryDue.next',
        },
        retryWhileUnprocessed: {
            from: 'failedWhileUnprocessed',
            to: 'unprocessed',
            on: 'retry.next',
        },

        closedWhileUndetermined: {
            from: 'undetermined',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileUnprocessed: {
            from: 'unprocessed',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileWaitingForConnectionWhileUndetermined: {
            from: 'waitingForConnectionWhileUndetermined',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileWaitingForConnectionWhileUnprocessed: {
            from: 'waitingForConnectionWhileUnprocessed',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileFailedWhileUndetermined: {
            from: 'failedWhileUndetermined',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileFailedWhileUnprocessed: {
            from: 'failedWhileUnprocessed',
            to: 'closed',
            on: 'close.next',
        },
    },
});

export type ProofRequestStateNodeUnion = {
    [Node in keyof typeof ProofRequestStateGraph.nodes]: {
        node: Node;
        data: (typeof ProofRequestStateGraph.nodes)[Node];
    };
}[keyof typeof ProofRequestStateGraph.nodes];
