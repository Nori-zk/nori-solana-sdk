import { type ResolveNodeData } from '@yaw-rx/ystate';
import { filter, map, Subject } from 'rxjs';
import { type ProofRequestConnections } from './connectedRead.js';
import { fetchProofRequestHistoryPage$, type ProofRequestHistoryAddresses } from './fetchProofRequestHistory.js';
import { type ProofRequestsByTargetQuery } from '../rpc/eth/proofRequestsByTarget.js';
import {
    type ReadRetryBackoff,
    startReadThroughConnectionsMachine,
    dueOn,
} from '../rpc/connection/readThroughConnections.impl.js';
import { withOutcome } from '../utils/machines.js';
import { ProofRequestHistoryGraph } from './proofRequestHistory.js';

/** The submitting address, block range, order and page size of a paged history. */
export type ProofRequestHistoryQuery = Omit<
    ProofRequestsByTargetQuery,
    'after'
>;

/** The requests loaded so far, and where the next page starts (none once the block range is exhausted). */
type LoadedRequests = ResolveNodeData<typeof ProofRequestHistoryGraph.nodes, 'current'>;

/** The page that exhausts the block range leaves no cursor. */
const lastPage = ({ cursor }: LoadedRequests) => cursor === undefined;

/**
 * Starts a paged history of a submitting address's proof requests, reading
 * through both connections. The first page loads at once; each `loadMore()`
 * loads the next.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param addresses The queue and program addresses.
 * @param query The submitting address, block range, order and page size.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; its states carry every entry loaded so far. Its controls:
 *   - `loadMore()`: loads the next page, while holding the requests loaded so far.
 *   - `retry()`: reads the failed page again now, without waiting.
 *   - `close()`: moves the machine to `closed`.
 */
export function createProofRequestHistoryMachine(
    connections: ProofRequestConnections,
    addresses: ProofRequestHistoryAddresses,
    query: ProofRequestHistoryQuery,
    backoff: ReadRetryBackoff = {}
) {
    const loadMore$ = new Subject<void>();

    const started = startReadThroughConnectionsMachine(ProofRequestHistoryGraph, {
        connections,
        // The page after the cursor held, appended to the requests loaded so far.
        read: (clients, { target, loaded, cursor }) =>
            fetchProofRequestHistoryPage$(clients, addresses, { ...query, target, after: cursor }).pipe(
                map((page) => ({
                    target,
                    loaded: [...loaded, ...page.entries],
                    cursor: page.done ? undefined : page.cursor,
                }))
            ),
        refreshOn: () => dueOn(loadMore$),
        arrivedElsewhere: lastPage,
        kind: 'logs',
        backoff,
        ownTransitions: (read$) => ({
            lastPageArrived: {
                $: () => withOutcome(read$, 'succeeded').pipe(filter(({ value }) => lastPage(value))),
                next: ({ value }: { value: LoadedRequests }) => ({ target: value.target, loaded: value.loaded }),
            },
        }),
        // The submitting address is its starting data.
        start: { node: 'loading', data: { target: query.target, loaded: [], cursor: undefined, failedReads: 0 } },
    });

    return Object.assign(started, { loadMore: () => loadMore$.next() });
}
