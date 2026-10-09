import {
    catchError,
    combineLatest,
    defer,
    distinctUntilChanged,
    filter,
    from,
    map,
    merge,
    type Observable,
    of,
    skip,
    Subject,
    switchMap,
    take,
    takeUntil,
} from 'rxjs';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { messageOf } from '../utils/messageOf.js';
import { ConnectionNotReadyError } from '../rpc/connection/connectionNotReady.js';
import {
    type Ethereum,
    ethereumCallsUsable$,
    forCalls,
    forLogs,
    type Solana,
} from '../rpc/connection/connections.js';
import { SolanaRpcTransportError } from '../rpc/solana/errors.js';
import { type SolanaRpc } from '../rpc/solana/solanaHttp.js';

/**
 * The Ethereum and Solana chains a reading machine reads through. The
 * reading machines are coupled to the transports' machines through their
 * running instances' `state$`, as the heater and the room are coupled
 * through shared streams, not through graph `deps`: a graph's deps must be
 * implemented at definition time, and each transport's machine is
 * implemented per instance around its own endpoints.
 */
export interface ProofRequestConnections {
    ethereum: Ethereum;
    solana: Solana;
}

/** Which of the two chains. */
export type ConnectionName = keyof ProofRequestConnections;

/** The clients one read goes through. */
export interface ConnectedReadClients {
    provider: EthereumProvider;
    rpc: SolanaRpc;
}

/** How one read through both chains ended. */
export type ConnectedRead<T> =
    | { outcome: 'succeeded'; value: T }
    | { outcome: 'connectionLost'; waitingOn: ConnectionName[] }
    | { outcome: 'failedOnHealthyConnection'; error: string }
    | { outcome: 'failed'; error: string };

/**
 * The chains that cannot serve a read, each time either changes: Ethereum
 * while no transport in its calls order is usable, Solana while its http
 * is not `ready`.
 *
 * @param connections The two chains.
 * @param whileChecking Count a transport re-checking itself as able: what a
 *   failed request did is decided once its check settles.
 * @returns The names of the chains that cannot serve a read, in a fixed order.
 */
function notReady$(
    connections: ProofRequestConnections,
    whileChecking = false
): Observable<ConnectionName[]> {
    return combineLatest([
        ethereumCallsUsable$(connections.ethereum, whileChecking),
        connections.solana.http.connection.state$,
    ]).pipe(
        map(([ethereumUsable, solana]) => {
            const names: ConnectionName[] = [];
            if (!ethereumUsable) names.push('ethereum');
            if (solana.node !== 'ready' && !(whileChecking && solana.node === 'checking'))
                names.push('solana');
            return names;
        })
    );
}

/**
 * Whether two lists of connection names are the same.
 *
 * @param a A list of names.
 * @param b Another list of names.
 * @returns `true` when they hold the same names in the same order.
 */
function sameNames(a: ConnectionName[], b: ConnectionName[]): boolean {
    return a.length === b.length && a.every((name, i) => name === b[i]);
}

/**
 * Emits once both chains can serve a read: at once if they already can.
 *
 * @param connections The two chains.
 * @returns A single emission when both are ready.
 */
export function bothReady$(
    connections: ProofRequestConnections
): Observable<void> {
    return notReady$(connections).pipe(
        filter((names) => names.length === 0),
        take(1),
        map((): void => undefined)
    );
}

/**
 * The chains a waiting machine waits on, each time the set changes while
 * some still cannot serve a read. The set the machine entered with is not
 * repeated: the first emission, the set as it stands when subscribed, is
 * skipped.
 *
 * @param connections The two chains.
 * @returns The names of the chains that cannot serve a read, when that set changes.
 */
export function waitingOnChanged$(
    connections: ProofRequestConnections
): Observable<ConnectionName[]> {
    return notReady$(connections).pipe(
        distinctUntilChanged(sameNames),
        skip(1),
        filter((names) => names.length > 0)
    );
}

/**
 * Decides what a Solana transport failure was, once its http has re-checked
 * itself (its transport already told it): straight back to `ready` means
 * the http is fine and the read itself failed; anywhere else means it was
 * lost.
 *
 * @param connections The two chains.
 * @param error The read's error.
 * @returns The outcome, once the http has settled.
 */
function outcomeAfterSolanaRecheck$(
    connections: ProofRequestConnections,
    error: unknown
): Observable<ConnectedRead<never>> {
    return connections.solana.http.connection.state$.pipe(
        filter(({ node }) => node !== 'checking'),
        take(1),
        switchMap(({ node }) =>
            node === 'ready'
                ? of({
                      outcome: 'failedOnHealthyConnection' as const,
                      error: messageOf(error),
                  })
                : connectionLost$(connections)
        )
    );
}

/**
 * Decides what it was when every Ethereum transport tried failed to reach
 * its node, once those transports (each already told) have re-checked
 * themselves: one usable again means the read itself failed; none means the
 * connection was lost.
 *
 * @param connections The two chains.
 * @param error The failure, listing the transports that got no response.
 * @returns The outcome, once they have settled.
 */
function outcomeAfterEthereumRecheck$(
    connections: ProofRequestConnections,
    error: ConnectionNotReadyError
): Observable<ConnectedRead<never>> {
    const { ethereum } = connections;
    const machines = error.notReady
        .filter(({ state }) => state.node === 'noResponse')
        .map(({ transport }) =>
            transport === 'ethereum.http'
                ? ethereum.http.connection
                : transport === 'ethereum.websocket'
                  ? ethereum.websocket.connection
                  : ethereum.wallet.connection
        )
        .filter((machine): machine is NonNullable<typeof machine> => machine !== undefined)
        .map((machine) =>
            (machine.state$ as Observable<{ node: string }>).pipe(
                filter(({ node }) => node !== 'checking'),
                take(1)
            )
        );
    return combineLatest(machines).pipe(
        take(1),
        switchMap(() => ethereumCallsUsable$(ethereum).pipe(take(1))),
        switchMap((usable) =>
            usable
                ? of({
                      outcome: 'failedOnHealthyConnection' as const,
                      error: messageOf(error),
                  })
                : connectionLost$(connections)
        )
    );
}

/**
 * Reports the chains lost, as they stand.
 *
 * @param connections The two chains.
 * @returns A single `connectionLost` naming the chains that cannot serve a read.
 */
function connectionLost$(
    connections: ProofRequestConnections
): Observable<ConnectedRead<never>> {
    return notReady$(connections).pipe(
        take(1),
        map((waitingOn) => ({ outcome: 'connectionLost' as const, waitingOn }))
    );
}

/**
 * Runs one read through both chains and reports how it ended:
 *
 * - `connectionLost`, naming the chains that cannot serve a read, when one
 *   cannot to begin with, stops being able to during the read, or every
 *   Ethereum transport in the order failed to reach its node;
 * - `failedOnHealthyConnection` when the read fails to reach Solana's node
 *   and its http, re-checked, is fine: the read itself is the problem;
 * - `failed` for any other error;
 * - `succeeded` with the read's value otherwise.
 *
 * Ethereum goes through `forCalls` (or `forLogs`), which runs the read on
 * the next transport in the caller's order when one fails to reach its
 * node; Solana goes through its http, whose transport tells its machine.
 *
 * @param connections The two chains.
 * @param read The read, given the clients to read through.
 * @param kind Whether Ethereum's calls or logs order applies.
 * @returns The outcome, once.
 */
export function readThroughConnections$<T>(
    connections: ProofRequestConnections,
    read: (clients: ConnectedReadClients) => Promise<T>,
    kind: 'calls' | 'logs' = 'calls'
): Observable<ConnectedRead<T>> {
    const readWhileReady$ = defer((): Observable<ConnectedRead<T>> => {
        // A failed request sends its transport to re-check itself, which is
        // not a loss: what the failure was is decided once the read settles.
        const readSettled$ = new Subject<void>();
        const lostWhileReading$ = notReady$(connections, true).pipe(
            filter((names) => names.length > 0),
            take(1),
            map((waitingOn) => ({
                outcome: 'connectionLost' as const,
                waitingOn,
            })),
            takeUntil(readSettled$)
        );
        const { ethereum, solana } = connections;
        const through = kind === 'logs' ? forLogs : forCalls;
        const result$ = defer(() =>
            from(
                solana.http
                    .ready()
                    .then((rpc) => through(ethereum, (provider) => read({ provider, rpc })))
            )
        ).pipe(
            map((value) => ({ outcome: 'succeeded' as const, value })),
            catchError((error: unknown) => {
                readSettled$.next();
                if (
                    error instanceof ConnectionNotReadyError &&
                    error.notReady.some(({ state }) => state.node === 'noResponse')
                )
                    return outcomeAfterEthereumRecheck$(connections, error);
                if (error instanceof ConnectionNotReadyError)
                    return of({
                        outcome: 'connectionLost' as const,
                        waitingOn: [
                            ...new Set(
                                error.notReady.map(
                                    ({ transport }) => transport.split('.')[0] as ConnectionName
                                )
                            ),
                        ],
                    });
                if (error instanceof SolanaRpcTransportError)
                    return outcomeAfterSolanaRecheck$(connections, error);
                return of({
                    outcome: 'failed' as const,
                    error: messageOf(error),
                });
            })
        );
        return merge(lostWhileReading$, result$).pipe(take(1));
    });

    return notReady$(connections).pipe(
        take(1),
        switchMap((waitingOn) =>
            waitingOn.length === 0
                ? readWhileReady$
                : of({ outcome: 'connectionLost' as const, waitingOn })
        )
    );
}
