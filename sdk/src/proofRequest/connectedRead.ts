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
import type { EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { messageOf } from './messageOf.js';
import {
    type EthereumProviderConnectivityOptions,
    type EthereumProviderWithConnectivity,
    getEthereumProviderWithConnectivity$,
} from './rpc/eth/ethereumProvider.js';
import { EthRpcTransportError } from './rpc/eth/errors.js';
import { SolanaRpcTransportError } from './rpc/solana/errors.js';
import {
    getSolanaRpcWithConnectivity$,
    type SolanaRpc,
    type SolanaRpcConnectivityOptions,
    type SolanaRpcWithConnectivity,
} from './rpc/solana/solanaRpc.js';
import {
    createNetworkMachine,
    type NetworkMachine,
    type NetworkOptions,
} from './rpc/ystate/network.impl.js';

/**
 * The Ethereum provider and Solana RPC a reading machine reads through,
 * each with its running connectivity machine. The reading machines are
 * coupled to the connectivity machines through these running instances'
 * `state$`, as the heater and the room are coupled through shared streams,
 * not through graph `deps`: a graph's deps must be implemented at definition
 * time, and each connectivity machine is implemented per instance around its
 * own endpoints.
 */
export interface ProofRequestConnections {
    ethereum: EthereumProviderWithConnectivity;
    solana: SolanaRpcWithConnectivity;
}

/** Which of the two connections. */
export type ConnectionName = keyof ProofRequestConnections;

/** The clients one read goes through. */
export interface ConnectedReadClients {
    provider: EthereumProvider;
    rpc: SolanaRpc;
}

/** How one read through both connections ended. */
export type ConnectedRead<T> =
    | { outcome: 'succeeded'; value: T }
    | { outcome: 'connectionLost'; waitingOn: ConnectionName[] }
    | { outcome: 'failedOnHealthyConnection'; error: string }
    | { outcome: 'failed'; error: string };

/** A running machine's states, as far as readiness is concerned. */
type NodeStream = Observable<{ node: string }>;

/**
 * The connections that are not `ready`, each time either changes state.
 *
 * @param connections The two connections.
 * @returns The names of the connections not `ready`, in a fixed order.
 */
function notReady$(
    connections: ProofRequestConnections
): Observable<ConnectionName[]> {
    return combineLatest([
        connections.ethereum.ethereumProviderConnectivity.state$,
        connections.solana.solanaRpcConnectivity.state$,
    ]).pipe(
        map(([ethereum, solana]) => {
            const names: ConnectionName[] = [];
            if (ethereum.node !== 'ready') names.push('ethereum');
            if (solana.node !== 'ready') names.push('solana');
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
 * Emits once both connections are at `ready`: at once if they already are.
 *
 * @param connections The two connections.
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
 * The connections a waiting machine waits on, each time the set changes
 * while some are still not `ready`. The set the machine entered with is not
 * repeated: the first emission, the set as it stands when subscribed, is
 * skipped.
 *
 * @param connections The two connections.
 * @returns The names of the connections not `ready`, when that set changes.
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
 * Decides what a transport failure was, once its connection has re-checked
 * itself: if the connection's health check passes straight back to `ready`,
 * the connection is fine and the read itself failed; if it goes anywhere
 * else (`unreachable`, the wrong network, waiting for the wallet, offline,
 * `closed`), the connection was lost.
 *
 * @param connections The two connections.
 * @param failed The connection the read failed to reach, already sent `readFailed`.
 * @param error The read's error.
 * @returns The outcome, once the connection has settled.
 */
function outcomeAfterRecheck$(
    connections: ProofRequestConnections,
    failed: ConnectionName,
    error: unknown
): Observable<ConnectedRead<never>> {
    const failed$: NodeStream =
        failed === 'ethereum'
            ? connections.ethereum.ethereumProviderConnectivity.state$
            : connections.solana.solanaRpcConnectivity.state$;
    return failed$.pipe(
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
 * Reports the connections lost, as they stand.
 *
 * @param connections The two connections.
 * @returns A single `connectionLost` naming the connections not `ready`.
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
 * Runs one read through both connections and reports how it ended:
 *
 * - `connectionLost`, naming the connections not `ready`, when one is not
 *   `ready` to begin with, leaves `ready` during the read, or fails the read
 *   and then turns out to be down;
 * - `failedOnHealthyConnection` when the read fails to reach a connection
 *   that, re-checked, is fine: the read itself is the problem;
 * - `failed` for any other error;
 * - `succeeded` with the read's value otherwise.
 *
 * A transport failure (`EthRpcTransportError` or `SolanaRpcTransportError`,
 * each after its own retries) is first reported to its connection with
 * `reportReadFailed()`, which moves it from `ready` to `checking`, and the
 * outcome waits for that check to settle.
 *
 * @param connections The two connections.
 * @param read The read, given the clients to read through.
 * @returns The outcome, once.
 */
export function readThroughConnections$<T>(
    connections: ProofRequestConnections,
    read: (clients: ConnectedReadClients) => Promise<T>
): Observable<ConnectedRead<T>> {
    const readWhileReady$ = defer((): Observable<ConnectedRead<T>> => {
        // Stops watching for a lost connection once the read has failed:
        // reporting the failure takes its connection out of `ready` on purpose.
        const readSettled$ = new Subject<void>();
        const lostWhileReading$ = notReady$(connections).pipe(
            filter((names) => names.length > 0),
            take(1),
            map((waitingOn) => ({
                outcome: 'connectionLost' as const,
                waitingOn,
            })),
            takeUntil(readSettled$)
        );
        const result$ = defer(() =>
            from(
                read({
                    provider: connections.ethereum.currentProvider(),
                    rpc: connections.solana.currentRpc(),
                })
            )
        ).pipe(
            map((value) => ({ outcome: 'succeeded' as const, value })),
            catchError((error: unknown) => {
                readSettled$.next();
                if (error instanceof EthRpcTransportError) {
                    connections.ethereum.reportReadFailed();
                    return outcomeAfterRecheck$(connections, 'ethereum', error);
                }
                if (error instanceof SolanaRpcTransportError) {
                    connections.solana.reportReadFailed();
                    return outcomeAfterRecheck$(connections, 'solana', error);
                }
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

export interface ProofRequestConnectionsOptions {
    /** The Ethereum chain and, optionally, an RPC URL; without one, reads go through the user's wallet. */
    ethereum: EthereumProviderConnectivityOptions;
    /** The Solana cluster and, optionally, RPC URLs; without them, its public endpoint. */
    solana: SolanaRpcConnectivityOptions;
    /** Connectivity probing. */
    network?: NetworkOptions;
}

/**
 * Starts everything reads need: the network machine, and the Ethereum and
 * Solana connections following it.
 *
 * @param options The Ethereum chain, the Solana cluster, and optional endpoints and timings.
 * @returns
 *   - `network`: the running network machine.
 *   - `connections`: the two connections, for the reading machines.
 *   - `close()`: closes all three.
 */
export function createProofRequestConnections(
    options: ProofRequestConnectionsOptions
) {
    const { network, close: closeNetwork }: NetworkMachine =
        createNetworkMachine(options.network);
    const connections: ProofRequestConnections = {
        ethereum: getEthereumProviderWithConnectivity$(
            options.ethereum,
            network
        ),
        solana: getSolanaRpcWithConnectivity$(options.solana, network),
    };
    return {
        network,
        connections,
        close: () => {
            connections.ethereum.close();
            connections.solana.close();
            closeNetwork();
        },
    };
}
