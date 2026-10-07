import {
    catchError,
    defer,
    filter,
    from,
    type Observable,
    of,
    share,
    switchMap,
    timeout,
    timer,
} from 'rxjs';
import { messageOf } from '../../../messageOf.js';
import { dataOnEntry$ } from '../../../ystate/dataOnEntry.js';
import {
    type HealthCheckTimings,
    resolveHealthCheckTimings,
    retryDelayMs,
} from '../../healthCheckTimings.js';
import {
    type SolanaRpcConnection,
    SolanaRpcConnectivityGraph,
} from './solanaRpcConnectivity.js';

/** What one health check of a Solana RPC endpoint found. */
export type SolanaHealthCheck =
    | {
          outcome: 'onExpectedCluster';
          rpcUrl: string;
          slot: bigint;
          checkedAt: number;
      }
    | { outcome: 'onOtherCluster'; rpcUrl: string; genesisHash: string }
    | { outcome: 'failed'; rpcUrl: string; error: string };

/** Everything outside the graph that the connectivity machine reacts to. */
export interface SolanaRpcConnectivityEnvironment extends HealthCheckTimings {
    /** Genesis hash of the cluster the bridge program lives on. */
    expectedGenesisHash: string;
    /** The endpoints, in the order they are tried. */
    rpcUrls: string[];
    /**
     * Checks one endpoint once. Resolves with what it found on a cluster it
     * reached; rejects when it cannot be reached.
     */
    checkHealth: (
        rpcUrl: string
    ) => Promise<Exclude<SolanaHealthCheck, { outcome: 'failed' }>>;
    /** The network machine went offline. */
    networkWentOffline$: Observable<unknown>;
    /** The network machine came back online. */
    networkCameOnline$: Observable<unknown>;
    /** A read against the RPC failed to reach it. */
    readFailed$: Observable<unknown>;
    /** The owner is closing the connection. */
    close$: Observable<unknown>;
    /** The running machine's states, from `stateOf$`. */
    connection$: Observable<{ node: string; data: unknown }>;
}

/**
 * Keeps only the health check results with one outcome, narrowed to it.
 *
 * @param check$ Health check results.
 * @param outcome The outcome to keep.
 * @returns The results with that outcome.
 */
function withOutcome<TOutcome extends SolanaHealthCheck['outcome']>(
    check$: Observable<SolanaHealthCheck>,
    outcome: TOutcome
) {
    return check$.pipe(
        filter(
            (
                check
            ): check is Extract<SolanaHealthCheck, { outcome: TOutcome }> =>
                check.outcome === outcome
        )
    );
}

/**
 * Implements the Solana RPC connectivity graph over its environment: health
 * checks with a timeout, moving on to the next endpoint after each failed
 * check, background checks while ready, retries with backoff while
 * unreachable or on the wrong network, going offline and back, read
 * failures and closing.
 *
 * @param environment The endpoints, the health check, the network, read
 *   and close signals, the machine's own states and the timings.
 * @returns The implemented YState machine; `.close().start('checking')` runs it.
 */
export function createSolanaRpcConnectivityMachine(
    environment: SolanaRpcConnectivityEnvironment
) {
    const { expectedGenesisHash, rpcUrls } = environment;
    const timings = resolveHealthCheckTimings(environment);

    /**
     * One health check of `rpcUrl`, timed out, with a failure as a result
     * rather than an error.
     *
     * @param rpcUrl The endpoint to check.
     * @returns The check's result, once.
     */
    const runHealthCheck$ = (rpcUrl: string): Observable<SolanaHealthCheck> =>
        defer(() => from(environment.checkHealth(rpcUrl))).pipe(
            timeout(timings.healthCheckTimeoutMs),
            catchError((error: unknown) =>
                of({
                    outcome: 'failed' as const,
                    rpcUrl,
                    error: messageOf(error),
                })
            )
        );
    // The three outcome edges of `checking` share one check per entry into
    // it, of the endpoint chosen by how many checks have failed in a row:
    // each failure moves on to the next endpoint.
    const check$ = dataOnEntry$<SolanaRpcConnection, 'checking'>(
        environment.connection$,
        'checking'
    ).pipe(
        switchMap(({ failedChecks }) =>
            runHealthCheck$(rpcUrls[failedChecks % rpcUrls.length])
        ),
        share()
    );
    // Background checks while ready stay on the endpoint that passed. `ready`
    // has the `stillReady` self-loop, where the state replayed on re-entry is
    // the previous `ready` rather than the new one; it carries the same
    // endpoint, because a passing background check stays on it, so the check
    // goes to the right endpoint either way.
    const backgroundCheck$ = dataOnEntry$<SolanaRpcConnection, 'ready'>(
        environment.connection$,
        'ready'
    ).pipe(
        switchMap(({ rpcUrl }) =>
            timer(timings.healthCheckIntervalMs).pipe(
                switchMap(() => runHealthCheck$(rpcUrl))
            )
        ),
        share()
    );

    return SolanaRpcConnectivityGraph.implement({
        checkFoundExpectedCluster: {
            $: () => withOutcome(check$, 'onExpectedCluster'),
            next: ({ rpcUrl, slot, checkedAt }) => ({
                rpcUrl,
                slot,
                checkedAt,
            }),
        },
        checkFoundOtherCluster: {
            $: () => withOutcome(check$, 'onOtherCluster'),
            next: ({ rpcUrl, genesisHash }, _dest, source) => ({
                rpcUrl,
                genesisHash,
                expectedGenesisHash,
                failedChecks: source.failedChecks + 1,
            }),
        },
        checkFailed: {
            $: () => withOutcome(check$, 'failed'),
            next: ({ rpcUrl, error }, _dest, source) => ({
                rpcUrl,
                failedChecks: source.failedChecks + 1,
                error,
            }),
        },
        backgroundCheckPassed: {
            $: () => withOutcome(backgroundCheck$, 'onExpectedCluster'),
            next: ({ rpcUrl, slot, checkedAt }) => ({
                rpcUrl,
                slot,
                checkedAt,
            }),
        },
        backgroundCheckFoundOtherCluster: {
            $: () => withOutcome(backgroundCheck$, 'onOtherCluster'),
            next: ({ rpcUrl, genesisHash }) => ({
                rpcUrl,
                genesisHash,
                expectedGenesisHash,
                failedChecks: 1,
            }),
        },
        backgroundCheckFailed: {
            $: () => withOutcome(backgroundCheck$, 'failed'),
            next: ({ rpcUrl, error }) => ({ rpcUrl, failedChecks: 1, error }),
        },
        readFailed: {
            $: () => environment.readFailed$,
            next: () => ({ failedChecks: 0 }),
        },
        retryDue: {
            $: () =>
                dataOnEntry$<SolanaRpcConnection, 'unreachable'>(
                    environment.connection$,
                    'unreachable'
                ).pipe(
                    switchMap(({ failedChecks }) =>
                        timer(retryDelayMs(failedChecks, timings))
                    )
                ),
            next: (_due, _dest, source) => ({
                failedChecks: source.failedChecks,
            }),
        },
        recheckDue: {
            $: () =>
                dataOnEntry$<SolanaRpcConnection, 'wrongNetwork'>(
                    environment.connection$,
                    'wrongNetwork'
                ).pipe(
                    switchMap(({ failedChecks }) =>
                        timer(retryDelayMs(failedChecks, timings))
                    )
                ),
            next: (_due, _dest, source) => ({
                failedChecks: source.failedChecks,
            }),
        },
        networkWentOffline: {
            $: () => environment.networkWentOffline$,
            next: () => ({}),
        },
        networkCameOnline: {
            $: () => environment.networkCameOnline$,
            next: () => ({ failedChecks: 0 }),
        },
        close: {
            $: () => environment.close$,
            next: () => ({}),
        },
    });
}
