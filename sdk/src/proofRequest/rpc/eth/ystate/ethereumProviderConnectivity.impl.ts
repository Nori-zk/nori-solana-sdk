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
    type EthereumProviderConnection,
    EthereumProviderConnectivityGraph,
} from './ethereumProviderConnectivity.js';

/** What one health check of the Ethereum provider found. */
export type EthereumHealthCheck =
    | {
          outcome: 'onExpectedChain';
          chainId: bigint;
          blockNumber: number;
          checkedAt: number;
      }
    | { outcome: 'onOtherChain'; chainId: bigint }
    | { outcome: 'failed'; error: string };

/** Everything outside the graph that the connectivity machine reacts to. */
export interface EthereumProviderConnectivityEnvironment extends HealthCheckTimings {
    /** The chain the proof request queue lives on. */
    expectedChainId: bigint;
    /**
     * Checks the provider once. Resolves with what it found on a chain it
     * reached; rejects when it cannot be reached.
     */
    checkHealth: () => Promise<
        Exclude<EthereumHealthCheck, { outcome: 'failed' }>
    >;
    /** The wallet reached the expected chain; never, for an RPC URL. */
    walletOnExpectedChain$: Observable<unknown>;
    /** The wallet left the expected chain (or has none, or is choosing); never, for an RPC URL. */
    walletNotOnExpectedChain$: Observable<unknown>;
    /** The wallet can reach a chain again (`connect`); never, for an RPC URL. */
    walletConnected$: Observable<unknown>;
    /** The wallet can no longer reach any chain (`disconnect`); never, for an RPC URL. */
    walletDisconnected$: Observable<unknown>;
    /** The network machine went offline. */
    networkWentOffline$: Observable<unknown>;
    /** The network machine came back online. */
    networkCameOnline$: Observable<unknown>;
    /** A read against the provider failed to reach it. */
    readFailed$: Observable<unknown>;
    /** The owner is closing the connection. */
    close$: Observable<unknown>;
    /** The running machine's states, from `stateOf$`. */
    connection$: Observable<EthereumProviderConnection>;
}

/**
 * Keeps only the health check results with one outcome, narrowed to it.
 *
 * @param check$ Health check results.
 * @param outcome The outcome to keep.
 * @returns The results with that outcome.
 */
function withOutcome<TOutcome extends EthereumHealthCheck['outcome']>(
    check$: Observable<EthereumHealthCheck>,
    outcome: TOutcome
) {
    return check$.pipe(
        filter(
            (
                check
            ): check is Extract<EthereumHealthCheck, { outcome: TOutcome }> =>
                check.outcome === outcome
        )
    );
}

/**
 * Implements the Ethereum provider connectivity graph over its environment:
 * waiting for the wallet, health checks with a timeout, background checks
 * while ready, retries with backoff while unreachable or on the wrong
 * network, wallet connect and disconnect, going offline and back, read
 * failures and closing.
 *
 * @param environment The health check, the wallet, network, read and close
 *   signals, the machine's own states and the timings.
 * @returns The implemented YState machine.
 */
export function createEthereumProviderConnectivityMachine(
    environment: EthereumProviderConnectivityEnvironment
) {
    const { expectedChainId } = environment;
    const timings = resolveHealthCheckTimings(environment);

    // One health check, timed out, with a failure as a result rather than an error.
    const runHealthCheck$ = (): Observable<EthereumHealthCheck> =>
        defer(() => from(environment.checkHealth())).pipe(
            timeout(timings.healthCheckTimeoutMs),
            catchError((error: unknown) =>
                of({ outcome: 'failed' as const, error: messageOf(error) })
            )
        );
    // The three outcome edges of a node share one check per entry into it:
    // the first subscribes and starts it, the others join it, and `share`
    // starts a fresh one on the next entry.
    const check$ = runHealthCheck$().pipe(share());
    const backgroundCheck$ = timer(timings.healthCheckIntervalMs).pipe(
        switchMap(runHealthCheck$),
        share()
    );

    return EthereumProviderConnectivityGraph.implement({
        walletOnExpectedChain: {
            $: () => environment.walletOnExpectedChain$,
            next: () => ({ failedChecks: 0 }),
        },
        walletNotOnExpectedChain: {
            $: () => environment.walletNotOnExpectedChain$,
            next: () => ({}),
        },
        checkFoundExpectedChain: {
            $: () => withOutcome(check$, 'onExpectedChain'),
            next: ({ chainId, blockNumber, checkedAt }) => ({
                chainId,
                blockNumber,
                checkedAt,
            }),
        },
        checkFoundOtherChain: {
            $: () => withOutcome(check$, 'onOtherChain'),
            next: ({ chainId }, _dest, source) => ({
                chainId,
                expectedChainId,
                failedChecks: source.failedChecks + 1,
            }),
        },
        checkFailed: {
            $: () => withOutcome(check$, 'failed'),
            next: ({ error }, _dest, source) => ({
                failedChecks: source.failedChecks + 1,
                error,
            }),
        },
        backgroundCheckPassed: {
            $: () => withOutcome(backgroundCheck$, 'onExpectedChain'),
            next: ({ chainId, blockNumber, checkedAt }) => ({
                chainId,
                blockNumber,
                checkedAt,
            }),
        },
        backgroundCheckFoundOtherChain: {
            $: () => withOutcome(backgroundCheck$, 'onOtherChain'),
            next: ({ chainId }) => ({
                chainId,
                expectedChainId,
                failedChecks: 1,
            }),
        },
        backgroundCheckFailed: {
            $: () => withOutcome(backgroundCheck$, 'failed'),
            next: ({ error }) => ({ failedChecks: 1, error }),
        },
        readFailed: {
            $: () => environment.readFailed$,
            next: () => ({ failedChecks: 0 }),
        },
        retryDue: {
            $: () =>
                dataOnEntry$<EthereumProviderConnection, 'unreachable'>(
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
                dataOnEntry$<EthereumProviderConnection, 'wrongNetwork'>(
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
        // A disconnect counts as a failed check.
        walletDisconnected: {
            $: () => environment.walletDisconnected$,
            next: (_disconnect, _dest, source) => ({
                failedChecks:
                    'failedChecks' in source ? source.failedChecks + 1 : 1,
                error: 'The wallet cannot reach any chain.',
            }),
        },
        walletConnected: {
            $: () => environment.walletConnected$,
            next: (_connect, _dest, source) => ({
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
