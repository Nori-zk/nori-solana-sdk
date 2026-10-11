import { FetchRequest, JsonRpcProvider, Network } from 'ethers';
import { defer, filter, map, type Observable, of, Subject, Subscription, switchMap } from 'rxjs';
import {
    type EthereumProvider,
    parseRpcUrl,
} from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { atNode } from '../../utils/machines.js';
import { type HealthCheckTimings, resolveHealthCheckTimings } from '../connection/healthCheckTimings.js';
import { type HttpConnectionState } from '../connection/httpConnection.js';
import { httpConnection, type HttpHealthCheck } from '../connection/httpConnection.impl.js';
import { type NetworkMachine } from '../connection/network.impl.js';

/** The expected chain, the HTTP(S) RPC URL to read through, and timings. */
export interface EthereumHttpOptions extends HealthCheckTimings {
    /** The chain the proof request queue lives on. */
    expectedChainId: bigint;
    /** The HTTP(S) RPC URL. */
    rpcUrl: string;
}

/** What a passing health check of an Ethereum RPC node found: its latest block. */
export interface EthereumHealth {
    blockNumber: number;
}

/** The Ethereum HTTP connection's state: a node of the HTTP connection graph and its data. */
export type EthereumHttpConnectionState = HttpConnectionState<EthereumHealth>;

/**
 * A chain's health check, through whatever serves its requests (an RPC URL
 * or the wallet): its chain with `eth_chainId`, then, on the expected chain,
 * its latest block with `eth_blockNumber`.
 *
 * @param request$ One request of the method, its result as a number.
 * @param expectedChainId The chain it should be on.
 * @param url What is checked, for the result.
 * @returns What the check found, once; errors when it cannot be reached.
 */
export const chainHealthCheck$ = (
    request$: (method: 'eth_chainId' | 'eth_blockNumber') => Observable<bigint>,
    expectedChainId: bigint,
    url: string
): Observable<Exclude<HttpHealthCheck<EthereumHealth>, { outcome: 'failed' }>> =>
    request$('eth_chainId').pipe(
        switchMap((chainId) =>
            chainId !== expectedChainId
                ? of({
                      outcome: 'onOtherNetwork' as const,
                      url,
                      found: chainId.toString(),
                      expected: expectedChainId.toString(),
                  })
                : request$('eth_blockNumber').pipe(
                      map((blockNumber) => ({
                          outcome: 'onExpectedNetwork' as const,
                          url,
                          health: { blockNumber: Number(blockNumber) },
                          checkedAt: Date.now(),
                      }))
                  )
        )
    );

/**
 * Opens the Ethereum RPC URL reads go through and runs its HTTP connection
 * machine. The URL's chain is fixed, so ethers is given it up front and
 * never re-detects it; the health check reads it with a raw `eth_chainId`,
 * then the latest block with `eth_blockNumber`.
 *
 * @param options The expected chain, the RPC URL and timings.
 * @param network The running network machine the connection follows.
 * @returns
 *   - `connection`: the running HTTP connection machine.
 *   - `current()`: the ethers provider requests go through; one for the URL's life.
 *   - `reportReadFailed()`: tells the machine a read failed to reach the node.
 *   - `close()`: moves the machine to `closed`.
 */
export function ethereumHttp(
    options: EthereumHttpOptions,
    network: NetworkMachine
) {
    const { expectedChainId, rpcUrl } = options;
    // Every request times out, so one to a node that stopped answering fails
    // and reports it instead of hanging.
    const fetchRequest = new FetchRequest(parseRpcUrl(rpcUrl));
    fetchRequest.timeout = resolveHealthCheckTimings(options).requestTimeoutMs;
    const client: EthereumProvider = new JsonRpcProvider(fetchRequest, undefined, {
        staticNetwork: Network.from(expectedChainId),
    });
    const request$ = (method: 'eth_chainId' | 'eth_blockNumber') =>
        defer(() => (client as JsonRpcProvider).send(method, []) as Promise<string>).pipe(map(BigInt));
    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();
    const subscriptions = new Subscription();

    const connection = httpConnection<EthereumHealth>({
        ...options,
        urls: [rpcUrl],
        checkHealth: (url) => chainHealthCheck$(request$, expectedChainId, url),
        networkWentOffline$: network.state$.pipe(
            filter(atNode('offline'))
        ),
        networkCameOnline$: network.state$.pipe(
            filter(atNode('online'))
        ),
        readFailed$,
        close$,
    });

    subscriptions.add(
        connection.status$
            .pipe(filter((status) => status !== 'running'))
            .subscribe(() => {
                client.destroy();
                subscriptions.unsubscribe();
            })
    );

    return {
        connection,
        current: (): EthereumProvider => client,
        reportReadFailed: () => readFailed$.next(),
        close: () => close$.next(),
    };
}

/** An Ethereum RPC URL with its running HTTP connection machine. */
export type EthereumHttp = ReturnType<typeof ethereumHttp>;
