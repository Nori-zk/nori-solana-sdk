import { JsonRpcProvider, Network } from 'ethers';
import { filter, Subject, Subscription } from 'rxjs';
import {
    type EthereumProvider,
    parseRpcUrl,
} from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { atNode } from '../../utils/machines.js';
import { type HealthCheckTimings } from '../connection/healthCheckTimings.js';
import { type HttpConnectionState } from '../connection/httpConnection.js';
import { httpConnection } from '../connection/httpConnection.impl.js';
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
    network: NetworkMachine['network']
) {
    const { expectedChainId, rpcUrl } = options;
    const client: EthereumProvider = new JsonRpcProvider(parseRpcUrl(rpcUrl), undefined, {
        staticNetwork: Network.from(expectedChainId),
    });
    const request = (method: 'eth_chainId' | 'eth_blockNumber') =>
        (client as JsonRpcProvider).send(method, []) as Promise<string>;
    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();
    const subscriptions = new Subscription();

    const connection = httpConnection<EthereumHealth>({
        ...options,
        urls: [rpcUrl],
        checkHealth: async (url) => {
            const chainId = BigInt(await request('eth_chainId'));
            if (chainId !== expectedChainId)
                return {
                    outcome: 'onOtherNetwork',
                    url,
                    found: chainId.toString(),
                    expected: expectedChainId.toString(),
                };
            const blockNumber = Number(BigInt(await request('eth_blockNumber')));
            return {
                outcome: 'onExpectedNetwork',
                url,
                health: { blockNumber },
                checkedAt: Date.now(),
            };
        },
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
