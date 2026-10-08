import {
    createDefaultRpcTransport,
    createSolanaRpcFromTransport,
    isSolanaError,
    type RpcTransport,
} from '@solana/kit';
import { filter, Subject } from 'rxjs';
import { atNode } from '../../utils/machines.js';
import { type HealthCheckTimings } from '../connection/healthCheckTimings.js';
import { type HttpConnectionState } from '../connection/httpConnection.js';
import { httpConnection } from '../connection/httpConnection.impl.js';
import { type NetworkMachine } from '../connection/network.impl.js';
import { SolanaRpcTransportError } from './errors.js';

/** The public Solana clusters, their public RPC endpoints and genesis hashes. */
export const PUBLIC_SOLANA_CLUSTERS = {
    mainnet: {
        rpcUrl: 'https://api.mainnet-beta.solana.com',
        genesisHash: '5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d',
    },
    devnet: {
        rpcUrl: 'https://api.devnet.solana.com',
        genesisHash: 'EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG',
    },
    testnet: {
        rpcUrl: 'https://api.testnet.solana.com',
        genesisHash: '4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY',
    },
} as const;

/** A public Solana cluster. */
export type SolanaCluster = keyof typeof PUBLIC_SOLANA_CLUSTERS;

/**
 * Which Solana cluster to read from, and through which endpoints:
 *
 * - a public cluster, through its public endpoint unless `rpcUrls` are given;
 * - any other cluster (e.g. a local validator), through `rpcUrls`, checked
 *   against its `expectedGenesisHash`.
 *
 * With several endpoints, a failed health check moves on to the next one.
 */
export type SolanaHttpOptions = HealthCheckTimings &
    (
        | { cluster: SolanaCluster; rpcUrls?: string[] }
        | { rpcUrls: string[]; expectedGenesisHash: string }
    );

/** What a passing health check of a Solana RPC node found: its latest finalized slot. */
export interface SolanaHealth {
    slot: bigint;
}

/** The Solana HTTP connection's state: a node of the HTTP connection graph and its data. */
export type SolanaHttpConnectionState = HttpConnectionState<SolanaHealth>;

/**
 * The default HTTP transport for `rpcUrl`, reporting a request that never
 * got a response as `SolanaRpcTransportError`.
 *
 * `fetch` rejects with a plain `TypeError` when a request never reaches the
 * node, and its message differs between runtimes, so it cannot be told
 * apart from other errors once it has left the transport. Inside the
 * transport nothing runs but the request itself, so any error here that is
 * not a `SolanaError` (which the transport raises for HTTP error responses)
 * is a failed request. A request the caller aborted is passed on as is.
 *
 * @param rpcUrl HTTP(S) URL of the Solana RPC node.
 * @param onNoResponse Called for each request that got no response.
 * @returns The transport.
 */
function createTransportReportingFailedRequests(
    rpcUrl: string,
    onNoResponse: () => void
): RpcTransport {
    const transport = createDefaultRpcTransport({ url: rpcUrl });
    return async (config) => {
        try {
            return await transport(config);
        } catch (error) {
            if (isSolanaError(error) || config.signal?.aborted) throw error;
            onNoResponse();
            throw new SolanaRpcTransportError(
                `The request to ${rpcUrl} got no response.`,
                error
            );
        }
    };
}

/**
 * The Solana RPC for `rpcUrl`, reporting requests that got no response.
 *
 * @param rpcUrl HTTP(S) URL of the Solana RPC node.
 * @param onNoResponse Called for each request that got no response.
 * @returns The RPC.
 */
function createSolanaRpc(rpcUrl: string, onNoResponse: () => void = () => undefined) {
    return createSolanaRpcFromTransport(
        createTransportReportingFailedRequests(rpcUrl, onNoResponse)
    );
}

/** The Solana RPC reads go through. */
export type SolanaRpc = ReturnType<typeof createSolanaRpc>;

/**
 * Opens the Solana RPC endpoints reads go through and runs their HTTP
 * connection machine. Solana wallets do not serve reads, so reads always go
 * through an RPC node: the cluster's public endpoint, unless the app gives
 * its own. The health check reads the genesis hash (the cluster) and the
 * latest finalized slot.
 *
 * @param options The cluster and endpoints, and timings.
 * @param network The running network machine the connection follows.
 * @returns
 *   - `connection`: the running HTTP connection machine.
 *   - `current()`: the RPC of the endpoint that last passed its check, if one has.
 *   - `reportReadFailed()`: tells the machine a request failed to reach the node; its
 *     own requests already do.
 *   - `close()`: moves the machine to `closed`.
 */
export function solanaHttp(
    options: SolanaHttpOptions,
    network: NetworkMachine['network']
) {
    const [rpcUrls, expectedGenesisHash] =
        'cluster' in options
            ? [
                  options.rpcUrls ?? [
                      PUBLIC_SOLANA_CLUSTERS[options.cluster].rpcUrl,
                  ],
                  PUBLIC_SOLANA_CLUSTERS[options.cluster].genesisHash,
              ]
            : [options.rpcUrls, options.expectedGenesisHash];
    if (rpcUrls.length === 0)
        throw new RangeError('At least one Solana RPC URL is needed.');

    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();
    // A request that got no response tells the machine, which checks again.
    const rpcs = new Map(
        rpcUrls.map((rpcUrl) => [rpcUrl, createSolanaRpc(rpcUrl, () => readFailed$.next())])
    );

    const connection = httpConnection<SolanaHealth>({
        ...options,
        urls: rpcUrls,
        checkHealth: async (url) => {
            const rpc = rpcs.get(url);
            if (!rpc) throw new Error(`No Solana RPC for ${url}.`);
            const genesisHash = await rpc.getGenesisHash().send();
            if (genesisHash !== expectedGenesisHash) {
                return {
                    outcome: 'onOtherNetwork',
                    url,
                    found: genesisHash,
                    expected: expectedGenesisHash,
                };
            }
            const slot = await rpc.getSlot({ commitment: 'finalized' }).send();
            return {
                outcome: 'onExpectedNetwork',
                url,
                health: { slot },
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

    // The RPC of the endpoint that last passed its check.
    let currentRpc: SolanaRpc | undefined;
    const subscription = connection.state$
        .pipe(filter(atNode('ready')))
        .subscribe((state) => {
            currentRpc = rpcs.get(state.data.url);
        });
    connection.status$
        .pipe(filter((status) => status !== 'running'))
        .subscribe(() => subscription.unsubscribe());

    return {
        connection,
        current: (): SolanaRpc | undefined => currentRpc,
        reportReadFailed: () => readFailed$.next(),
        close: () => close$.next(),
    };
}

/** A Solana RPC with its running HTTP connection machine. */
export type SolanaHttp = ReturnType<typeof solanaHttp>;
