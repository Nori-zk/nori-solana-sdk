import {
    createDefaultRpcTransport,
    createSolanaRpcFromTransport,
    isSolanaError,
    type RpcTransport,
} from '@solana/kit';
import { type RunningMachine } from '@yaw-rx/ystate';
import { filter, ReplaySubject, Subject, Subscription } from 'rxjs';
import { stateOf$ } from '../../ystate/dataOnEntry.js';
import { type HealthCheckTimings } from '../healthCheckTimings.js';
import { type NetworkMachine } from '../ystate/network.impl.js';
import { SolanaRpcTransportError } from './errors.js';
import { createSolanaRpcConnectivityMachine } from './ystate/solanaRpcConnectivity.impl.js';

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
export type SolanaRpcConnectivityOptions = HealthCheckTimings &
    (
        | { cluster: SolanaCluster; rpcUrls?: string[] }
        | { rpcUrls: string[]; expectedGenesisHash: string }
    );

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
 * @returns The transport.
 */
function createTransportReportingFailedRequests(rpcUrl: string): RpcTransport {
    const transport = createDefaultRpcTransport({ url: rpcUrl });
    return async (config) => {
        try {
            return await transport(config);
        } catch (error) {
            if (isSolanaError(error) || config.signal?.aborted) throw error;
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
 * @returns The RPC.
 */
function createSolanaRpc(rpcUrl: string) {
    return createSolanaRpcFromTransport(
        createTransportReportingFailedRequests(rpcUrl)
    );
}

/** The Solana RPC reads go through. */
export type SolanaRpc = ReturnType<typeof createSolanaRpc>;

/**
 * Opens the Solana RPC endpoints reads go through and runs their
 * connectivity machine. Solana wallets do not serve reads, so reads always
 * go through an RPC node: the cluster's public endpoint, unless the app
 * gives its own.
 *
 * @param options The cluster and endpoints, and timings.
 * @param network The running network machine both connections follow.
 * @returns
 *   - `solanaRpcConnectivity`: the running connectivity machine.
 *   - `currentRpc()`: the RPC of the endpoint that last passed its check, while `ready`.
 *   - `reportReadFailed()`: tells the machine a read failed to reach the node.
 *   - `close()`: moves the machine to `closed`.
 */
export function getSolanaRpcWithConnectivity$(
    options: SolanaRpcConnectivityOptions,
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

    const rpcs = new Map(
        rpcUrls.map((rpcUrl) => [rpcUrl, createSolanaRpc(rpcUrl)])
    );
    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<RunningMachine>(1);
    const subscriptions = new Subscription();

    const machine = createSolanaRpcConnectivityMachine({
        ...options,
        rpcUrls,
        expectedGenesisHash,
        checkHealth: async (rpcUrl) => {
            const rpc = rpcs.get(rpcUrl);
            if (!rpc) throw new Error(`No Solana RPC for ${rpcUrl}.`);
            const genesisHash = await rpc.getGenesisHash().send();
            if (genesisHash !== expectedGenesisHash) {
                return { outcome: 'onOtherCluster', rpcUrl, genesisHash };
            }
            const slot = await rpc.getSlot({ commitment: 'finalized' }).send();
            return {
                outcome: 'onExpectedCluster',
                rpcUrl,
                slot,
                checkedAt: Date.now(),
            };
        },
        networkWentOffline$: network.state$.pipe(
            filter((state) => state.node === 'offline')
        ),
        networkCameOnline$: network.state$.pipe(
            filter((state) => state.node === 'online')
        ),
        readFailed$,
        close$,
        connection$: stateOf$(started$),
    });
    const solanaRpcConnectivity = machine.close().start('checking');
    started$.next(solanaRpcConnectivity);

    // The endpoint reads go through: the one that last passed its check.
    let currentRpc: SolanaRpc | undefined;
    subscriptions.add(
        solanaRpcConnectivity.state$.subscribe(({ node, data }) => {
            if (node !== 'ready' || typeof data !== 'object' || data === null)
                return;
            if (!('rpcUrl' in data) || typeof data.rpcUrl !== 'string') return;
            currentRpc = rpcs.get(data.rpcUrl);
        })
    );
    subscriptions.add(
        solanaRpcConnectivity.status$
            .pipe(filter((status) => status !== 'running'))
            .subscribe(() => subscriptions.unsubscribe())
    );

    return {
        solanaRpcConnectivity,
        currentRpc: (): SolanaRpc => {
            if (!currentRpc)
                throw new Error(
                    'No Solana RPC endpoint has passed its check yet.'
                );
            return currentRpc;
        },
        reportReadFailed: () => readFailed$.next(),
        close: () => close$.next(),
    };
}

/** A Solana RPC with its running connectivity machine. */
export type SolanaRpcWithConnectivity = ReturnType<
    typeof getSolanaRpcWithConnectivity$
>;
