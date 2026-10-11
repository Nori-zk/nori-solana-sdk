import {
    createDefaultRpcTransport,
    createSolanaRpcFromTransport,
    isSolanaError,
    type RpcResponse,
    type RpcTransport,
    SOLANA_ERROR__RPC__TRANSPORT_HTTP_ERROR,
} from '@solana/kit';
import { defer, filter, finalize, from, map, type Observable, of, Subject, switchMap } from 'rxjs';
import { atNode } from '../../utils/machines.js';
import {
    type GrantedSend,
    type RequestResult,
    retryAfterMsOf,
    TOO_MANY_REQUESTS,
} from '../connection/connectionGrants.js';
import { type HealthCheckTimings, resolveHealthCheckTimings } from '../connection/healthCheckTimings.js';
import { type HttpConnectionState } from '../connection/httpConnection.js';
import { type HealthCheck } from '../connection/connection.impl.js';
import { httpConnection } from '../connection/httpConnection.impl.js';
import { type NetworkMachine } from '../connection/network.impl.js';
import { SolanaRpcTransportError } from './errors.js';
import { solanaRpcRead$ } from './solanaRpcRead.js';

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
 * The default HTTP transport for `rpcUrl`, its requests failing with
 * `SolanaRpcTransportError` when they get no response or take longer than
 * `timeoutMs`.
 *
 * `fetch` rejects with a plain `TypeError` when a request never reaches the
 * node, and its message differs between runtimes, so it cannot be told
 * apart from other errors once it has left the transport. Inside the
 * transport nothing runs but the request itself, so any error here that is
 * not a `SolanaError` (which the transport raises for HTTP error responses)
 * is a failed request. A request the caller aborted is passed on as is.
 *
 * @param rpcUrl HTTP(S) URL of the Solana RPC node.
 * @param timeoutMs How long a request may take before it counts as unanswered.
 * @returns The transport.
 */
function createTransportNamingUnansweredRequests(rpcUrl: string, timeoutMs: number): RpcTransport {
    const transport = createDefaultRpcTransport({ url: rpcUrl });
    return async (config) => {
        const timedOut = new AbortController();
        const timer = setTimeout(
            () => timedOut.abort(new Error(`The request to ${rpcUrl} took over ${timeoutMs} ms.`)),
            timeoutMs
        );
        const forwardAbort = () => timedOut.abort(config.signal?.reason);
        config.signal?.addEventListener('abort', forwardAbort);
        try {
            return await transport({ ...config, signal: timedOut.signal });
        } catch (error) {
            if (isSolanaError(error) || config.signal?.aborted) throw error;
            throw new SolanaRpcTransportError(`The request to ${rpcUrl} got no response.`, error);
        } finally {
            clearTimeout(timer);
            config.signal?.removeEventListener('abort', forwardAbort);
        }
    };
}

/**
 * The Solana RPC for `rpcUrl`, every request of which goes through the
 * grants (`send$`), failing with `SolanaRpcTransportError` when it gets no
 * response. Kit takes a transport that returns a promise, so the grants'
 * answer becomes one here; a request aborted while it waits for its turn is
 * never sent.
 *
 * @param rpcUrl HTTP(S) URL of the Solana RPC node.
 * @param timeoutMs How long a request may take, once sent, before it counts as unanswered.
 * @param send$ Sends a request on its subscriber's turn.
 * @returns The RPC.
 */
function createSolanaRpc(rpcUrl: string, timeoutMs: number, send$: GrantedSend) {
    const transport = createTransportNamingUnansweredRequests(rpcUrl, timeoutMs);
    const granted: RpcTransport = <TResponse>(config: Parameters<RpcTransport>[0]) =>
        new Promise<RpcResponse<TResponse>>((resolve, reject) => {
            // A request may be shared with others asking the same, so it runs on its own signal,
            // aborted only once no one waits for it; each caller's signal drops that caller only.
            const sending = send$(
                () =>
                    defer(() => {
                        const shared = new AbortController();
                        return from(transport<TResponse>({ ...config, signal: shared.signal })).pipe(
                            finalize(() => shared.abort())
                        );
                    }),
                coalescingKeyOf(config.payload)
            ).subscribe({
                next: resolve,
                error: reject,
            });
            config.signal?.addEventListener(
                'abort',
                () => {
                    sending.unsubscribe();
                    reject(config.signal?.reason);
                },
                { once: true }
            );
        });
    return createSolanaRpcFromTransport(granted);
}

/**
 * What a JSON-RPC request asks, as a key: its method and params, so two
 * requests asking the same share one answer. A bigint is written as text.
 *
 * @param payload The request kit sends.
 * @returns The key, or `undefined` for a payload that is not one request.
 */
function coalescingKeyOf(payload: unknown): string | undefined {
    if (typeof payload !== 'object' || payload === null || !('method' in payload)) return undefined;
    const { method, params } = payload as { method: unknown; params?: unknown };
    return JSON.stringify([method, params], (_key, value: unknown) =>
        typeof value === 'bigint' ? `${value.toString()}n` : value
    );
}

/**
 * Whether a Solana RPC node turned a request away for too many requests:
 * kit raises an HTTP error response as a `SolanaError` carrying its status.
 *
 * @param outcome How the request came back.
 * @returns `true` for HTTP 429.
 */
const turnedAway = (outcome: RequestResult<unknown>): boolean =>
    'failed' in outcome &&
    isSolanaError(outcome.failed, SOLANA_ERROR__RPC__TRANSPORT_HTTP_ERROR) &&
    outcome.failed.context.statusCode === TOO_MANY_REQUESTS;

/**
 * How long a Solana RPC node that turned a request away asked to be left
 * alone: its `Retry-After`, which kit's HTTP error carries in its headers.
 *
 * @param outcome How the request came back.
 * @returns The wait in ms, or `undefined` when it gave none.
 */
const retryAfterMs = (outcome: RequestResult<unknown>): number | undefined =>
    'failed' in outcome && isSolanaError(outcome.failed, SOLANA_ERROR__RPC__TRANSPORT_HTTP_ERROR)
        ? retryAfterMsOf(outcome.failed.context.headers.get('retry-after'))
        : undefined;

/** The Solana RPC reads go through. */
export type SolanaRpc = ReturnType<typeof createSolanaRpc>;

/**
 * A Solana RPC node's health check: its cluster by genesis hash, then, on
 * the expected cluster, its latest finalized slot.
 *
 * @param rpc The node's RPC.
 * @param expectedGenesisHash The cluster it should serve.
 * @param url What is checked, for the result.
 * @returns What the check found, once; errors when it cannot be reached.
 */
const clusterHealthCheck$ = (
    rpc: SolanaRpc,
    expectedGenesisHash: string,
    url: string
): Observable<Exclude<HealthCheck<SolanaHealth>, { outcome: 'failed' }>> =>
    solanaRpcRead$((abortSignal) => rpc.getGenesisHash().send({ abortSignal }), 'Failed to read the genesis hash.').pipe(
        switchMap((genesisHash) =>
            genesisHash !== expectedGenesisHash
                ? of({ outcome: 'onOtherNetwork' as const, url, found: genesisHash, expected: expectedGenesisHash })
                : solanaRpcRead$(
                      (abortSignal) => rpc.getSlot({ commitment: 'finalized' }).send({ abortSignal }),
                      'Failed to read the finalized slot.'
                  ).pipe(
                      map((slot) => ({
                          outcome: 'onExpectedNetwork' as const,
                          url,
                          health: { slot },
                          checkedAt: Date.now(),
                      }))
                  )
        )
    );

/**
 * Opens the Solana RPC endpoints reads go through and runs their HTTP
 * connection machine. Solana wallets do not serve reads, so reads always go
 * through an RPC node: the cluster's public endpoint, unless the app gives
 * its own. The health check reads the genesis hash (the cluster) and the
 * latest finalized slot. Every request times out, so one to a node that
 * stopped answering fails instead of hanging.
 *
 * @param options The cluster and endpoints, and timings.
 * @param network The running network machine the connection follows.
 * @returns
 *   - `connection`: the running HTTP connection machine; `ready` names the URL that passed its check.
 *   - `rpcOf(url, subscriber)`: the RPC of one of its URLs, every request of which takes the subscriber's turn.
 *   - `load$`: each second's count of the requests to its endpoints, and the rate after it.
 *   - `reportReadFailed()`: tells the machine a request failed to reach the node.
 *   - `close()`: moves the machine to `closed`.
 */
export function solanaHttp(options: SolanaHttpOptions, network: NetworkMachine) {
    const [rpcUrls, expectedGenesisHash] =
        'cluster' in options
            ? [
                  options.rpcUrls ?? [PUBLIC_SOLANA_CLUSTERS[options.cluster].rpcUrl],
                  PUBLIC_SOLANA_CLUSTERS[options.cluster].genesisHash,
              ]
            : [options.rpcUrls, options.expectedGenesisHash];
    if (rpcUrls.length === 0) throw new RangeError('At least one Solana RPC URL is needed.');

    const { requestTimeoutMs } = resolveHealthCheckTimings(options);
    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();

    const connection = httpConnection<SolanaHealth, SolanaRpc>({
        ...options,
        urls: rpcUrls,
        transport: {
            clientOn: (url, send$) => createSolanaRpc(url, requestTimeoutMs, send$),
            turnedAway,
            retryAfterMs,
        },
        checkHealth: (rpc, url) => clusterHealthCheck$(rpc, expectedGenesisHash, url),
        networkWentOffline$: network.state$.pipe(filter(atNode('offline'))),
        networkCameOnline$: network.state$.pipe(filter(atNode('online'))),
        readFailed$,
        close$,
    });

    return {
        connection,
        rpcOf: connection.clientOf,
        load$: connection.load$,
        reportReadFailed: () => readFailed$.next(),
        close: () => close$.next(),
    };
}

/** A Solana RPC with its running HTTP connection machine. */
export type SolanaHttp = ReturnType<typeof solanaHttp>;
