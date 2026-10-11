import { type RunningMachine } from '@yaw-rx/ystate';
import { FetchRequest, type GetUrlResponse, JsonRpcProvider, Network } from 'ethers';
import { defer, filter, map, type Observable, of, Subject, switchMap } from 'rxjs';
import {
    type EthereumProvider,
    parseRpcUrl,
} from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { atNode } from '../../utils/machines.js';
import {
    type GrantedSend,
    type RequestResult,
    retryAfterMsOf,
    TOO_MANY_REQUESTS,
} from '../connection/connectionGrants.js';
import { type HealthCheckTimings, resolveHealthCheckTimings } from '../connection/healthCheckTimings.js';
import { LIMIT_EXCEEDED } from '../connection/jsonRpcTopic.js';
import { type HttpConnectionState } from '../connection/httpConnection.js';
import { type HealthCheck } from '../connection/connection.impl.js';
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
): Observable<Exclude<HealthCheck<EthereumHealth>, { outcome: 'failed' }>> =>
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

/** How ethers fetches a URL, before any request goes through the grants. */
const getUrl = FetchRequest.createGetUrlFunc();

/**
 * The ethers provider for `rpcUrl`, every request of which goes through the
 * grants (`send$`). Ethers fetches through `getUrlFunc`, which takes a
 * promise, so the grants' answer becomes one here; a request cancelled while
 * it waits for its turn is never sent. Every request times out once sent, so
 * one to a node that stopped answering fails and reports it instead of
 * hanging. The URL's chain is fixed, so ethers is given it up front and
 * never re-detects it. A call asking what another call already waiting or
 * in flight asks joins it (ethers' own deduplication works per provider,
 * and each machine has its own), and gets the answer under its own id.
 *
 * @param rpcUrl The HTTP(S) RPC URL.
 * @param expectedChainId The chain it serves.
 * @param timeoutMs How long a request may take, once sent.
 * @param send$ Sends a request on its subscriber's turn.
 * @returns The provider.
 */
function grantedProvider(rpcUrl: string, expectedChainId: bigint, timeoutMs: number, send$: GrantedSend) {
    const fetchRequest = new FetchRequest(parseRpcUrl(rpcUrl));
    fetchRequest.timeout = timeoutMs;
    fetchRequest.getUrlFunc = (request, signal) => {
        const call = callOf(request.body);
        return new Promise<GetUrlResponse>((resolve, reject) => {
            const sending = send$(() => defer(() => getUrl(request, signal)), call?.key)
                .pipe(map((response) => (call === undefined ? response : answeredAs(response, call.id))))
                .subscribe({
                    next: resolve,
                    error: reject,
                });
            signal?.addListener(() => {
                sending.unsubscribe();
                reject(new Error(`The request to ${rpcUrl} was cancelled.`));
            });
        });
    };
    // One call per request: each takes its own turn, and a call the node turns away is sent again on its own.
    return new JsonRpcProvider(fetchRequest, undefined, {
        staticNetwork: Network.from(expectedChainId),
        batchMaxCount: 1,
    });
}

/**
 * The one JSON-RPC call a request body carries: its id, and what it asks
 * (its method and params) as the key two calls asking the same share.
 *
 * @param body The request's body.
 * @returns The call, or `undefined` for a body that is not one call.
 */
function callOf(body: Uint8Array | null): { id: unknown; key: string } | undefined {
    if (body === null) return undefined;
    let payload: unknown;
    try {
        payload = JSON.parse(new TextDecoder().decode(body));
    } catch {
        return undefined;
    }
    if (typeof payload !== 'object' || payload === null || Array.isArray(payload) || !('method' in payload))
        return undefined;
    const { id, method, params } = payload as { id?: unknown; method: unknown; params?: unknown };
    return { id, key: JSON.stringify([method, params]) };
}

/**
 * A shared answer as the caller's own: its reply carrying the caller's
 * call id, which ethers matches replies to calls by.
 *
 * @param response The answer, to whichever call was sent.
 * @param id The caller's call id.
 * @returns The answer with the caller's id.
 */
function answeredAs(response: GetUrlResponse, id: unknown): GetUrlResponse {
    if (response.body === null) return response;
    let reply: unknown;
    try {
        reply = JSON.parse(new TextDecoder().decode(response.body));
    } catch {
        return response;
    }
    if (typeof reply !== 'object' || reply === null || Array.isArray(reply)) return response;
    return { ...response, body: new TextEncoder().encode(JSON.stringify({ ...reply, id })) };
}

/**
 * Whether an Ethereum RPC node turned a request away for too many requests:
 * ethers' fetch answers with the HTTP response, its status included.
 *
 * @param outcome How the request came back.
 * @returns `true` for HTTP 429.
 */
const turnedAway = (outcome: RequestResult<unknown>): boolean => {
    if (!('answered' in outcome)) return false;
    const response = outcome.answered as GetUrlResponse;
    return response.statusCode === TOO_MANY_REQUESTS || limitedInBody(response.body);
};

/**
 * Whether a JSON-RPC reply says the node turned the call away for too many
 * requests: EIP-1474's "Limit exceeded" (-32005), or 429, inside a 200.
 *
 * @param body The reply's body.
 * @returns `true` when it does.
 */
function limitedInBody(body: Uint8Array | null): boolean {
    if (body === null) return false;
    let reply: unknown;
    try {
        reply = JSON.parse(new TextDecoder().decode(body));
    } catch {
        return false;
    }
    return (Array.isArray(reply) ? reply : [reply]).some((message: unknown) => {
        const code = (message as { error?: { code?: unknown } } | null)?.error?.code;
        return code === LIMIT_EXCEEDED || code === TOO_MANY_REQUESTS;
    });
}

/**
 * How long an Ethereum RPC node that turned a request away asked to be left
 * alone: its `Retry-After` header.
 *
 * @param outcome How the request came back.
 * @returns The wait in ms, or `undefined` when it gave none.
 */
const retryAfterMs = (outcome: RequestResult<unknown>): number | undefined =>
    'answered' in outcome ? retryAfterMsOf((outcome.answered as GetUrlResponse).headers['retry-after']) : undefined;

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
 *   - `current(subscriber)`: the ethers provider for the URL, every request of which takes the subscriber's turn.
 *   - `load$`: each second's count of the requests to the URL, and the rate after it.
 *   - `reportReadFailed()`: tells the machine a read failed to reach the node.
 *   - `close()`: moves the machine to `closed`.
 */
export function ethereumHttp(
    options: EthereumHttpOptions,
    network: NetworkMachine
) {
    const { expectedChainId, rpcUrl } = options;
    const { requestTimeoutMs } = resolveHealthCheckTimings(options);
    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();

    const connection = httpConnection<EthereumHealth, JsonRpcProvider>({
        ...options,
        urls: [rpcUrl],
        transport: {
            clientOn: (url, send$) => grantedProvider(url, expectedChainId, requestTimeoutMs, send$),
            turnedAway,
            retryAfterMs,
        },
        checkHealth: (client, url) =>
            chainHealthCheck$(
                (method) => defer(() => client.send(method, []) as Promise<string>).pipe(map(BigInt)),
                expectedChainId,
                url
            ),
        networkWentOffline$: network.state$.pipe(
            filter(atNode('offline'))
        ),
        networkCameOnline$: network.state$.pipe(
            filter(atNode('online'))
        ),
        readFailed$,
        close$,
    });

    return {
        connection,
        current: (subscriber: RunningMachine): EthereumProvider => connection.clientOf(rpcUrl, subscriber),
        load$: connection.load$,
        reportReadFailed: () => readFailed$.next(),
        close: () => close$.next(),
    };
}

/** An Ethereum RPC URL with its running HTTP connection machine. */
export type EthereumHttp = ReturnType<typeof ethereumHttp>;
