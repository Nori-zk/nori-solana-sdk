import { map, switchMap } from 'rxjs';
import { type HealthCheck } from '../connection/connection.impl.js';
import { resolveHealthCheckTimings } from '../connection/healthCheckTimings.js';
import { TOO_MANY_REQUESTS } from '../connection/connectionGrants.js';
import { jsonRpcHeartbeat, turnedAwayWith, type WebSocketRpc, webSocketRpcTransport } from '../connection/jsonRpcTopic.js';
import { type NetworkMachine } from '../connection/network.impl.js';
import { rateLimitedWebsocketConnection, type ReconnectingWebSocketConfig } from '../connection/websocket.js';

/**
 * The websocket address of an HTTP(S) Solana RPC endpoint: the same host and
 * path over `ws(s)://`, where Solana RPC nodes serve their subscriptions.
 *
 * @param rpcUrl The HTTP(S) RPC endpoint.
 * @returns Its websocket address.
 */
export function solanaWebsocketUrlOf(rpcUrl: string): string {
    return rpcUrl.replace(/^http(s?):\/\//, 'ws$1://');
}

/**
 * Opens a Solana RPC node's reconnecting websocket, which the Solana changes
 * machine subscribes on. A Solana node's websocket serves subscriptions
 * only, so each socket is checked by one subscription the node acknowledges
 * (`slotSubscribe`, then `slotUnsubscribe`); the cluster is checked over
 * http, whose checked URL the websocket follows unless the app gives its
 * own. Every subscription over it takes a turn from its grants. Once
 * nothing has been received for a check interval it pings
 * (`jsonRpcHeartbeat`), so a quiet socket that died is found.
 *
 * @param config The websocket URL, the attempts allowed and the timings.
 * @param network The running network machine the connection follows.
 * @returns
 *   - `socket`: the reconnecting websocket.
 *   - `connection`: its running connection machine.
 *   - `clientOf(subscriber)`: the requests and subscriptions over the socket, on the subscriber's turns.
 *   - `load$`: each second's count of the requests over the socket, and the rate after it.
 */
export function solanaWebsocket(config: ReconnectingWebSocketConfig<unknown>, network: NetworkMachine) {
    const { healthCheckTimeoutMs, healthCheckIntervalMs } = resolveHealthCheckTimings(config);
    return rateLimitedWebsocketConnection<unknown, undefined, WebSocketRpc>(
        { heartbeat: jsonRpcHeartbeat(healthCheckIntervalMs), ...config },
        network,
        {
            // A Solana node turns a request away with 429; its -32005 means the node is unhealthy, which is a failure.
            transport: (held) => webSocketRpcTransport(held, healthCheckTimeoutMs, turnedAwayWith(TOO_MANY_REQUESTS)),
            // A Solana node's websocket serves subscriptions only, so its cluster cannot be asked over it.
            hasWrongNetwork: false,
            checkHealth: (client, url) =>
                client.request$('slotSubscribe', []).pipe(
                    switchMap((subscriptionId) => client.request$('slotUnsubscribe', [subscriptionId])),
                    map(
                        (): Extract<HealthCheck<undefined>, { outcome: 'onExpectedNetwork' }> => ({
                            outcome: 'onExpectedNetwork',
                            url,
                            health: undefined,
                            checkedAt: Date.now(),
                        })
                    )
                ),
        }
    );
}

/** A Solana RPC node's reconnecting websocket and its running connection machine. */
export type SolanaWebsocket = ReturnType<typeof solanaWebsocket>;
