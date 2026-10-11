import { type RunningMachine } from '@yaw-rx/ystate';
import { BrowserProvider, Network } from 'ethers';
import { firstValueFrom, map } from 'rxjs';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { resolveHealthCheckTimings } from '../connection/healthCheckTimings.js';
import { TOO_MANY_REQUESTS } from '../connection/connectionGrants.js';
import {
    jsonRpcHeartbeat,
    LIMIT_EXCEEDED,
    turnedAwayWith,
    type WebSocketRpc,
    webSocketRpcTransport,
} from '../connection/jsonRpcTopic.js';
import { type NetworkMachine } from '../connection/network.impl.js';
import { rateLimitedWebsocketConnection, type ReconnectingWebSocketConfig } from '../connection/websocket.js';
import { chainHealthCheck$, type EthereumHealth } from './ethereumHttp.js';

/**
 * Opens an Ethereum node's reconnecting websocket (a `wss://` RPC URL),
 * checking on each socket that it serves the expected chain (`eth_chainId`,
 * then `eth_blockNumber`). Every request and subscription over it takes a
 * turn from its grants. Once nothing has been received for a check interval
 * it pings (`jsonRpcHeartbeat`), so a quiet socket that died is found. Its requests go through an ethers provider per
 * subscriber over the socket, matched to their replies by request id, on
 * the expected chain (ethers never detects it). A request the socket cannot
 * carry, or that gets no reply within the health check timeout, fails as
 * disconnected.
 *
 * @param config The websocket URL, the attempts allowed and the timings.
 * @param network The running network machine the connection follows.
 * @param expectedChainId The chain the proof request queue lives on.
 * @returns
 *   - `socket`: the reconnecting websocket.
 *   - `connection`: its running connection machine.
 *   - `current(subscriber)`: the ethers provider whose requests go over the socket on the subscriber's turns.
 *   - `clientOf(subscriber)`: the requests and subscriptions over the socket, on the subscriber's turns.
 *   - `load$`: each second's count of the requests over the socket, and the rate after it.
 *   - `reportReadFailed()`: tells the machine a request over the socket got no answer.
 *   - `close()`: closes the socket and moves the machine to `closed`.
 */
export function ethereumWebsocket(
    config: ReconnectingWebSocketConfig<unknown>,
    network: NetworkMachine,
    expectedChainId: bigint
) {
    const { healthCheckTimeoutMs, healthCheckIntervalMs } = resolveHealthCheckTimings(config);
    const { socket, connection, clientOf, load$ } = rateLimitedWebsocketConnection<unknown, EthereumHealth, WebSocketRpc>(
        { heartbeat: jsonRpcHeartbeat(healthCheckIntervalMs), ...config },
        network,
        {
            // An Ethereum node turns a request away with EIP-1474's "Limit exceeded", or 429.
            transport: (held) =>
                webSocketRpcTransport(held, healthCheckTimeoutMs, turnedAwayWith(LIMIT_EXCEEDED, TOO_MANY_REQUESTS)),
            // `eth_chainId` over the socket finds another chain: the connection has `wrongNetwork`.
            hasWrongNetwork: true,
            checkHealth: (client, url) =>
                chainHealthCheck$(
                    (method) => client.request$(method, []).pipe(map((result) => BigInt(result as string))),
                    expectedChainId,
                    url
                ),
        }
    );

    // One ethers provider per subscriber, over its client.
    const providers = new WeakMap<RunningMachine, EthereumProvider>();
    const current = (subscriber: RunningMachine): EthereumProvider => {
        const made = providers.get(subscriber);
        if (made !== undefined) return made;
        const client = clientOf(subscriber);
        const provider = new BrowserProvider(
            {
                // ethers takes an EIP-1193 provider, whose `request` returns a promise.
                request: ({ method, params }) =>
                    firstValueFrom(
                        client.request$(method, Array.isArray(params) ? params : params === undefined ? [] : [params])
                    ),
            },
            Network.from(expectedChainId),
            { staticNetwork: Network.from(expectedChainId) }
        );
        providers.set(subscriber, provider);
        return provider;
    };

    return {
        socket,
        connection,
        current,
        clientOf,
        load$,
        reportReadFailed: () => socket.readFailed$.next(),
        close: () => socket.complete(),
    };
}

/** An Ethereum node's reconnecting websocket, its running connection machine and its providers. */
export type EthereumWebsocket = ReturnType<typeof ethereumWebsocket>;
