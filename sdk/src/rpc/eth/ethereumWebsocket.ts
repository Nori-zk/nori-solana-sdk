import { BrowserProvider, Network } from 'ethers';
import { filter } from 'rxjs';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { resolveHealthCheckTimings } from '../connection/healthCheckTimings.js';
import { jsonRpcRequest } from '../connection/jsonRpcTopic.js';
import { type NetworkMachine } from '../connection/network.impl.js';
import {
    type ReconnectingWebSocketConfig,
    websocketConnection,
} from '../connection/websocket.js';

/**
 * Opens an Ethereum node's reconnecting websocket (a `wss://` RPC URL). Its
 * subscriptions are the topics of `topics.ts`; its requests go through an
 * ethers provider over the same socket, matched to their replies by request
 * id, on the expected chain (ethers never detects it). A request the socket
 * cannot carry, or that gets no reply within the health check timeout, fails
 * as disconnected.
 *
 * @param config The websocket URL, the attempts allowed and the timings.
 * @param network The running network machine the connection follows.
 * @param expectedChainId The chain the proof request queue lives on.
 * @returns
 *   - `socket`: the reconnecting websocket.
 *   - `connection`: its running connection machine.
 *   - `current()`: the ethers provider whose requests go over the socket.
 *   - `close()`: closes the socket and moves the machine to `closed`.
 */
export function ethereumWebsocket(
    config: ReconnectingWebSocketConfig<unknown>,
    network: NetworkMachine['network'],
    expectedChainId: bigint
) {
    const { socket, connection } = websocketConnection<unknown>(config, network);
    const { healthCheckTimeoutMs } = resolveHealthCheckTimings(config);
    const provider = new BrowserProvider(
        {
            request: ({ method, params }) =>
                jsonRpcRequest(
                    socket,
                    method,
                    Array.isArray(params) ? params : params === undefined ? [] : [params],
                    healthCheckTimeoutMs
                ),
        },
        Network.from(expectedChainId),
        { staticNetwork: Network.from(expectedChainId) }
    );
    connection.status$
        .pipe(filter((status) => status !== 'running'))
        .subscribe(() => provider.destroy());

    return {
        socket,
        connection,
        current: (): EthereumProvider => provider,
        close: () => socket.complete(),
    };
}

/** An Ethereum node's reconnecting websocket, its running connection machine and its provider. */
export type EthereumWebsocket = ReturnType<typeof ethereumWebsocket>;
