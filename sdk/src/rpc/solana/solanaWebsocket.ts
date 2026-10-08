import { type NetworkMachine } from '../connection/network.impl.js';
import {
    type ReconnectingWebSocketConfig,
    websocketConnection,
} from '../connection/websocket.js';

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
 * Opens a Solana RPC node's reconnecting websocket. Its subscriptions are
 * the topics of `topics.ts`.
 *
 * @param config The websocket URL, the attempts allowed and the timings.
 * @param network The running network machine the connection follows.
 * @returns
 *   - `socket`: the reconnecting websocket.
 *   - `connection`: its running connection machine.
 */
export function solanaWebsocket(
    config: ReconnectingWebSocketConfig<unknown>,
    network: NetworkMachine['network']
) {
    return websocketConnection<unknown>(config, network);
}

/** A Solana RPC node's reconnecting websocket and its running connection machine. */
export type SolanaWebsocket = ReturnType<typeof solanaWebsocket>;
