import { type NetworkMachine } from '../connection/network.impl.js';
import {
    type ReconnectingWebSocketConfig,
    websocketConnection,
} from '../connection/websocket.js';

/** Nori's websocket server. */
export const DEFAULT_NORI_WEBSOCKET_URL = 'wss://wss.solana.nori.it.com';

/**
 * Opens Nori's reconnecting websocket, with the server's ping/pong
 * heartbeat: a pong missing for `pongTimeoutMultiplier` heartbeats counts as
 * a drop. Its topics are the topics of `topics.ts`.
 *
 * @param config The URL (default: `DEFAULT_NORI_WEBSOCKET_URL`), the
 *   attempts allowed and the timings.
 * @param network The running network machine the connection follows.
 * @param heartBeatInterval Interval in ms for sending pings (default: 3000)
 * @param pongTimeoutMultiplier Multiplier to determine allowed pong delay before reconnection (default: 2)
 * @returns
 *   - `socket`: the reconnecting websocket.
 *   - `connection`: its running connection machine.
 */
export function noriWebsocket(
    config: Partial<ReconnectingWebSocketConfig<unknown>>,
    network: NetworkMachine['network'],
    heartBeatInterval: number = 3000,
    pongTimeoutMultiplier: number = 2
) {
    return websocketConnection<unknown>(
        {
            heartbeat: {
                ping: { method: 'ping' },
                isPong: (message) =>
                    typeof message === 'object' &&
                    message !== null &&
                    'data' in message &&
                    message.data === 'pong',
                intervalMs: heartBeatInterval,
                timeoutMs: heartBeatInterval * pongTimeoutMultiplier,
            },
            ...config,
            url: config.url ?? DEFAULT_NORI_WEBSOCKET_URL,
        },
        network
    );
}

/** Nori's reconnecting websocket and its running connection machine. */
export type NoriWebsocket = ReturnType<typeof noriWebsocket>;
