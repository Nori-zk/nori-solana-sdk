import { getReconnectingBridgeSocket$ } from './socket.js';
import { createBridgeSocketConnectivityMachine } from './ystate/connectivity.impl.js';

/**
 * Opens the reconnecting bridge websocket and returns its stable message
 * stream together with the connectivity machine that tracks it.
 *
 * @param url WebSocket server URL (default: wss://wss.nori.it.com)
 * @param heartBeatInterval Interval in ms for sending pings (default: 3000)
 * @param pongTimeoutMultiplier Multiplier to determine allowed pong delay before reconnection (default: 2)
 * @returns `bridgeSocket$`, its `bridgeSocketConnectionState$`, and `bridgeSocketConnectivity`.
 */
export function getBridgeSocketWithConnectivity$(
    url?: string,
    heartBeatInterval?: number,
    pongTimeoutMultiplier?: number
) {
    const { bridgeSocket$, bridgeSocketConnectionState$ } = getReconnectingBridgeSocket$(
        url,
        heartBeatInterval,
        pongTimeoutMultiplier
    );
    return {
        bridgeSocket$,
        bridgeSocketConnectionState$,
        bridgeSocketConnectivity: createBridgeSocketConnectivityMachine(
            bridgeSocketConnectionState$
        ),
    };
}
