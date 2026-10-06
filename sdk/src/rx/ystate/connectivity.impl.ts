import { filter, type Observable } from 'rxjs';
import { BridgeSocketConnectivityGraph, type BridgeSocketConnectionState } from './connectivity.js';

function onState(
    connectionState$: Observable<BridgeSocketConnectionState>,
    state: BridgeSocketConnectionState
) {
    return connectionState$.pipe(filter((current) => current === state));
}

/**
 * Creates the connectivity machine driven by a reconnecting socket's
 * connection state stream.
 *
 * @param connectionState$ The `bridgeSocketConnectionState$` of `getReconnectingBridgeSocket$`.
 * @returns An implemented YState machine for the socket's connectivity.
 */
export function createBridgeSocketConnectivityMachine(
    connectionState$: Observable<BridgeSocketConnectionState>
) {
    return BridgeSocketConnectivityGraph.implement({
        socketOpened: {
            $: () => onState(connectionState$, 'open'),
            next: () => ({}),
        },
        socketClosed: {
            $: () => onState(connectionState$, 'closed'),
            next: () => ({}),
        },
        socketReconnecting: {
            $: () => onState(connectionState$, 'reconnecting'),
            next: () => ({}),
        },
        socketPermanentlyClosed: {
            $: () => onState(connectionState$, 'permanently-closed'),
            next: () => ({}),
        },
        socketConnecting: {
            $: () => onState(connectionState$, 'connecting'),
            next: () => ({}),
        },
    });
}
