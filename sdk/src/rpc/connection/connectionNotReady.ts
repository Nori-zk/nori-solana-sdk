/** A transport, by chain and kind. */
export type TransportName =
    | 'ethereum.http'
    | 'ethereum.websocket'
    | 'ethereum.wallet'
    | 'solana.http'
    | 'solana.websocket'
    | 'nori.websocket';

/** A transport's state when it was asked for: a node of its machine, or not configured. */
export interface TransportState {
    transport: TransportName;
    state: { node: string; data: unknown };
}

/**
 * Thrown when no transport that could serve a request is ready; carries
 * every transport tried and the state it was in.
 */
export class ConnectionNotReadyError extends Error {
    constructor(readonly notReady: TransportState[]) {
        super(
            notReady.length === 0
                ? 'No transport is configured for this request.'
                : `No transport is ready: ${notReady
                      .map(({ transport, state }) => `${transport} is ${state.node}`)
                      .join(', ')}.`
        );
        this.name = 'ConnectionNotReadyError';
    }
}
