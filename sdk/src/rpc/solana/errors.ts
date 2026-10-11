import {
    isSolanaError,
    SOLANA_ERROR__JSON_RPC__SERVER_ERROR_NODE_UNHEALTHY,
    SOLANA_ERROR__RPC__TRANSPORT_HTTP_ERROR,
} from '@solana/kit';

/** The app gave no Solana wallet: a setup mistake, not a state to wait out. */
export class NoSolanaWalletConfiguredError extends Error {
    constructor() {
        super('No Solana wallet is configured: give solana.wallet to createConnections to use one.');
        this.name = 'NoSolanaWalletConfiguredError';
    }
}

/** A Solana RPC read failed to reach a healthy node. */
export class SolanaRpcTransportError extends Error {
    constructor(message: string, readonly cause: unknown) {
        super(message);
        this.name = 'SolanaRpcTransportError';
    }
}

/**
 * Whether a Solana RPC read failed to reach a healthy node rather than
 * failing on its own terms (e.g. a missing account):
 *
 * - a `SolanaRpcTransportError`, which the RPC from `solanaHttp` raises
 *   when a request never got a response;
 * - an HTTP error response from the node;
 * - the node reporting itself unhealthy.
 *
 * @param error The error a read threw.
 * @returns `true` for a transport failure.
 */
export function isSolanaRpcTransportFailure(error: unknown): boolean {
    return (
        error instanceof SolanaRpcTransportError ||
        isSolanaError(error, SOLANA_ERROR__RPC__TRANSPORT_HTTP_ERROR) ||
        isSolanaError(error, SOLANA_ERROR__JSON_RPC__SERVER_ERROR_NODE_UNHEALTHY)
    );
}

export class SolanaAccountMismatchError extends Error {
    constructor(readonly address: string, message: string) {
        super(message);
        this.name = 'SolanaAccountMismatchError';
    }
}

export class ProofQueueBatchSearchError extends Error {
    constructor(readonly requestId: bigint, message: string) {
        super(message);
        this.name = 'ProofQueueBatchSearchError';
    }
}
