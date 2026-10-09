import {
    isSolanaError,
    SOLANA_ERROR__JSON_RPC__SERVER_ERROR_NODE_UNHEALTHY,
    SOLANA_ERROR__RPC__TRANSPORT_HTTP_ERROR,
} from '@solana/kit';
import { withBackoff } from '../../utils/withBackoff.js';

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

/**
 * Runs a Solana RPC read, retrying transport failures with backoff and
 * reporting one that persists as `SolanaRpcTransportError`. Every other
 * error is thrown at once, as is.
 *
 * @param description What the read does, for the error message.
 * @param read The read to run.
 * @returns The read's result.
 * @throws SolanaRpcTransportError When the read still fails to reach a
 *   healthy node after its retries.
 */
export async function withSolanaRpcTransportErrors<T>(
    description: string,
    read: () => Promise<T>
): Promise<T> {
    try {
        return await withBackoff(read, isSolanaRpcTransportFailure);
    } catch (error) {
        if (isSolanaRpcTransportFailure(error)) {
            throw new SolanaRpcTransportError(`${description} failed.`, error);
        }
        throw error;
    }
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
