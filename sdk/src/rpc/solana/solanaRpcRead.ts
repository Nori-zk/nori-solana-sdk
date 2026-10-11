import { catchError, defer, finalize, type Observable, throwError } from 'rxjs';
import { isSolanaRpcTransportFailure, SolanaRpcTransportError } from './errors.js';

/**
 * One read from a Solana RPC node, the lowest step every Solana read is
 * built from: run when subscribed and aborted when unsubscribed first, with
 * a failure to reach a healthy node as `SolanaRpcTransportError`, which tells
 * the transport. Any other failure (a missing or mismatched account) is
 * passed on as it is.
 *
 * @param read The RPC call, given the signal that aborts it.
 * @param failure What failed, for the error's message.
 * @returns What the call returns, once.
 */
export const solanaRpcRead$ = <T>(read: (abortSignal: AbortSignal) => Promise<T>, failure: string): Observable<T> =>
    defer(() => {
        const controller = new AbortController();
        return defer(() => read(controller.signal)).pipe(finalize(() => controller.abort()));
    }).pipe(
        catchError((error: unknown) =>
            throwError(() => (isSolanaRpcTransportFailure(error) ? new SolanaRpcTransportError(failure, error) : error))
        )
    );
