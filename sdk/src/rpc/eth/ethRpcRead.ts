import { catchError, defer, type Observable, throwError } from 'rxjs';
import { EthRpcTransportError } from './errors.js';

/**
 * One read from an Ethereum node through ethers, the lowest step every
 * Ethereum read is built from: run when subscribed, with a failure as
 * `EthRpcTransportError`, which tells the transport it may not have reached
 * the node.
 *
 * @param read The ethers call.
 * @param failure What failed, for the error's message.
 * @returns What the call returns, once.
 */
export const ethRpcRead$ = <T>(read: () => Promise<T>, failure: string): Observable<T> =>
    defer(read).pipe(catchError((error: unknown) => throwError(() => new EthRpcTransportError(failure, error))));
