import { type Eip1193Provider } from 'ethers';
import { Observable } from 'rxjs';

/**
 * An EIP-1193 provider that may emit the standard `connect`, `disconnect`
 * and `chainChanged` events, e.g. MetaMask's.
 */
export type Eip1193EventProvider = Eip1193Provider & {
    on?(event: string, listener: (...args: unknown[]) => void): unknown;
    removeListener?(
        event: string,
        listener: (...args: unknown[]) => void
    ): unknown;
};

/** The EIP-1193 error code for a request the user rejected. */
export const USER_REJECTED_REQUEST = 4001;

/**
 * A wallet provider's EIP-1193 event, for as long as it is subscribed. A
 * provider without event support never emits.
 *
 * @param provider The wallet provider.
 * @param event `chainChanged`, `connect` or `disconnect`.
 * @returns Each event's first argument, as it arrives.
 */
export function eip1193Event$(
    provider: Eip1193EventProvider,
    event: 'chainChanged' | 'connect' | 'disconnect'
): Observable<unknown> {
    return new Observable<unknown>((subscriber) => {
        const listener = (value: unknown) => subscriber.next(value);
        provider.on?.(event, listener);
        return () => {
            provider.removeListener?.(event, listener);
        };
    });
}

/**
 * The `code` of an EIP-1193 request error, if it has one.
 *
 * @param error What a wallet request rejected with.
 * @returns The numeric code, or `undefined`.
 */
export function requestErrorCode(error: unknown): number | undefined {
    if (typeof error !== 'object' || error === null || !('code' in error))
        return undefined;
    return typeof error.code === 'number' ? error.code : undefined;
}
