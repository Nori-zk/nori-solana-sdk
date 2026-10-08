import { Observable } from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import { type Eip1193EventProvider } from './eip1193.js';

/** Thrown when a wallet does not serve `eth_subscribe`; subscriptions move to the next transport. */
export class WalletSubscriptionUnsupportedError extends Error {
    constructor(readonly cause: unknown) {
        super(`The wallet does not serve eth_subscribe: ${messageOf(cause)}`);
        this.name = 'WalletSubscriptionUnsupportedError';
    }
}

/**
 * One `eth_subscribe` through a wallet's EIP-1193 provider, for as long as
 * it is subscribed: the subscription's results arrive as the provider's
 * `message` events. Unsubscribing sends `eth_unsubscribe`.
 *
 * @param provider The wallet's provider.
 * @param params `eth_subscribe`'s params, e.g. `['newHeads']`.
 * @returns The subscription's results.
 * @throws WalletSubscriptionUnsupportedError (as an error notification) When the wallet refuses `eth_subscribe`.
 */
export function walletSubscription$<TResult>(
    provider: Eip1193EventProvider,
    params: unknown[]
): Observable<TResult> {
    return new Observable<TResult>((subscriber) => {
        let subscriptionId: string | undefined;
        let unsubscribed = false;
        const onMessage = (message: unknown) => {
            if (
                typeof message !== 'object' ||
                message === null ||
                !('type' in message) ||
                message.type !== 'eth_subscription' ||
                !('data' in message) ||
                typeof message.data !== 'object' ||
                message.data === null ||
                !('subscription' in message.data) ||
                message.data.subscription !== subscriptionId ||
                !('result' in message.data)
            )
                return;
            subscriber.next(message.data.result as TResult);
        };
        provider.on?.('message', onMessage);
        provider
            .request({ method: 'eth_subscribe', params })
            .then((id) => {
                subscriptionId = String(id);
                if (unsubscribed)
                    void provider
                        .request({ method: 'eth_unsubscribe', params: [subscriptionId] })
                        .catch((): undefined => undefined);
            })
            .catch((error: unknown) =>
                subscriber.error(new WalletSubscriptionUnsupportedError(error))
            );
        return () => {
            unsubscribed = true;
            provider.removeListener?.('message', onMessage);
            if (subscriptionId !== undefined)
                void provider
                    .request({ method: 'eth_unsubscribe', params: [subscriptionId] })
                    .catch((): undefined => undefined);
        };
    });
}
