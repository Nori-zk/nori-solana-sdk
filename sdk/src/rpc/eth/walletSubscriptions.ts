import { filter, map, Observable } from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import { type SubscriptionEvent, SubscriptionRefusedError } from '../connection/jsonRpcTopic.js';
import { type Eip1193EventProvider } from './eip1193.js';

/** Thrown when a wallet does not serve `eth_subscribe`; subscriptions move to the next transport. */
export class WalletSubscriptionUnsupportedError extends SubscriptionRefusedError {
    constructor(readonly cause: unknown) {
        super(`The wallet does not serve eth_subscribe: ${messageOf(cause)}`);
        this.name = 'WalletSubscriptionUnsupportedError';
    }
}

/**
 * One `eth_subscribe` through a wallet's EIP-1193 provider, for as long as
 * it is subscribed: the wallet answering with the subscription's id is its
 * acknowledgement, and the subscription's results arrive as the provider's
 * `message` events. Unsubscribing sends `eth_unsubscribe`.
 *
 * @param provider The wallet's provider.
 * @param params `eth_subscribe`'s params, e.g. `['newHeads']`.
 * @returns The acknowledgement, then each result.
 * @throws WalletSubscriptionUnsupportedError (as an error notification) When the wallet refuses `eth_subscribe`.
 */
export function walletSubscriptionEvents$<TResult>(
    provider: Eip1193EventProvider,
    params: unknown[]
): Observable<SubscriptionEvent<TResult>> {
    return new Observable<SubscriptionEvent<TResult>>((subscriber) => {
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
            subscriber.next({ kind: 'notification', result: message.data.result as TResult });
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
                else subscriber.next({ kind: 'acknowledged' });
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

/**
 * One `eth_subscribe` through a wallet's EIP-1193 provider, as
 * `walletSubscriptionEvents$` makes it: its results only.
 *
 * @param provider The wallet's provider.
 * @param params `eth_subscribe`'s params, e.g. `['newHeads']`.
 * @returns The subscription's results.
 * @throws WalletSubscriptionUnsupportedError (as an error notification) When the wallet refuses `eth_subscribe`.
 */
export function walletSubscription$<TResult>(provider: Eip1193EventProvider, params: unknown[]): Observable<TResult> {
    return walletSubscriptionEvents$<TResult>(provider, params).pipe(
        filter((event): event is Extract<SubscriptionEvent<TResult>, { kind: 'notification' }> => event.kind === 'notification'),
        map(({ result }) => result)
    );
}
