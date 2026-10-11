import { catchError, defer, filter, map, Observable, share, take, throwError, timeout } from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import { type ReconnectingWebSocketSubject } from './websocket.js';

/** A JSON-RPC message received on a node's websocket. */
interface JsonRpcMessage {
    id?: number | string;
    result?: unknown;
    error?: { code?: number; message?: string };
    params?: { subscription?: number | string; result?: unknown };
}

/**
 * Builds a JSON-RPC request with an id of its own, so its reply is told
 * apart from every other on any socket.
 *
 * @param method The method.
 * @param params Its params.
 * @returns The request.
 */
function request(method: string, params: unknown[]) {
    return { jsonrpc: '2.0', id: crypto.randomUUID(), method, params };
}

/** The EIP-1193 code for a provider that is disconnected from every chain. */
const DISCONNECTED = 4900;

/**
 * One JSON-RPC request on a reconnecting websocket, answered by the reply
 * carrying its id: the reply is listened for before the request is sent. A
 * request the socket cannot carry, or that gets no reply within
 * `timeoutMs`, fails with EIP-1193's disconnected code (4900); a reply
 * carrying an error fails with that error's code and message.
 *
 * @param socket The reconnecting websocket.
 * @param method The method.
 * @param params Its params.
 * @param timeoutMs How long to wait for the reply.
 * @returns The reply's result, once.
 */
export function jsonRpcRequest$(
    socket: ReconnectingWebSocketSubject<unknown>,
    method: string,
    params: unknown[],
    timeoutMs: number
): Observable<unknown> {
    return defer(() => {
        const sent = request(method, params);
        return new Observable<JsonRpcMessage>((subscriber) => {
            const reply = socket
                .pipe(
                    map((value) => value as JsonRpcMessage),
                    filter((message) => message.id === sent.id),
                    take(1)
                )
                .subscribe(subscriber);
            socket.next(sent);
            return reply;
        });
    }).pipe(
        timeout(timeoutMs),
        catchError((error: unknown) =>
            throwError(() =>
                Object.assign(new Error(`${method} got no reply: ${messageOf(error)}`), { code: DISCONNECTED })
            )
        ),
        map((message) => {
            if (message.error !== undefined)
                throw Object.assign(new Error(message.error.message ?? `${method} failed.`), {
                    code: message.error.code,
                });
            return message.result;
        })
    );
}

/** Thrown when a node or a wallet refuses a subscription: the transport does not serve it. */
export class SubscriptionRefusedError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'SubscriptionRefusedError';
    }
}

/** What a subscription brings: the node acknowledging it, each time it is made, then each notification. */
export type SubscriptionEvent<TResult> = { kind: 'acknowledged' } | { kind: 'notification'; result: TResult };

/**
 * One JSON-RPC subscription on a reconnecting websocket, as Solana and
 * Ethereum nodes serve them, through `multiplex`: `subscribeMethod` is sent
 * when the stream is subscribed and again every time the socket opens; each
 * reply gives a new subscription id, which is the node acknowledging it, and
 * notifications carrying it are passed on; `unsubscribeMethod` is sent when
 * the stream is unsubscribed. Notifications arrive only while the socket is
 * open.
 *
 * @param socket The reconnecting websocket.
 * @param subscribeMethod The subscribe method (`accountSubscribe`, `eth_subscribe`, …).
 * @param params Its params.
 * @param unsubscribeMethod The matching unsubscribe method.
 * @returns Each acknowledgement and each notification's result; errors if the node refuses the subscription.
 */
export function jsonRpcSubscription$<TResult>(
    socket: ReconnectingWebSocketSubject<unknown>,
    subscribeMethod: string,
    params: unknown[],
    unsubscribeMethod: string
): Observable<SubscriptionEvent<TResult>> {
    return defer(() => {
        const subscribe = request(subscribeMethod, params);
        let subscriptionId: number | string | undefined;
        return socket
            .multiplex<JsonRpcMessage>(
                () => {
                    subscriptionId = undefined;
                    return subscribe;
                },
                () => request(unsubscribeMethod, [subscriptionId]),
                (value) => {
                    const message = value as JsonRpcMessage;
                    if (message.id === subscribe.id) {
                        if (
                            typeof message.result === 'number' ||
                            typeof message.result === 'string'
                        ) {
                            subscriptionId = message.result;
                            return true;
                        }
                        return 'error' in message;
                    }
                    return (
                        subscriptionId !== undefined &&
                        message.params?.subscription === subscriptionId
                    );
                }
            )
            .pipe(
                map((message): SubscriptionEvent<TResult> => {
                    if (message.id !== subscribe.id)
                        return { kind: 'notification', result: message.params?.result as TResult };
                    if ('error' in message)
                        throw new SubscriptionRefusedError(
                            `${subscribeMethod} was refused: ${JSON.stringify(message.error)}`
                        );
                    return { kind: 'acknowledged' };
                })
            );
    }).pipe(share());
}

/**
 * One JSON-RPC subscription on a reconnecting websocket, as
 * `jsonRpcSubscription$` makes it: its notifications' results only.
 *
 * @param socket The reconnecting websocket.
 * @param subscribeMethod The subscribe method (`accountSubscribe`, `eth_subscribe`, …).
 * @param params Its params.
 * @param unsubscribeMethod The matching unsubscribe method.
 * @returns The notifications' results; errors if the node refuses the subscription.
 */
export function jsonRpcTopic$<TResult>(
    socket: ReconnectingWebSocketSubject<unknown>,
    subscribeMethod: string,
    params: unknown[],
    unsubscribeMethod: string
): Observable<TResult> {
    return jsonRpcSubscription$<TResult>(socket, subscribeMethod, params, unsubscribeMethod).pipe(
        filter((event): event is Extract<SubscriptionEvent<TResult>, { kind: 'notification' }> => event.kind === 'notification'),
        map(({ result }) => result)
    );
}
