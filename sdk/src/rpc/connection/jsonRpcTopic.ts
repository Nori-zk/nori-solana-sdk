import { catchError, defer, EMPTY, filter, map, Observable, of, share, switchMap, take, throwError, timeout } from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import { requestErrorCode } from '../eth/eip1193.js';
import { type GrantedSend, type GrantedTransport, type RequestResult } from './connectionGrants.js';
import { type ReconnectingWebSocketSubject, type WebSocketHeartbeat } from './websocket.js';

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

/** The JSON-RPC error code (EIP-1474 "Limit exceeded") a node or a wallet turns a request away with when it gets too many. */
export const LIMIT_EXCEEDED = -32005;

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

/** The id of an RPC node's heartbeat request, which its reply carries. */
const HEARTBEAT_ID = 'heartbeat';

/**
 * The heartbeat of an RPC node's websocket: a JSON-RPC request, sent once
 * nothing has been received for an interval, whose reply proves the socket
 * alive. A node replies to every request carrying an id, with an answer or
 * with an error for a method it does not serve, so any reply counts; a
 * socket that receives nothing for two intervals counts as dropped.
 *
 * @param intervalMs How long without receiving anything before sending it.
 * @returns The heartbeat.
 */
export function jsonRpcHeartbeat(intervalMs: number): WebSocketHeartbeat<unknown> {
    return {
        ping: { jsonrpc: '2.0', id: HEARTBEAT_ID, method: 'ping', params: [] },
        isPong: (message) =>
            typeof message === 'object' && message !== null && 'id' in message && message.id === HEARTBEAT_ID,
        intervalMs,
    };
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
 * Ethereum nodes serve them: `subscribeMethod` is sent through the grants
 * (`send$`) each time the connection comes to serve; its reply gives the
 * subscription id, which is the node acknowledging it, and notifications
 * carrying it are passed on; `unsubscribeMethod` is sent when the stream is
 * unsubscribed, on the socket the subscription lives on only. A subscribe
 * that gets no reply tells the connection, which checks the socket again,
 * and the subscription is made again once it serves; one the node refuses
 * errors.
 *
 * @param socket The reconnecting websocket.
 * @param subscribeMethod The subscribe method (`accountSubscribe`, `eth_subscribe`, …).
 * @param params Its params.
 * @param unsubscribeMethod The matching unsubscribe method.
 * @param timeoutMs How long to wait for the subscribe's reply.
 * @param send$ Sends a request on its subscriber's turn.
 * @returns Each acknowledgement and each notification's result; errors if the node refuses the subscription.
 */
export function jsonRpcSubscription$<TResult>(
    socket: ReconnectingWebSocketSubject<unknown>,
    subscribeMethod: string,
    params: unknown[],
    unsubscribeMethod: string,
    timeoutMs: number,
    send$: GrantedSend
): Observable<SubscriptionEvent<TResult>> {
    return socket.serving$.pipe(
        switchMap((socketNumber) =>
            send$(() => jsonRpcRequest$(socket, subscribeMethod, params, timeoutMs)).pipe(
                catchError((error: unknown) => {
                    if (requestErrorCode(error) !== DISCONNECTED)
                        return throwError(
                            () => new SubscriptionRefusedError(`${subscribeMethod} was refused: ${messageOf(error)}`)
                        );
                    // No reply: the connection checks the socket again, and once it serves, the subscription is made again.
                    socket.readFailed$.next();
                    return EMPTY;
                }),
                switchMap(
                    (subscriptionId) =>
                        new Observable<SubscriptionEvent<TResult>>((subscriber) => {
                            subscriber.next({ kind: 'acknowledged' });
                            const notifications = socket
                                .pipe(
                                    map((value) => value as JsonRpcMessage),
                                    filter((message) => message.params?.subscription === subscriptionId)
                                )
                                .subscribe((message) =>
                                    subscriber.next({ kind: 'notification', result: message.params?.result as TResult })
                                );
                            return () => {
                                notifications.unsubscribe();
                                // Ids are the socket's own: a socket that closed took the subscription with it,
                                // and the same id on another socket is someone else's.
                                if (!socket.isOpenSocket(socketNumber)) return;
                                // The unsubscribe takes a turn too; no reply is waited for.
                                send$(() =>
                                    defer(() => {
                                        socket.next(request(unsubscribeMethod, [subscriptionId]));
                                        return of(undefined);
                                    })
                                ).subscribe();
                            };
                        })
                )
            )
        ),
        share()
    );
}

/**
 * One JSON-RPC subscription on a reconnecting websocket, as
 * `jsonRpcSubscription$` makes it: its notifications' results only.
 *
 * @param socket The reconnecting websocket.
 * @param subscribeMethod The subscribe method (`accountSubscribe`, `eth_subscribe`, …).
 * @param params Its params.
 * @param unsubscribeMethod The matching unsubscribe method.
 * @param timeoutMs How long to wait for the subscribe's reply.
 * @param send$ Sends a request on its subscriber's turn.
 * @returns The notifications' results; errors if the node refuses the subscription.
 */
export function jsonRpcTopic$<TResult>(
    socket: ReconnectingWebSocketSubject<unknown>,
    subscribeMethod: string,
    params: unknown[],
    unsubscribeMethod: string,
    timeoutMs: number,
    send$: GrantedSend
): Observable<TResult> {
    return jsonRpcSubscription$<TResult>(socket, subscribeMethod, params, unsubscribeMethod, timeoutMs, send$).pipe(
        filter((event): event is Extract<SubscriptionEvent<TResult>, { kind: 'notification' }> => event.kind === 'notification'),
        map(({ result }) => result)
    );
}

/** Requests and subscriptions over an RPC node's websocket, each on its subscriber's turns. */
export interface WebSocketRpc {
    /** One JSON-RPC request: the reply's result, once. */
    request$(method: string, params: unknown[]): Observable<unknown>;
    /** One JSON-RPC subscription: each acknowledgement and each notification's result. */
    subscription$<TResult>(
        subscribeMethod: string,
        params: unknown[],
        unsubscribeMethod: string
    ): Observable<SubscriptionEvent<TResult>>;
    /** One JSON-RPC subscription's notifications' results only. */
    topic$<TResult>(subscribeMethod: string, params: unknown[], unsubscribeMethod: string): Observable<TResult>;
}

/**
 * Whether a request failed with one of `codes`, the JSON-RPC error codes a
 * node turns a request away with when it gets too many: what too many
 * requests look like to that kind of node.
 *
 * @param codes The codes.
 * @returns The test on how a request came back.
 */
export const turnedAwayWith =
    (...codes: number[]) =>
    (result: RequestResult<unknown>): boolean => {
        if (!('failed' in result)) return false;
        const code = requestErrorCode(result.failed);
        return code !== undefined && codes.includes(code);
    };

/**
 * The transport of an RPC node's websocket: its client over the socket,
 * every request and subscription of which goes through the grants, and
 * what too many requests look like to the node.
 *
 * @param socket The reconnecting websocket.
 * @param timeoutMs How long a request waits for its reply, once sent.
 * @param turnedAway Whether the node turned a request away for too many (`turnedAwayWith`).
 * @returns The transport.
 */
export function webSocketRpcTransport(
    socket: ReconnectingWebSocketSubject<unknown>,
    timeoutMs: number,
    turnedAway: (result: RequestResult<unknown>) => boolean
): GrantedTransport<WebSocketRpc> {
    return {
        clientOn: (_url, send$) => ({
            request$: (method, params) => send$(() => jsonRpcRequest$(socket, method, params, timeoutMs)),
            subscription$: <TResult>(subscribeMethod: string, params: unknown[], unsubscribeMethod: string) =>
                jsonRpcSubscription$<TResult>(socket, subscribeMethod, params, unsubscribeMethod, timeoutMs, send$),
            topic$: <TResult>(subscribeMethod: string, params: unknown[], unsubscribeMethod: string) =>
                jsonRpcTopic$<TResult>(socket, subscribeMethod, params, unsubscribeMethod, timeoutMs, send$),
        }),
        turnedAway,
    };
}
