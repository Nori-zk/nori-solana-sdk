import {
    defer,
    filter,
    firstValueFrom,
    map,
    type Observable,
    share,
    take,
    timeout,
} from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import { type ReconnectingWebSocketSubject } from './websocket.js';

/** A JSON-RPC message received on a node's websocket. */
interface JsonRpcMessage {
    id?: number;
    result?: unknown;
    error?: unknown;
    params?: { subscription?: number | string; result?: unknown };
}

/** Request ids, unique across every socket. */
let nextRequestId = 1;

/**
 * Builds a JSON-RPC request.
 *
 * @param id The request id.
 * @param method The method.
 * @param params Its params.
 * @returns The request.
 */
function request(id: number, method: string, params: unknown[]) {
    return { jsonrpc: '2.0', id, method, params };
}

/** The EIP-1193 code for a provider that is disconnected from every chain. */
const DISCONNECTED = 4900;

/**
 * One JSON-RPC request on a reconnecting websocket, answered by the reply
 * carrying its id. A request the socket cannot carry, or that gets no reply
 * within `timeoutMs`, fails with EIP-1193's disconnected code (4900); a reply
 * carrying an error fails with that error's code and message.
 *
 * @param socket The reconnecting websocket.
 * @param method The method.
 * @param params Its params.
 * @param timeoutMs How long to wait for the reply.
 * @returns The reply's result.
 */
export function jsonRpcRequest(
    socket: ReconnectingWebSocketSubject<unknown>,
    method: string,
    params: unknown[],
    timeoutMs: number
): Promise<unknown> {
    const id = nextRequestId++;
    const reply = firstValueFrom(
        socket.pipe(
            filter((value) => (value as JsonRpcMessage).id === id),
            take(1),
            timeout(timeoutMs)
        )
    );
    socket.next(request(id, method, params));
    return reply.then(
        (value) => {
            const message = value as JsonRpcMessage & {
                error?: { code?: number; message?: string };
            };
            if (message.error !== undefined)
                throw Object.assign(
                    new Error(message.error.message ?? `${method} failed.`),
                    { code: message.error.code }
                );
            return message.result;
        },
        (error: unknown) => {
            throw Object.assign(
                new Error(`${method} got no reply: ${messageOf(error)}`),
                { code: DISCONNECTED }
            );
        }
    );
}

/**
 * One JSON-RPC subscription on a reconnecting websocket, as Solana and
 * Ethereum nodes serve them, through `multiplex`: `subscribeMethod` is sent
 * when the stream is subscribed and again every time the socket opens; each
 * reply gives a new subscription id, and notifications carrying it are
 * passed on; `unsubscribeMethod` is sent when the stream is unsubscribed.
 * Notifications arrive only while the socket is open.
 *
 * @param socket The reconnecting websocket.
 * @param subscribeMethod The subscribe method (`accountSubscribe`, `eth_subscribe`, …).
 * @param params Its params.
 * @param unsubscribeMethod The matching unsubscribe method.
 * @returns The notifications' results; errors if the node refuses the subscription.
 */
export function getJsonRpcTopic$<TResult>(
    socket: ReconnectingWebSocketSubject<unknown>,
    subscribeMethod: string,
    params: unknown[],
    unsubscribeMethod: string
): Observable<TResult> {
    return defer(() => {
        const id = nextRequestId++;
        let subscriptionId: number | string | undefined;
        return socket
            .multiplex<JsonRpcMessage>(
                () => {
                    subscriptionId = undefined;
                    return request(id, subscribeMethod, params);
                },
                () => request(nextRequestId++, unsubscribeMethod, [subscriptionId]),
                (value) => {
                    const message = value as JsonRpcMessage;
                    if (message.id === id) {
                        if (
                            typeof message.result === 'number' ||
                            typeof message.result === 'string'
                        ) {
                            subscriptionId = message.result;
                            return false;
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
                map((message) => {
                    if (message.id === id)
                        throw new Error(
                            `${subscribeMethod} was refused: ${JSON.stringify(message.error)}`
                        );
                    return message.params?.result as TResult;
                })
            );
    }).pipe(share());
}
