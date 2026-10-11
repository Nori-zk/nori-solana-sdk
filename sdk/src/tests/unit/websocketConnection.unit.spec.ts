import {
    BehaviorSubject,
    concatMap,
    filter,
    firstValueFrom,
    forkJoin,
    interval,
    lastValueFrom,
    take,
    toArray,
} from 'rxjs';
import { LIMIT_EXCEEDED } from '../../rpc/connection/jsonRpcTopic.js';
import { type NetworkMachine } from '../../rpc/connection/network.impl.js';
import { ethereumWebsocket } from '../../rpc/eth/ethereumWebsocket.js';
import { solanaWebsocket } from '../../rpc/solana/solanaWebsocket.js';
import { EXPECTED_CHAIN_ID, FAST_TIMINGS, statesDuring, testSubscriber, waitForNode } from '../testUtils.js';

/** A JSON-RPC request as the server receives it. */
interface JsonRpcRequest {
    id: string;
    method: string;
    params: unknown[];
}

/**
 * How the server answers one request: a result, an error, "method not found"
 * (`undefined`, as a node answers what it does not serve), or no reply at all.
 */
type Reply = { result: unknown } | { error: { code: number; message: string } } | 'noReply' | undefined;

/**
 * A websocket server answering JSON-RPC requests with `answer`, behind the
 * `WebSocketCtor` the connection opens its sockets with. A request `answer`
 * leaves unanswered gets "method not found", as a node replies to any
 * request carrying an id; `silent.on` stops every reply, as a socket whose
 * other end went away.
 *
 * @param answer How it answers each request.
 * @returns The socket constructor, every socket opened, every request received, and `silent`.
 */
function websocketServer(answer: (request: JsonRpcRequest) => Reply) {
    const sockets: { drop(): void }[] = [];
    const received: (JsonRpcRequest & { socket: number })[] = [];
    const silent = { on: false };
    class ServerSocket {
        /** Which socket this is, in the order they opened. */
        readonly index = sockets.length;
        readyState = 0;
        binaryType = 'blob';
        onopen: ((event: unknown) => void) | null = null;
        onclose: ((event: { code: number; reason: string; wasClean: boolean }) => void) | null = null;
        onerror: ((event: unknown) => void) | null = null;
        onmessage: ((event: { data: string }) => void) | null = null;

        constructor(readonly url: string) {
            sockets.push(this);
            setTimeout(() => {
                this.readyState = 1;
                this.onopen?.({});
            }, 0);
        }

        send(data: string) {
            const request = JSON.parse(data) as JsonRpcRequest;
            received.push({ ...request, socket: this.index });
            if (silent.on) return;
            const answered = answer(request);
            if (answered === 'noReply') return;
            const reply = answered ?? { error: { code: -32601, message: 'Method not found' } };
            setTimeout(
                () => this.onmessage?.({ data: JSON.stringify({ jsonrpc: '2.0', id: request.id, ...reply }) }),
                0
            );
        }

        close() {
            if (this.readyState === 3) return;
            this.readyState = 3;
            setTimeout(() => this.onclose?.({ code: 1000, reason: '', wasClean: true }), 0);
        }

        /** The server drops the socket. */
        drop() {
            this.readyState = 3;
            this.onclose?.({ code: 1006, reason: 'dropped', wasClean: false });
        }
    }
    return { WebSocketCtor: ServerSocket as unknown as new (url: string) => WebSocket, sockets, received, silent };
}

/** A network that stays online. */
const online = { state$: new BehaviorSubject({ node: 'online', data: {} }) } as unknown as NetworkMachine;

/** The expected chain's id, as a node answers `eth_chainId`. */
const EXPECTED_CHAIN_HEX = `0x${EXPECTED_CHAIN_ID.toString(16)}`;

describe('websocket connection', () => {
    test('an Ethereum websocket on another chain is wrongNetwork, says which, and rechecks on a fresh socket until it is right', async () => {
        let chainId = '0x1';
        const server = websocketServer(({ method }) =>
            method === 'eth_chainId' ? { result: chainId } : method === 'eth_blockNumber' ? { result: '0x10' } : undefined
        );
        const websocket = ethereumWebsocket(
            { url: 'wss://ethereum.test', WebSocketCtor: server.WebSocketCtor, ...FAST_TIMINGS },
            online,
            EXPECTED_CHAIN_ID
        );

        const wrong = await waitForNode(websocket.connection, 'wrongNetwork');
        expect(wrong.data).toEqual(
            expect.objectContaining({ url: 'wss://ethereum.test', found: '1', expected: EXPECTED_CHAIN_ID.toString() })
        );
        chainId = EXPECTED_CHAIN_HEX;
        const ready = await waitForNode(websocket.connection, 'ready');
        expect(ready.data).toEqual(expect.objectContaining({ health: { blockNumber: 16 } }));
        expect(server.sockets.length).toBeGreaterThan(1);
        websocket.close();
    });

    test('a Solana websocket is ready once the node acknowledges a subscription, which it then unsubscribes', async () => {
        const server = websocketServer(({ method }) =>
            method === 'slotSubscribe'
                ? { result: 7 }
                : method === 'slotUnsubscribe'
                  ? { result: true }
                  : { error: { code: -32601, message: 'Method not found' } }
        );
        const websocket = solanaWebsocket(
            { url: 'wss://solana.test', WebSocketCtor: server.WebSocketCtor, ...FAST_TIMINGS },
            online
        );

        await waitForNode(websocket.connection, 'ready');
        const checked = server.received.map(({ method }) => method).filter((method) => method !== 'ping');
        expect(checked).toEqual(['slotSubscribe', 'slotUnsubscribe']);
        websocket.socket.complete();
    });

    test('a node refusing the subscription waits to retry, gives up once the attempts allowed run out, and retry connects again', async () => {
        let refuses = true;
        const server = websocketServer(({ method }) =>
            refuses
                ? { error: { code: -32601, message: 'Method not found' } }
                : method === 'slotSubscribe'
                  ? { result: 7 }
                  : { result: true }
        );
        const websocket = solanaWebsocket(
            { url: 'wss://solana.test', WebSocketCtor: server.WebSocketCtor, ...FAST_TIMINGS, maxFailedAttempts: 2 },
            online
        );

        await waitForNode(websocket.connection, 'waitingToRetry');
        await waitForNode(websocket.connection, 'gaveUp');
        refuses = false;
        websocket.socket.retry();
        await waitForNode(websocket.connection, 'ready');
        websocket.socket.complete();
    });

    test('a quiet socket pings once nothing has been received for an interval, stays ready while the node replies, and is dropped once it stops replying', async () => {
        const server = websocketServer(({ method }) =>
            method === 'eth_chainId' ? { result: EXPECTED_CHAIN_HEX } : method === 'eth_blockNumber' ? { result: '0x10' } : undefined
        );
        const websocket = ethereumWebsocket(
            { url: 'wss://ethereum.test', WebSocketCtor: server.WebSocketCtor, ...FAST_TIMINGS },
            online,
            EXPECTED_CHAIN_ID
        );

        await waitForNode(websocket.connection, 'ready');
        const quiet = await statesDuring(websocket.connection, 500);
        expect(new Set(quiet.map(({ node }) => node))).toEqual(new Set(['ready']));
        expect(server.received.some(({ method }) => method === 'ping')).toBe(true);

        const dropped = waitForNode(websocket.connection, 'waitingToRetry');
        server.silent.on = true;
        await dropped;
        websocket.close();
    });

    test('a socket receiving traffic sends no pings: what it receives proves it alive', async () => {
        const server = websocketServer(({ method }) =>
            method === 'eth_chainId' ? { result: EXPECTED_CHAIN_HEX } : method === 'eth_blockNumber' ? { result: '0x10' } : undefined
        );
        const websocket = ethereumWebsocket(
            { url: 'wss://ethereum.test', WebSocketCtor: server.WebSocketCtor, ...FAST_TIMINGS },
            online,
            EXPECTED_CHAIN_ID
        );
        await waitForNode(websocket.connection, 'ready');
        const client = websocket.clientOf(testSubscriber());
        const before = server.received.length;

        // A reply every 40 ms, more often than the 100 ms heartbeat interval.
        await lastValueFrom(
            interval(40).pipe(
                take(12),
                concatMap(() => client.request$('eth_blockNumber', []))
            )
        );

        expect(server.received.slice(before).map(({ method }) => method)).not.toContain('ping');
        websocket.close();
    });

    test('a reconnect makes the subscription again on the new socket, and sends no unsubscribe there for the old one', async () => {
        const server = websocketServer(({ method }) =>
            method === 'eth_chainId'
                ? { result: EXPECTED_CHAIN_HEX }
                : method === 'eth_subscribe'
                  ? { result: '0x1' }
                  : method === 'eth_blockNumber'
                    ? { result: '0x10' }
                    : undefined
        );
        const websocket = ethereumWebsocket(
            { url: 'wss://ethereum.test', WebSocketCtor: server.WebSocketCtor, ...FAST_TIMINGS },
            online,
            EXPECTED_CHAIN_ID
        );
        await waitForNode(websocket.connection, 'ready');
        const acknowledged$ = websocket
            .clientOf(testSubscriber())
            .subscription$('eth_subscribe', ['newHeads'], 'eth_unsubscribe')
            .pipe(filter(({ kind }) => kind === 'acknowledged'));

        // Someone keeps listening throughout, so the subscription on the new socket stays wanted.
        const listening = acknowledged$.subscribe();
        const twice = firstValueFrom(acknowledged$.pipe(take(2), toArray()));
        await firstValueFrom(acknowledged$);
        server.sockets[0].drop();
        await twice;

        // Ids are each socket's own: the old subscription's id on the new socket would be someone else's.
        expect(server.received.filter(({ socket, method }) => socket === 1 && method === 'eth_unsubscribe')).toEqual([]);
        expect(server.received.filter(({ method }) => method === 'eth_subscribe').map(({ socket }) => socket)).toEqual([
            0, 1,
        ]);
        listening.unsubscribe();
        websocket.close();
    });

    test('a subscribe that gets no reply makes the connection check its socket again, and the subscription is made again once it serves', async () => {
        let subscribes = 0;
        const server = websocketServer(({ method }) =>
            method === 'eth_chainId'
                ? { result: EXPECTED_CHAIN_HEX }
                : method === 'eth_blockNumber'
                  ? { result: '0x10' }
                  : method === 'eth_subscribe'
                    ? subscribes++ === 0
                        ? 'noReply'
                        : { result: '0x1' }
                    : undefined
        );
        const websocket = ethereumWebsocket(
            { url: 'wss://ethereum.test', WebSocketCtor: server.WebSocketCtor, ...FAST_TIMINGS },
            online,
            EXPECTED_CHAIN_ID
        );
        await waitForNode(websocket.connection, 'ready');

        const acknowledged = await firstValueFrom(
            websocket
                .clientOf(testSubscriber())
                .subscription$('eth_subscribe', ['newHeads'], 'eth_unsubscribe')
                .pipe(filter(({ kind }) => kind === 'acknowledged'))
        );

        expect(acknowledged).toEqual({ kind: 'acknowledged' });
        expect(subscribes).toBe(2);
        websocket.close();
    });

    test('a dropped socket waits to retry, then connects again on a fresh socket', async () => {
        const server = websocketServer(({ method }) =>
            method === 'eth_chainId' ? { result: EXPECTED_CHAIN_HEX } : { result: '0x10' }
        );
        const websocket = ethereumWebsocket(
            { url: 'wss://ethereum.test', WebSocketCtor: server.WebSocketCtor, ...FAST_TIMINGS },
            online,
            EXPECTED_CHAIN_ID
        );

        await waitForNode(websocket.connection, 'ready');
        const reconnected = waitForNode(websocket.connection, 'waitingToRetry');
        server.sockets[server.sockets.length - 1].drop();
        await reconnected;
        await waitForNode(websocket.connection, 'ready');
        expect(server.sockets.length).toBe(2);
        websocket.close();
    });

    test('requests the node turns away for too many are sent again on their turn, so every one is answered; it is overwhelmed while limited', async () => {
        const answeredAt: number[] = [];
        const server = websocketServer(({ method }) => {
            if (method === 'eth_chainId') return { result: EXPECTED_CHAIN_HEX };
            if (method !== 'eth_blockNumber') return undefined;
            const now = Date.now();
            while (answeredAt.length > 0 && now - answeredAt[0] >= 1000) answeredAt.shift();
            if (answeredAt.length >= 5) return { error: { code: LIMIT_EXCEEDED, message: 'Limit exceeded' } };
            answeredAt.push(now);
            return { result: '0x10' };
        });
        const websocket = ethereumWebsocket(
            {
                url: 'wss://ethereum.test',
                WebSocketCtor: server.WebSocketCtor,
                ...FAST_TIMINGS,
                healthCheckTimeoutMs: 5_000,
            },
            online,
            EXPECTED_CHAIN_ID
        );
        await waitForNode(websocket.connection, 'ready');
        const client = websocket.clientOf(testSubscriber());
        const overwhelmed = waitForNode(websocket.connection, 'overwhelmed', 10_000);

        const answers = await firstValueFrom(
            forkJoin(Array.from({ length: 15 }, () => client.request$('eth_blockNumber', [])))
        );

        expect(answers).toEqual(Array.from({ length: 15 }, () => '0x10'));
        expect((await overwhelmed).data).toEqual(expect.objectContaining({ ratePerSecond: expect.any(Number) }));
        websocket.close();
    }, 20_000);
});
