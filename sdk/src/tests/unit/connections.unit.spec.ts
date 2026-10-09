import { BehaviorSubject, Observable, Subject } from 'rxjs';
import { ConnectionNotReadyError } from '../../rpc/connection/connectionNotReady.js';
import {
    ethereumChain,
    type EthereumTransports,
    forCalls,
    forLogs,
    forSubscriptions,
} from '../../rpc/connection/connections.js';
import {
    EthRpcTransportError,
    NoEthereumHttpConfiguredError,
    NoEthereumWebsocketConfiguredError,
    NoWalletConfiguredError,
} from '../../rpc/eth/errors.js';
import { WalletSubscriptionUnsupportedError } from '../../rpc/eth/walletSubscriptions.js';
import { sleep } from '../testUtils.js';

/** A transport whose machine's node the test sets, standing in for a real one. */
function fakeTransport(name: string, node: string) {
    const state$ = new BehaviorSubject<{ node: string; data: unknown }>({ node, data: {} });
    const reported = { count: 0 };
    return {
        name,
        state$,
        reported,
        transport: {
            connection: { state$ },
            current: () => ({ name }),
            reportReadFailed: () => {
                reported.count++;
            },
            close: (): void => undefined,
        },
    };
}

/** A websocket transport whose socket subscribes through `subscribe$`. */
function fakeWebsocket(node: string, subscribe$: Subject<unknown>) {
    const fake = fakeTransport('websocket', node);
    const socket = {
        multiplex: () =>
            new Observable((subscriber) => {
                const subscription = subscribe$.subscribe(subscriber);
                return () => subscription.unsubscribe();
            }),
        forceReconnect: () => {
            fake.reported.count++;
        },
    };
    return { ...fake, transport: { ...fake.transport, socket } };
}

const chainOf = (transports: Record<string, unknown>, order: Parameters<typeof ethereumChain>[1]) =>
    ethereumChain(transports as unknown as EthereumTransports, order, 20);

describe('forCalls and forLogs', () => {
    test('run on the first ready transport in the caller order', async () => {
        const http = fakeTransport('http', 'ready');
        const wallet = fakeTransport('wallet', 'ready');
        const ethereum = chainOf(
            { http: http.transport, wallet: wallet.transport },
            { calls: ['wallet', 'http'], logs: ['http', 'wallet'] }
        );
        const used = async (provider: unknown) => (provider as unknown as { name: string }).name;
        expect(await forCalls(ethereum, used)).toBe('wallet');
        expect(await forLogs(ethereum, used)).toBe('http');
    });

    test('skip a transport that is not ready', async () => {
        const http = fakeTransport('http', 'unreachable');
        const wallet = fakeTransport('wallet', 'ready');
        const ethereum = chainOf(
            { http: http.transport, wallet: wallet.transport },
            { calls: ['http', 'wallet'], logs: ['http'] }
        );
        expect(await forCalls(ethereum, async (provider) => (provider as unknown as { name: string }).name)).toBe(
            'wallet'
        );
    });

    test('a request that never reached the node tells that transport and runs again on the next', async () => {
        const http = fakeTransport('http', 'ready');
        const wallet = fakeTransport('wallet', 'ready');
        const ethereum = chainOf(
            { http: http.transport, wallet: wallet.transport },
            { calls: ['http', 'wallet'], logs: ['http'] }
        );
        const runs: string[] = [];
        const result = await forCalls(ethereum, async (provider) => {
            const { name } = provider as unknown as { name: string };
            runs.push(name);
            if (name === 'http') throw new EthRpcTransportError('no response', undefined);
            return name;
        });
        expect(result).toBe('wallet');
        expect(runs).toEqual(['http', 'wallet']);
        expect(http.reported.count).toBe(1);
        expect(wallet.reported.count).toBe(0);
    });

    test('any other failure is thrown as is, without running again', async () => {
        const http = fakeTransport('http', 'ready');
        const wallet = fakeTransport('wallet', 'ready');
        const ethereum = chainOf(
            { http: http.transport, wallet: wallet.transport },
            { calls: ['http', 'wallet'], logs: ['http'] }
        );
        let runs = 0;
        await expect(
            forCalls(ethereum, async () => {
                runs++;
                throw new Error('execution reverted');
            })
        ).rejects.toThrow('execution reverted');
        expect(runs).toBe(1);
        expect(http.reported.count).toBe(0);
    });

    test('with nothing ready, fail at once with every transport state', async () => {
        const http = fakeTransport('http', 'unreachable');
        const ethereum = chainOf({ http: http.transport }, { calls: ['http', 'wallet'] });
        const failure = await forCalls(ethereum, async () => 'never').catch((error: unknown) => error);
        expect(failure).toBeInstanceOf(ConnectionNotReadyError);
        expect((failure as ConnectionNotReadyError).notReady).toEqual([
            { transport: 'ethereum.http', state: { node: 'unreachable', data: {} } },
        ]);
    });

    test('several transports for a kind of request and no order given is an error', () => {
        const http = fakeTransport('http', 'ready');
        const wallet = fakeTransport('wallet', 'ready');
        expect(() => chainOf({ http: http.transport, wallet: wallet.transport }, {})).toThrow(
            'give ethereum.order.calls'
        );
    });

    test('ready() of a transport that is not configured throws its own error', async () => {
        const ethereum = chainOf({}, {});
        await expect(ethereum.wallet.ready()).rejects.toBeInstanceOf(NoWalletConfiguredError);
        await expect(ethereum.http.ready()).rejects.toBeInstanceOf(NoEthereumHttpConfiguredError);
        await expect(ethereum.websocket.ready()).rejects.toBeInstanceOf(
            NoEthereumWebsocketConfiguredError
        );
    });
});

describe('forSubscriptions', () => {
    test('pushed while the websocket is open, polled while it is not, pushed again when it reopens', async () => {
        const pushed$ = new Subject<unknown>();
        const websocket = fakeWebsocket('open', pushed$);
        const http = fakeTransport('http', 'ready');
        const ethereum = chainOf(
            { http: http.transport, websocket: websocket.transport },
            { calls: ['http'], logs: ['http'], subscriptions: ['websocket'] }
        );
        const seen: unknown[] = [];
        let polls = 0;
        const subscription = forSubscriptions(ethereum, 
                (socket) => socket.ethSubscribe(['newHeads']),
                async () => [`polled ${++polls}`]
            )
            .subscribe((value) => seen.push(value));

        const notification = (result: string) => ({ params: { subscription: 1, result } });
        pushed$.next(notification('pushed 1'));
        websocket.state$.next({ node: 'reconnecting', data: {} });
        await sleep(30);
        websocket.state$.next({ node: 'open', data: {} });
        pushed$.next(notification('pushed 2'));
        subscription.unsubscribe();

        expect(seen[0]).toBe('pushed 1');
        expect(seen).toContain('polled 1');
        expect(seen[seen.length - 1]).toBe('pushed 2');
        expect(polls).toBeGreaterThanOrEqual(1);
    });

    test('a wallet that does not serve eth_subscribe is passed over', async () => {
        const http = fakeTransport('http', 'ready');
        const wallet = fakeTransport('wallet', 'ready');
        const walletTransport = {
            ...wallet.transport,
            currentWalletProvider: () => ({ request: async (): Promise<undefined> => undefined }),
        };
        const ethereum = chainOf(
            { http: http.transport, wallet: walletTransport },
            { calls: ['http'], logs: ['http'], subscriptions: ['wallet'] }
        );
        const seen: unknown[] = [];
        const subscription = forSubscriptions(ethereum, 
                () =>
                    new Observable<string>((subscriber) =>
                        subscriber.error(new WalletSubscriptionUnsupportedError('refused'))
                    ),
                async () => ['polled']
            )
            .subscribe((value) => seen.push(value));
        await sleep(10);
        subscription.unsubscribe();
        expect(seen).toContain('polled');
    });
});
