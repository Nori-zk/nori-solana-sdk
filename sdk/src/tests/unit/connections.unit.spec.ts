import { type Address } from '@solana/kit';
import { BehaviorSubject, defer, firstValueFrom, Observable, of, Subject, throwError } from 'rxjs';
import { type ChainChangesState } from '../../rpc/connection/chainChanges.js';
import { type ChainChanges, createChainChangesMachine } from '../../rpc/connection/chainChanges.impl.js';
import { ConnectionNotReadyError } from '../../rpc/connection/connectionNotReady.js';
import {
    ethereumChain,
    type EthereumTransports,
    forCalls$,
    forLogs$,
    forSolana$,
    solanaChain,
    type SolanaTransports,
} from '../../rpc/connection/connections.js';
import { MAX_BLOCK_RANGE_PER_QUERY } from '../../rpc/eth/blockRanges.js';
import { EthRpcTransportError, NoWalletConfiguredError } from '../../rpc/eth/errors.js';
import { SolanaRpcTransportError } from '../../rpc/solana/errors.js';
import { createSolanaChangesMachine } from '../../rpc/solana/solanaChanges.impl.js';
import { type SolanaRpc } from '../../rpc/solana/solanaHttp.js';
import { QUEUE_ADDRESS, statesDuring, waitForNode, waitForState } from '../testUtils.js';
import { type GraphState } from '../../utils/machines.js';

/** A transport whose machine's node the test sets, standing in for a real one. */
function fakeTransport(name: string, node: string) {
    const state$ = new BehaviorSubject<GraphState>({ node, data: {} });
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

/**
 * A websocket transport whose socket subscribes through `subscribe$`;
 * `acknowledge()` answers the last subscribe request as a node does, with
 * the subscription's id.
 */
function fakeWebsocket(node: string, subscribe$: Subject<unknown>) {
    const fake = fakeTransport('websocket', node);
    const requests = { lastId: 0, lastMethod: '', lastParams: [] as unknown[] };
    const socket = {
        multiplex: (subscribeMessage: () => { id: number; method: string; params: unknown[] }) =>
            new Observable((subscriber) => {
                const message = subscribeMessage();
                requests.lastId = message.id;
                requests.lastMethod = message.method;
                requests.lastParams = message.params;
                const subscription = subscribe$.subscribe(subscriber);
                return () => subscription.unsubscribe();
            }),
        forceReconnect: () => {
            fake.reported.count++;
        },
    };
    const acknowledge = () => subscribe$.next({ id: requests.lastId, result: '0x1' });
    return { ...fake, transport: { ...fake.transport, socket }, acknowledge, requests };
}

const chainOf = (transports: Record<string, unknown>, order: Parameters<typeof ethereumChain>[1]) =>
    ethereumChain(transports as unknown as EthereumTransports, order, 20);

/** The fake transport's name, from its provider. */
const nameOf = (provider: unknown) => (provider as { name: string }).name;

/** The machine's first state matching `matches`, failing after a second. */
const stateWhere = (machine: ChainChanges, matches: (state: ChainChangesState) => boolean) =>
    waitForState(machine, matches, 1_000);

describe('forCalls$ and forLogs$', () => {
    test('run on the first ready transport in the caller order', async () => {
        const http = fakeTransport('http', 'ready');
        const wallet = fakeTransport('wallet', 'ready');
        const ethereum = chainOf(
            { http: http.transport, wallet: wallet.transport },
            { calls: ['wallet', 'http'], logs: ['http', 'wallet'] }
        );
        const used = (provider: unknown) => of(nameOf(provider));
        expect(await firstValueFrom(forCalls$(ethereum, used))).toBe('wallet');
        expect(await firstValueFrom(forLogs$(ethereum, used))).toBe('http');
    });

    test('skip a transport that is not ready', async () => {
        const http = fakeTransport('http', 'unreachable');
        const wallet = fakeTransport('wallet', 'ready');
        const ethereum = chainOf(
            { http: http.transport, wallet: wallet.transport },
            { calls: ['http', 'wallet'], logs: ['http'] }
        );
        expect(await firstValueFrom(forCalls$(ethereum, (provider) => of(nameOf(provider))))).toBe('wallet');
    });

    test('a request that never reached the node tells that transport and runs again on the next', async () => {
        const http = fakeTransport('http', 'ready');
        const wallet = fakeTransport('wallet', 'ready');
        const ethereum = chainOf(
            { http: http.transport, wallet: wallet.transport },
            { calls: ['http', 'wallet'], logs: ['http'] }
        );
        const runs: string[] = [];
        const result = await firstValueFrom(
            forCalls$(ethereum, (provider) =>
                defer(() => {
                    const name = nameOf(provider);
                    runs.push(name);
                    return name === 'http'
                        ? throwError(() => new EthRpcTransportError('no response', undefined))
                        : of(name);
                })
            )
        );
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
            firstValueFrom(
                forCalls$(ethereum, () =>
                    defer(() => {
                        runs++;
                        return throwError(() => new Error('execution reverted'));
                    })
                )
            )
        ).rejects.toThrow('execution reverted');
        expect(runs).toBe(1);
        expect(http.reported.count).toBe(0);
    });

    test('with nothing ready, fail at once with every transport state', async () => {
        const http = fakeTransport('http', 'unreachable');
        const ethereum = chainOf({ http: http.transport }, { calls: ['http', 'wallet'] });
        const failure = await firstValueFrom(forCalls$(ethereum, () => of('never'))).catch(
            (error: unknown) => error
        );
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

    test('the wallet ready$() errors with its own error when no wallet is configured', async () => {
        const ethereum = chainOf({}, {});
        await expect(firstValueFrom(ethereum.wallet.ready$())).rejects.toBeInstanceOf(NoWalletConfiguredError);
    });
});

describe('the chain changes machine', () => {
    /** An http transport whose provider serves `head` as the latest block and `logs` for any range. */
    const fakeHttp = (head: { number: number }, logs: { found: unknown[]; reads: number }) => {
        const http = fakeTransport('http', 'ready');
        const provider = {
            getBlock: async () => ({ ...head }),
            getLogs: async () => {
                logs.reads++;
                return logs.found;
            },
        };
        return { ...http.transport, current: () => provider };
    };

    test('subscribes while the websocket is open, polls while it is not, subscribes again when it reopens', async () => {
        const pushed$ = new Subject<unknown>();
        const websocket = fakeWebsocket('open', pushed$);
        const head = { number: 100 };
        const ethereum = chainOf(
            { http: fakeHttp(head, { found: [], reads: 0 }), websocket: websocket.transport },
            { calls: ['http'], logs: ['http'], subscriptions: ['websocket'] }
        );
        const changes = createChainChangesMachine(ethereum);
        const push = () => pushed$.next({ params: { subscription: 1, result: {} } });

        await stateWhere(changes, (state) => state.node === 'subscribed' && !state.data.acknowledged);
        websocket.acknowledge();
        await stateWhere(changes, (state) => state.node === 'subscribed' && state.data.acknowledged);
        push();
        await stateWhere(changes, (state) => state.node === 'subscribed' && state.data.changes === 1);

        // Losing the websocket is a change; the first poll only reads the latest block.
        websocket.state$.next({ node: 'reconnecting', data: {} });
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.changes === 2 && state.data.lastPolled === 100
        );
        head.number = 101;
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.changes === 3 && state.data.lastPolled === 101
        );

        // Subscribing again after polling is a change.
        websocket.state$.next({ node: 'open', data: {} });
        await stateWhere(changes, (state) => state.node === 'subscribed' && state.data.changes === 4);
        push();
        await stateWhere(changes, (state) => state.node === 'subscribed' && state.data.changes === 5);

        changes.close();
        await waitForNode(changes, 'closed', 1_000);
    });

    test('a wallet that does not serve eth_subscribe is recorded, and the machine polls', async () => {
        const wallet = fakeTransport('wallet', 'ready');
        const walletTransport = {
            ...wallet.transport,
            currentWalletProvider: () => ({
                request: async () => {
                    throw new Error('eth_subscribe is not supported.');
                },
            }),
        };
        const ethereum = chainOf(
            { http: fakeHttp({ number: 100 }, { found: [], reads: 0 }), wallet: walletTransport },
            { calls: ['http'], logs: ['http'], subscriptions: ['wallet'] }
        );
        const changes = createChainChangesMachine(ethereum);

        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.unsupported.includes('wallet')
        );
        const nodes = (await statesDuring(changes, 200)).map(({ node }) => node);
        expect(new Set(nodes)).toEqual(new Set(['polling']));

        changes.close();
    });

    test('with a logs filter, a block holding a matching log is a change, and a gap counts once unread', async () => {
        const head = { number: 100 };
        const logs = { found: [] as unknown[], reads: 0 };
        const ethereum = chainOf({ http: fakeHttp(head, logs) }, { calls: ['http'], logs: ['http'] });
        const changes = createChainChangesMachine(ethereum, { address: QUEUE_ADDRESS });

        await stateWhere(changes, (state) => state.node === 'polling' && state.data.lastPolled === 100);
        head.number = 101;
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.lastPolled === 101 && state.data.changes === 0
        );
        logs.found = [{}];
        head.number = 102;
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.lastPolled === 102 && state.data.changes === 1
        );

        const readsBeforeGap = logs.reads;
        head.number = 102 + MAX_BLOCK_RANGE_PER_QUERY + 1;
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.lastPolled === head.number && state.data.changes === 2
        );
        expect(logs.reads).toBe(readsBeforeGap);

        changes.close();
    });
});

/** The address whose transactions the Solana tests watch. */
const WATCHED = 'Watched11111111111111111111111111111111111' as Address;

/**
 * A Solana RPC serving `chain.slot` as the finalized slot and `chain.watchedSlot`
 * as the slot of the watched address's newest transaction; `failNext` makes
 * the next request get no response.
 */
function fakeSolanaRpc(chain: { slot: number; watchedSlot?: number; failNext: boolean }) {
    const answer = <T>(value: () => T) => ({
        send: async () => {
            if (chain.failNext) {
                chain.failNext = false;
                throw new SolanaRpcTransportError('The request got no response.', new TypeError('fetch failed'));
            }
            return value();
        },
    });
    return {
        getSlot: () => answer(() => BigInt(chain.slot)),
        getSignaturesForAddress: (address: Address) =>
            answer(() =>
                address === WATCHED && chain.watchedSlot !== undefined
                    ? [{ signature: 'watched', slot: BigInt(chain.watchedSlot) }]
                    : []
            ),
    } as unknown as SolanaRpc;
}

/** A Solana chain over a fake http (`node`) serving `rpc`, and a websocket subscribing through `pushed$`. */
function solanaOf(node: string, rpc: SolanaRpc, websocketNode = 'closed', pushed$ = new Subject<unknown>()) {
    const http = fakeTransport('http', node);
    const websocket = fakeWebsocket(websocketNode, pushed$);
    const solana = solanaChain(
        {
            http: { ...http.transport, rpcOf: () => rpc },
            websocket: { ...websocket.transport, close: (): void => undefined },
        } as unknown as SolanaTransports,
        20
    );
    return { solana, http, websocket };
}

describe('forSolana$', () => {
    test('runs on the RPC while http is ready, and errors with its state while it is not', async () => {
        const rpc = fakeSolanaRpc({ slot: 7, failNext: false });
        const { solana, http } = solanaOf('ready', rpc);
        expect(await firstValueFrom(forSolana$(solana, (served) => of(served === rpc)))).toBe(true);

        http.state$.next({ node: 'unreachable', data: {} });
        const failure = await firstValueFrom(forSolana$(solana, () => of('never'))).catch(
            (error: unknown) => error
        );
        expect(failure).toBeInstanceOf(ConnectionNotReadyError);
        expect((failure as ConnectionNotReadyError).notReady).toEqual([
            { transport: 'solana.http', state: { node: 'unreachable', data: {} } },
        ]);
    });

    test('a request that got no response tells http and errors as unanswered; any other failure is thrown as is', async () => {
        const { solana, http } = solanaOf('ready', fakeSolanaRpc({ slot: 7, failNext: false }));
        const unanswered = await firstValueFrom(
            forSolana$(solana, () => throwError(() => new SolanaRpcTransportError('no response', undefined)))
        ).catch((error: unknown) => error);
        expect(unanswered).toBeInstanceOf(ConnectionNotReadyError);
        expect((unanswered as ConnectionNotReadyError).notReady[0].state.node).toBe('noResponse');
        expect(http.reported.count).toBe(1);

        await expect(
            firstValueFrom(forSolana$(solana, () => throwError(() => new Error('account not found'))))
        ).rejects.toThrow('account not found');
        expect(http.reported.count).toBe(1);
    });
});

describe('the Solana changes machine', () => {
    test('polls the finalized slot while the websocket is closed: each new slot is a change', async () => {
        const chain = { slot: 100, failNext: false };
        const { solana } = solanaOf('ready', fakeSolanaRpc(chain));
        const changes = createSolanaChangesMachine(solana);

        await stateWhere(changes, (state) => state.node === 'polling' && state.data.lastPolled === 100);
        chain.slot = 101;
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.lastPolled === 101 && state.data.changes === 1
        );
        changes.close();
        await waitForNode(changes, 'closed', 1_000);
    });

    test('with an address, a new slot is a change only when a transaction mentioning it landed after the last one polled', async () => {
        const chain: { slot: number; watchedSlot?: number; failNext: boolean } = { slot: 100, failNext: false };
        const { solana } = solanaOf('ready', fakeSolanaRpc(chain));
        const changes = createSolanaChangesMachine(solana, { mentions: WATCHED });

        await stateWhere(changes, (state) => state.node === 'polling' && state.data.lastPolled === 100);
        chain.slot = 101;
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.lastPolled === 101 && state.data.changes === 0
        );
        chain.watchedSlot = 102;
        chain.slot = 102;
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.lastPolled === 102 && state.data.changes === 1
        );
        chain.slot = 103;
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.lastPolled === 103 && state.data.changes === 1
        );
        changes.close();
    });

    test('subscribes for transactions mentioning the address while the websocket is open, and polls once it closes', async () => {
        const pushed$ = new Subject<unknown>();
        const chain = { slot: 100, failNext: false };
        const { solana, websocket } = solanaOf('ready', fakeSolanaRpc(chain), 'open', pushed$);
        const changes = createSolanaChangesMachine(solana, { mentions: WATCHED });

        await stateWhere(changes, (state) => state.node === 'subscribed' && !state.data.acknowledged);
        expect(websocket.requests.lastMethod).toBe('logsSubscribe');
        expect(websocket.requests.lastParams).toEqual([{ mentions: [WATCHED] }, { commitment: 'finalized' }]);
        websocket.acknowledge();
        await stateWhere(changes, (state) => state.node === 'subscribed' && state.data.acknowledged);
        pushed$.next({ params: { subscription: 1, result: {} } });
        await stateWhere(changes, (state) => state.node === 'subscribed' && state.data.changes === 1);

        websocket.state$.next({ node: 'reconnecting', data: {} });
        await stateWhere(
            changes,
            (state) => state.node === 'polling' && state.data.changes === 2 && state.data.lastPolled === 100
        );
        changes.close();
    });

    test('waits in polling while http cannot serve a read, and polls once it can', async () => {
        const chain = { slot: 100, failNext: false };
        const { solana, http } = solanaOf('unreachable', fakeSolanaRpc(chain));
        const changes = createSolanaChangesMachine(solana);

        const waiting = await statesDuring(changes, 100);
        expect(waiting.map(({ node, data }) => [node, 'lastPolled' in data ? data.lastPolled : 'none'])).toEqual([
            ['polling', undefined],
        ]);
        http.state$.next({ node: 'ready', data: { url: 'https://solana.test' } });
        await stateWhere(changes, (state) => state.node === 'polling' && state.data.lastPolled === 100);
        changes.close();
    });
});
