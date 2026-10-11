import { createServer } from 'node:http';
import { type AddressInfo } from 'node:net';
import { type RunningMachine } from '@yaw-rx/ystate';
import { BehaviorSubject, firstValueFrom, NEVER } from 'rxjs';
import { ConnectionNotReadyError } from '../../rpc/connection/connectionNotReady.js';
import {
    ethereumChain,
    forCalls$,
    forSolana$,
    solanaChain,
    type SolanaTransports,
} from '../../rpc/connection/connections.js';
import { type NetworkMachine } from '../../rpc/connection/network.impl.js';
import { ethereumHttp } from '../../rpc/eth/ethereumHttp.js';
import { ethRpcRead$ } from '../../rpc/eth/ethRpcRead.js';
import { solanaHttp } from '../../rpc/solana/solanaHttp.js';
import { solanaRpcRead$ } from '../../rpc/solana/solanaRpcRead.js';
import {
    EXPECTED_CHAIN_ID,
    EXPECTED_GENESIS_HASH,
    FAST_TIMINGS,
    nodesUntil,
    statesDuring,
    testSubscriber,
    waitForNode,
    waitForState,
} from '../testUtils.js';

/** A JSON-RPC call as the endpoint receives it. */
interface Call {
    id: number | string;
    method: string;
    params?: unknown[];
}

/**
 * A local HTTP JSON-RPC endpoint that rate-limits as a public RPC does:
 * `behaviour.perSecond` calls a second are answered, the rest turned away,
 * with a 429 (and `Retry-After` when `retryAfterSeconds` is set), or, with
 * `errorInBody`, with a 200 carrying that JSON-RPC error code. Every HTTP
 * request it receives is recorded: when, the calls in it, and whether it was
 * turned away.
 *
 * @param answer The result of each answered call.
 * @returns The endpoint: `start()` gives its URL, `stop()`, `behaviour`, `requests`.
 */
function rateLimitingEndpoint(answer: (call: Call) => unknown) {
    const behaviour = {
        perSecond: Infinity,
        retryAfterSeconds: undefined as number | undefined,
        errorInBody: undefined as number | undefined,
        delayMs: 0,
    };
    const requests: { at: number; methods: string[]; turnedAway: boolean }[] = [];
    const answeredAt: number[] = [];
    const server = createServer((request, response) => {
        let body = '';
        request.on('data', (chunk: Buffer) => (body += chunk.toString()));
        request.on('end', () => setTimeout(() => {
            const payload = JSON.parse(body) as Call | Call[];
            const calls = Array.isArray(payload) ? payload : [payload];
            const now = Date.now();
            while (answeredAt.length > 0 && now - answeredAt[0] >= 1000) answeredAt.shift();
            const turnedAway = answeredAt.length >= behaviour.perSecond;
            requests.push({ at: now, methods: calls.map(({ method }) => method), turnedAway });
            if (turnedAway && behaviour.errorInBody === undefined) {
                response.writeHead(429, {
                    'content-type': 'application/json',
                    ...(behaviour.retryAfterSeconds !== undefined && {
                        'retry-after': String(behaviour.retryAfterSeconds),
                    }),
                });
                response.end(JSON.stringify({ jsonrpc: '2.0', id: calls[0].id, error: { code: 429, message: 'Too many requests' } }));
                return;
            }
            if (!turnedAway) answeredAt.push(now);
            const reply = (call: Call) =>
                turnedAway
                    ? { jsonrpc: '2.0', id: call.id, error: { code: behaviour.errorInBody, message: 'Refused' } }
                    : { jsonrpc: '2.0', id: call.id, result: answer(call) };
            response.writeHead(200, { 'content-type': 'application/json' });
            response.end(JSON.stringify(Array.isArray(payload) ? calls.map(reply) : reply(calls[0])));
        }, behaviour.delayMs));
    });
    return {
        behaviour,
        requests,
        reset: () => {
            Object.assign(behaviour, { perSecond: Infinity, retryAfterSeconds: undefined, errorInBody: undefined, delayMs: 0 });
            requests.length = 0;
        },
        start: () =>
            new Promise<string>((resolve) =>
                server.listen(0, '127.0.0.1', () => resolve(`http://127.0.0.1:${(server.address() as AddressInfo).port}`))
            ),
        stop: () =>
            new Promise<void>((resolve) => {
                server.closeAllConnections();
                server.close(() => resolve());
            }),
    };
}

/** A network that stays online. */
const online = { state$: new BehaviorSubject({ node: 'online', data: {} }) } as unknown as NetworkMachine;

/** Timings for these tests: checks rarely, so the reads are what the endpoint counts; a limit lifts after 2 quiet seconds. */
const TIMINGS = { ...FAST_TIMINGS, healthCheckIntervalMs: 60_000, healthCheckTimeoutMs: 1_000, quietSecondsToLift: 2 };

/** A predicate on a connection's states: it was overwhelmed and is ready again. */
const relievedAfterOverwhelmed = () => {
    let overwhelmed = false;
    return ({ node }: { node: string }) => {
        if (node === 'overwhelmed') overwhelmed = true;
        return overwhelmed && node === 'ready';
    };
};

/** What a promise settles to within `ms`, or `'pending'`. */
const settledWithin = <T>(promise: Promise<T>, ms: number) =>
    Promise.race([
        promise.then(
            (value) => ({ value }),
            (error: unknown) => ({ error })
        ),
        new Promise<'pending'>((resolve) => setTimeout(() => resolve('pending'), ms)),
    ]);

describe('Solana http (kit) against an endpoint that rate-limits', () => {
    const answer = ({ method }: Call) =>
        method === 'getGenesisHash' ? EXPECTED_GENESIS_HASH : method === 'getSlot' ? 100 : null;
    const endpoint = rateLimitingEndpoint(answer);
    const backup = rateLimitingEndpoint(answer);
    let url = '';
    let backupUrl = '';
    beforeAll(async () => {
        url = await endpoint.start();
        backupUrl = await backup.start();
    });
    afterAll(async () => {
        await endpoint.stop();
        await backup.stop();
    });
    beforeEach(() => {
        endpoint.reset();
        backup.reset();
    });

    /**
     * Solana over the endpoints, and one slot read on a machine's turns,
     * through the runner the read machines use. Each read is a distinct call
     * (its own `minContextSlot`) unless one is given, so identical calls are
     * merged only where a test means them to be.
     */
    const solanaOver = (rpcUrls = [url], timings: Partial<typeof TIMINGS> = {}) => {
        const http = solanaHttp(
            { rpcUrls, expectedGenesisHash: EXPECTED_GENESIS_HASH, ...TIMINGS, ...timings },
            online
        );
        const solana = solanaChain(
            {
                http,
                websocket: {
                    connection: { state$: new BehaviorSubject({ node: 'closed', data: {} }) },
                    socket: {},
                    clientOf: () => ({}),
                    load$: NEVER,
                    close: (): void => undefined,
                } as unknown as SolanaTransports['websocket'],
            },
            50
        );
        let reads = 1_000;
        const readSlot = (machine: RunningMachine, minContextSlot = BigInt(reads++)) =>
            firstValueFrom(
                forSolana$(solana, machine, (rpc) =>
                    solanaRpcRead$(
                        (abortSignal) => rpc.getSlot({ minContextSlot }).send({ abortSignal }),
                        'Failed to read the slot.'
                    )
                )
            );
        return { http, readSlot };
    };

    test('reads over the rate are sent again on their turn: every one is answered, and the connection is overwhelmed, never waiting to retry', async () => {
        const { http, readSlot } = solanaOver();
        await waitForNode(http.connection, 'ready');
        endpoint.behaviour.perSecond = 5;
        const moves = nodesUntil(http.connection, relievedAfterOverwhelmed(), 30_000);

        const machines = [testSubscriber(), testSubscriber(), testSubscriber()];
        const slots = await Promise.all(machines.flatMap((machine) => Array.from({ length: 10 }, () => readSlot(machine))));

        expect(slots).toEqual(Array.from({ length: 30 }, () => 100n));
        expect(endpoint.requests.some(({ turnedAway }) => turnedAway)).toBe(true);
        expect(await moves).not.toContain('waitingToRetry');
        http.close();
    }, 40_000);

    test('the limit is kept through a lull shorter than quietSecondsToLift: the connection stays overwhelmed rather than flapping', async () => {
        const { http, readSlot } = solanaOver([url], { quietSecondsToLift: 5 });
        await waitForNode(http.connection, 'ready');
        endpoint.behaviour.perSecond = 5;
        const machine = testSubscriber();
        await Promise.all(Array.from({ length: 12 }, () => readSlot(machine)));
        await waitForNode(http.connection, 'overwhelmed');

        const lull = await statesDuring(http.connection, 2_000);
        expect(new Set(lull.map(({ node }) => node))).toEqual(new Set(['overwhelmed']));
        await waitForNode(http.connection, 'ready', 15_000);
        http.close();
    }, 40_000);

    test('identical reads from different machines at once are one request to the endpoint', async () => {
        const { http, readSlot } = solanaOver();
        await waitForNode(http.connection, 'ready');
        endpoint.requests.length = 0;
        // The endpoint answers after 200 ms, so the identical calls are in flight together.
        endpoint.behaviour.delayMs = 200;

        const slots = await Promise.all(Array.from({ length: 5 }, () => readSlot(testSubscriber(), 7n)));

        expect(slots).toEqual(Array.from({ length: 5 }, () => 100n));
        expect(endpoint.requests.filter(({ methods }) => methods.includes('getSlot'))).toHaveLength(1);
        http.close();
    }, 40_000);

    test('Retry-After is honoured: nothing is sent to the endpoint until it has passed', async () => {
        const { http, readSlot } = solanaOver();
        await waitForNode(http.connection, 'ready');
        Object.assign(endpoint.behaviour, { perSecond: 5, retryAfterSeconds: 2 });

        const machine = testSubscriber();
        await Promise.all(Array.from({ length: 12 }, () => readSlot(machine)));

        const firstTurnedAway = endpoint.requests.find(({ turnedAway }) => turnedAway);
        expect(firstTurnedAway).toBeDefined();
        const sentDuringRetryAfter = endpoint.requests.filter(
            ({ at }) => at > (firstTurnedAway?.at ?? 0) + 50 && at < (firstTurnedAway?.at ?? 0) + 2_000
        );
        expect(sentDuringRetryAfter).toEqual([]);
        http.close();
    }, 40_000);

    test('-32005 in a 200 is a node that is unhealthy, not too many requests: the read fails rather than being sent again', async () => {
        const { http, readSlot } = solanaOver();
        await waitForNode(http.connection, 'ready');
        Object.assign(endpoint.behaviour, { perSecond: 0, errorInBody: -32005 });

        const read = await settledWithin(readSlot(testSubscriber()), 3_000);

        expect(read).not.toBe('pending');
        expect(read).toHaveProperty('error');
        http.close();
    }, 40_000);

    test('with one URL, an endpoint that turns every request away keeps them waiting their turns: none fails, and each is answered once it answers again', async () => {
        const { http, readSlot } = solanaOver();
        await waitForNode(http.connection, 'ready');
        endpoint.behaviour.perSecond = 0;

        const read = readSlot(testSubscriber());
        await waitForNode(http.connection, 'overwhelmed', 10_000);
        expect(await settledWithin(read, 3_000)).toBe('pending');

        endpoint.behaviour.perSecond = Infinity;
        expect(await read).toBe(100n);
        http.close();
    }, 40_000);

    test('with several URLs, one that turns every request away is left for the next: what waited there fails as never sent, and reads are answered by the next', async () => {
        const { http, readSlot } = solanaOver([url, backupUrl]);
        await waitForNode(http.connection, 'ready');
        endpoint.behaviour.perSecond = 0;

        const stranded = readSlot(testSubscriber()).catch((error: unknown) => error);
        const movedOn = await waitForState(
            http.connection,
            ({ node, data }) => node === 'ready' && (data as { url: string }).url === backupUrl,
            15_000
        );
        expect(movedOn).toBeDefined();
        expect(await stranded).toBeInstanceOf(ConnectionNotReadyError);
        expect(await readSlot(testSubscriber())).toBe(100n);
        expect(backup.requests.some(({ methods }) => methods.includes('getSlot'))).toBe(true);
        http.close();
    }, 40_000);
});

describe('Ethereum http (ethers) against an endpoint that rate-limits', () => {
    const endpoint = rateLimitingEndpoint(({ method }) =>
        method === 'eth_chainId'
            ? `0x${EXPECTED_CHAIN_ID.toString(16)}`
            : method === 'eth_blockNumber' || method === 'eth_getBalance'
              ? '0x64'
              : null
    );
    let url = '';
    beforeAll(async () => {
        url = await endpoint.start();
    });
    afterAll(() => endpoint.stop());
    beforeEach(() => endpoint.reset());

    /**
     * Ethereum over the endpoint, and one balance read on a machine's turns,
     * through the runner the read machines use. Each read is a distinct call
     * (its own address) unless one is given.
     */
    const ethereumOver = () => {
        const http = ethereumHttp({ rpcUrl: url, expectedChainId: EXPECTED_CHAIN_ID, ...TIMINGS }, online);
        const ethereum = ethereumChain({ http }, { calls: ['http'], logs: ['http'] }, 50);
        let reads = 0;
        const readBalance = (machine: RunningMachine, address = `0x${(++reads).toString(16).padStart(40, '0')}`) =>
            firstValueFrom(
                forCalls$(ethereum, machine, (provider) =>
                    ethRpcRead$(() => provider.getBalance(address), 'Failed to read the balance.')
                )
            );
        return { http, readBalance };
    };

    test('reads over the rate are sent again on their turn: every one is answered, and the connection is overwhelmed, never waiting to retry', async () => {
        const { http, readBalance } = ethereumOver();
        await waitForNode(http.connection, 'ready');
        endpoint.behaviour.perSecond = 5;
        const moves = nodesUntil(http.connection, relievedAfterOverwhelmed(), 30_000);

        const machines = [testSubscriber(), testSubscriber(), testSubscriber()];
        const balances = await Promise.all(
            machines.flatMap((machine) => Array.from({ length: 10 }, () => readBalance(machine)))
        );

        expect(balances).toEqual(Array.from({ length: 30 }, () => 100n));
        expect(endpoint.requests.some(({ turnedAway }) => turnedAway)).toBe(true);
        expect(await moves).not.toContain('waitingToRetry');
        http.close();
    }, 40_000);

    test('each call is its own request, so each takes its own turn: nothing is batched', async () => {
        const { http, readBalance } = ethereumOver();
        await waitForNode(http.connection, 'ready');
        endpoint.requests.length = 0;

        const machine = testSubscriber();
        await Promise.all(Array.from({ length: 8 }, () => readBalance(machine)));

        expect(endpoint.requests.length).toBeGreaterThanOrEqual(8);
        expect(endpoint.requests.every(({ methods }) => methods.length === 1)).toBe(true);
        http.close();
    }, 40_000);

    test('identical reads from different machines at once are one request to the endpoint', async () => {
        const { http, readBalance } = ethereumOver();
        await waitForNode(http.connection, 'ready');
        endpoint.requests.length = 0;
        const address = `0x${'ab'.repeat(20)}`;
        // The endpoint answers after 200 ms, so the identical calls are in flight together.
        endpoint.behaviour.delayMs = 200;

        const balances = await Promise.all(Array.from({ length: 5 }, () => readBalance(testSubscriber(), address)));

        expect(balances).toEqual(Array.from({ length: 5 }, () => 100n));
        expect(endpoint.requests.filter(({ methods }) => methods.includes('eth_getBalance'))).toHaveLength(1);
        http.close();
    }, 40_000);

    test('a limit reported in a 200 (JSON-RPC -32005, EIP-1474) is turned away too: the read is sent again and answered, never failed', async () => {
        const { http, readBalance } = ethereumOver();
        await waitForNode(http.connection, 'ready');
        Object.assign(endpoint.behaviour, { perSecond: 5, errorInBody: -32005 });

        const machine = testSubscriber();
        const balances = await Promise.all(Array.from({ length: 15 }, () => readBalance(machine)));

        expect(balances).toEqual(Array.from({ length: 15 }, () => 100n));
        expect(endpoint.requests.some(({ turnedAway }) => turnedAway)).toBe(true);
        http.close();
    }, 40_000);

    test('Retry-After is honoured: nothing is sent to the endpoint until it has passed', async () => {
        const { http, readBalance } = ethereumOver();
        await waitForNode(http.connection, 'ready');
        Object.assign(endpoint.behaviour, { perSecond: 5, retryAfterSeconds: 2 });

        const machine = testSubscriber();
        await Promise.all(Array.from({ length: 12 }, () => readBalance(machine)));

        const firstTurnedAway = endpoint.requests.find(({ turnedAway }) => turnedAway);
        expect(firstTurnedAway).toBeDefined();
        const sentDuringRetryAfter = endpoint.requests.filter(
            ({ at }) => at > (firstTurnedAway?.at ?? 0) + 50 && at < (firstTurnedAway?.at ?? 0) + 2_000
        );
        expect(sentDuringRetryAfter).toEqual([]);
        http.close();
    }, 40_000);

    test('an endpoint that turns every request away keeps them waiting their turns: none fails, and each is answered once it answers again', async () => {
        const { http, readBalance } = ethereumOver();
        await waitForNode(http.connection, 'ready');
        endpoint.behaviour.perSecond = 0;

        const read = readBalance(testSubscriber());
        await waitForNode(http.connection, 'overwhelmed', 10_000);
        expect(await settledWithin(read, 3_000)).toBe('pending');

        endpoint.behaviour.perSecond = Infinity;
        expect(await read).toBe(100n);
        http.close();
    }, 40_000);
});
