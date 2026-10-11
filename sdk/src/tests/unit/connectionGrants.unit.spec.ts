import { type RunningMachine } from '@yaw-rx/ystate';
import { defer, filter, firstValueFrom, forkJoin, lastValueFrom, map, NEVER, type Observable, of, Subject } from 'rxjs';
import { type HealthCheck } from '../../rpc/connection/connection.impl.js';
import { RequestNotSentError, TOO_MANY_REQUESTS } from '../../rpc/connection/connectionGrants.js';
import { httpConnection } from '../../rpc/connection/httpConnection.impl.js';
import { createEthereumBlocksMachine } from '../../proofQueue/ethereumBlocks.impl.js';
import {
    createFakeEthereumProvider,
    createFakeSolanaRpc,
    createTestConnections,
    FAST_TIMINGS,
    nodesUntil,
    testSubscriber,
    waitForNode,
} from '../testUtils.js';

const URL = 'https://rate-limited.test';

/**
 * An endpoint that answers `perSecond` requests in any second and turns the
 * rest away with 429, as a public RPC does.
 *
 * @param perSecond How many requests it answers in any second.
 * @returns Its request, and how many it answered and turned away.
 */
function rateLimitedEndpoint(perSecond: number) {
    const answeredAt: number[] = [];
    const counts = { answered: 0, turnedAway: 0 };
    const request = (): { status: number } => {
        const now = Date.now();
        while (answeredAt.length > 0 && now - answeredAt[0] >= 1000) answeredAt.shift();
        if (answeredAt.length >= perSecond) {
            counts.turnedAway++;
            return { status: TOO_MANY_REQUESTS };
        }
        answeredAt.push(now);
        counts.answered++;
        return { status: 200 };
    };
    return { request, counts };
}

/** A client of the endpoint: one request, answered with its HTTP status. */
interface EndpointClient {
    get$(): Observable<{ status: number }>;
}

/**
 * A running HTTP connection to the endpoint, every request of which goes
 * through its grants, its health check included.
 *
 * @param endpoint The endpoint.
 * @param healthCheckTimeoutMs How long each check request may take once sent.
 * @returns The connection, a request on a machine's turn, `readFailed()`, and `close()`.
 */
function connectionTo(endpoint: ReturnType<typeof rateLimitedEndpoint>, healthCheckTimeoutMs = 5_000) {
    const close$ = new Subject<void>();
    const readFailed$ = new Subject<void>();
    const connection = httpConnection<undefined, EndpointClient>({
        ...FAST_TIMINGS,
        // A check waits its turn like any request; background checks stay out of the count.
        healthCheckTimeoutMs,
        healthCheckIntervalMs: 60_000,
        quietSecondsToLift: 2,
        urls: [URL],
        transport: {
            clientOn: (_url, send$) => ({ get$: () => send$(() => defer(() => of(endpoint.request()))) }),
            turnedAway: (result) =>
                'answered' in result && (result.answered as { status: number }).status === TOO_MANY_REQUESTS,
        },
        checkHealth: (client, url) =>
            client.get$().pipe(
                map(
                    (): Extract<HealthCheck<undefined>, { outcome: 'onExpectedNetwork' }> => ({
                        outcome: 'onExpectedNetwork',
                        url,
                        health: undefined,
                        checkedAt: Date.now(),
                    })
                )
            ),
        networkWentOffline$: NEVER,
        networkCameOnline$: NEVER,
        readFailed$,
        close$,
    });
    const get$ = (machine: RunningMachine) => connection.clientOf(URL, machine).get$();
    return { connection, get$, readFailed: () => readFailed$.next(), close: () => close$.next() };
}

describe('connection grants', () => {
    test('requests over the endpoint rate are sent again on their turn, so every one is answered; the connection is overwhelmed while limited, and ready once the limit lifts', async () => {
        const endpoint = rateLimitedEndpoint(20);
        const { connection, get$, close } = connectionTo(endpoint);
        await waitForNode(connection, 'ready');
        const machines = [testSubscriber(), testSubscriber(), testSubscriber()];
        const overwhelmed = waitForNode(connection, 'overwhelmed', 10_000);
        const firstLimitedSecond = firstValueFrom(connection.load$.pipe(filter(({ tooMany }) => tooMany > 0)));

        const answers = await firstValueFrom(
            forkJoin(machines.flatMap((machine) => Array.from({ length: 20 }, () => get$(machine))))
        );

        expect(answers.every(({ status }) => status === 200)).toBe(true);
        expect(endpoint.counts.turnedAway).toBeGreaterThan(0);
        expect((await overwhelmed).data.ratePerSecond).toBeGreaterThan(0);
        // Each machine is counted; the connection's own health checks are a subscriber too.
        const second = await firstLimitedSecond;
        expect(machines.every((machine) => second.perSubscriber.has(machine))).toBe(true);
        expect(second.ratePerSecond).not.toBe(Infinity);
        await waitForNode(connection, 'ready', 10_000);
        close();
    }, 20_000);

    test('a machine with many waiting requests does not hold back another: they take turns', async () => {
        const endpoint = rateLimitedEndpoint(10);
        const { connection, get$, close } = connectionTo(endpoint);
        await waitForNode(connection, 'ready');
        const busy = testSubscriber();
        const light = testSubscriber();
        const finished: string[] = [];

        const busyDone = lastValueFrom(
            forkJoin(Array.from({ length: 40 }, () => get$(busy))).pipe(map(() => finished.push('busy')))
        );
        await waitForNode(connection, 'overwhelmed', 10_000);
        const lightDone = lastValueFrom(forkJoin([get$(light), get$(light)]).pipe(map(() => finished.push('light'))));
        await Promise.all([busyDone, lightDone]);

        expect(finished).toEqual(['light', 'busy']);
        close();
    }, 20_000);

    test('a recheck while overwhelmed waits its turn and passes: the wait for a turn never counts against the endpoint', async () => {
        const endpoint = rateLimitedEndpoint(5);
        // Each check request may take 300 ms once sent; its wait behind three busy machines is longer.
        const { connection, get$, readFailed, close } = connectionTo(endpoint, 300);
        await waitForNode(connection, 'ready');
        const busy = [testSubscriber(), testSubscriber(), testSubscriber()];
        const pending = lastValueFrom(forkJoin(busy.flatMap((machine) => Array.from({ length: 15 }, () => get$(machine)))));
        await waitForNode(connection, 'overwhelmed', 10_000);

        const recheck = nodesUntil(connection, (state) => state.node !== 'connecting', 10_000);
        readFailed();
        expect(await recheck).toEqual(['connecting', 'ready']);
        await pending;
        close();
    }, 30_000);
});

describe('closing a rate-limited connection', () => {
    test('the requests still waiting their turn fail as never sent: none hangs', async () => {
        const endpoint = rateLimitedEndpoint(1);
        const { connection, get$, close } = connectionTo(endpoint);
        await waitForNode(connection, 'ready');
        const machine = testSubscriber();
        const pending = Promise.allSettled(Array.from({ length: 20 }, () => lastValueFrom(get$(machine))));
        await waitForNode(connection, 'overwhelmed', 10_000);

        close();
        const settled = await pending;

        expect(
            settled.some(
                (result) => result.status === 'rejected' && result.reason instanceof RequestNotSentError
            )
        ).toBe(true);
    }, 30_000);
});

describe('read machines over a rate-limited endpoint', () => {
    test('a read machine reaches current while its connection is overwhelmed: its requests are paced and sent again, never failed, and counted under it', async () => {
        const ethereum = createFakeEthereumProvider([], { latestBlock: 400, finalizedBlock: 350 });
        const solana = await createFakeSolanaRpc([]);
        const test = createTestConnections(ethereum.provider, solana.rpc);
        await waitForNode(test.connections.ethereum.http.connection, 'ready');
        test.world.ethereumPerSecond = 5;

        const overwhelmed = waitForNode(test.connections.ethereum.http.connection, 'overwhelmed', 15_000);
        const machines = Array.from({ length: 6 }, () => createEthereumBlocksMachine(test.connections, FAST_TIMINGS));
        const countedUnderMachines = firstValueFrom(
            test.connections.ethereum.http.load$.pipe(
                filter(({ perSubscriber }) => machines.some((machine) => perSubscriber.has(machine)))
            )
        );
        const nodes = await Promise.all(machines.map((machine) => nodesUntil(machine, 'current', 20_000)));

        await overwhelmed;
        await countedUnderMachines;
        expect(test.turnedAway.ethereum.turnedAway).toBeGreaterThan(0);
        // None waited on the connection or failed: an overwhelmed connection still serves.
        for (const path of nodes) expect(path.filter((node) => /waiting|failed/i.test(node))).toEqual([]);
        for (const machine of machines) machine.close();
        test.close();
    }, 40_000);
});
