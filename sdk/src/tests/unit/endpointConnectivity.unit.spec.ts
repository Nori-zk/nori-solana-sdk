import { type RunningMachine } from '@yaw-rx/ystate';
import { BehaviorSubject, filter, ReplaySubject, Subject } from 'rxjs';
import {
    createEthereumProviderConnectivityMachine,
    type EthereumHealthCheck,
} from '../../proofRequest/rpc/eth/ystate/ethereumProviderConnectivity.impl.js';
import { createSolanaRpcConnectivityMachine } from '../../proofRequest/rpc/solana/ystate/solanaRpcConnectivity.impl.js';
import { stateOf$ } from '../../proofRequest/ystate/dataOnEntry.js';
import {
    EXPECTED_CHAIN_ID,
    EXPECTED_GENESIS_HASH,
    FAST_TIMINGS,
    reach,
    recordNodes,
    sleep,
} from '../testUtils.js';

/**
 * An Ethereum endpoint whose answers, wallet and network the test controls.
 * The first check runs as the machine starts, so its answer is given up front.
 */
function startEthereumEndpoint(
    startIn: 'checking' | 'waitingForWallet' = 'checking',
    initialAnswer: EthereumHealthCheck['outcome'] = 'onExpectedChain'
) {
    const world = { answer: initialAnswer, checks: 0 };
    const wallet$ = new BehaviorSubject<'onExpectedChain' | 'onOtherChain'>(
        'onExpectedChain'
    );
    const network$ = new BehaviorSubject<'online' | 'offline'>('online');
    const walletConnected$ = new Subject<void>();
    const walletDisconnected$ = new Subject<void>();
    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<RunningMachine>(1);
    const machine = createEthereumProviderConnectivityMachine({
        ...FAST_TIMINGS,
        expectedChainId: EXPECTED_CHAIN_ID,
        checkHealth: async () => {
            world.checks++;
            if (world.answer === 'failed')
                throw new Error('The node did not answer.');
            if (world.answer === 'onOtherChain')
                return { outcome: 'onOtherChain', chainId: 1n };
            return {
                outcome: 'onExpectedChain',
                chainId: EXPECTED_CHAIN_ID,
                blockNumber: world.checks,
                checkedAt: 0,
            };
        },
        walletOnExpectedChain$: wallet$.pipe(
            filter((node) => node === 'onExpectedChain')
        ),
        walletNotOnExpectedChain$: wallet$.pipe(
            filter((node) => node !== 'onExpectedChain')
        ),
        walletConnected$,
        walletDisconnected$,
        networkWentOffline$: network$.pipe(
            filter((status) => status === 'offline')
        ),
        networkCameOnline$: network$.pipe(
            filter((status) => status === 'online')
        ),
        readFailed$,
        close$,
        connection$: stateOf$(started$),
    })
        .close()
        .start(startIn);
    started$.next(machine);
    return {
        machine,
        world,
        wallet$,
        network$,
        walletConnected$,
        walletDisconnected$,
        readFailed$,
        close$,
    };
}

describe('Ethereum provider connectivity machine', () => {
    test('checks, becomes ready, and keeps checking in the background without leaving ready', async () => {
        const { machine, world, close$ } = startEthereumEndpoint();
        await reach(machine, 'ready');
        const visited = recordNodes(machine);
        await sleep(350);
        close$.next();
        expect(world.checks).toBeGreaterThan(2);
        expect(
            visited.filter((node) => node !== 'ready' && node !== 'closed')
        ).toEqual([]);
    });

    test('an unreachable provider is retried with backoff until it answers', async () => {
        const { machine, world, close$ } = startEthereumEndpoint(
            'checking',
            'failed'
        );
        const unreachable = await reach(machine, 'unreachable');
        expect(unreachable.data).toEqual({
            failedChecks: 1,
            error: 'The node did not answer.',
        });
        await sleep(60);
        expect(world.checks).toBeGreaterThan(1);
        world.answer = 'onExpectedChain';
        await reach(machine, 'ready');
        close$.next();
    });

    test('a provider on the wrong network says which, and keeps checking until it is fixed', async () => {
        const { machine, world, close$ } = startEthereumEndpoint(
            'checking',
            'onOtherChain'
        );
        const wrong = await reach(machine, 'wrongNetwork');
        expect(wrong.data).toEqual({
            chainId: 1n,
            expectedChainId: EXPECTED_CHAIN_ID,
            failedChecks: 1,
        });
        world.answer = 'onExpectedChain';
        await reach(machine, 'ready');
        close$.next();
    });

    test('a failed read checks the provider at once', async () => {
        const { machine, world, readFailed$, close$ } = startEthereumEndpoint();
        await reach(machine, 'ready');
        const checksBefore = world.checks;
        world.answer = 'failed';
        readFailed$.next();
        await reach(machine, 'unreachable');
        expect(world.checks).toBe(checksBefore + 1);
        close$.next();
    });

    test('waits for the wallet, and leaves ready when the wallet leaves the expected chain', async () => {
        const { machine, wallet$, close$ } =
            startEthereumEndpoint('waitingForWallet');
        await reach(machine, 'ready');
        wallet$.next('onOtherChain');
        await reach(machine, 'waitingForWallet');
        wallet$.next('onExpectedChain');
        await reach(machine, 'ready');
        close$.next();
    });

    test('a wallet disconnect is unreachable, and its reconnect checks at once', async () => {
        const {
            machine,
            walletDisconnected$,
            walletConnected$,
            world,
            close$,
        } = startEthereumEndpoint();
        await reach(machine, 'ready');
        world.answer = 'failed';
        walletDisconnected$.next();
        const unreachable = await reach(machine, 'unreachable');
        expect(unreachable.data).toEqual({
            failedChecks: 1,
            error: 'The wallet cannot reach any chain.',
        });
        world.answer = 'onExpectedChain';
        walletConnected$.next();
        await reach(machine, 'ready');
        close$.next();
    });

    test('going offline pauses it, and coming back online checks at once', async () => {
        const { machine, network$, world, close$ } = startEthereumEndpoint();
        await reach(machine, 'ready');
        network$.next('offline');
        await reach(machine, 'offline');
        const checksWhileOffline = world.checks;
        await sleep(250);
        expect(world.checks).toBe(checksWhileOffline);
        network$.next('online');
        await reach(machine, 'ready');
        close$.next();
        await reach(machine, 'closed');
    });
});

type SolanaAnswer = 'answers' | 'down' | 'otherCluster';

/**
 * A Solana endpoint set whose answers per endpoint and network the test
 * controls. The first check runs as the machine starts, so answers that
 * matter to it are given up front.
 */
function startSolanaEndpoints(
    rpcUrls: string[],
    initialAnswers: Record<string, SolanaAnswer> = {}
) {
    const answers = new Map<string, SolanaAnswer>(
        rpcUrls.map((url) => [url, initialAnswers[url] ?? 'answers'])
    );
    const checked: string[] = [];
    const network$ = new BehaviorSubject<'online' | 'offline'>('online');
    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<RunningMachine>(1);
    const machine = createSolanaRpcConnectivityMachine({
        ...FAST_TIMINGS,
        expectedGenesisHash: EXPECTED_GENESIS_HASH,
        rpcUrls,
        checkHealth: async (rpcUrl) => {
            checked.push(rpcUrl);
            const answer = answers.get(rpcUrl);
            if (answer === 'down') throw new Error(`${rpcUrl} did not answer.`);
            if (answer === 'otherCluster')
                return {
                    outcome: 'onOtherCluster',
                    rpcUrl,
                    genesisHash: 'devnet',
                };
            return {
                outcome: 'onExpectedCluster',
                rpcUrl,
                slot: 1n,
                checkedAt: 0,
            };
        },
        networkWentOffline$: network$.pipe(
            filter((status) => status === 'offline')
        ),
        networkCameOnline$: network$.pipe(
            filter((status) => status === 'online')
        ),
        readFailed$,
        close$,
        connection$: stateOf$(started$),
    })
        .close()
        .start('checking');
    started$.next(machine);
    return { machine, answers, checked, network$, readFailed$, close$ };
}

describe('Solana RPC connectivity machine', () => {
    test('moves on to the next endpoint when one is down', async () => {
        const { machine, checked, close$ } = startSolanaEndpoints(
            ['https://first.test', 'https://second.test'],
            { 'https://first.test': 'down' }
        );
        const ready = await reach(machine, 'ready');
        expect(ready.data).toEqual(
            expect.objectContaining({ rpcUrl: 'https://second.test' })
        );
        expect(checked.slice(0, 2)).toEqual([
            'https://first.test',
            'https://second.test',
        ]);
        close$.next();
    });

    test('an endpoint on another cluster says which, and keeps checking until one is right', async () => {
        const { machine, answers, close$ } = startSolanaEndpoints(
            ['https://only.test'],
            {
                'https://only.test': 'otherCluster',
            }
        );
        const wrong = await reach(machine, 'wrongNetwork');
        expect(wrong.data).toEqual({
            rpcUrl: 'https://only.test',
            genesisHash: 'devnet',
            expectedGenesisHash: EXPECTED_GENESIS_HASH,
            failedChecks: 1,
        });
        answers.set('https://only.test', 'answers');
        await reach(machine, 'ready');
        close$.next();
    });

    test('background checks stay on the endpoint that passed', async () => {
        const { machine, checked, close$ } = startSolanaEndpoints([
            'https://first.test',
            'https://second.test',
        ]);
        await reach(machine, 'ready');
        await sleep(350);
        close$.next();
        expect(new Set(checked)).toEqual(new Set(['https://first.test']));
    });

    test('a failed read checks at once, and going offline and back recovers', async () => {
        const { machine, answers, readFailed$, network$, close$ } =
            startSolanaEndpoints(['https://only.test']);
        await reach(machine, 'ready');
        answers.set('https://only.test', 'down');
        readFailed$.next();
        await reach(machine, 'unreachable');
        network$.next('offline');
        await reach(machine, 'offline');
        answers.set('https://only.test', 'answers');
        network$.next('online');
        await reach(machine, 'ready');
        close$.next();
    });
});
