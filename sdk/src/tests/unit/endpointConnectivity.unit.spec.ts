import { BehaviorSubject, defer, filter, of, Subject, throwError } from 'rxjs';
import { httpConnection } from '../../rpc/connection/httpConnection.impl.js';
import type { SolanaHealth } from '../../rpc/solana/solanaHttp.js';
import { EXPECTED_GENESIS_HASH, FAST_TIMINGS, statesDuring, waitForNode } from '../testUtils.js';

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
    const machine = httpConnection<SolanaHealth>({
        ...FAST_TIMINGS,
        urls: rpcUrls,
        checkHealth: (url) =>
            defer(() => {
                checked.push(url);
                const answer = answers.get(url);
                if (answer === 'down') return throwError(() => new Error(`${url} did not answer.`));
                if (answer === 'otherCluster')
                    return of({
                        outcome: 'onOtherNetwork' as const,
                        url,
                        found: 'devnet',
                        expected: EXPECTED_GENESIS_HASH,
                    });
                return of({
                    outcome: 'onExpectedNetwork' as const,
                    url,
                    health: { slot: 1n },
                    checkedAt: 0,
                });
            }),
        networkWentOffline$: network$.pipe(
            filter((status) => status === 'offline')
        ),
        networkCameOnline$: network$.pipe(
            filter((status) => status === 'online')
        ),
        readFailed$,
        close$,
    });
    return { machine, answers, checked, network$, readFailed$, close$ };
}

describe('HTTP connection machine, over Solana RPC endpoints', () => {
    test('moves on to the next endpoint when one is down', async () => {
        const { machine, checked, close$ } = startSolanaEndpoints(
            ['https://first.test', 'https://second.test'],
            { 'https://first.test': 'down' }
        );
        const ready = await waitForNode(machine, 'ready');
        expect(ready.data).toEqual(
            expect.objectContaining({ url: 'https://second.test' })
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
        const wrong = await waitForNode(machine, 'wrongNetwork');
        expect(wrong.data).toEqual({
            url: 'https://only.test',
            found: 'devnet',
            expected: EXPECTED_GENESIS_HASH,
            failedChecks: 1,
        });
        answers.set('https://only.test', 'answers');
        await waitForNode(machine, 'ready');
        close$.next();
    });

    test('background checks stay on the endpoint that passed', async () => {
        const { machine, checked, close$ } = startSolanaEndpoints([
            'https://first.test',
            'https://second.test',
        ]);
        await waitForNode(machine, 'ready');
        const states = await statesDuring(machine, 350);
        close$.next();
        expect(new Set(states.map(({ node }) => node))).toEqual(new Set(['ready']));
        expect(new Set(checked)).toEqual(new Set(['https://first.test']));
    });

    test('a failed read checks at once, and going offline and back recovers', async () => {
        const { machine, answers, readFailed$, network$, close$ } =
            startSolanaEndpoints(['https://only.test']);
        await waitForNode(machine, 'ready');
        answers.set('https://only.test', 'down');
        readFailed$.next();
        await waitForNode(machine, 'unreachable');
        network$.next('offline');
        await waitForNode(machine, 'offline');
        answers.set('https://only.test', 'answers');
        network$.next('online');
        await waitForNode(machine, 'ready');
        close$.next();
    });
});
