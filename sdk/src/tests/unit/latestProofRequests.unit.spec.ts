import { type RunningMachine } from '@yaw-rx/ystate';
import { filter, firstValueFrom, map } from 'rxjs';
import { type ProofRequestHistoryEntry } from '../../proofRequest/fetchProofRequestHistory.js';
import { createLatestProofRequestsMachine } from '../../proofRequest/latestProofRequests.impl.js';
import {
    createContiguousBatches,
    createFakeEthereumProvider,
    createFakeSolanaRpc,
    createTestConnections,
    FAST_TIMINGS,
    QUEUE_ADDRESS,
    reach,
    TARGET_A,
    TARGET_B,
    type FakeProofRequest,
} from '../testUtils.js';

const addresses = { proofQueueAddress: QUEUE_ADDRESS };

/** 40 requests three blocks apart from block 100; target A enqueued the even ids. */
function createRequests(): FakeProofRequest[] {
    return Array.from({ length: 40 }, (_, id) => ({
        requestId: BigInt(id),
        blockNumber: 100 + id * 3,
        target: id % 2 === 0 ? TARGET_A : TARGET_B,
    }));
}

const batchesUpTo = (queueCursor: number) =>
    createContiguousBatches(Array.from({ length: queueCursor / 4 }, () => 4));

const describeView = (data: unknown) =>
    typeof data === 'object' && data !== null && 'view' in data
        ? (data.view as ProofRequestHistoryEntry[])
              .map(({ requestId, snapshot }) =>
                  snapshot.state === 'proofAvailable'
                      ? `${requestId}:batch${snapshot.proofQueueBatchIndex}`
                      : `${requestId}:unprocessed`
              )
              .join(',')
        : '';

async function setUp(requests: FakeProofRequest[], queueCursor: number) {
    const ethereum = createFakeEthereumProvider(requests, { latestBlock: 400 });
    const solana = await createFakeSolanaRpc(batchesUpTo(queueCursor));
    const test = createTestConnections(ethereum.provider, solana.rpc);
    await Promise.all([
        reach(test.connections.ethereum.http.connection, 'ready'),
        reach(test.connections.solana.http.connection, 'ready'),
    ]);
    return { ethereum, solana, ...test };
}

/**
 * Waits for the view the machine shows while watching to match `view`.
 *
 * @param machine The running live view machine.
 * @param view The view, described as `requestId:state` entries.
 * @returns Once the machine is watching with that view.
 */
const watchingWith = (machine: RunningMachine, view: string) =>
    firstValueFrom(
        machine.state$.pipe(
            filter(({ node }) => node === 'watching'),
            map(({ data }) => describeView(data)),
            filter((described) => described === view)
        )
    );

describe('latest proof requests machine', () => {
    test('follows new requests and their batches being committed', async () => {
        const requests = createRequests();
        const { solana, connections, close } = await setUp(requests, 32);
        const { latestProofRequests, close: closeView } =
            createLatestProofRequestsMachine(
                connections,
                addresses,
                { target: TARGET_A, fromBlock: 0, count: 4 },
                50,
                undefined,
                FAST_TIMINGS
            );
        await watchingWith(
            latestProofRequests,
            '38:unprocessed,36:unprocessed,34:unprocessed,32:unprocessed'
        );

        await solana.setBatches(batchesUpTo(40));
        await watchingWith(
            latestProofRequests,
            '38:batch9,36:batch9,34:batch8,32:batch8'
        );

        requests.push({ requestId: 40n, blockNumber: 395, target: TARGET_A });
        await watchingWith(
            latestProofRequests,
            '40:unprocessed,38:batch9,36:batch9,34:batch8'
        );
        closeView();
        close();
    });

    test('a request a reorg removed leaves the view, which refills from older blocks', async () => {
        const requests = createRequests();
        const { connections, close } = await setUp(requests, 40);
        const { latestProofRequests, close: closeView } =
            createLatestProofRequestsMachine(
                connections,
                addresses,
                { target: TARGET_A, fromBlock: 0, count: 3 },
                50,
                undefined,
                FAST_TIMINGS
            );
        await watchingWith(
            latestProofRequests,
            '38:batch9,36:batch9,34:batch8'
        );
        requests.splice(
            requests.findIndex((request) => request.requestId === 36n),
            1
        );
        await reach(latestProofRequests, 'loading');
        await watchingWith(
            latestProofRequests,
            '38:batch9,34:batch8,32:batch8'
        );
        closeView();
        close();
    });

    test('going offline keeps the view and resumes refreshing when back online', async () => {
        const { connections, network$, close } = await setUp(
            createRequests(),
            40
        );
        const { latestProofRequests, close: closeView } =
            createLatestProofRequestsMachine(
                connections,
                addresses,
                { target: TARGET_A, fromBlock: 0, count: 2 },
                50,
                undefined,
                FAST_TIMINGS
            );
        await watchingWith(latestProofRequests, '38:batch9,36:batch9');
        network$.next('offline');
        const waiting = await reach(
            latestProofRequests,
            'waitingForConnection'
        );
        expect(describeView(waiting.data)).toBe('38:batch9,36:batch9');
        expect(waiting.data).toEqual(
            expect.objectContaining({ waitingOn: ['ethereum', 'solana'] })
        );
        network$.next('online');
        await watchingWith(latestProofRequests, '38:batch9,36:batch9');
        closeView();
        close();
        await reach(latestProofRequests, 'closed');
    });
});
