import {
    fetchProofRequestCountsByTarget,
    fetchProofRequestHistoryPage,
    type ProofRequestHistoryEntry,
} from '../../proofRequest/fetchProofRequestHistory.js';
import { createProofRequestHistoryMachine } from '../../proofRequest/proofRequestHistory.impl.js';
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

/** The bridge has committed batches of four requests up to `queueCursor`. */
const batchesUpTo = (queueCursor: number) =>
    createContiguousBatches(Array.from({ length: queueCursor / 4 }, () => 4));

const describeEntry = ({ requestId, snapshot }: ProofRequestHistoryEntry) =>
    snapshot.state === 'proofAvailable'
        ? `${requestId}:batch${snapshot.proofQueueBatchIndex}`
        : `${requestId}:unprocessed`;

/** The entries a history state carries. */
const loadedOf = (data: unknown) =>
    typeof data === 'object' && data !== null && 'loaded' in data
        ? (data.loaded as ProofRequestHistoryEntry[])
        : [];

async function setUp(queueCursor = 16) {
    const ethereum = createFakeEthereumProvider(createRequests(), {
        latestBlock: 400,
    });
    const solana = await createFakeSolanaRpc(batchesUpTo(queueCursor));
    const test = createTestConnections(ethereum.provider, solana.rpc);
    await Promise.all([
        reach(test.connections.ethereum.http.connection, 'ready'),
        reach(test.connections.solana.http.connection, 'ready'),
    ]);
    return { ethereum, solana, ...test };
}

describe('proof request history reads', () => {
    test('a page classifies each request against the committed batches', async () => {
        const { ethereum, solana, close } = await setUp();
        const page = await fetchProofRequestHistoryPage(
            ethereum.provider,
            solana.rpc,
            addresses,
            { target: TARGET_A, fromBlock: 0, order: 'asc', pageSize: 10 }
        );
        expect(page.entries.map(describeEntry)).toEqual([
            '0:batch0',
            '2:batch0',
            '4:batch1',
            '6:batch1',
            '8:batch2',
            '10:batch2',
            '12:batch3',
            '14:batch3',
            '16:unprocessed',
            '18:unprocessed',
        ]);
        const first = page.entries[0].snapshot;
        const later = page.entries[4].snapshot;
        if (
            first.state !== 'proofAvailable' ||
            later.state !== 'proofAvailable'
        ) {
            throw new Error('expected proven requests');
        }
        expect(first.previousOutputBlockNumber).toBe(-1n);
        expect(later.previousOutputBlockNumber).toBe(1001n);
        expect(page.done).toBe(false);
        close();
    });

    test('counts proven and unprocessed requests per submitting address', async () => {
        const { ethereum, solana, close } = await setUp();
        expect(
            await fetchProofRequestCountsByTarget(ethereum.provider, solana.rpc, addresses, {
                target: TARGET_A,
                fromBlock: 0,
            })
        ).toEqual({ total: 20, proofAvailable: 8, unprocessed: 12 });
        close();
    });
});

describe('proof request history machine', () => {
    test('pages through on demand until the range is exhausted', async () => {
        const { connections, close } = await setUp();
        const { proofRequestHistory, loadMore } =
            createProofRequestHistoryMachine(
                connections,
                addresses,
                { target: TARGET_A, fromBlock: 0, order: 'desc', pageSize: 8 },
                FAST_TIMINGS
            );
        const first = await reach(proofRequestHistory, 'waitingForMore');
        expect(loadedOf(first.data).map((entry) => entry.requestId)).toEqual([
            38n,
            36n,
            34n,
            32n,
            30n,
            28n,
            26n,
            24n,
        ]);
        loadMore();
        await reach(proofRequestHistory, 'loadingPage');
        await reach(proofRequestHistory, 'waitingForMore');
        loadMore();
        const all = await reach(proofRequestHistory, 'allLoaded');
        expect(loadedOf(all.data).map((entry) => entry.requestId)).toEqual(
            Array.from({ length: 20 }, (_, i) => BigInt(38 - i * 2))
        );
        close();
    });

    test('going offline is waited for, with the connections named, and the page resumes after', async () => {
        const { connections, network$, close } = await setUp();
        const { proofRequestHistory, loadMore } =
            createProofRequestHistoryMachine(
                connections,
                addresses,
                { target: TARGET_A, fromBlock: 0, order: 'asc', pageSize: 5 },
                FAST_TIMINGS
            );
        await reach(proofRequestHistory, 'waitingForMore');
        network$.next('offline');
        await reach(
            connections.ethereum.http.connection,
            'offline'
        );
        loadMore();
        const waiting = await reach(
            proofRequestHistory,
            'waitingForConnection'
        );
        expect(waiting.data).toEqual(
            expect.objectContaining({ waitingOn: ['ethereum', 'solana'] })
        );
        network$.next('online');
        const second = await reach(proofRequestHistory, 'waitingForMore');
        expect(loadedOf(second.data).map((entry) => entry.requestId)).toEqual([
            0n,
            2n,
            4n,
            6n,
            8n,
            10n,
            12n,
            14n,
            16n,
            18n,
        ]);
        close();
    });

    test('a read that fails while its connection is healthy fails, then retries by itself', async () => {
        const { ethereum, connections, ethereumReadFailures, close } =
            await setUp();
        // More failures than the read's own retries: it gives up and reports.
        ethereum.state.failNextReads = 6;
        const { proofRequestHistory } = createProofRequestHistoryMachine(
            connections,
            addresses,
            { target: TARGET_A, fromBlock: 0, order: 'asc', pageSize: 5 },
            FAST_TIMINGS
        );
        const failed = await reach(proofRequestHistory, 'failed');
        expect(failed.data).toEqual(
            expect.objectContaining({ failedReads: 1 })
        );
        expect(ethereumReadFailures.count).toBe(1);
        const recovered = await reach(proofRequestHistory, 'waitingForMore');
        expect(loadedOf(recovered.data)).toHaveLength(5);
        close();
    }, 60_000);

    test('a read whose connection turns out to be down waits for it, then resumes', async () => {
        const { solana, connections, world, solanaReadFailures, close } =
            await setUp();
        world.solanaGoesDownOnReadFailure = true;
        solana.state.failNextReads = Infinity;
        const { proofRequestHistory } = createProofRequestHistoryMachine(
            connections,
            addresses,
            { target: TARGET_A, fromBlock: 0, order: 'asc', pageSize: 5 },
            FAST_TIMINGS
        );
        const waiting = await reach(
            proofRequestHistory,
            'waitingForConnection'
        );
        expect(waiting.data).toEqual(
            expect.objectContaining({ waitingOn: ['solana'] })
        );
        expect(solanaReadFailures.count).toBe(1);
        world.solanaAnswers = true;
        solana.state.failNextReads = 0;
        await reach(proofRequestHistory, 'waitingForMore');
        close();
    }, 60_000);
});
