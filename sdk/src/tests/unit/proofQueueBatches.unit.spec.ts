import { filter, firstValueFrom, map, throwError, timeout } from 'rxjs';
import { createBridgeStateMachine } from '../../proofQueue/bridgeState.impl.js';
import { createCommittedProofQueueBatchesMachine } from '../../proofQueue/committedProofQueueBatches.impl.js';
import { type CommittedProofQueueBatchesState } from '../../proofQueue/committedProofQueueBatches.js';
import { createEthereumBlocksMachine } from '../../proofQueue/ethereumBlocks.impl.js';
import { createProofQueueBatchesMachine } from '../../proofQueue/proofQueueBatches.impl.js';
import { createProofQueueBatchRequestsMachine } from '../../proofQueue/proofQueueBatchRequests.impl.js';
import { type ProofQueueBatchesView } from '../../proofQueue/proofQueueBatches.js';
import {
    createContiguousBatches,
    createFakeEthereumProvider,
    createFakeSolanaRpc,
    createTestConnections,
    FAST_TIMINGS,
    QUEUE_ADDRESS,
    waitForNode,
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

/** Batches of four requests up to `queueCursor`. */
const batchesUpTo = (queueCursor: number) =>
    createContiguousBatches(Array.from({ length: queueCursor / 4 }, () => 4));

/** Batch `k` holds requests `4k`–`4k+3`; its proof read the queue at the last one's block. */
const outputBlockOf = (index: number) => 109 + 12 * index;

/**
 * The fake chains, their connections and the machines the batches view
 * reads from; `close()` closes them all.
 *
 * @param queueCursor How many requests the committed batches cover.
 */
async function setUp(queueCursor: number) {
    const ethereum = createFakeEthereumProvider(createRequests(), { latestBlock: 400, finalizedBlock: 350 });
    const solana = await createFakeSolanaRpc(batchesUpTo(queueCursor), { outputBlockOf });
    const test = createTestConnections(ethereum.provider, solana.rpc);
    await Promise.all([
        waitForNode(test.connections.ethereum.http.connection, 'ready'),
        waitForNode(test.connections.solana.http.connection, 'ready'),
    ]);
    const startSources = () => ({
        bridgeState: createBridgeStateMachine(test.connections, undefined, FAST_TIMINGS),
        ethereumBlocks: createEthereumBlocksMachine(test.connections, FAST_TIMINGS),
    });
    return { ethereum, solana, ...test, startSources };
}

/**
 * Closes the view, its sources and the connections.
 *
 * @param machines The running machines, each with `close()`.
 * @param closeConnections Closes the connections.
 */
const closeAll = (machines: { close(): void }[], closeConnections: () => void) => {
    for (const machine of machines) machine.close();
    closeConnections();
};

const ids = (requests: { requestId: bigint }[]) => requests.map(({ requestId }) => requestId.toString()).join(',');

/** A view as `batches:<index[matching]>…|waiting:<ids>|finalized:<block>`. */
const describeView = ({ batches, waiting, finalizedBlock }: ProofQueueBatchesView) =>
    [
        'batches:' +
            batches
                .map(
                    ({ proofQueueBatchIndex, matching }) =>
                        proofQueueBatchIndex.toString() +
                        (matching ? `[${matching.map(String).join(',')}]` : '')
                )
                .join(','),
        'waiting:' + ids(waiting),
        `finalized:${finalizedBlock}`,
    ].join('|');

/**
 * Waits for the machine to hold `view` in `current`.
 *
 * @param machine The running batches machine.
 * @param view The view, as `describeView` writes it.
 * @returns Once the machine is `current` with that view.
 */
const currentWith = (machine: ReturnType<typeof createProofQueueBatchesMachine>, view: string, timeoutMs = 10_000) =>
    firstValueFrom(
        machine.state$.pipe(
            filter((state) => state.node === 'current'),
            map(({ data }) => describeView((data as { view: ProofQueueBatchesView }).view)),
            filter((described) => described === view),
            timeout({
                first: timeoutMs,
                with: () => throwError(() => new Error(`Was not current with ${view} within ${timeoutMs}ms.`)),
            })
        )
    );

describe('proof queue batches machine', () => {
    test('shows the newest batches and the waiting requests, and follows a commit on Solana', async () => {
        const { solana, connections, close, startSources } = await setUp(32);
        const sources = startSources();
        const proofQueueBatches = createProofQueueBatchesMachine(
            connections,
            addresses,
            { fromBlock: 0, count: 3 },
            sources,
            undefined,
            FAST_TIMINGS
        );
        await currentWith(proofQueueBatches, 'batches:7,6,5|waiting:32,33,34,35,36,37,38,39|finalized:350');

        // The commit's program transaction moves the bridge state machine, which refreshes the view.
        await solana.setBatches(batchesUpTo(40));
        await currentWith(proofQueueBatches, 'batches:9,8,7|waiting:|finalized:350');
        closeAll([proofQueueBatches, sources.bridgeState, sources.ethereumBlocks], close);
    });

    test('with a target, shows only the batches holding its requests', async () => {
        const { connections, close, startSources } = await setUp(36);
        const sources = startSources();
        const proofQueueBatches = createProofQueueBatchesMachine(
            connections,
            addresses,
            { target: TARGET_A, fromBlock: 0, count: 2 },
            sources,
            undefined,
            FAST_TIMINGS
        );
        await currentWith(proofQueueBatches, 'batches:8[34,32],7[30,28]|waiting:36,38|finalized:350');
        closeAll([proofQueueBatches, sources.bridgeState, sources.ethereumBlocks], close);
    });

    test('waits for Solana before its first view, then reads it', async () => {
        const { world, connections, close, startSources } = await setUp(32);
        world.solanaAnswers = false;
        await waitForNode(connections.solana.http.connection, 'waitingToRetry');
        const sources = startSources();
        const proofQueueBatches = createProofQueueBatchesMachine(
            connections,
            addresses,
            { fromBlock: 0, count: 1 },
            sources,
            undefined,
            FAST_TIMINGS
        );
        const waiting = await waitForNode(proofQueueBatches, 'waitingForConnectionWhileLoading');
        expect(waiting.data.waitingOn).toEqual(['solana']);

        world.solanaAnswers = true;
        await currentWith(proofQueueBatches, 'batches:7|waiting:32,33,34,35,36,37,38,39|finalized:350');
        closeAll([proofQueueBatches, sources.bridgeState, sources.ethereumBlocks], close);
    });

    test('fails while the bridge state fails, then reads once it recovers', async () => {
        const { solana, connections, close, startSources } = await setUp(32);
        solana.state.failNextAccountReads = 1;
        const sources = startSources();
        const proofQueueBatches = createProofQueueBatchesMachine(
            connections,
            addresses,
            { fromBlock: 0, count: 1 },
            sources,
            undefined,
            FAST_TIMINGS
        );
        await waitForNode(proofQueueBatches, 'failedWhileLoading');
        await currentWith(proofQueueBatches, 'batches:7|waiting:32,33,34,35,36,37,38,39|finalized:350');
        closeAll([proofQueueBatches, sources.bridgeState, sources.ethereumBlocks], close);
    });
});

describe('committed proof queue batches machine', () => {
    /** The indices a `current` state holds, joined. */
    const committedOf = (state: CommittedProofQueueBatchesState) =>
        state.node === 'current' ? state.data.committed.map(({ proofQueueBatchIndex }) => proofQueueBatchIndex).join(',') : undefined;

    test('reads the batches from the index given, then each batch the bridge commits after them, once', async () => {
        const { solana, connections, close } = await setUp(16);
        const committed = createCommittedProofQueueBatchesMachine(connections, 2n, undefined, FAST_TIMINGS);
        const first = await waitForNode(committed, 'current');
        expect(committedOf(first)).toBe('2,3');
        expect(first.data.committed[0]).toEqual(
            expect.objectContaining({
                inputQueueCursor: 8n,
                outputQueueCursor: 12n,
                outputBlockNumber: BigInt(outputBlockOf(2)),
                previousOutputBlockNumber: BigInt(outputBlockOf(1)),
            })
        );

        await solana.setBatches(batchesUpTo(24));
        await firstValueFrom(committed.state$.pipe(filter((state) => committedOf(state) === '4,5'), timeout(10_000)));
        committed.close();
        close();
    });
});

describe('proof queue batch requests machine', () => {
    /** Batch 2: requests 8–11, enqueued after batch 1's output block. */
    const batch = {
        proofQueueBatchIndex: 2n,
        proofQueueBatchAddress: 'Batch11111111111111111111111111111111111111',
        root: '0x' + '02'.repeat(32),
        inputQueueCursor: 8n,
        outputQueueCursor: 12n,
        outputBlockNumber: BigInt(outputBlockOf(2)),
        previousOutputBlockNumber: BigInt(outputBlockOf(1)),
    };
    const loadedIds = async (machine: ReturnType<typeof createProofQueueBatchRequestsMachine>) =>
        ids((await waitForNode(machine, 'current')).data.requests);

    test("reads the batch's requests, or only a target's", async () => {
        const { connections, close } = await setUp(40);
        const all = createProofQueueBatchRequestsMachine(connections, QUEUE_ADDRESS, batch, { fromBlock: 0 });
        expect(await loadedIds(all)).toBe('8,9,10,11');
        const targets = createProofQueueBatchRequestsMachine(connections, QUEUE_ADDRESS, batch, {
            target: TARGET_A,
            fromBlock: 0,
        });
        expect(await loadedIds(targets)).toBe('8,10');
        all.close();
        targets.close();
        close();
    });

    test('reads through Ethereum alone, waiting for it and not for Solana', async () => {
        const { world, connections, close } = await setUp(40);
        world.solanaAnswers = false;
        world.ethereumAnswers = false;
        await Promise.all([
            waitForNode(connections.solana.http.connection, 'waitingToRetry'),
            waitForNode(connections.ethereum.http.connection, 'waitingToRetry'),
        ]);
        const proofQueueBatchRequests = createProofQueueBatchRequestsMachine(
            connections,
            QUEUE_ADDRESS,
            batch,
            { fromBlock: 0 },
            FAST_TIMINGS
        );
        const waiting = await waitForNode(proofQueueBatchRequests, 'waitingForConnectionWhileLoading');
        expect((waiting.data as { waitingOn: string[] }).waitingOn).toEqual(['ethereum']);

        world.ethereumAnswers = true;
        expect(await loadedIds(proofQueueBatchRequests)).toBe('8,9,10,11');
        proofQueueBatchRequests.close();
        close();
    });
});
