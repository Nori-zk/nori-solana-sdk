import { filter, firstValueFrom, timeout } from 'rxjs';
import { createProofRequestStateMachine } from '../../proofRequest/proofRequest.impl.js';
import { type ProofRequestStateNodeUnion } from '../../proofRequest/proofRequest.js';
import { ProofRequestState } from '../../proofRequest/types.js';
import {
    createContiguousBatches,
    createFakeEthereumProvider,
    createFakeSolanaRpc,
    createTestConnections,
    FAST_TIMINGS,
    QUEUE_ADDRESS,
    nodesUntil,
    statesDuring,
    waitForNode,
    TARGET_A,
    transactionHashOf,
    type FakeProofRequest,
} from '../testUtils.js';

const batchesUpTo = (queueCursor: number) =>
    createContiguousBatches(Array.from({ length: queueCursor / 4 }, () => 4));

/** 12 requests three blocks apart from block 100. */
const createRequests = (): FakeProofRequest[] =>
    Array.from({ length: 12 }, (_, id) => ({
        requestId: BigInt(id),
        blockNumber: 100 + id * 3,
        target: TARGET_A,
    }));

async function setUp(latestBlock: number, queueCursor: number) {
    const ethereum = createFakeEthereumProvider(createRequests(), {
        latestBlock,
    });
    const solana = await createFakeSolanaRpc(batchesUpTo(queueCursor));
    const test = createTestConnections(ethereum.provider, solana.rpc);
    await Promise.all([
        waitForNode(test.connections.ethereum.http.connection, 'ready'),
        waitForNode(test.connections.solana.http.connection, 'ready'),
    ]);
    return { ethereum, solana, ...test };
}

const follow = (requestId: bigint) => ({
    proofQueueAddress: QUEUE_ADDRESS,
    proofRequestTxHash: transactionHashOf(requestId),
});

type ProofRequestMachine = ReturnType<typeof createProofRequestStateMachine>;

type Current = Extract<ProofRequestStateNodeUnion, { node: 'current' }>;

/** Waits until `current` holds a snapshot in `state`, and gives its data. */
const waitForSnapshot = (machine: ProofRequestMachine, state: ProofRequestState): Promise<Current> =>
    firstValueFrom(
        machine.state$.pipe(
            filter(
                (current): current is Current =>
                    current.node === 'current' && current.data.snapshot.state === state
            ),
            timeout(30_000)
        )
    );

describe('proof request state machine', () => {
    test('waits for an unmined transaction, then follows the request until a batch covers it', async () => {
        // Request 10 is at block 130; the chain is at block 120.
        const { ethereum, solana, connections, close } = await setUp(120, 8);
        const proofRequestState =
            createProofRequestStateMachine(
                connections,
                follow(10n),
                undefined,
                50,
                undefined,
                FAST_TIMINGS
            );
        const snapshots = (await statesDuring(proofRequestState, 200)).flatMap((state) =>
            state.node === 'current' ? [state.data.snapshot.state] : []
        );
        expect(snapshots.length).toBeGreaterThan(0);
        expect(new Set(snapshots)).toEqual(new Set([ProofRequestState.Undetermined]));

        ethereum.state.latestBlock = 200;
        const unprocessed = await waitForSnapshot(proofRequestState, ProofRequestState.Unprocessed);
        expect(unprocessed.data.snapshot).toEqual(
            expect.objectContaining({
                state: ProofRequestState.Unprocessed,
                requestId: 10n,
            })
        );

        await solana.setBatches(batchesUpTo(12));
        const available = await waitForNode(proofRequestState, 'proofAvailable');
        expect(available.data.snapshot).toEqual(
            expect.objectContaining({
                requestId: 10n,
                proofQueueBatchIndex: 2n,
                indexInBatch: 2n,
            })
        );
        proofRequestState.close();
        close();
    });

    test('resumes from a known snapshot without looking the request up again', async () => {
        const { connections, solana, close } = await setUp(200, 4);
        const proofRequestState =
            createProofRequestStateMachine(
                connections,
                follow(6n),
                {
                    state: ProofRequestState.Unprocessed,
                    requestId: 6n,
                    requestBlockNumber: 118n,
                    queueCursor: 4n,
                    proofQueueBatchCount: 1n,
                },
                50,
                undefined,
                FAST_TIMINGS
            );
        expect((await firstValueFrom(proofRequestState.state$)).node).toBe('current');
        const moves = nodesUntil(proofRequestState, 'proofAvailable');
        await solana.setBatches(batchesUpTo(8));
        expect(await moves).not.toContain('loading');
        proofRequestState.close();
        close();
    });

    test('a known proven snapshot starts in proofAvailable, which is terminal', async () => {
        const { connections, close } = await setUp(200, 8);
        const known = {
            state: ProofRequestState.ProofAvailable,
            requestId: 6n,
            requestBlockNumber: 118n,
            queueCursor: 8n,
            proofQueueBatchIndex: 1n,
            proofQueueBatchAddress: 'Batch11111111111111111111111111111111111111',
            root: '0x' + '00'.repeat(32),
            inputQueueCursor: 4n,
            outputQueueCursor: 8n,
            outputBlockNumber: 1001n,
            previousOutputBlockNumber: 1000n,
            indexInBatch: 2n,
        } as const;
        const proofRequestState = createProofRequestStateMachine(connections, follow(6n), known, 50, undefined, FAST_TIMINGS);

        const states = await statesDuring(proofRequestState, 100);
        expect(new Set(states.map(({ node }) => node))).toEqual(new Set(['proofAvailable']));
        expect(states[0].data).toEqual({ proofRequestTxHash: follow(6n).proofRequestTxHash, snapshot: known });
        close();
    });

    test('going offline is waited for, and following resumes where it was', async () => {
        const { connections, network$, solana, close } = await setUp(200, 4);
        const proofRequestState =
            createProofRequestStateMachine(
                connections,
                follow(6n),
                undefined,
                50,
                undefined,
                FAST_TIMINGS
            );
        await waitForSnapshot(proofRequestState, ProofRequestState.Unprocessed);
        network$.next('offline');
        const waiting = await waitForNode(
            proofRequestState,
            'waitingForConnectionWhileRefreshing'
        );
        expect(waiting.data).toEqual(
            expect.objectContaining({
                snapshot: expect.objectContaining({ requestId: 6n }),
                waitingOn: ['ethereum', 'solana'],
            })
        );
        await solana.setBatches(batchesUpTo(8));
        network$.next('online');
        await waitForNode(proofRequestState, 'proofAvailable');
        proofRequestState.close();
        close();
    });

    test("an enqueuing transaction whose nonce its sender's mined nonce moved past ends transactionReplaced", async () => {
        const { ethereum, connections, close } = await setUp(200, 8);
        const enqueuing = transactionHashOf(99n);
        ethereum.state.pending.set(enqueuing, { from: TARGET_A, nonce: 3 });
        ethereum.state.minedNonces.set(TARGET_A, 3);
        const proofRequestState = createProofRequestStateMachine(
            connections,
            { ...follow(0n), proofRequestTxHash: enqueuing },
            undefined,
            50,
            undefined,
            FAST_TIMINGS
        );

        await waitForSnapshot(proofRequestState, ProofRequestState.Undetermined);
        ethereum.state.pending.delete(enqueuing);
        ethereum.state.minedNonces.set(TARGET_A, 4);
        const replaced = await waitForNode(proofRequestState, 'transactionReplaced');
        expect(replaced.data).toEqual({
            proofRequestTxHash: enqueuing,
            transaction: expect.objectContaining({ from: TARGET_A, nonce: 3, minedNonce: 4 }),
        });
        close();
    });

    test('an enqueuing transaction the node has not known for the blocks allowed ends transactionDropped', async () => {
        const { ethereum, connections, close } = await setUp(200, 8);
        const enqueuing = transactionHashOf(99n);
        const proofRequestState = createProofRequestStateMachine(
            connections,
            { ...follow(0n), proofRequestTxHash: enqueuing },
            undefined,
            50,
            undefined,
            FAST_TIMINGS,
            2
        );

        await waitForSnapshot(proofRequestState, ProofRequestState.Undetermined);
        ethereum.state.latestBlock += 2;
        const dropped = await waitForNode(proofRequestState, 'transactionDropped');
        expect(dropped.data).toEqual({
            proofRequestTxHash: enqueuing,
            transaction: expect.objectContaining({ unknownSinceBlock: 200, readAtBlock: 202 }),
        });
        close();
    });
});
