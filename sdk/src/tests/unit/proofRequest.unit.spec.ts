import { createProofRequestStateMachine } from '../../proofRequest/proofRequest.impl.js';
import { ProofRequestState } from '../../proofRequest/types.js';
import {
    createContiguousBatches,
    createFakeEthereumProvider,
    createFakeSolanaRpc,
    createTestConnections,
    FAST_TIMINGS,
    QUEUE_ADDRESS,
    reach,
    recordNodes,
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
        reach(test.connections.ethereum.http.connection, 'ready'),
        reach(test.connections.solana.http.connection, 'ready'),
    ]);
    return { ethereum, solana, ...test };
}

const follow = (requestId: bigint) => ({
    proofQueueAddress: QUEUE_ADDRESS,
    proofRequestTxHash: transactionHashOf(requestId),
});

describe('proof request state machine', () => {
    test('waits for an unmined transaction, then follows the request until a batch covers it', async () => {
        // Request 10 is at block 130; the chain is at block 120.
        const { ethereum, solana, connections, close } = await setUp(120, 8);
        const { proofRequestState, close: closeRequest } =
            createProofRequestStateMachine(
                connections,
                follow(10n),
                undefined,
                50,
                undefined,
                FAST_TIMINGS
            );
        const visited = recordNodes(proofRequestState);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(visited).toEqual(['undetermined']);

        ethereum.state.latestBlock = 200;
        const unprocessed = await reach(proofRequestState, 'unprocessed');
        expect(unprocessed.data).toEqual(
            expect.objectContaining({
                state: ProofRequestState.Unprocessed,
                requestId: 10n,
                failedReads: 0,
            })
        );

        await solana.setBatches(batchesUpTo(12));
        const available = await reach(proofRequestState, 'proofAvailable');
        expect(available.data).toEqual(
            expect.objectContaining({
                requestId: 10n,
                proofQueueBatchIndex: 2n,
                indexInBatch: 2n,
            })
        );
        closeRequest();
        close();
    });

    test('resumes from a known snapshot without looking the request up again', async () => {
        const { connections, solana, close } = await setUp(200, 4);
        const { proofRequestState, close: closeRequest } =
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
        const visited = recordNodes(proofRequestState);
        await solana.setBatches(batchesUpTo(8));
        await reach(proofRequestState, 'proofAvailable');
        expect(visited[0]).toBe('unprocessed');
        expect(visited).not.toContain('undetermined');
        closeRequest();
        close();
    });

    test('going offline is waited for, and following resumes where it was', async () => {
        const { connections, network$, solana, close } = await setUp(200, 4);
        const { proofRequestState, close: closeRequest } =
            createProofRequestStateMachine(
                connections,
                follow(6n),
                undefined,
                50,
                undefined,
                FAST_TIMINGS
            );
        await reach(proofRequestState, 'unprocessed');
        network$.next('offline');
        const waiting = await reach(
            proofRequestState,
            'waitingForConnectionWhileUnprocessed'
        );
        expect(waiting.data).toEqual(
            expect.objectContaining({
                requestId: 6n,
                waitingOn: ['ethereum', 'solana'],
            })
        );
        await solana.setBatches(batchesUpTo(8));
        network$.next('online');
        await reach(proofRequestState, 'proofAvailable');
        closeRequest();
        close();
    });
});
