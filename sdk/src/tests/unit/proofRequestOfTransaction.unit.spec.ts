import { createProofRequestOfTransactionMachine } from '../../proofRequest/proofRequestOfTransaction.impl.js';
import {
    createFakeEthereumProvider,
    createFakeSolanaRpc,
    createTestConnections,
    FAST_TIMINGS,
    QUEUE_ADDRESS,
    statesDuring,
    TARGET_A,
    transactionHashOf,
    waitForNode,
    waitForState,
    type FakeProofRequest,
} from '../testUtils.js';

/** 12 requests three blocks apart from block 100. */
const createRequests = (): FakeProofRequest[] =>
    Array.from({ length: 12 }, (_, id) => ({
        requestId: BigInt(id),
        blockNumber: 100 + id * 3,
        target: TARGET_A,
    }));

async function setUp(latestBlock: number) {
    const ethereum = createFakeEthereumProvider(createRequests(), { latestBlock });
    const solana = await createFakeSolanaRpc([]);
    const test = createTestConnections(ethereum.provider, solana.rpc);
    await waitForNode(test.connections.ethereum.http.connection, 'ready');
    return { ethereum, ...test };
}

describe('proof request of transaction machine', () => {
    test('reads the request a mined transaction enqueued', async () => {
        const { connections, close } = await setUp(200);
        const ofTransaction = createProofRequestOfTransactionMachine(
            connections,
            QUEUE_ADDRESS,
            transactionHashOf(4n),
            FAST_TIMINGS
        );
        const current = await waitForNode(ofTransaction, 'current');
        expect(current.data.proofRequest).toEqual(
            expect.objectContaining({ requestId: 4n, target: TARGET_A, blockNumber: 112 })
        );
        ofTransaction.close();
        close();
    });

    test('holds no request while the transaction is not mined, and reads it on the block that mines it', async () => {
        // Request 10 is at block 130; the chain is at block 120.
        const { ethereum, connections, close } = await setUp(120);
        const ofTransaction = createProofRequestOfTransactionMachine(
            connections,
            QUEUE_ADDRESS,
            transactionHashOf(10n),
            FAST_TIMINGS
        );
        const before = (await statesDuring(ofTransaction, 200)).flatMap((state) =>
            state.node === 'current' ? [state.data.proofRequest] : []
        );
        expect(before.length).toBeGreaterThan(0);
        expect(before.every((proofRequest) => proofRequest === undefined)).toBe(true);

        ethereum.state.latestBlock = 130;
        const mined = await waitForState(
            ofTransaction,
            (state) => state.node === 'current' && state.data.proofRequest !== undefined,
            10_000
        );
        expect(mined.node === 'current' && mined.data.proofRequest).toEqual(
            expect.objectContaining({ requestId: 10n, blockNumber: 130 })
        );
        ofTransaction.close();
        close();
    });
});
