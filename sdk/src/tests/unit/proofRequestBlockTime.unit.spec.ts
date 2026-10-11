import { createProofRequestBlockTimeMachine } from '../../proofRequest/proofRequestBlockTime.impl.js';
import {
    createFakeEthereumProvider,
    createFakeSolanaRpc,
    createTestConnections,
    FAST_TIMINGS,
    waitForNode,
} from '../testUtils.js';

async function setUp(latestBlock: number) {
    const ethereum = createFakeEthereumProvider([], { latestBlock });
    const solana = await createFakeSolanaRpc([]);
    const test = createTestConnections(ethereum.provider, solana.rpc);
    await waitForNode(test.connections.ethereum.http.connection, 'ready');
    return { ethereum, ...test };
}

describe('proof request block time machine', () => {
    test('reads when the block was produced', async () => {
        const { connections, close } = await setUp(200);
        const blockTime = createProofRequestBlockTimeMachine(connections, 112, FAST_TIMINGS);
        const current = await waitForNode(blockTime, 'current');
        expect(current.data).toEqual(expect.objectContaining({ blockNumber: 112, timestampMs: 112 * 12 * 1000 }));
        blockTime.close();
        close();
    });

    test('a block the node does not have yet fails the read, which succeeds once the node has it', async () => {
        const { ethereum, connections, close } = await setUp(100);
        const blockTime = createProofRequestBlockTimeMachine(connections, 112, FAST_TIMINGS);
        await waitForNode(blockTime, 'failedWhileLoading');

        ethereum.state.latestBlock = 112;
        const current = await waitForNode(blockTime, 'current');
        expect(current.data).toEqual(expect.objectContaining({ blockNumber: 112, timestampMs: 112 * 12 * 1000 }));
        blockTime.close();
        close();
    }, 60_000);
});
