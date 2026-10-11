import { firstValueFrom } from 'rxjs';
import { proofQueueBatchesCovering$ } from '../../rpc/solana/proofQueueBatchCovering.js';
import { ProofQueueBatchSearchError } from '../../rpc/solana/errors.js';
import {
    createContiguousBatches,
    createFakeSolanaRpc,
    createRandom,
    type FakeProofQueueBatch,
} from '../testUtils.js';

describe('proofQueueBatchesCovering$', () => {
    const random = createRandom(7);
    const batches = createContiguousBatches(
        Array.from({ length: 4321 }, () => 1 + random(5))
    );
    const queueCursor = batches[batches.length - 1].outputQueueCursor;
    const batchCount = BigInt(batches.length);
    const coveringIndex = (requestId: bigint) =>
        BigInt(
            batches.findIndex(
                (batch: FakeProofQueueBatch) =>
                    batch.inputQueueCursor <= requestId &&
                    requestId < batch.outputQueueCursor
            )
        );
    const requestIds = [
        0n,
        queueCursor - 1n,
        ...Array.from({ length: 300 }, () =>
            BigInt(random(Number(queueCursor)))
        ),
    ];

    test('finds the covering batch of every request id', async () => {
        const { rpc } = await createFakeSolanaRpc(batches);
        const found = await firstValueFrom(proofQueueBatchesCovering$(rpc, requestIds, batchCount));
        for (const requestId of requestIds) {
            const result = found.get(requestId);
            expect(result?.proofQueueBatchIndex).toBe(coveringIndex(requestId));
            expect(
                result?.proofQueueBatch.inputQueueCursor
            ).toBeLessThanOrEqual(requestId);
            expect(result?.proofQueueBatch.outputQueueCursor).toBeGreaterThan(
                requestId
            );
        }
    });

    test('agrees with one search per id while reading far fewer times', async () => {
        const { rpc, state } = await createFakeSolanaRpc(batches);
        const found = await firstValueFrom(proofQueueBatchesCovering$(rpc, requestIds, batchCount));
        const sharedCalls = state.multipleAccountsCalls;

        state.multipleAccountsCalls = 0;
        for (const requestId of requestIds) {
            const single = await firstValueFrom(proofQueueBatchesCovering$(rpc, [requestId], batchCount));
            expect(found.get(requestId)?.proofQueueBatchIndex).toBe(
                single.get(requestId)?.proofQueueBatchIndex
            );
        }
        expect(sharedCalls).toBeLessThan(state.multipleAccountsCalls / 5);
    });

    test('resolves duplicate ids once', async () => {
        const { rpc } = await createFakeSolanaRpc(batches);
        const found = await firstValueFrom(proofQueueBatchesCovering$(rpc, [5n, 5n, 5n], batchCount));
        expect(found.size).toBe(1);
        expect(found.get(5n)?.proofQueueBatchIndex).toBe(coveringIndex(5n));
    });

    test('returns nothing for no ids', async () => {
        const { rpc, state } = await createFakeSolanaRpc(batches);
        expect((await firstValueFrom(proofQueueBatchesCovering$(rpc, [], batchCount))).size).toBe(0);
        expect(state.multipleAccountsCalls).toBe(0);
    });

    test('errors for an id no committed batch covers', async () => {
        const { rpc } = await createFakeSolanaRpc(batches);
        await expect(
            firstValueFrom(proofQueueBatchesCovering$(rpc, [queueCursor], batchCount))
        ).rejects.toBeInstanceOf(ProofQueueBatchSearchError);
    });
});
