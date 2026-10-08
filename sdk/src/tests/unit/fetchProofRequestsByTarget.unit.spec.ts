import { EthRpcTransportError } from '../../rpc/eth/errors.js';
import {
    fetchProofRequestsByTarget,
    type ProofRequestHistoryCursor,
    type ProofRequestHistoryOrder,
} from '../../rpc/eth/fetchProofRequestsByTarget.js';
import {
    createFakeEthereumProvider,
    createRandom,
    QUEUE_ADDRESS,
    TARGET_A,
    TARGET_B,
    type FakeProofRequest,
} from '../testUtils.js';

/** 120 requests from two targets over blocks 1000..; several share a block. */
function createRequests(): FakeProofRequest[] {
    const random = createRandom(3);
    const requests: FakeProofRequest[] = [];
    let blockNumber = 1000;
    for (let id = 0; id < 120; id++) {
        if (id === 0 || random(3) === 0) blockNumber += 1 + random(60);
        requests.push({
            requestId: BigInt(id),
            blockNumber,
            target: random(2) ? TARGET_A : TARGET_B,
        });
    }
    return requests;
}

describe('fetchProofRequestsByTarget', () => {
    const requests = createRequests();
    const idsOf = (target: string) =>
        requests
            .filter((request) => request.target === target)
            .map((request) => request.requestId);

    async function readAllPages(
        order: ProofRequestHistoryOrder,
        pageSize: number,
        maxBlockRangePerQuery: number
    ) {
        const { provider } = createFakeEthereumProvider(requests, {
            latestBlock: 5000,
        });
        const ids: bigint[] = [];
        let after: ProofRequestHistoryCursor | undefined;
        for (let pages = 0; ; pages++) {
            if (pages > 500) throw new Error('pagination did not finish');
            const page = await fetchProofRequestsByTarget(
                provider,
QUEUE_ADDRESS,
                {
                    target: TARGET_A,
                    fromBlock: 900,
                    order,
                    pageSize,
                    after,
                    maxBlockRangePerQuery,
                }            );
            expect(page.requests.length).toBeLessThanOrEqual(pageSize);
            ids.push(...page.requests.map((request) => request.requestId));
            after = page.cursor;
            if (page.done) return ids;
        }
    }

    test.each([
        ['asc', 7, 500],
        ['desc', 7, 500],
        ['asc', 1, 37],
        ['desc', 1, 37],
        ['asc', 1000, 2000],
        ['desc', 3, 1],
    ] as const)(
        'pages %s with page size %i and block range %i return every request once, in order',
        async (order, pageSize, maxBlockRangePerQuery) => {
            const expected =
                order === 'asc'
                    ? idsOf(TARGET_A)
                    : [...idsOf(TARGET_A)].reverse();
            expect(
                await readAllPages(order, pageSize, maxBlockRangePerQuery)
            ).toEqual(expected);
        }
    );

    test('returns only the submitting address requests', async () => {
        const { provider } = createFakeEthereumProvider(requests, {
            latestBlock: 5000,
        });
        const page = await fetchProofRequestsByTarget(
            provider,
QUEUE_ADDRESS,
            { target: TARGET_B, fromBlock: 900, order: 'asc', pageSize: 1000 }        );
        expect(
            page.requests.every((request) => request.target === TARGET_B)
        ).toBe(true);
        expect(page.requests.map((request) => request.requestId)).toEqual(
            idsOf(TARGET_B)
        );
        expect(page.done).toBe(true);
    });

    test('keeps the cursor when an exhausted range returns nothing', async () => {
        const { provider } = createFakeEthereumProvider(requests, {
            latestBlock: 5000,
        });
        const after = { requestId: 999n, blockNumber: 5000 };
        const page = await fetchProofRequestsByTarget(
            provider,
QUEUE_ADDRESS,
            {
                target: TARGET_A,
                fromBlock: 900,
                order: 'asc',
                pageSize: 5,
                after,
            }        );
        expect(page).toEqual({ requests: [], cursor: after, done: true });
    });

    test('rejects a page size that is not a positive integer', async () => {
        const { provider } = createFakeEthereumProvider(requests, {
            latestBlock: 5000,
        });
        await expect(
            fetchProofRequestsByTarget(
                provider,
QUEUE_ADDRESS,
                { target: TARGET_A, fromBlock: 0, order: 'asc', pageSize: 0 }            )
        ).rejects.toBeInstanceOf(RangeError);
    });

    test('reports a log query that keeps failing as a transport error', async () => {
        const { provider, state } = createFakeEthereumProvider(requests, {
            latestBlock: 5000,
        });
        state.failNextReads = Infinity;
        await expect(
            fetchProofRequestsByTarget(
                provider,
QUEUE_ADDRESS,
                {
                    target: TARGET_A,
                    fromBlock: 900,
                    toBlock: 1000,
                    order: 'asc',
                    pageSize: 5,
                }            )
        ).rejects.toBeInstanceOf(EthRpcTransportError);
    });
});
