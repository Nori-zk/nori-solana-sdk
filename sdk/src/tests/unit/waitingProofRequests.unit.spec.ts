import { MAX_BATCH_SIZE } from '../../rpc/nori/commitTimes.js';
import { sortWaitingProofRequests } from '../../proofRequest/waitingProofRequests.js';
import { type EnqueuedProofRequest } from '../../rpc/eth/fetchEnqueuedProofRequests.js';
import { TARGET_A, transactionHashOf } from '../testUtils.js';

/** The job Nori's `state.bridge` reported on Sepolia: one epoch of blocks after the last job's. */
const PROVING_JOB = {
    stage_name: 'BridgeHeadJobCreated',
    input_block_number: 11870714,
    output_block_number: 11870746,
};

const requestAt = (requestId: number, blockNumber: number): EnqueuedProofRequest => ({
    requestId: BigInt(requestId),
    blockNumber,
    target: TARGET_A,
    slotKey: '0x' + '00'.repeat(32),
    transactionHash: transactionHashOf(BigInt(requestId)),
    collectionKeys: [],
});

const ids = (requests: EnqueuedProofRequest[]) => requests.map(({ requestId }) => Number(requestId));

describe('sorting the requests no batch covers yet', () => {
    // 51 in the job, 52 after it but finalized, 53 not finalized.
    const waiting = [requestAt(53, 11870800), requestAt(51, 11870730), requestAt(52, 11870750)];

    test('with Nori proving a job: finality, scheduled and processing, each oldest first', () => {
        const sorted = sortWaitingProofRequests(waiting, 11870760, PROVING_JOB);
        expect(ids(sorted.waitingForFinality)).toEqual([53]);
        expect(ids(sorted.scheduled)).toEqual([52]);
        expect(ids(sorted.processing)).toEqual([51]);
    });

    test("a request in the job's input block was in the job before it", () => {
        const sorted = sortWaitingProofRequests([requestAt(50, 11870714)], 11870760, PROVING_JOB);
        expect(ids(sorted.processing)).toEqual([]);
        expect(ids(sorted.scheduled)).toEqual([50]);
    });

    test('without Nori, every finalized request is scheduled', () => {
        const sorted = sortWaitingProofRequests(waiting, 11870760);
        expect(ids(sorted.waitingForFinality)).toEqual([53]);
        expect(ids(sorted.scheduled)).toEqual([51, 52]);
        expect(sorted.processing).toEqual([]);
    });

    test('between jobs, nothing is processing', () => {
        const sorted = sortWaitingProofRequests(waiting, 11870760, {
            ...PROVING_JOB,
            stage_name: 'EthProcessorTransactionFinalizationSucceeded',
        });
        expect(ids(sorted.scheduled)).toEqual([51, 52]);
        expect(sorted.processing).toEqual([]);
    });

    test('a job takes at most MAX_BATCH_SIZE requests; the rest are scheduled for the jobs after it', () => {
        const many = Array.from({ length: MAX_BATCH_SIZE + 2 }, (_, i) => requestAt(i, 11870720));
        const sorted = sortWaitingProofRequests(many, 11870760, PROVING_JOB);
        expect(sorted.processing).toHaveLength(MAX_BATCH_SIZE);
        expect(ids(sorted.scheduled)).toEqual([MAX_BATCH_SIZE, MAX_BATCH_SIZE + 1]);
    });
});
