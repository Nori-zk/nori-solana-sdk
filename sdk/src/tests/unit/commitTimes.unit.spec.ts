import {
    ETHEREUM_EPOCH_SEC,
    FALLBACK_NORI_JOB_TIMINGS,
    getCommitTimes,
    getFinalityTimeRemainingSec,
    jobTimingsOf,
} from '../../rpc/nori/commitTimes.js';

/** Nori's timings as its websocket sent them on Sepolia, stale proof-conversion stages included. */
const RECORDED_TIMINGS = {
    BridgeHeadJobCreated: 80.926502298,
    BridgeHeadJobSucceeded: 1,
    ProofConversionJobReceived: 337.25,
    ProofConversionJobSucceeded: 1,
    EthProcessorProofRequest: 71.506,
    EthProcessorProofSucceeded: 1,
    EthProcessorTransactionSubmitting: 2.436,
    EthProcessorTransactionSubmitSucceeded: 0.2,
};

/** Ethereum's finality as Nori's `state.eth` sent it alongside those timings. */
const RECORDED_FINALITY = {
    latest_finality_block_number: 11870746,
    latest_finality_slot: 11311328,
};

describe('commit times', () => {
    test('counts only the stages of the job the Solana loop goes through', () => {
        expect(jobTimingsOf(RECORDED_TIMINGS)).toEqual({
            BridgeHeadJobCreated: 80.926502298,
            BridgeHeadJobSucceeded: 1,
            EthProcessorTransactionSubmitting: 2.436,
            EthProcessorTransactionSubmitSucceeded: 0.2,
        });
    });

    test('uses the rule of thumb for the stages Nori has not timed', () => {
        expect(jobTimingsOf(undefined)).toEqual(FALLBACK_NORI_JOB_TIMINGS);
        expect(jobTimingsOf({ BridgeHeadJobCreated: 60 })).toEqual({
            ...FALLBACK_NORI_JOB_TIMINGS,
            BridgeHeadJobCreated: 60,
        });
    });

    test('while a job is proving: its commit is the rest of the job, the next one a finality transition later', () => {
        const times = getCommitTimes(
            { stage_name: 'BridgeHeadJobCreated', elapsed_sec: 50 },
            RECORDED_TIMINGS
        );
        expect(times.jobSec).toBeCloseTo(80.926502298 + 1 + 2.436 + 0.2, 6);
        expect(times.currentJobSec).toBeCloseTo(80.926502298 - 50 + 1 + 2.436 + 0.2, 6);
        // The next job is created on the next finality transition, 384 s after this one.
        expect(times.nextJobSec).toBeCloseTo(ETHEREUM_EPOCH_SEC - 50 + times.jobSec, 3);
    });

    test('while a job is submitting: only the stages after it are left', () => {
        const times = getCommitTimes(
            { stage_name: 'EthProcessorTransactionSubmitting', elapsed_sec: 1 },
            RECORDED_TIMINGS
        );
        expect(times.currentJobSec).toBeCloseTo(2.436 - 1 + 0.2, 3);
    });

    test('once the job is committed: no job runs, and the next commits one transition after the last job started', () => {
        const times = getCommitTimes(
            { stage_name: 'EthProcessorTransactionFinalizationSucceeded', elapsed_sec: 30 },
            RECORDED_TIMINGS
        );
        expect(times.currentJobSec).toBeUndefined();
        expect(times.nextJobSec).toBeCloseTo(ETHEREUM_EPOCH_SEC - 30, 3);
    });

    test('a job taking longer than expected goes negative', () => {
        const times = getCommitTimes(
            { stage_name: 'BridgeHeadJobCreated', elapsed_sec: 500 },
            RECORDED_TIMINGS
        );
        expect(times.currentJobSec).toBeCloseTo(80.926502298 - 500 + 1 + 2.436 + 0.2, 6);
    });

    test('a job still running at the next finality transition: the next starts straight after it', () => {
        const slow = { ...RECORDED_TIMINGS, BridgeHeadJobCreated: 400 };
        const times = getCommitTimes({ stage_name: 'BridgeHeadJobCreated', elapsed_sec: 50 }, slow);
        // The transition is 334 s away, but this job needs 353.636 s more.
        expect(times.currentJobSec).toBeCloseTo(400 - 50 + 1 + 2.436 + 0.2, 6);
        expect(times.nextJobSec).toBeCloseTo((times.currentJobSec ?? 0) + times.jobSec, 6);
    });
});

describe('finality time remaining', () => {
    test('a finalized block has nothing left to wait', () => {
        expect(getFinalityTimeRemainingSec(11870746, RECORDED_FINALITY)).toBe(1);
    });

    test('a block in the next epoch waits for finality to reach its boundary, one slot every 12 s', () => {
        // Its slot is 11311342; the boundary is 11311360, 32 slots past finality.
        expect(getFinalityTimeRemainingSec(11870760, RECORDED_FINALITY)).toBe(32 * 12 + 1);
    });

    test('a block two epochs out waits for both', () => {
        expect(getFinalityTimeRemainingSec(11870746 + 40, RECORDED_FINALITY)).toBe(64 * 12 + 1);
    });
});
