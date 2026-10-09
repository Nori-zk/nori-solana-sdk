/** The most proof requests one job settles; the rest wait for the jobs after it. */
export const MAX_BATCH_SIZE = 2 ** 16;

/** Seconds between Ethereum finality transitions: one epoch of 32 slots of 12 seconds. */
export const ETHEREUM_EPOCH_SEC = 384;

/**
 * The stages of one job that Nori's timings measure, in the order the job
 * goes through them: proving it, then submitting it to Solana. The job is
 * committed on Solana when it leaves the last one
 * (`EthProcessorTransactionFinalizationSucceeded`).
 */
export const NORI_JOB_STAGES = [
    'BridgeHeadJobCreated',
    'BridgeHeadJobSucceeded',
    'EthProcessorTransactionSubmitting',
    'EthProcessorTransactionSubmitSucceeded',
] as const;

/** A stage of one job that Nori's timings measure. */
export type NoriJobStage = (typeof NORI_JOB_STAGES)[number];

/** Seconds each job stage takes, as Nori's timings report them. */
export type NoriJobTimings = Record<NoriJobStage, number>;

/**
 * Rule-of-thumb seconds for each job stage, used while Nori's timings are
 * unknown: a proof takes about two minutes, and submitting it to Solana and
 * its finalization take seconds.
 */
export const FALLBACK_NORI_JOB_TIMINGS: NoriJobTimings = {
    BridgeHeadJobCreated: 120,
    BridgeHeadJobSucceeded: 1,
    EthProcessorTransactionSubmitting: 13,
    EthProcessorTransactionSubmitSucceeded: 1,
};

/** Where Nori is in its loop: the stage it is in and the seconds it has spent there. */
export interface NoriStage {
    stage_name: string;
    elapsed_sec: number;
}

/** When the next commits on Solana are due, in seconds from now. */
export interface CommitTimes {
    /** Until the job Nori is running now is committed; `undefined` while no job is running. */
    currentJobSec: number | undefined;
    /** Until the job after that one, or the next job while none is running, is committed. */
    nextJobSec: number;
    /** How long one job takes, from its creation to its commit. */
    jobSec: number;
}

/**
 * The seconds each job stage takes: Nori's timings where it reports them,
 * the rule of thumb otherwise.
 *
 * @param timings Nori's timings, if known.
 * @returns The seconds for every job stage.
 */
export function jobTimingsOf(timings?: Partial<Record<string, number>>): NoriJobTimings {
    const known = { ...FALLBACK_NORI_JOB_TIMINGS };
    for (const stage of NORI_JOB_STAGES) {
        const seconds = timings?.[stage];
        if (typeof seconds === 'number' && Number.isFinite(seconds)) known[stage] = seconds;
    }
    return known;
}

/**
 * When the job Nori is running now, and the job after it, are committed on
 * Solana. A job is created on an Ethereum finality transition, one epoch
 * after the last, or straight after the job before it when that one overran.
 * Between its commit and the next job, Nori is idle. A negative time means
 * the step is taking longer than expected.
 *
 * @param stage Where Nori is in its loop.
 * @param timings Nori's timings, if known.
 * @returns The seconds until each commit, and how long one job takes.
 */
export function getCommitTimes(stage: NoriStage, timings?: Partial<Record<string, number>>): CommitTimes {
    const known = jobTimingsOf(timings);
    const jobSec = NORI_JOB_STAGES.reduce((total, name) => total + known[name], 0);
    const index = NORI_JOB_STAGES.indexOf(stage.stage_name as NoriJobStage);
    if (index === -1) {
        // Idle since the last commit, which ended a job created one job's time before it.
        const sinceCreated = jobSec + stage.elapsed_sec;
        const nextStartSec = ETHEREUM_EPOCH_SEC - sinceCreated;
        return { currentJobSec: undefined, nextJobSec: nextStartSec + jobSec, jobSec };
    }
    const earlier = NORI_JOB_STAGES.slice(0, index).reduce((total, name) => total + known[name], 0);
    const later = NORI_JOB_STAGES.slice(index + 1).reduce((total, name) => total + known[name], 0);
    const currentJobSec = known[NORI_JOB_STAGES[index]] - stage.elapsed_sec + later;
    const sinceCreated = earlier + stage.elapsed_sec;
    const nextStartSec = Math.max(currentJobSec, ETHEREUM_EPOCH_SEC - sinceCreated);
    return { currentJobSec, nextJobSec: nextStartSec + jobSec, jobSec };
}

/** Ethereum's finality, as Nori's `state.eth` reports it. */
export interface EthereumFinality {
    latest_finality_block_number: number;
    latest_finality_slot: number;
}

/**
 * Seconds until a block is finalized: until finality reaches the epoch
 * boundary at or after the block's slot, at one slot every 12 seconds. The
 * slot is the block number shifted by finality's own slot minus block
 * number, which accounts for missed slots.
 *
 * @param blockNumber The block.
 * @param finality Ethereum's finality.
 * @returns The seconds left, plus one.
 */
export function getFinalityTimeRemainingSec(blockNumber: number, finality: EthereumFinality): number {
    const delta = finality.latest_finality_slot - finality.latest_finality_block_number;
    const slot = blockNumber + delta;
    const rounded = Math.ceil(slot / 32) * 32;
    const blocksRemaining = rounded - delta - finality.latest_finality_block_number;
    return Math.max(0, blocksRemaining * 12) + 1;
}
