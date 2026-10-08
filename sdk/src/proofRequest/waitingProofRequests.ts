import { type EnqueuedProofRequest } from '../rpc/eth/fetchEnqueuedProofRequests.js';
import { MAX_BATCH_SIZE, NORI_JOB_STAGES, type NoriJobStage } from '../rpc/nori/commitTimes.js';

/** The job Nori is in, as its `state.bridge` reports it: the stage and the Ethereum blocks the job proves. */
export interface NoriJob {
    stage_name: string;
    /** The previous job's output block; this job proves the blocks after it. */
    input_block_number: number;
    /** The last block this job proves. */
    output_block_number: number;
}

/** The proof requests no batch covers yet, by where each one waits, each oldest first. */
export interface WaitingProofRequests {
    /** Their block is not finalized yet. */
    waitingForFinality: EnqueuedProofRequest[];
    /**
     * Finalized, and not in the job Nori is running now; every finalized
     * request while that job is not known.
     */
    scheduled: EnqueuedProofRequest[];
    /** In the job Nori is running now, at most `MAX_BATCH_SIZE`; empty while that job is not known. */
    processing: EnqueuedProofRequest[];
}

/**
 * Sorts the proof requests no batch covers yet by where each one waits. A
 * request whose block is above Ethereum's finalized block waits for
 * finality. A finalized one is in the job Nori is running now when its block
 * is in that job's blocks and it is among the job's first `MAX_BATCH_SIZE`
 * requests; otherwise it is scheduled for a later job. Without Nori's job
 * (its websocket is down, or it is between jobs), every finalized request is
 * scheduled.
 *
 * @param waiting The requests no batch covers yet.
 * @param finalizedBlock Ethereum's finalized block.
 * @param job The job Nori is in, if known.
 * @returns The requests by where each one waits, each oldest first.
 */
export function sortWaitingProofRequests(
    waiting: readonly EnqueuedProofRequest[],
    finalizedBlock: number,
    job?: NoriJob
): WaitingProofRequests {
    const running = job !== undefined && NORI_JOB_STAGES.includes(job.stage_name as NoriJobStage);
    const sorted: WaitingProofRequests = { waitingForFinality: [], scheduled: [], processing: [] };
    const oldestFirst = [...waiting].sort((a, b) =>
        a.requestId < b.requestId ? -1 : a.requestId > b.requestId ? 1 : 0
    );
    for (const request of oldestFirst) {
        if (request.blockNumber > finalizedBlock) sorted.waitingForFinality.push(request);
        else if (
            running &&
            request.blockNumber > job.input_block_number &&
            request.blockNumber <= job.output_block_number &&
            sorted.processing.length < MAX_BATCH_SIZE
        )
            sorted.processing.push(request);
        else sorted.scheduled.push(request);
    }
    return sorted;
}
