import { jest } from '@jest/globals';
import { filter, Subject } from 'rxjs';
import { createUnprocessedProofRequestStateMachine } from '../../proofRequest/unprocessed.impl.js';
import { type UnprocessedProofRequestStateNodeUnion } from '../../proofRequest/unprocessed.js';
import { type NoriWebsocket } from '../../rpc/nori/noriWebsocket.js';

/** Nori's timings as its websocket sent them on Sepolia. */
const TIMINGS = {
    BridgeHeadJobCreated: 80.926502298,
    BridgeHeadJobSucceeded: 1,
    ProofConversionJobReceived: 337.25,
    ProofConversionJobSucceeded: 1,
    EthProcessorProofRequest: 71.506,
    EthProcessorProofSucceeded: 1,
    EthProcessorTransactionSubmitting: 2.436,
    EthProcessorTransactionSubmitSucceeded: 0.2,
};
const JOB_SEC = 80.926502298 + 1 + 2.436 + 0.2;

/** The job Nori's `state.bridge` reported, and the one after it. */
const JOB = { input_slot: 11311296, input_block_number: 11870714, output_slot: 11311328, output_block_number: 11870746 };
const NEXT_JOB = { input_slot: 11311328, input_block_number: 11870746, output_slot: 11311360, output_block_number: 11870778 };

/** The block the followed request was enqueued in: after the job, before the next finality. */
const REQUEST_BLOCK = 11870760;

/**
 * Nori's websocket as the topics read it: `multiplex` serves the messages
 * pushed with `send` that its filter accepts.
 *
 * @returns The websocket and `send`.
 */
function createFakeNori() {
    const messages$ = new Subject<unknown>();
    const socket = {
        multiplex: (_subscribe: unknown, _unsubscribe: unknown, accepts: (message: unknown) => boolean) =>
            messages$.pipe(filter(accepts)),
    };
    const send = (topic: string, extension: unknown) => messages$.next({ topic, extension });
    return { nori: { socket } as unknown as NoriWebsocket, send };
}

const bridgeState = (stage_name: string, job: typeof JOB, elapsed_sec: number, lastFinalized = JOB) => ({
    stage_name,
    ...job,
    elapsed_sec,
    last_finalized_job: lastFinalized,
});

describe('unprocessed proof request machine', () => {
    beforeEach(() => jest.useFakeTimers());
    afterEach(() => jest.useRealTimers());

    test('follows a request from finality to its job committing on Solana, recomputing on every observation', () => {
        const { nori, send } = createFakeNori();
        const machine = createUnprocessedProofRequestStateMachine(REQUEST_BLOCK, nori)
            .close()
            .start('WaitingForEthFinality');
        let latest: UnprocessedProofRequestStateNodeUnion | undefined;
        machine.state$.subscribe((state) => (latest = state as UnprocessedProofRequestStateNodeUnion));
        const data = () => latest?.data as Record<string, number>;

        // A tick before the first observation leaves the empty start alone.
        jest.advanceTimersByTime(1000);
        expect(data().time_remaining_sec).toBeUndefined();

        // The first observation: the block is 32 slots short of finality.
        send('timings.notices.transition', TIMINGS);
        send('state.bridge', bridgeState('BridgeHeadJobCreated', JOB, 50));
        send('state.eth', { latest_finality_block_number: 11870746, latest_finality_slot: 11311328 });
        expect(latest?.node).toBe('WaitingForEthFinality');
        expect(data().time_remaining_sec).toBe(32 * 12 + 1);
        expect(data().commit_time_remaining_sec).toBeCloseTo(32 * 12 + 1 + JOB_SEC, 3);
        expect(data().waiting_elapsed_sec).toBe(0);

        // It counts down each second.
        jest.advanceTimersByTime(3000);
        expect(data().time_remaining_sec).toBe(32 * 12 + 1 - 3);
        expect(data().elapsed_sec).toBe(3);
        expect(data().waiting_elapsed_sec).toBe(3);

        // A new observation while still waiting recomputes it, and finality's elapsed time carries on.
        send('state.bridge', bridgeState('BridgeHeadJobSucceeded', JOB, 0));
        expect(latest?.node).toBe('WaitingForEthFinality');
        expect(data().time_remaining_sec).toBe(32 * 12 + 1);
        expect(data().elapsed_sec).toBe(3);
        expect(data().waiting_elapsed_sec).toBe(3);

        // The job commits, then finality passes the block while Nori is idle: it waits for the next job.
        send('state.bridge', bridgeState('EthProcessorTransactionFinalizationSucceeded', JOB, 0));
        send('state.eth', { latest_finality_block_number: 11870778, latest_finality_slot: 11311360 });
        expect(latest?.node).toBe('WaitingForPreviousJobCompletion');
        expect(data().commit_time_remaining_sec).toBeCloseTo(384, 3);
        expect(data().waiting_elapsed_sec).toBe(0);

        // Nori creates the job that includes it: its commit is that job's time away.
        jest.advanceTimersByTime(5000);
        send('state.bridge', bridgeState('BridgeHeadJobCreated', NEXT_JOB, 0));
        expect(latest?.node).toBe('WaitingForCurrentJobCompletion');
        expect(data().commit_time_remaining_sec).toBeCloseTo(JOB_SEC, 3);
        jest.advanceTimersByTime(10_000);
        expect(data().commit_time_remaining_sec).toBeCloseTo(JOB_SEC - 10, 3);

        // A later stage of the same job recomputes the countdown from that stage.
        send('state.bridge', bridgeState('EthProcessorTransactionSubmitting', NEXT_JOB, 0));
        expect(latest?.node).toBe('WaitingForCurrentJobCompletion');
        expect(data().commit_time_remaining_sec).toBeCloseTo(2.436 + 0.2, 3);

        // The job is committed on Solana: the request has finished waiting.
        send('state.bridge', bridgeState('EthProcessorTransactionFinalizationSucceeded', NEXT_JOB, 0, NEXT_JOB));
        expect(latest?.node).toBe('FinishedWaiting');
        machine.stop();
    });

    test('a request whose block is already finalized goes straight to the job that includes it', () => {
        const { nori, send } = createFakeNori();
        const machine = createUnprocessedProofRequestStateMachine(11870730, nori)
            .close()
            .start('WaitingForEthFinality');
        let latest: UnprocessedProofRequestStateNodeUnion | undefined;
        machine.state$.subscribe((state) => (latest = state as UnprocessedProofRequestStateNodeUnion));

        send('timings.notices.transition', TIMINGS);
        send('state.bridge', bridgeState('BridgeHeadJobCreated', JOB, 50));
        send('state.eth', { latest_finality_block_number: 11870746, latest_finality_slot: 11311328 });
        expect(latest?.node).toBe('WaitingForCurrentJobCompletion');
        expect((latest?.data as Record<string, number>).commit_time_remaining_sec).toBeCloseTo(JOB_SEC - 50, 3);
        machine.stop();
    });
});
