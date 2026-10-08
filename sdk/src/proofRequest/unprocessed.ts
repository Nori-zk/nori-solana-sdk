import { define, type StateUnion } from '@yaw-rx/ystate';
import type { ObservedValueOf } from 'rxjs';
import type {
    getBridgeStateTopic$,
    getBridgeTimingsTopic$,
    getEthStateTopic$,
} from '../rpc/nori/topics.js';
import type { BridgeProofRequestProcessingStatus } from '../rpc/nori/proofRequest.js';

type BridgeState = ObservedValueOf<ReturnType<typeof getBridgeStateTopic$>>;

export type UnprocessedProofRequestStateData = BridgeState & {
    time_remaining_sec: number;
    proof_request_processing_status: BridgeProofRequestProcessingStatus;
    proof_request_block_number: number;
    /** Seconds until the batch covering the request is committed on Solana; negative when overdue. */
    commit_time_remaining_sec: number;
    /** Seconds the request has spent in this node. */
    waiting_elapsed_sec: number;
};

export const UnprocessedProofRequestStateGraph = define({
    nodes: {
        WaitingForEthFinality: {} as UnprocessedProofRequestStateData & {
            proof_request_processing_status: BridgeProofRequestProcessingStatus.WaitingForEthFinality;
        },
        WaitingForPreviousJobCompletion: {} as UnprocessedProofRequestStateData & {
            proof_request_processing_status: BridgeProofRequestProcessingStatus.WaitingForPreviousJobCompletion;
        },
        WaitingForCurrentJobCompletion: {} as UnprocessedProofRequestStateData & {
            proof_request_processing_status: BridgeProofRequestProcessingStatus.WaitingForCurrentJobCompletion;
        },
        FinishedWaiting: {},
    },
    edges: {
        ethFinalityPending: {
            from: 'WaitingForEthFinality',
            to: 'WaitingForEthFinality',
            on: 'tickWaitingForEthFinality.next',
        },
        ethFinalityRechecked: {
            from: 'WaitingForEthFinality',
            to: 'WaitingForEthFinality',
            on: 'checkWhetherWaitingForEthFinality.next',
        },
        previousProofRequestsRechecked: {
            from: 'WaitingForPreviousJobCompletion',
            to: 'WaitingForPreviousJobCompletion',
            on: 'checkWhetherWaitingForPreviousJobCompletion.next',
        },
        currentProofRequestRechecked: {
            from: 'WaitingForCurrentJobCompletion',
            to: 'WaitingForCurrentJobCompletion',
            on: 'checkWhetherWaitingForCurrentJobCompletion.next',
        },
        ethFinalityReached: {
            from: 'WaitingForEthFinality',
            to: 'WaitingForPreviousJobCompletion',
            on: 'checkWhetherWaitingForPreviousJobCompletion.next',
        },
        previousProofRequestsComplete: {
            from: 'WaitingForEthFinality',
            to: 'WaitingForCurrentJobCompletion',
            on: 'checkWhetherWaitingForCurrentJobCompletion.next',
        },
        proofRequestAlreadyComplete: {
            from: 'WaitingForEthFinality',
            to: 'FinishedWaiting',
            on: 'checkWhetherFinishedWaiting.next',
        },
        previousProofRequestsPending: {
            from: 'WaitingForPreviousJobCompletion',
            to: 'WaitingForPreviousJobCompletion',
            on: 'tickWaitingForPreviousJobCompletion.next',
        },
        previousProofRequestsCompleted: {
            from: 'WaitingForPreviousJobCompletion',
            to: 'WaitingForCurrentJobCompletion',
            on: 'checkWhetherWaitingForCurrentJobCompletion.next',
        },
        proofRequestCompletedAfterPrevious: {
            from: 'WaitingForPreviousJobCompletion',
            to: 'FinishedWaiting',
            on: 'checkWhetherFinishedWaiting.next',
        },
        currentProofRequestPending: {
            from: 'WaitingForCurrentJobCompletion',
            to: 'WaitingForCurrentJobCompletion',
            on: 'tickWaitingForCurrentJobCompletion.next',
        },
        currentProofRequestCompleted: {
            from: 'WaitingForCurrentJobCompletion',
            to: 'FinishedWaiting',
            on: 'checkWhetherFinishedWaiting.next',
        },
    },
});

export type UnprocessedProofRequestStateNodeUnion = StateUnion<
    typeof UnprocessedProofRequestStateGraph.nodes
>;

export type UnprocessedProofRequestTopics = {
    ethStateTopic$: ReturnType<typeof getEthStateTopic$>;
    bridgeStateTopic$: ReturnType<typeof getBridgeStateTopic$>;
    bridgeTimingsTopic$: ReturnType<typeof getBridgeTimingsTopic$>;
};
