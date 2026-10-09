import {
    combineLatest,
    distinctUntilChanged,
    filter,
    interval,
    map,
    type ObservedValueOf,
    shareReplay,
} from 'rxjs';
import { TransitionNoticeMessageType } from '@nori-zk/pts-types';
import { BridgeProofRequestProcessingStatus } from '../rpc/nori/proofRequest.js';
import { type NoriWebsocket } from '../rpc/nori/noriWebsocket.js';
import {
    ETHEREUM_EPOCH_SEC,
    getCommitTimes,
    getFinalityTimeRemainingSec,
} from '../rpc/nori/commitTimes.js';
import {
    getBridgeStateTopic$,
    getBridgeTimingsTopic$,
    getEthStateTopic$,
} from '../rpc/nori/topics.js';
import {
    UnprocessedProofRequestStateGraph,
    type UnprocessedProofRequestTopics,
} from './unprocessed.js';

/** What a waiting node's data carries from one node to the next. */
type WaitingNodeData = {
    proof_request_processing_status: string;
    stage_name: string;
    time_remaining_sec: number;
    elapsed_sec: number;
    commit_time_remaining_sec: number;
    waiting_elapsed_sec: number;
};

type EthState = ObservedValueOf<UnprocessedProofRequestTopics['ethStateTopic$']>;
type BridgeState = ObservedValueOf<UnprocessedProofRequestTopics['bridgeStateTopic$']>;
type BridgeTimings = ObservedValueOf<UnprocessedProofRequestTopics['bridgeTimingsTopic$']>;
type UnprocessedProofRequestProcessingStatus =
    | BridgeProofRequestProcessingStatus.WaitingForEthFinality
    | BridgeProofRequestProcessingStatus.WaitingForPreviousJobCompletion
    | BridgeProofRequestProcessingStatus.WaitingForCurrentJobCompletion;
type ProofRequestScopedBridgeObservation = {
    proof_request_processing_status: UnprocessedProofRequestProcessingStatus | undefined;
    ethState: EthState;
    bridgeState: BridgeState;
    bridgeTimings: BridgeTimings;
};
type ProofRequestScopedBridgeObservation$ = ReturnType<
    typeof createProofRequestScopedBridgeObservation$
>;
type UnprocessedProofRequestMachineScope = {
    proofRequestBlockNumber: number;
    proofRequestScopedBridgeObservation$: ProofRequestScopedBridgeObservation$;
    /**
     * The observation the machine last moved on. ystate subscribes a node's
     * `$` again on every entry, self-loops included, and the observation
     * stream replays its latest, so the checks skip the one already applied.
     */
    applied?: ProofRequestScopedBridgeObservation;
};

function getUnprocessedProofRequestProcessingStatus(
    proofRequestBlockNumber: number,
    ethState: EthState,
    bridgeState: BridgeState
): UnprocessedProofRequestProcessingStatus | undefined {
    if (ethState.latest_finality_block_number < proofRequestBlockNumber) {
        return BridgeProofRequestProcessingStatus.WaitingForEthFinality;
    }

    if (
        bridgeState.input_block_number <= proofRequestBlockNumber &&
        proofRequestBlockNumber <= bridgeState.output_block_number
    ) {
        if (
            bridgeState.stage_name ===
            TransitionNoticeMessageType.EthProcessorTransactionFinalizationSucceeded
        ) {
            return undefined;
        }

        return BridgeProofRequestProcessingStatus.WaitingForCurrentJobCompletion;
    }

    if (bridgeState.output_block_number < proofRequestBlockNumber) {
        return BridgeProofRequestProcessingStatus.WaitingForPreviousJobCompletion;
    }

    return undefined;
}

function getBridgeTimeRemaining(
    bridgeState: BridgeState,
    bridgeTimings: BridgeTimings
) {
    return (
        (bridgeTimings.extension[bridgeState.stage_name] ?? 15) -
        bridgeState.elapsed_sec +
        1
    );
}

function getTimeRemaining(
    proofRequestBlockNumber: number,
    status: UnprocessedProofRequestProcessingStatus,
    ethState: EthState,
    bridgeState: BridgeState,
    bridgeTimings: BridgeTimings
) {
    if (status === BridgeProofRequestProcessingStatus.WaitingForEthFinality) {
        return getFinalityTimeRemainingSec(proofRequestBlockNumber, ethState);
    }

    return getBridgeTimeRemaining(bridgeState, bridgeTimings);
}

/**
 * The seconds counted as elapsed on each new observation: while waiting for
 * finality they keep counting from the first observation; in a bridge stage
 * they start again from 0.
 *
 * @param status Where the request waits now.
 * @param source The node the machine moves from, if it carries data.
 * @returns The elapsed seconds to carry on from.
 */
function getElapsed(
    status: UnprocessedProofRequestProcessingStatus,
    source: WaitingNodeData | undefined
) {
    if (
        status === BridgeProofRequestProcessingStatus.WaitingForEthFinality &&
        source?.proof_request_processing_status === status
    ) {
        return source.elapsed_sec;
    }

    return 0;
}

/**
 * The seconds until the batch covering the request is committed on Solana:
 * after finality, the job a finality transition creates; outside the job
 * Nori is running, the job after it; inside it, that job.
 *
 * @param status Where the request waits now.
 * @param timeRemaining The seconds until the request's block is finalized, while it waits for that.
 * @param bridgeState The bridge's stage.
 * @param bridgeTimings The bridge's timings.
 * @returns The seconds until the commit.
 */
function getCommitTimeRemaining(
    status: UnprocessedProofRequestProcessingStatus,
    timeRemaining: number,
    bridgeState: BridgeState,
    bridgeTimings: BridgeTimings
) {
    const commits = getCommitTimes(bridgeState, bridgeTimings.extension);
    if (status === BridgeProofRequestProcessingStatus.WaitingForEthFinality)
        return timeRemaining + commits.jobSec;
    if (status === BridgeProofRequestProcessingStatus.WaitingForCurrentJobCompletion)
        return commits.currentJobSec ?? commits.nextJobSec;
    return commits.nextJobSec;
}

/**
 * Wraps a bridge stage's countdown into one finality transition's period
 * while the bridge's last job is committed and it waits for the next
 * transition, as the bridge does not time that wait.
 *
 * @param timeRemaining The countdown.
 * @param status Where the request waits now.
 * @param bridgeState The bridge's stage.
 * @returns The countdown, wrapped when the bridge waits for finality.
 */
function wrapToFinalityTransition(
    timeRemaining: number,
    status: UnprocessedProofRequestProcessingStatus,
    bridgeState: { stage_name: string }
) {
    if (
        bridgeState.stage_name ===
            TransitionNoticeMessageType.EthProcessorTransactionFinalizationSucceeded &&
        status !== BridgeProofRequestProcessingStatus.WaitingForEthFinality
    ) {
        return ((timeRemaining % ETHEREUM_EPOCH_SEC) + ETHEREUM_EPOCH_SEC) % ETHEREUM_EPOCH_SEC;
    }

    return timeRemaining;
}

function isWaitingForPreviousProofRequestCompletion(
    proofRequestBlockNumber: number,
    bridgeState: BridgeState
) {
    return bridgeState.output_block_number < proofRequestBlockNumber;
}

function isWaitingForCurrentProofRequestCompletion(
    proofRequestBlockNumber: number,
    bridgeState: BridgeState
) {
    return (
        bridgeState.input_block_number <= proofRequestBlockNumber &&
        proofRequestBlockNumber <= bridgeState.output_block_number &&
        bridgeState.stage_name !==
            TransitionNoticeMessageType.EthProcessorTransactionFinalizationSucceeded
    );
}

function hasFinishedWaitingForProofRequest(
    proofRequestBlockNumber: number,
    ethState: EthState,
    bridgeState: BridgeState
) {
    if (ethState.latest_finality_block_number < proofRequestBlockNumber) {
        return false;
    }

    if (proofRequestBlockNumber < bridgeState.input_block_number) {
        return true;
    }

    return (
        bridgeState.input_block_number <= proofRequestBlockNumber &&
        proofRequestBlockNumber <= bridgeState.output_block_number &&
        bridgeState.stage_name ===
            TransitionNoticeMessageType.EthProcessorTransactionFinalizationSucceeded
    );
}

/**
 * A waiting node's data, computed afresh from one observation of the
 * bridge, as `getDepositProcessingStatus$` did on every change of
 * `state.eth`, `state.bridge` or the timings.
 *
 * @param proofRequestBlockNumber The block the proof request was enqueued in.
 * @param update The observation and the request's status in it.
 * @param source The node the machine moves from, if it carries data.
 * @returns The node's data.
 */
function toWaitingNodeData(
    proofRequestBlockNumber: number,
    {
        proof_request_processing_status,
        ethState,
        bridgeState,
        bridgeTimings,
    }: ProofRequestScopedBridgeObservation & {
        proof_request_processing_status: UnprocessedProofRequestProcessingStatus;
    },
    source?: Partial<WaitingNodeData>
) {
    const timeRemaining = getTimeRemaining(
        proofRequestBlockNumber,
        proof_request_processing_status,
        ethState,
        bridgeState,
        bridgeTimings
    );
    const carried =
        source?.proof_request_processing_status === proof_request_processing_status
            ? (source as WaitingNodeData)
            : undefined;
    return {
        ...bridgeState,
        time_remaining_sec: wrapToFinalityTransition(
            timeRemaining,
            proof_request_processing_status,
            bridgeState
        ),
        elapsed_sec: getElapsed(proof_request_processing_status, carried),
        proof_request_processing_status,
        proof_request_block_number: proofRequestBlockNumber,
        commit_time_remaining_sec: getCommitTimeRemaining(
            proof_request_processing_status,
            timeRemaining,
            bridgeState,
            bridgeTimings
        ),
        waiting_elapsed_sec: carried?.waiting_elapsed_sec ?? 0,
    };
}

/**
 * A waiting node's data one second later.
 *
 * @param source The node's data now.
 * @returns The data a second later.
 */
function tickWaitingNodeData<T extends WaitingNodeData>(source: T): T {
    // Before the first observation the starting node carries no estimate yet.
    if (typeof source.time_remaining_sec !== 'number') return source;
    return {
        ...source,
        time_remaining_sec: wrapToFinalityTransition(
            source.time_remaining_sec - 1,
            source.proof_request_processing_status as UnprocessedProofRequestProcessingStatus,
            source
        ),
        elapsed_sec: source.elapsed_sec + 1,
        commit_time_remaining_sec: source.commit_time_remaining_sec - 1,
        waiting_elapsed_sec: source.waiting_elapsed_sec + 1,
    };
}

function createProofRequestScopedBridgeObservation$(
    proofRequestBlockNumber: number,
    {
        ethStateTopic$,
        bridgeStateTopic$,
        bridgeTimingsTopic$,
    }: UnprocessedProofRequestTopics
) {
    return combineLatest([
        ethStateTopic$,
        bridgeStateTopic$,
        bridgeTimingsTopic$,
    ]).pipe(
        distinctUntilChanged(
            ([previousEth, previousBridge, previousTimings], [
                currentEth,
                currentBridge,
                currentTimings,
            ]) =>
                JSON.stringify(previousEth) === JSON.stringify(currentEth) &&
                JSON.stringify(previousBridge) ===
                    JSON.stringify(currentBridge) &&
                JSON.stringify(previousTimings) ===
                    JSON.stringify(currentTimings)
        ),
        map(
            ([ethState, bridgeState, bridgeTimings]): ProofRequestScopedBridgeObservation => ({
                proof_request_processing_status: getUnprocessedProofRequestProcessingStatus(
                    proofRequestBlockNumber,
                    ethState,
                    bridgeState
                ),
                ethState,
                bridgeState,
                bridgeTimings,
            })
        ),
        shareReplay(1)
    );
}

function checkWhetherWaitingForEthFinality$(scope: UnprocessedProofRequestMachineScope) {
    return scope.proofRequestScopedBridgeObservation$.pipe(
        filter(
            (
                update
            ): update is ProofRequestScopedBridgeObservation & {
                proof_request_processing_status: BridgeProofRequestProcessingStatus.WaitingForEthFinality;
            } =>
                update !== scope.applied &&
                update.proof_request_processing_status ===
                    BridgeProofRequestProcessingStatus.WaitingForEthFinality
        )
    );
}

function checkWhetherWaitingForPreviousJobCompletion$(scope: UnprocessedProofRequestMachineScope) {
    return scope.proofRequestScopedBridgeObservation$.pipe(
        filter(
            (
                update
            ): update is ProofRequestScopedBridgeObservation & {
                proof_request_processing_status: BridgeProofRequestProcessingStatus.WaitingForPreviousJobCompletion;
            } =>
                update !== scope.applied &&
                update.proof_request_processing_status ===
                    BridgeProofRequestProcessingStatus.WaitingForPreviousJobCompletion &&
                isWaitingForPreviousProofRequestCompletion(
                    scope.proofRequestBlockNumber,
                    update.bridgeState
                )
        )
    );
}

function checkWhetherWaitingForCurrentJobCompletion$(scope: UnprocessedProofRequestMachineScope) {
    return scope.proofRequestScopedBridgeObservation$.pipe(
        filter(
            (
                update
            ): update is ProofRequestScopedBridgeObservation & {
                proof_request_processing_status: BridgeProofRequestProcessingStatus.WaitingForCurrentJobCompletion;
            } =>
                update !== scope.applied &&
                update.proof_request_processing_status ===
                    BridgeProofRequestProcessingStatus.WaitingForCurrentJobCompletion &&
                isWaitingForCurrentProofRequestCompletion(
                    scope.proofRequestBlockNumber,
                    update.bridgeState
                )
        )
    );
}

function checkWhetherFinishedWaiting$(scope: UnprocessedProofRequestMachineScope) {
    return scope.proofRequestScopedBridgeObservation$.pipe(
        filter(
            (update) =>
                update !== scope.applied &&
                hasFinishedWaitingForProofRequest(
                    scope.proofRequestBlockNumber,
                    update.ethState,
                    update.bridgeState
                )
        )
    );
}

function tickWaitingForEthFinality$() {
    return interval(1000);
}

function tickWaitingForPreviousJobCompletion$() {
    return interval(1000);
}

function tickWaitingForCurrentJobCompletion$() {
    return interval(1000);
}

/**
 * Follows an unprocessed proof request through the bridge's stages, from
 * Nori's `state.eth`, `state.bridge` and timings topics.
 *
 * @param proofRequestBlockNumber The block the proof request was enqueued in.
 * @param nori Nori's websocket, whose topics the machine subscribes to.
 * @returns The implemented YState machine.
 */
export function createUnprocessedProofRequestStateMachine(
    proofRequestBlockNumber: number,
    nori: NoriWebsocket
) {
    const scope: UnprocessedProofRequestMachineScope = {
        proofRequestBlockNumber,
        proofRequestScopedBridgeObservation$: createProofRequestScopedBridgeObservation$(
            proofRequestBlockNumber,
            {
                ethStateTopic$: getEthStateTopic$(nori.socket),
                bridgeStateTopic$: getBridgeStateTopic$(nori.socket),
                bridgeTimingsTopic$: getBridgeTimingsTopic$(nori.socket),
            }
        ),
    };

    return UnprocessedProofRequestStateGraph.implement({
        tickWaitingForEthFinality: {
            $: tickWaitingForEthFinality$,
            next: (_tick, _dest, source) => tickWaitingNodeData(source),
        },
        checkWhetherWaitingForEthFinality: {
            $: () => checkWhetherWaitingForEthFinality$(scope),
            next: (update, _dest, source) => {
                scope.applied = update;
                return toWaitingNodeData(proofRequestBlockNumber, update, source);
            },
        },
        checkWhetherWaitingForPreviousJobCompletion: {
            $: () => checkWhetherWaitingForPreviousJobCompletion$(scope),
            next: (update, _dest, source) => {
                scope.applied = update;
                return toWaitingNodeData(proofRequestBlockNumber, update, source);
            },
        },
        checkWhetherWaitingForCurrentJobCompletion: {
            $: () => checkWhetherWaitingForCurrentJobCompletion$(scope),
            next: (update, _dest, source) => {
                scope.applied = update;
                return toWaitingNodeData(proofRequestBlockNumber, update, source);
            },
        },
        checkWhetherFinishedWaiting: {
            $: () => checkWhetherFinishedWaiting$(scope),
            next: (update) => {
                scope.applied = update;
                return {};
            },
        },
        tickWaitingForPreviousJobCompletion: {
            $: tickWaitingForPreviousJobCompletion$,
            next: (_tick, _dest, source) => tickWaitingNodeData(source),
        },
        tickWaitingForCurrentJobCompletion: {
            $: tickWaitingForCurrentJobCompletion$,
            next: (_tick, _dest, source) => tickWaitingNodeData(source),
        },
    });
}
