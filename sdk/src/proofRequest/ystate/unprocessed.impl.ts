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
import { BridgeProofRequestProcessingStatus } from '../../rx/proofRequest.js';
import {
    UnprocessedProofRequestStateGraph,
    type UnprocessedProofRequestTopics,
} from './unprocessed.js';

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

function getEthFinalityTimeRemaining(
    proofRequestBlockNumber: number,
    ethState: EthState
) {
    const delta =
        ethState.latest_finality_slot - ethState.latest_finality_block_number;
    const proofRequestSlot = proofRequestBlockNumber + delta;
    const rounded = Math.ceil(proofRequestSlot / 32) * 32;
    const blocksRemaining =
        rounded - delta - ethState.latest_finality_block_number;

    return Math.max(0, blocksRemaining * 12) + 1;
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
        return getEthFinalityTimeRemaining(proofRequestBlockNumber, ethState);
    }

    return getBridgeTimeRemaining(bridgeState, bridgeTimings);
}

function getElapsed(
    status: UnprocessedProofRequestProcessingStatus,
    bridgeState: BridgeState
) {
    if (status === BridgeProofRequestProcessingStatus.WaitingForEthFinality) {
        return 0;
    }

    return bridgeState.elapsed_sec;
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

function toWaitingNodeData(
    proofRequestBlockNumber: number,
    {
        proof_request_processing_status,
        ethState,
        bridgeState,
        bridgeTimings,
    }: ProofRequestScopedBridgeObservation & {
        proof_request_processing_status: UnprocessedProofRequestProcessingStatus;
    }
) {
    return {
        ...bridgeState,
        time_remaining_sec: getTimeRemaining(
            proofRequestBlockNumber,
            proof_request_processing_status,
            ethState,
            bridgeState,
            bridgeTimings
        ),
        elapsed_sec: getElapsed(proof_request_processing_status, bridgeState),
        proof_request_processing_status,
        proof_request_block_number: proofRequestBlockNumber,
    };
}

function tickWaitingNodeData<
    T extends {
        time_remaining_sec: number;
        elapsed_sec: number;
    },
>(source: T): T {
    return {
        ...source,
        time_remaining_sec: source.time_remaining_sec - 1,
        elapsed_sec: source.elapsed_sec + 1,
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

function checkWhetherWaitingForPreviousJobCompletion$(
    {
        proofRequestBlockNumber,
        proofRequestScopedBridgeObservation$,
    }: UnprocessedProofRequestMachineScope
) {
    return proofRequestScopedBridgeObservation$.pipe(
        filter(
            (
                update
            ): update is ProofRequestScopedBridgeObservation & {
                proof_request_processing_status: BridgeProofRequestProcessingStatus.WaitingForPreviousJobCompletion;
            } =>
                update.proof_request_processing_status ===
                    BridgeProofRequestProcessingStatus.WaitingForPreviousJobCompletion &&
                isWaitingForPreviousProofRequestCompletion(
                    proofRequestBlockNumber,
                    update.bridgeState
                )
        )
    );
}

function checkWhetherWaitingForCurrentJobCompletion$(
    {
        proofRequestBlockNumber,
        proofRequestScopedBridgeObservation$,
    }: UnprocessedProofRequestMachineScope
) {
    return proofRequestScopedBridgeObservation$.pipe(
        filter(
            (
                update
            ): update is ProofRequestScopedBridgeObservation & {
                proof_request_processing_status: BridgeProofRequestProcessingStatus.WaitingForCurrentJobCompletion;
            } =>
                update.proof_request_processing_status ===
                    BridgeProofRequestProcessingStatus.WaitingForCurrentJobCompletion &&
                isWaitingForCurrentProofRequestCompletion(
                    proofRequestBlockNumber,
                    update.bridgeState
                )
        )
    );
}

function checkWhetherFinishedWaiting$(
    {
        proofRequestBlockNumber,
        proofRequestScopedBridgeObservation$,
    }: UnprocessedProofRequestMachineScope
) {
    return proofRequestScopedBridgeObservation$.pipe(
        filter((update) =>
            hasFinishedWaitingForProofRequest(
                proofRequestBlockNumber,
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

export function createUnprocessedProofRequestStateMachine(
    proofRequestBlockNumber: number,
    topics: UnprocessedProofRequestTopics
) {
    const scope: UnprocessedProofRequestMachineScope = {
        proofRequestBlockNumber,
        proofRequestScopedBridgeObservation$: createProofRequestScopedBridgeObservation$(
            proofRequestBlockNumber,
            topics
        ),
    };

    return UnprocessedProofRequestStateGraph.implement({
        tickWaitingForEthFinality: {
            $: tickWaitingForEthFinality$,
            next: (_tick, _dest, source) => tickWaitingNodeData(source),
        },
        checkWhetherWaitingForPreviousJobCompletion: {
            $: () => checkWhetherWaitingForPreviousJobCompletion$(scope),
            next: (update) => toWaitingNodeData(proofRequestBlockNumber, update),
        },
        checkWhetherWaitingForCurrentJobCompletion: {
            $: () => checkWhetherWaitingForCurrentJobCompletion$(scope),
            next: (update) => toWaitingNodeData(proofRequestBlockNumber, update),
        },
        checkWhetherFinishedWaiting: {
            $: () => checkWhetherFinishedWaiting$(scope),
            next: () => ({}),
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
