import { define, type StateUnion } from '@yaw-rx/ystate';
import type {
    TransitionNoticeExtensionBridgeHeadAdvanced,
    TransitionNoticeExtensionBridgeHeadJobCreated,
    TransitionNoticeExtensionBridgeHeadJobFailed,
    TransitionNoticeExtensionBridgeHeadJobSucceeded,
    TransitionNoticeExtensionEthProcessorTransactionFinalizationFailed,
    TransitionNoticeExtensionEthProcessorTransactionFinalizationSucceeded,
    TransitionNoticeExtensionEthProcessorTransactionSubmitFailed,
    TransitionNoticeExtensionEthProcessorTransactionSubmitSucceeded,
    TransitionNoticeExtensionEthProcessorTransactionSubmitting,
} from '@nori-zk/pts-types';
import { type AsNodeData } from '../../utils/machines.js';

/**
 * Nori's prover pipeline, as its transition notices report it: a loop, one
 * job at a time. Each node is a notice and carries that notice's extension,
 * as Nori sent it.
 *
 * - The loop: `BridgeHeadJobCreated` → `BridgeHeadJobSucceeded` (its
 *   `verified_requests` are the batch) → `EthProcessorTransactionSubmitting`
 *   → `…SubmitSucceeded` (its `tx_hash`) → `…FinalizationSucceeded` →
 *   `BridgeHeadAdvanced` → the next `BridgeHeadJobCreated`.
 * - `BridgeHeadJobFailed`: the proof is staged again (`BridgeHeadJobCreated`).
 * - `…SubmitFailed` or `…FinalizationFailed`: the bridge head checks the Nori
 *   program on Solana. Still aligned, it stages the job again
 *   (`BridgeHeadJobCreated`); moved on, it advances to it (`BridgeHeadAdvanced`).
 */
export const NoriBridgeInfraTransitionGraph = define({
    nodes: {
        BridgeHeadJobCreated: {} as AsNodeData<TransitionNoticeExtensionBridgeHeadJobCreated>,
        BridgeHeadJobSucceeded:
            {} as AsNodeData<TransitionNoticeExtensionBridgeHeadJobSucceeded>,
        BridgeHeadJobFailed: {} as AsNodeData<TransitionNoticeExtensionBridgeHeadJobFailed>,
        EthProcessorTransactionSubmitting:
            {} as AsNodeData<TransitionNoticeExtensionEthProcessorTransactionSubmitting>,
        EthProcessorTransactionSubmitSucceeded:
            {} as AsNodeData<TransitionNoticeExtensionEthProcessorTransactionSubmitSucceeded>,
        EthProcessorTransactionSubmitFailed:
            {} as AsNodeData<TransitionNoticeExtensionEthProcessorTransactionSubmitFailed>,
        EthProcessorTransactionFinalizationSucceeded:
            {} as AsNodeData<TransitionNoticeExtensionEthProcessorTransactionFinalizationSucceeded>,
        EthProcessorTransactionFinalizationFailed:
            {} as AsNodeData<TransitionNoticeExtensionEthProcessorTransactionFinalizationFailed>,
        BridgeHeadAdvanced: {} as AsNodeData<TransitionNoticeExtensionBridgeHeadAdvanced>,
    },
    edges: {
        // ---- the loop
        jobSucceeded: { from: 'BridgeHeadJobCreated', to: 'BridgeHeadJobSucceeded', on: 'jobSucceeded.next' },
        submitting: { from: 'BridgeHeadJobSucceeded', to: 'EthProcessorTransactionSubmitting', on: 'transactionSubmitting.next' },
        submitSucceeded: { from: 'EthProcessorTransactionSubmitting', to: 'EthProcessorTransactionSubmitSucceeded', on: 'transactionSubmitSucceeded.next' },
        finalizationSucceeded: { from: 'EthProcessorTransactionSubmitSucceeded', to: 'EthProcessorTransactionFinalizationSucceeded', on: 'transactionFinalizationSucceeded.next' },
        headAdvanced: { from: 'EthProcessorTransactionFinalizationSucceeded', to: 'BridgeHeadAdvanced', on: 'headAdvanced.next' },
        nextJobCreated: { from: 'BridgeHeadAdvanced', to: 'BridgeHeadJobCreated', on: 'jobCreated.next' },

        // ---- the proof failed: staged again
        jobFailed: { from: 'BridgeHeadJobCreated', to: 'BridgeHeadJobFailed', on: 'jobFailed.next' },
        jobRestaged: { from: 'BridgeHeadJobFailed', to: 'BridgeHeadJobCreated', on: 'jobCreated.next' },

        // ---- the submit or finalization failed: staged again while aligned, otherwise realigned
        submitFailed: { from: 'EthProcessorTransactionSubmitting', to: 'EthProcessorTransactionSubmitFailed', on: 'transactionSubmitFailed.next' },
        jobRestagedAfterSubmitFailed: { from: 'EthProcessorTransactionSubmitFailed', to: 'BridgeHeadJobCreated', on: 'jobCreated.next' },
        realignedAfterSubmitFailed: { from: 'EthProcessorTransactionSubmitFailed', to: 'BridgeHeadAdvanced', on: 'headAdvanced.next' },
        finalizationFailed: { from: 'EthProcessorTransactionSubmitSucceeded', to: 'EthProcessorTransactionFinalizationFailed', on: 'transactionFinalizationFailed.next' },
        jobRestagedAfterFinalizationFailed: { from: 'EthProcessorTransactionFinalizationFailed', to: 'BridgeHeadJobCreated', on: 'jobCreated.next' },
        realignedAfterFinalizationFailed: { from: 'EthProcessorTransactionFinalizationFailed', to: 'BridgeHeadAdvanced', on: 'headAdvanced.next' },
    },
});

/** Nori's pipeline state: a node of the graph and its notice's extension. */
export type NoriBridgeInfraTransitionState = StateUnion<typeof NoriBridgeInfraTransitionGraph.nodes>;
