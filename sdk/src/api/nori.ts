import { type Nori } from '../rpc/connection/connections.js';
import { bridgeStateWithTimings$ as bridgeStateWithTimingsFrom } from '../rpc/nori/state.js';
import {
    bridgeStateTopic$ as bridgeStateFrom,
    bridgeTimingsTopic$ as bridgeTimingsFrom,
    ethStateTopic$ as ethStateFrom,
    systemNoticesTopic$ as systemNoticesFrom,
    transitionNoticesTopic$ as transitionNoticesFrom,
} from '../rpc/nori/topics.js';

export { NoriBridgeInfraTransitionGraph, type NoriBridgeInfraTransitionState } from '../rpc/nori/noriBridgeInfraTransitions.js';
export {
    ETHEREUM_EPOCH_SEC,
    FALLBACK_NORI_JOB_TIMINGS,
    getCommitTimes,
    getFinalityTimeRemainingSec,
    jobTimingsOf,
    MAX_BATCH_SIZE,
    NORI_JOB_STAGES,
    type CommitTimes,
    type EthereumFinality,
    type NoriJobStage,
    type NoriJobTimings,
    type NoriStage,
} from '../rpc/nori/commitTimes.js';
export {
    type NoriBridgeInfraStage,
    type NoriBridgeInfraStageSince,
    type NoriBridgeInfraTransitions,
} from '../rpc/nori/noriBridgeInfraTransitions.impl.js';
export { arrived, type Arrived } from '../rpc/nori/state.js';

/**
 * Nori's prover stage, from its `state.bridge`; replays the latest.
 *
 * @param nori Nori.
 * @returns The prover's stage, each time it changes.
 */
export const noriBridgeInfraState$ = (nori: Nori) => bridgeStateFrom(nori.websocket.socket);

/**
 * The expected time per stage, from Nori's `timings.notices.transition`; replays the latest.
 *
 * @param nori Nori.
 * @returns The timings, each time they change.
 */
export const noriBridgeInfraTimings$ = (nori: Nori) => bridgeTimingsFrom(nori.websocket.socket);

/**
 * Ethereum's latest finalized block and slot, from Nori's `state.eth`; replays the latest.
 *
 * @param nori Nori.
 * @returns Ethereum's finality, each time it moves.
 */
export const noriBridgeInfraEthState$ = (nori: Nori) => ethStateFrom(nori.websocket.socket);

/**
 * The prover pipeline's transition notices, as they happen.
 *
 * @param nori Nori.
 * @returns Each transition notice.
 */
export const noriBridgeInfraTransitionNotices$ = (nori: Nori) =>
    transitionNoticesFrom(nori.websocket.socket);

/**
 * Nori's services starting and heartbeating.
 *
 * @param nori Nori.
 * @returns Each system notice.
 */
export const noriBridgeInfraSystemNotices$ = (nori: Nori) => systemNoticesFrom(nori.websocket.socket);

/**
 * Nori's prover stage with the time left in it, ticking every second.
 *
 * @param nori Nori.
 * @returns The stage and the time left.
 */
export const noriBridgeInfraStateWithTimings$ = (nori: Nori) =>
    bridgeStateWithTimingsFrom(nori.websocket.socket);

/**
 * Nori's pipeline transitions and the stage it is at: the one instance
 * `createConnections` started for this websocket, shared by every caller.
 *
 * @param nori Nori, from `createConnections`.
 * @returns The transitions, `stage$` among them.
 */
export const getNoriBridgeInfraTransitions = (nori: Nori) => nori.transitions;
