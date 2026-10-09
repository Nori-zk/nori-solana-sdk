import { type Nori } from '../rpc/connection/connections.js';
import { startNoriBridgeInfraTransitions as startNoriBridgeInfraTransitionsFrom } from '../rpc/nori/noriBridgeInfraTransitions.impl.js';
import { getBridgeStateWithTimings$ as bridgeStateWithTimingsFrom } from '../rpc/nori/state.js';
import {
    getBridgeStateTopic$ as bridgeStateFrom,
    getBridgeTimingsTopic$ as bridgeTimingsFrom,
    getEthStateTopic$ as ethStateFrom,
    getSystemNoticesTopic$ as systemNoticesFrom,
    getTransitionNoticesTopic$ as transitionNoticesFrom,
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
export { type NoriBridgeInfraTransitions } from '../rpc/nori/noriBridgeInfraTransitions.impl.js';

/**
 * Nori's prover stage, from its `state.bridge`; replays the latest.
 *
 * @param nori Nori.
 * @returns The prover's stage, each time it changes.
 */
export const getNoriBridgeInfraState$ = (nori: Nori) => bridgeStateFrom(nori.websocket.socket);

/**
 * The expected time per stage, from Nori's `timings.notices.transition`; replays the latest.
 *
 * @param nori Nori.
 * @returns The timings, each time they change.
 */
export const getNoriBridgeInfraTimings$ = (nori: Nori) => bridgeTimingsFrom(nori.websocket.socket);

/**
 * Ethereum's latest finalized block and slot, from Nori's `state.eth`; replays the latest.
 *
 * @param nori Nori.
 * @returns Ethereum's finality, each time it moves.
 */
export const getNoriBridgeInfraEthState$ = (nori: Nori) => ethStateFrom(nori.websocket.socket);

/**
 * The prover pipeline's transition notices, as they happen.
 *
 * @param nori Nori.
 * @returns Each transition notice.
 */
export const getNoriBridgeInfraTransitionNotices$ = (nori: Nori) =>
    transitionNoticesFrom(nori.websocket.socket);

/**
 * Nori's services starting and heartbeating.
 *
 * @param nori Nori.
 * @returns Each system notice.
 */
export const getNoriBridgeInfraSystemNotices$ = (nori: Nori) => systemNoticesFrom(nori.websocket.socket);

/**
 * Nori's prover stage with the time left in it, ticking every second.
 *
 * @param nori Nori.
 * @returns The stage and the time left.
 */
export const getNoriBridgeInfraStateWithTimings$ = (nori: Nori) =>
    bridgeStateWithTimingsFrom(nori.websocket.socket);

/**
 * Follows Nori's prover pipeline from its transition notices.
 *
 * @param nori Nori.
 * @returns `noriBridgeInfraTransitions` (`{ state$ }`), `finalityTransitions$` and `warnings$`.
 */
export const startNoriBridgeInfraTransitions = (nori: Nori) => startNoriBridgeInfraTransitionsFrom(nori.websocket);
