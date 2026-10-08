import {
    type AllTransitionNoticeMessages,
    isBridgeHeadFinalityTransitionDetected,
    isBridgeHeadWarning,
} from '@nori-zk/pts-types';
import { filter, map, type Observable, shareReplay } from 'rxjs';
import { type NoriWebsocket } from './noriWebsocket.js';
import { getTransitionNoticesTopic$ } from './topics.js';
import { NoriBridgeInfraTransitionGraph, type NoriBridgeInfraTransitionState } from './noriBridgeInfraTransitions.js';

/**
 * Keeps only the notices one guard accepts, as their extensions.
 *
 * @param notices$ Nori's transition notices.
 * @param isNotice The `pts-types` guard for one notice.
 * @returns That notice's extensions.
 */
function noticesOf<TNotice extends AllTransitionNoticeMessages>(
    notices$: Observable<AllTransitionNoticeMessages>,
    isNotice: (notice: AllTransitionNoticeMessages) => notice is TNotice
): Observable<TNotice['extension']> {
    return notices$.pipe(
        filter(isNotice),
        map((notice) => notice.extension)
    );
}

/**
 * Whether a notice is one of the graph's nodes.
 *
 * @param notice A transition notice.
 * @returns `true` when its `message_type` names a node.
 */
function isNode(notice: AllTransitionNoticeMessages): boolean {
    return notice.message_type in NoriBridgeInfraTransitionGraph.nodes;
}

/**
 * Follows Nori's prover pipeline from its transition notices. Nori reports
 * each state itself, so the graph is not implemented: `state$` stands in
 * for its running machine, with each notice as the node it names and its
 * extension as the node's data.
 *
 * @param nori Nori's reconnecting websocket and its connection machine.
 * @returns
 *   - `noriBridgeInfraTransitions`: `{ state$ }`, the pipeline's states as the graph types them.
 *   - `finalityTransitions$`: each time the bridge head sees Ethereum finality move on.
 *   - `warnings$`: the bridge head's warnings.
 */
export function startNoriBridgeInfraTransitions(nori: NoriWebsocket) {
    const notices$ = getTransitionNoticesTopic$(nori.socket);

    const state$ = notices$.pipe(
        filter(isNode),
        map(
            (notice) =>
                ({ node: notice.message_type, data: notice.extension }) as NoriBridgeInfraTransitionState
        ),
        shareReplay(1)
    );

    return {
        noriBridgeInfraTransitions: { state$ },
        finalityTransitions$: noticesOf(notices$, isBridgeHeadFinalityTransitionDetected),
        warnings$: noticesOf(notices$, isBridgeHeadWarning),
    };
}

/** Nori's pipeline states and its event streams. */
export type NoriBridgeInfraTransitions = ReturnType<typeof startNoriBridgeInfraTransitions>;
