import {
    type AllTransitionNoticeMessages,
    isBridgeHeadFinalityTransitionDetected,
    isBridgeHeadWarning,
} from '@nori-zk/pts-types';
import { distinctUntilChanged, filter, map, merge, type Observable, shareReplay } from 'rxjs';
import { type NoriWebsocket } from './noriWebsocket.js';
import { bridgeStateTopic$, transitionNoticesTopic$ } from './topics.js';
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

/** A node of the graph: the stage Nori's pipeline is at. */
export type NoriBridgeInfraStage = NoriBridgeInfraTransitionState['node'];

/** The stage the pipeline is at and since when, in ms since the epoch. */
export interface NoriBridgeInfraStageSince {
    stage: NoriBridgeInfraStage;
    sinceMs: number;
}

/**
 * Whether a stage name is a node of the graph.
 *
 * @param name A stage name from `state.bridge` or a transition notice.
 * @returns `true` for a node.
 */
const isStage = (name: string): name is NoriBridgeInfraStage =>
    name in NoriBridgeInfraTransitionGraph.nodes;

/**
 * Follows Nori's prover pipeline from its transition notices. The pipeline
 * runs on Nori's server, which moves it from state to state and sends a
 * notice for each one; the app observes it and drives none of its edges.
 * `NoriBridgeInfraTransitionGraph` is that loop as the server runs it, and
 * it is not implemented here: an app joins mid-loop, reconnects, and can
 * miss notices, so a machine following them would need an edge from every
 * node to every node, which is not the pipeline. `state$` is the pipeline's
 * state instead: each notice as the node it names, its extension as the
 * node's data, typed by the graph.
 *
 * Transition notices are live only: the server sends none on subscribing.
 * `stage$` is therefore positioned by `state.bridge`, whose current value
 * the server sends on subscribing and again after every reconnect, then
 * moved on by each notice. Both are started here and kept subscribed until
 * `close()`, so a subscriber joining at any time gets the stage the
 * pipeline is at. Start one per Nori websocket and share it
 * (`createConnections` does).
 *
 * @param nori Nori's reconnecting websocket and its connection machine.
 * @returns
 *   - `noriBridgeInfraTransitions`: `{ state$ }`, the pipeline's states as the graph types them.
 *   - `stage$`: the stage the pipeline is at and since when; replays the latest.
 *   - `finalityTransitions$`: each time the bridge head sees Ethereum finality move on.
 *   - `warnings$`: the bridge head's warnings.
 *   - `close()`: stops following.
 */
export function startNoriBridgeInfraTransitions(nori: NoriWebsocket) {
    const notices$ = transitionNoticesTopic$(nori.socket);

    const state$ = notices$.pipe(
        filter(isNode),
        map(
            (notice) =>
                ({ node: notice.message_type, data: notice.extension }) as NoriBridgeInfraTransitionState
        ),
        shareReplay({ bufferSize: 1, refCount: false })
    );
    const stage$ = merge(
        // The current stage, with the time already spent in it.
        bridgeStateTopic$(nori.socket).pipe(
            filter(({ stage_name }) => isStage(stage_name)),
            map(
                ({ stage_name, elapsed_sec }): NoriBridgeInfraStageSince => ({
                    stage: stage_name as NoriBridgeInfraStage,
                    sinceMs: Date.now() - elapsed_sec * 1000,
                })
            )
        ),
        // Each stage as it starts.
        state$.pipe(map(({ node }): NoriBridgeInfraStageSince => ({ stage: node, sinceMs: Date.now() })))
    ).pipe(
        distinctUntilChanged((previous, current) => previous.stage === current.stage),
        shareReplay({ bufferSize: 1, refCount: false })
    );
    const following = merge(state$, stage$).subscribe();

    return {
        noriBridgeInfraTransitions: { state$ },
        stage$,
        finalityTransitions$: noticesOf(notices$, isBridgeHeadFinalityTransitionDetected),
        warnings$: noticesOf(notices$, isBridgeHeadWarning),
        close: () => following.unsubscribe(),
    };
}

/** Nori's pipeline states and its event streams. */
export type NoriBridgeInfraTransitions = ReturnType<typeof startNoriBridgeInfraTransitions>;
