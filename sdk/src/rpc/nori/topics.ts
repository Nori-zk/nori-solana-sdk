import {
    type AllSystemNoticeMessages,
    type AllTransitionNoticeMessages,
    type KeyTransitionStageMessageTypes,
    type WebSocketServiceTopicSubscriptionMessage,
} from '@nori-zk/pts-types';
import { filter, map, type Observable, share, shareReplay } from 'rxjs';
import { type ReconnectingWebSocketSubject } from '../connection/websocket.js';

/** A topic Nori's websocket server publishes. */
type NoriTopic = WebSocketServiceTopicSubscriptionMessage['topic'];

/** Each socket's topic streams, one per topic, so listeners share one server subscription. */
const topicsBySocket = new WeakMap<
    ReconnectingWebSocketSubject<unknown>,
    Map<NoriTopic, Observable<WebSocketServiceTopicSubscriptionMessage>>
>();

/** The topics the server sends its current value on when one subscribes; every other topic is live only. */
const TOPICS_WITH_CURRENT_VALUE: ReadonlySet<NoriTopic> = new Set<NoriTopic>([
    'state.bridge',
    'state.eth',
    'timings.notices.transition',
]);

/**
 * Returns an observable emitting one topic's messages. The topic is
 * subscribed on the server while anyone listens, through `multiplex`: again
 * every time the socket opens, and unsubscribed when the last listener stops.
 * Every listener shares the one server subscription; on a topic the server
 * sends its current value for, a listener joining later gets the latest
 * message the first listener received.
 *
 * @param noriSocket Nori's reconnecting websocket.
 * @param topic The topic.
 * @returns Observable emitting the topic's messages.
 */
const getTopic$ = (noriSocket: ReconnectingWebSocketSubject<unknown>, topic: NoriTopic) => {
    let topics = topicsBySocket.get(noriSocket);
    if (topics === undefined) {
        topics = new Map();
        topicsBySocket.set(noriSocket, topics);
    }
    let topic$ = topics.get(topic);
    if (topic$ === undefined) {
        topic$ = noriSocket
            .multiplex<WebSocketServiceTopicSubscriptionMessage>(
                () => ({ method: 'subscribe', topic }),
                () => ({ method: 'unsubscribe', topic }),
                (message) =>
                    typeof message === 'object' &&
                    message !== null &&
                    'topic' in message &&
                    message.topic === topic
            )
            .pipe(
                TOPICS_WITH_CURRENT_VALUE.has(topic)
                    ? shareReplay({ bufferSize: 1, refCount: true })
                    : share()
            );
        topics.set(topic, topic$);
    }
    return topic$;
};

/**
 * Returns an observable emitting the bridge's current processing state.
 *
 * Filters for messages from the `state.bridge` topic, ignoring any with `elapsed_sec`
 * set to `'unknown'`, as those do not represent valid state data.
 *
 * The observable emits objects containing the current stage, slot/block input/output
 * positions, elapsed time in seconds, and details about the last finalized job—
 * if known.
 *
 * The stream is shared and replays the latest state to every subscriber.
 *
 * @param noriSocket Nori's reconnecting websocket.
 * @returns Observable emitting bridge state updates.
 */
export const bridgeStateTopic$ = (noriSocket: ReconnectingWebSocketSubject<unknown>) =>
    (
        getTopic$(noriSocket, 'state.bridge').pipe(
            // Suppress events when the state is 'unknown'
            filter(
                (message) =>
                    message.topic === 'state.bridge' &&
                    message.extension.elapsed_sec !== 'unknown'
            ),
            map((message) => message.extension)
        ) as Observable<{
            stage_name: KeyTransitionStageMessageTypes;
            input_slot: number;
            input_block_number: number;
            output_slot: number;
            output_block_number: number;
            elapsed_sec: number;
            last_finalized_job:
                | 'unknown'
                | {
                      input_slot: number;
                      input_block_number: number;
                      output_slot: number;
                      output_block_number: number;
                  };
        }>
    ).pipe(shareReplay({ bufferSize: 1, refCount: true }));

/**
 * Returns an observable emitting bridge timing data related to transition notices.
 *
 * Filters for messages from the `timings.notices.transition` topic.
 * Emits raw transition timing metadata from the bridge without transformation.
 *
 * The stream is shared and replays the latest timings to every subscriber.
 *
 * @param noriSocket Nori's reconnecting websocket.
 * @returns Observable emitting transition timing updates.
 */
export const bridgeTimingsTopic$ = (noriSocket: ReconnectingWebSocketSubject<unknown>) =>
    getTopic$(noriSocket, 'timings.notices.transition').pipe(
        filter(
            (message): message is Extract<
                WebSocketServiceTopicSubscriptionMessage,
                { topic: 'timings.notices.transition' }
            > => message.topic === 'timings.notices.transition'
        ),
        shareReplay({ bufferSize: 1, refCount: true })
    );

/**
 * Returns an observable emitting the current Ethereum finality state.
 *
 * Filters for messages from the `state.eth` topic, discarding any with
 * `latest_finality_block_number` set to `'unknown'`.
 *
 * Emits the latest known finality slot and block number from the Ethereum network.
 * The stream is shared and replays the latest state to every subscriber.
 *
 * @param noriSocket Nori's reconnecting websocket.
 * @returns Observable emitting Ethereum finality state updates.
 */
export const ethStateTopic$ = (noriSocket: ReconnectingWebSocketSubject<unknown>) =>
    (
        getTopic$(noriSocket, 'state.eth').pipe(
            // Suppress events when the state is 'unknown'
            filter(
                (message) =>
                    message.topic === 'state.eth' &&
                    message.extension.latest_finality_block_number !== 'unknown'
            ),
            map((message) => message.extension)
        ) as Observable<{
            latest_finality_block_number: number;
            latest_finality_slot: number;
        }>
    ).pipe(shareReplay({ bufferSize: 1, refCount: true }));

/**
 * Returns an observable emitting the prover pipeline's transition notices,
 * as they happen (`notices.transition.*`).
 *
 * @param noriSocket Nori's reconnecting websocket.
 * @returns Observable emitting each transition notice.
 */
export const transitionNoticesTopic$ = (noriSocket: ReconnectingWebSocketSubject<unknown>) =>
    getTopic$(noriSocket, 'notices.transition.*') as Observable<AllTransitionNoticeMessages>;

/**
 * Returns an observable emitting Nori's services starting and heartbeating
 * (`notices.system.*`).
 *
 * @param noriSocket Nori's reconnecting websocket.
 * @returns Observable emitting each system notice.
 */
export const systemNoticesTopic$ = (noriSocket: ReconnectingWebSocketSubject<unknown>) =>
    getTopic$(noriSocket, 'notices.system.*') as Observable<AllSystemNoticeMessages>;
