import {
    combineLatest,
    distinctUntilChanged,
    interval,
    map,
    type Observable,
    shareReplay,
    switchMap,
} from 'rxjs';
import { type ReconnectingWebSocketSubject } from '../connection/websocket.js';
import { bridgeStateTopic$, bridgeTimingsTopic$ } from './topics.js';

/** A value and when it arrived, in ms since the epoch: a time in it is true as of then. */
export interface Arrived<T> {
    value: T;
    atMs: number;
}

/**
 * Each value stamped once, as it arrives, with the latest replayed: a
 * replay keeps the time the value arrived, so a time in it (`elapsed_sec`)
 * is aged from then, never from when a subscriber joined.
 *
 * @param source$ The values.
 * @returns Each value and when it arrived.
 */
export const arrived = <T>(source$: Observable<T>): Observable<Arrived<T>> =>
    source$.pipe(
        map((value) => ({ value, atMs: Date.now() })),
        shareReplay({ bufferSize: 1, refCount: true })
    );

export const bridgeStateWithTimings$ = (
    noriSocket: ReconnectingWebSocketSubject<unknown>
) =>
    // Ensure both topics have fired and merge them into a single observable
    combineLatest([
        bridgeStateTopic$(noriSocket),
        bridgeTimingsTopic$(noriSocket),
    ]).pipe(
        // Supress bridgeTimingsTopic$ changes until bridgeStateTopic$ changes.
        distinctUntilChanged(
            (prev, curr) => JSON.stringify(prev[0]) === JSON.stringify(curr[0])
        ),
        // Calculate the time remaining
        map(([bridgeState, bridgeTimings]) => {
            // FIXME this mixed casing is awful.
            const { stage_name, elapsed_sec } = bridgeState;
            const expectedDuration = bridgeTimings.extension[stage_name];
            let timeRemaining = expectedDuration - elapsed_sec;
            return { bridgeState, timeRemaining };
        }),
        // Emit bridgeState with time_remaining_sec and elapsed_sec countdown.
        switchMap(({ bridgeState, timeRemaining }) => {
            return interval(1000).pipe(
                map((elapsedSeconds) => ({
                    ...bridgeState,
                    time_remaining_sec: timeRemaining - elapsedSeconds,
                    elapsed_sec: elapsedSeconds,
                }))
            );
        })
    );
