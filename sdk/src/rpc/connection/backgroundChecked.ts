/**
 * A connection that only finds out it went away by asking: while `ready`, it
 * checks the endpoint again every interval, through its own three outcome
 * edges rather than going back through `connecting`, so reads never pause
 * for routine checks. Graphs spread these edges next to `connectionOf`'s,
 * whose nodes they move between: the HTTP connection and the wallet. A
 * websocket does not: its socket tells it when it drops.
 *
 * - A passing check is the `stillReady` self-loop, emitting once per
 *   interval with what the check found and when.
 * - A check finding another network is `switchedToWrongNetwork`.
 * - A failed check is `stoppedAnswering`, to `waitingToRetry`.
 */
export const backgroundCheckedEdges = {
    stillReady: { from: 'ready', to: 'ready', on: 'backgroundCheckPassed.next' },
    switchedToWrongNetwork: { from: 'ready', to: 'wrongNetwork', on: 'backgroundCheckFoundOtherNetwork.next' },
    stoppedAnswering: { from: 'ready', to: 'waitingToRetry', on: 'backgroundCheckFailed.next' },
} as const;
