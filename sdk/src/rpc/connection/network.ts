import { define, type StateUnion } from '@yaw-rx/ystate';

/**
 * Whether the device can reach the internet, in a browser or in Node.
 *
 * It probes for itself: a request to a few well-known endpoints, where any
 * one answering counts as online. Browser events only make it probe sooner
 * (`online`) or go offline at once (`offline`); they are not trusted alone,
 * because a browser reports being online on a network with no internet.
 *
 * - `checking` probes once on start.
 * - `online` probes in the background every interval. A passing probe is
 *   the `stillOnline` self-loop, carrying when it was checked. A failing one,
 *   or the browser's `offline` event, is `wentOffline`.
 * - `offline` carries when it began, for the app's "you're offline"
 *   message, and probes every interval, and at once on the browser's
 *   `online` event, until a probe passes.
 * - `closed` ends the machine from any node and completes its streams.
 *
 * Both endpoint connectivity machines follow it: going `offline` pauses their
 * health checks instead of letting them fail and back off, and coming back
 * `online` checks every endpoint at once.
 */
export const NetworkGraph = define({
    nodes: {
        checking: {},
        online: { checkedAt: 0 },
        offline: { since: 0 },
        closed: {},
    },
    edges: {
        connectivityConfirmed: {
            from: 'checking',
            to: 'online',
            on: 'probePassed.next',
        },
        noConnectivity: {
            from: 'checking',
            to: 'offline',
            on: 'probeFailed.next',
        },
        stillOnline: {
            from: 'online',
            to: 'online',
            on: 'backgroundProbePassed.next',
        },
        wentOffline: {
            from: 'online',
            to: 'offline',
            on: 'connectivityLost.next',
        },
        cameOnline: {
            from: 'offline',
            to: 'online',
            on: 'connectivityRestored.next',
        },
        closedWhileChecking: {
            from: 'checking',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileOnline: {
            from: 'online',
            to: 'closed',
            on: 'close.next',
        },
        closedWhileOffline: {
            from: 'offline',
            to: 'closed',
            on: 'close.next',
        },
    },
});

/** The network's state: a node of the graph and its data. */
export type NetworkState = StateUnion<typeof NetworkGraph.nodes>;
