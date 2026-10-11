import { define, type StateUnion } from '@yaw-rx/ystate';
import { type SubscriptionTransport } from './connections.js';

/** What every live node carries: the changes counted so far, and the transports that refused to subscribe. */
const counted = { changes: 0, unsupported: [] as SubscriptionTransport[] };

/**
 * Whether a chain has changed, for the machines to read again: on Ethereum
 * a new block, or a log matching a filter; on Solana a new finalized root,
 * or a finalized transaction mentioning an address. Each change counts one
 * in `changes`; nothing else of a block, log or transaction is kept.
 *
 * - `subscribed`: subscribed on `transport`, the first transport in the
 *   chain's subscriptions order that is usable and not in `unsupported`
 *   (Solana's only one is its websocket). The node's first push
 *   acknowledges the subscription (`acknowledged`); each push after it is
 *   a change. Another transport becoming the first moves it there
 *   (`transportChanged`), and counts a change.
 * - A transport that refuses the subscription (a wallet without
 *   `eth_subscribe`, a node refusing the filter) joins `unsupported`
 *   (`subscriptionRefused`). A subscription that ends any other way (the
 *   socket closed, nothing to send it through) is lost
 *   (`subscriptionLost`): it polls, holding the transport in `lostOn`, and
 *   subscribes on it again only once it has been unusable and is usable
 *   again; another usable transport is subscribed on at once.
 * - `polling`: no transport can subscribe; it polls through the chain's
 *   reads every interval, and waits in `polling` while the chain cannot
 *   serve a read. `lastPolled` is the last block (on Solana, the last
 *   finalized slot) polled; the first poll only sets it. A new block is a
 *   change, or with a filter, a new block holding a matching log (on Solana,
 *   a transaction mentioning the address in a slot after it). On Ethereum,
 *   after a gap longer than one log query, it moves `lastPolled` to the
 *   latest block and counts one change instead of reading the gap.
 * - The machine follows the chain once subscribed and acknowledged, or once
 *   its first poll has read the latest block or slot. Moving between
 *   subscribing and polling counts a change once it has, as a change may fall
 *   between the two.
 * - `pollFailed`: a poll failed; it polls again after the interval, or
 *   subscribes once a transport can.
 * - It starts `polling` with no `lastPolled`, and subscribes at once when a
 *   transport can. `closed` is terminal.
 */
export const ChainChangesGraph = define({
    nodes: {
        subscribed: { ...counted, transport: 'websocket' as SubscriptionTransport, acknowledged: false },
        polling: {
            ...counted,
            lastPolled: undefined as number | undefined,
            lostOn: undefined as SubscriptionTransport | undefined,
        },
        pollFailed: {
            ...counted,
            lastPolled: undefined as number | undefined,
            lostOn: undefined as SubscriptionTransport | undefined,
            error: '',
        },
        closed: {},
    },
    edges: {
        pushed: { from: 'subscribed', to: 'subscribed', on: 'pushes.next' },
        subscriptionRefused: { from: 'subscribed', to: 'polling', on: 'pushes.error' },
        subscriptionLost: { from: 'subscribed', to: 'polling', on: 'pushes.complete' },
        transportChanged: { from: 'subscribed', to: 'subscribed', on: 'subscribable.next' },
        transportLost: { from: 'subscribed', to: 'polling', on: 'unsubscribable.next' },

        polled: { from: 'polling', to: 'polling', on: 'polls.next' },
        pollFailed: { from: 'polling', to: 'pollFailed', on: 'polls.error' },
        transportReady: { from: 'polling', to: 'subscribed', on: 'subscribable.next' },

        pollAgain: { from: 'pollFailed', to: 'polling', on: 'pollDue.next' },
        transportReadyAfterFailedPoll: { from: 'pollFailed', to: 'subscribed', on: 'subscribable.next' },

        closedWhileSubscribed: { from: 'subscribed', to: 'closed', on: 'close.next' },
        closedWhilePolling: { from: 'polling', to: 'closed', on: 'close.next' },
        closedAfterFailedPoll: { from: 'pollFailed', to: 'closed', on: 'close.next' },
    },
});

/** The chain changes machine's state: a node of the graph and its data. */
export type ChainChangesState = StateUnion<typeof ChainChangesGraph.nodes>;
