/**
 * A connection whose requests go through grants (`grantedClients`): every
 * request to its endpoint, whatever it is for, takes a turn from them, and
 * the grants work out the rate the endpoint takes. Graphs spread its node
 * and edges next to the shape whose `ready` serves requests
 * (`connectionOf`).
 *
 * - Once the endpoint turns requests away for too many requests, the grants
 *   limit the rate and `ready` moves to `overwhelmed` (`becameOverwhelmed`),
 *   which still serves requests, at that rate (`ratePerSecond`), round robin
 *   across the machines sending them.
 * - Each change of the limit is a move to itself (`rateChanged`).
 * - Once the grants lift the limit, it goes back to `ready` (`relieved`).
 * - As from `ready`, a failed read checks again, going offline pauses it,
 *   and closing ends it.
 *
 * @param serving The data `ready` carries, which `overwhelmed` carries too.
 * @returns The node and edges.
 */
export const pacedOf = <TServing extends object>(serving: TServing) => ({
    nodes: {
        overwhelmed: {
            ...serving,
            /** The requests a second its grants let through. */
            ratePerSecond: 0,
        },
    },
    edges: {
        becameOverwhelmed: { from: 'ready', to: 'overwhelmed', on: 'overwhelmed.next' },
        rateChanged: { from: 'overwhelmed', to: 'overwhelmed', on: 'rateChanged.next' },
        relieved: { from: 'overwhelmed', to: 'ready', on: 'relieved.next' },
        recheckStartedWhileOverwhelmed: { from: 'overwhelmed', to: 'connecting', on: 'readFailed.next' },
        wentOfflineWhileOverwhelmed: { from: 'overwhelmed', to: 'offline', on: 'networkWentOffline.next' },
        closedWhileOverwhelmed: { from: 'overwhelmed', to: 'closed', on: 'close.next' },
    } as const,
});
