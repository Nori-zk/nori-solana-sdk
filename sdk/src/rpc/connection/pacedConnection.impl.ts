import { filter, type Observable, switchMap, take } from 'rxjs';
import { dataOnEntry$, type GraphState } from '../../utils/machines.js';
import { type ConnectionLoad, type GrantedClients } from './connectionGrants.js';

/** What the transitions of a graph spreading `pacedOf` react to. */
export interface PacedTransitionsOptions<TServing extends object> {
    /** The connection's clients over its grants, whose count and rate it follows. */
    granted: GrantedClients<unknown>;
    /** The machine's states, from `stateOf$`. */
    state$: Observable<GraphState>;
    /** The endpoint a serving node serves, whose grants it follows (default: its `url`). */
    endpointOf?: (serving: TServing) => string;
}

/**
 * The transitions of a graph spreading `pacedOf`: the grants of the
 * endpoint the connection serves limiting the rate, changing the limit,
 * and lifting it. Each carries the serving node's data (`TServing`, what
 * `ready` carries) as it is. Spread into the graph's `implement` next to
 * `connectionTransitions`, whose failed read, going offline and closing
 * serve `overwhelmed`'s edges too.
 *
 * @param options The grants, the machine's states, and which endpoint a node serves.
 * @returns The transitions, by name.
 */
export function pacedTransitions<TServing extends object>(options: PacedTransitionsOptions<TServing>) {
    const { granted, state$ } = options;
    const endpointOf = options.endpointOf ?? ((serving: TServing) => (serving as unknown as { url: string }).url);
    type Overwhelmed = TServing & { ratePerSecond: number };
    return {
        // The grants limit the rate once the endpoint turns requests away for too many.
        overwhelmed: {
            $: (): Observable<ConnectionLoad> =>
                dataOnEntry$(state$, 'ready').pipe(
                    switchMap((data) =>
                        granted
                            .loadOf(endpointOf(data as TServing))
                            .pipe(filter(({ ratePerSecond }) => ratePerSecond !== Infinity))
                    )
                ),
            next: ({ ratePerSecond }: ConnectionLoad, _dest: unknown, source: TServing) => ({
                ...source,
                ratePerSecond,
            }),
        },
        // Each new limit, against the one the node holds, so entering it again does not fire it again.
        rateChanged: {
            $: (): Observable<number> =>
                dataOnEntry$(state$, 'overwhelmed').pipe(
                    switchMap((data) =>
                        granted.rateOf(endpointOf(data as TServing)).pipe(
                            filter((rate) => rate !== Infinity && rate !== (data as Overwhelmed).ratePerSecond),
                            take(1)
                        )
                    )
                ),
            next: (ratePerSecond: number, _dest: unknown, source: Overwhelmed) => ({ ...source, ratePerSecond }),
        },
        relieved: {
            $: (): Observable<ConnectionLoad> =>
                dataOnEntry$(state$, 'overwhelmed').pipe(
                    switchMap((data) =>
                        granted
                            .loadOf(endpointOf(data as TServing))
                            .pipe(filter(({ ratePerSecond }) => ratePerSecond === Infinity))
                    )
                ),
            next: (_load: ConnectionLoad, _dest: unknown, { ratePerSecond: _limit, ...serving }: Overwhelmed) =>
                serving,
        },
    };
}
