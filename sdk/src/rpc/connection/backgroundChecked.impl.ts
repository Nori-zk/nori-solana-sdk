import { type Observable } from 'rxjs';
import { withOutcome } from '../../utils/machines.js';
import { type HealthCheck } from './connection.impl.js';

/** What the transitions of a graph spreading `backgroundCheckedEdges` react to. */
export interface BackgroundCheckedTransitionsOptions<THealth, TCarried extends object> {
    /** One check an interval after each entry into `ready`, shared by its outcome edges. */
    backgroundCheck$: Observable<HealthCheck<THealth>>;
    /** The data every live node carries besides its own, from the node it leaves. */
    carry: (source: TCarried) => TCarried;
}

/**
 * The transitions of a graph spreading `backgroundCheckedEdges`: the
 * background check's three outcomes, each carrying the data every live node
 * carries (`carry`). Spread into the graph's `implement`, next to
 * `connectionTransitions`.
 *
 * @param options The background check and what every live node carries.
 * @returns The transitions, by name.
 */
export function backgroundCheckedTransitions<THealth, TCarried extends object>(
    options: BackgroundCheckedTransitionsOptions<THealth, TCarried>
) {
    const { backgroundCheck$, carry } = options;
    return {
        backgroundCheckPassed: {
            $: () => withOutcome(backgroundCheck$, 'onExpectedNetwork'),
            next: (
                { url, health, checkedAt }: Extract<HealthCheck<THealth>, { outcome: 'onExpectedNetwork' }>,
                _dest: unknown,
                source: TCarried
            ) => ({ ...carry(source), url, health, checkedAt }),
        },
        backgroundCheckFoundOtherNetwork: {
            $: () => withOutcome(backgroundCheck$, 'onOtherNetwork'),
            next: (
                { url, found, expected }: Extract<HealthCheck<THealth>, { outcome: 'onOtherNetwork' }>,
                _dest: unknown,
                source: TCarried
            ) => ({ ...carry(source), url, found, expected, failedChecks: 1 }),
        },
        backgroundCheckFailed: {
            $: () => withOutcome(backgroundCheck$, 'failed'),
            next: (
                { url, error }: Extract<HealthCheck<THealth>, { outcome: 'failed' }>,
                _dest: unknown,
                source: TCarried
            ) => ({ ...carry(source), url, failedChecks: 1, error }),
        },
    };
}
