import {
    EMPTY,
    exhaustMap,
    filter,
    from,
    fromEvent,
    interval,
    map,
    merge,
    type Observable,
    share,
    Subject,
} from 'rxjs';
import { NetworkGraph } from './network.js';

/** Endpoints probed for connectivity when the app gives none: any one answering means online. */
export const DEFAULT_CONNECTIVITY_PROBE_URLS = [
    'https://www.google.com/generate_204',
    'https://1.1.1.1/cdn-cgi/trace',
    'https://www.cloudflare.com/cdn-cgi/trace',
];

export interface NetworkOptions {
    /** Endpoints to probe; any one answering means online (default: `DEFAULT_CONNECTIVITY_PROBE_URLS`). */
    probeUrls?: string[];
    /** Delay between probes, online or offline, in ms (default: 15000). */
    probeIntervalMs?: number;
    /** Time a probe may take before it counts as failed, in ms (default: 5000). */
    probeTimeoutMs?: number;
}

/** What one connectivity probe found, and when. */
export interface Probe {
    answered: boolean;
    at: number;
}

/**
 * Whether any of `urls` answers within `timeoutMs`. Any response counts,
 * whatever its status: only a request that never gets one means offline.
 * `no-cors` lets a browser reach endpoints of another origin, where the
 * response is opaque but still a response.
 *
 * @param urls The endpoints to try.
 * @param timeoutMs How long each may take.
 * @returns Whether one answered, and when the probe finished.
 */
async function probe(urls: string[], timeoutMs: number): Promise<Probe> {
    const answered = await Promise.any(
        urls.map((url) =>
            fetch(url, {
                method: 'HEAD',
                mode: 'no-cors',
                cache: 'no-store',
                signal: AbortSignal.timeout(timeoutMs),
            })
        )
    ).then(
        () => true,
        () => false
    );
    return { answered, at: Date.now() };
}

/**
 * The browser's `online` or `offline` events, with when each arrived. In a
 * browser the global object is the window, an `EventTarget`; in Node it is
 * not, and there are no such events.
 *
 * @param type `online` or `offline`.
 * @returns The time of each event, in ms since the epoch.
 */
function browserEvent$(type: 'online' | 'offline'): Observable<number> {
    const target: unknown = globalThis;
    if (typeof EventTarget === 'undefined' || !(target instanceof EventTarget))
        return EMPTY;
    return fromEvent(target, type).pipe(map(() => Date.now()));
}

/**
 * Starts the network machine, which probes for connectivity itself, in a
 * browser or in Node.
 *
 * @param options The endpoints to probe and how often.
 * @returns
 *   - `network`: the running machine.
 *   - `close()`: moves the machine to `closed`.
 */
export function createNetworkMachine({
    probeUrls = DEFAULT_CONNECTIVITY_PROBE_URLS,
    probeIntervalMs = 15_000,
    probeTimeoutMs = 5000,
}: NetworkOptions = {}) {
    const close$ = new Subject<void>();
    const probe$ = () => from(probe(probeUrls, probeTimeoutMs));

    // `checking` probes once; its two outcome edges share that probe.
    const firstProbe$ = probe$().pipe(share());
    // `online` probes every interval; its two outcome edges share each probe.
    const backgroundProbe$ = interval(probeIntervalMs).pipe(
        exhaustMap(probe$),
        share()
    );

    const machine = NetworkGraph.implement({
        probePassed: {
            $: () => firstProbe$.pipe(filter(({ answered }) => answered)),
            next: ({ at }) => ({ checkedAt: at }),
        },
        probeFailed: {
            $: () => firstProbe$.pipe(filter(({ answered }) => !answered)),
            next: ({ at }) => ({ since: at }),
        },
        backgroundProbePassed: {
            $: () => backgroundProbe$.pipe(filter(({ answered }) => answered)),
            next: ({ at }) => ({ checkedAt: at }),
        },
        connectivityLost: {
            $: () =>
                merge(
                    backgroundProbe$.pipe(
                        filter(({ answered }) => !answered),
                        map(({ at }) => at)
                    ),
                    browserEvent$('offline')
                ),
            next: (since) => ({ since }),
        },
        connectivityRestored: {
            $: () =>
                merge(interval(probeIntervalMs), browserEvent$('online')).pipe(
                    exhaustMap(probe$),
                    filter(({ answered }) => answered)
                ),
            next: ({ at }) => ({ checkedAt: at }),
        },
        close: {
            $: () => close$,
            next: () => ({}),
        },
    });

    const network = machine.close().start('checking');
    return { network, close: () => close$.next() };
}

/** The running network machine and its `close()`. */
export type NetworkMachine = ReturnType<typeof createNetworkMachine>;
