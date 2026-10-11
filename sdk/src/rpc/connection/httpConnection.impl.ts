import {
    distinctUntilChanged,
    filter,
    map,
    type Observable,
    of,
    ReplaySubject,
    scan,
    startWith,
    switchMap,
    take,
    timer,
} from 'rxjs';
import { type RunningMachine } from '@yaw-rx/ystate';
import { backgroundCheckedTransitions } from './backgroundChecked.impl.js';
import {
    connectionTransitions,
    type HealthCheck,
    wrongNetworkTransitions,
    settledHealthCheck$,
} from './connection.impl.js';
import { type GrantedClients, grantedClients, type GrantedTransport } from './connectionGrants.js';
import { pacedTransitions } from './pacedConnection.impl.js';
import {
    dataOnEntry$,
    heldAcrossMoves,
    requestOnEntry$,
    stateOf$,
    type StartedMachine,
} from '../../utils/machines.js';
import { type HealthCheckTimings, resolveHealthCheckTimings, retryDelayMs } from './healthCheckTimings.js';
import { HttpConnectionGraph, type HttpConnectionState } from './httpConnection.js';

/** Everything outside the graph that an HTTP connection reacts to. */
export interface HttpConnectionEnvironment<THealth, TClient> extends HealthCheckTimings {
    /** The endpoint's URLs, in the order they are tried. */
    urls: string[];
    /**
     * Checks one URL once, through the client it is given. Emits what it
     * found on a network it reached; errors when it cannot be reached.
     */
    checkHealth: (client: TClient, url: string) => Observable<Exclude<HealthCheck<THealth>, { outcome: 'failed' }>>;
    /** The network machine went offline. */
    networkWentOffline$: Observable<unknown>;
    /** The network machine came back online. */
    networkCameOnline$: Observable<unknown>;
    /** A read against the endpoint failed to reach it. */
    readFailed$: Observable<unknown>;
    /** The transport's clients over the connection's grants, which every request to the endpoint goes through. */
    granted: GrantedClients<TClient>;
    /** The owner is closing the connection. */
    close$: Observable<unknown>;
    /** The running machine's states, from `stateOf$`. */
    connection$: Observable<HttpConnectionState<THealth>>;
    /** The running machine, once started: the subscriber its own health checks take their turns as. */
    started$: Observable<RunningMachine>;
}

/**
 * Implements the HTTP connection graph over its environment: health checks
 * with a timeout, moving on to the next URL after each failed check,
 * background checks while ready, retries with backoff while waiting to
 * retry or on the wrong network, going offline and back, read failures and
 * closing.
 *
 * @param environment The URLs, the health check, the clients over the
 *   grants, the network, read and close signals, the machine's own states
 *   and the timings.
 * @returns The implemented YState machine; `.close().start('connecting')` runs it.
 */
export function createHttpConnectionMachine<THealth, TClient>(environment: HttpConnectionEnvironment<THealth, TClient>) {
    const { urls, granted } = environment;
    const timings = resolveHealthCheckTimings(environment);
    // The connection's own health checks take its turns from its grants, like every other machine's
    // requests; each request times out from when it is sent, never while it waits for its turn.
    const check$ = (url: string) =>
        settledHealthCheck$(
            environment.started$.pipe(
                take(1),
                switchMap((machine) =>
                    environment.checkHealth(granted.clientOf(url, machine, timings.healthCheckTimeoutMs), url)
                )
            ),
            url
        );

    // Seconds in a row, while overwhelmed on one URL, in which every request sent was turned away; a second
    // with an answer starts it again. Kept while `rateChanged` moves the machine to `overwhelmed` again.
    const turnedAwayFor$: Observable<number> = environment.connection$.pipe(
        map((state) => (state.node === 'overwhelmed' ? state.data.url : undefined)),
        distinctUntilChanged(),
        switchMap((url) =>
            url === undefined
                ? of(0)
                : granted.loadOf(url).pipe(
                      scan(
                          (seconds, { total, tooMany }) =>
                              total === 0 ? seconds : tooMany === total ? seconds + 1 : 0,
                          0
                      ),
                      startWith(0)
                  )
        ),
        heldAcrossMoves<number>({ replayLatest: true })
    );

    // One check per entry into `connecting`, shared by its outcome edges. The URL checked is chosen by how many
    // checks have failed in a row: each failure moves on to the next.
    const connectingCheck$ = requestOnEntry$(environment.connection$, 'connecting', ({ failedChecks }) =>
        check$(urls[failedChecks % urls.length])
    );

    return HttpConnectionGraph.implement({
        ...pacedTransitions<{ url: string; checkedAt: number; health: unknown }>({
            granted,
            state$: environment.connection$,
        }),
        ...backgroundCheckedTransitions<THealth, object>({
            // Background checks while ready stay on the URL that passed.
            backgroundCheck$: requestOnEntry$(environment.connection$, 'ready', ({ url }) =>
                timer(timings.healthCheckIntervalMs).pipe(switchMap(() => check$(url)))
            ),
            carry: () => ({}),
        }),
        ...wrongNetworkTransitions<THealth, object>({ check$: connectingCheck$, carry: () => ({}) }),
        ...connectionTransitions<THealth, object>({
            check$: connectingCheck$,
            readFailed$: environment.readFailed$,
            networkWentOffline$: environment.networkWentOffline$,
            networkCameOnline$: environment.networkCameOnline$,
            close$: environment.close$,
            state$: environment.connection$,
            timings,
            carry: () => ({}),
        }),
        recheckDue: {
            $: () =>
                dataOnEntry$<HttpConnectionState<THealth>, 'wrongNetwork'>(environment.connection$, 'wrongNetwork').pipe(
                    switchMap(({ failedChecks }) => timer(retryDelayMs(failedChecks, timings)))
                ),
            next: (_due, _dest, source) => ({ failedChecks: source.failedChecks }),
        },
        // With several URLs, one that has answered nothing for a check's timeout while turning everything away is left.
        turnedAwayTooLong: {
            $: () =>
                turnedAwayFor$.pipe(
                    filter((seconds) => urls.length > 1 && seconds * 1000 >= timings.healthCheckTimeoutMs),
                    take(1)
                ),
            // The URL checked is chosen by `failedChecks`: one past the URL left is the next one.
            next: (_seconds, _dest, source) => ({ failedChecks: urls.indexOf(source.url) + 1 }),
        },
    });
}

/**
 * Starts an HTTP connection machine at `connecting`, its states typed with
 * the use's health: the graph declares `ready.health` as `unknown`, and
 * every health this machine carries comes from `checkHealth`, which returns
 * `THealth`.
 *
 * @param environment Everything but the machine itself, its states and its granted clients, which it is given here,
 *   and the transport they are made from.
 * @returns The running machine, with:
 *   - `clientOf(url, subscriber)`: the transport's client for the URL, every request of which takes the subscriber's turn.
 *   - `load$`: each second's count of the requests to the endpoint, and the rate after it.
 */
export function httpConnection<THealth, TClient>(
    environment: Omit<HttpConnectionEnvironment<THealth, TClient>, 'connection$' | 'started$' | 'granted'> & {
        transport: GrantedTransport<TClient>;
    }
) {
    const started$ = new ReplaySubject<StartedMachine<HttpConnectionState<THealth>>>(1);
    const machine$ = new ReplaySubject<RunningMachine>(1);
    const granted = grantedClients(environment.transport, environment.quietSecondsToLift);
    const connection$ = stateOf$(started$);
    const running = createHttpConnectionMachine<THealth, TClient>({
        ...environment,
        granted,
        connection$,
        started$: machine$,
    })
        .close()
        .start('connecting');
    const connection = running as Omit<typeof running, 'state$'> & {
        state$: Observable<HttpConnectionState<THealth>>;
    };
    started$.next(connection);
    machine$.next(running);

    // The URL the connection serves, as it changes.
    const servedUrl$ = connection$.pipe(
        filter((state) => state.node === 'ready' || state.node === 'overwhelmed'),
        map(({ data }) => data.url),
        distinctUntilChanged()
    );
    // Requests still waiting at a URL the connection left are never sent there: they fail as not reaching the node,
    // and their reads run again on the URL it serves now.
    const leaving = servedUrl$
        .pipe(
            scan<string, { left?: string; now?: string }>(({ now }, url) => ({ left: now, now: url }), {})
        )
        .subscribe(({ left }) => {
            if (left !== undefined) granted.release(left, 'the connection moved on from its URL.');
        });
    // The grants end with the machine.
    running.status$
        .pipe(
            filter((status) => status !== 'running'),
            take(1)
        )
        .subscribe(() => {
            leaving.unsubscribe();
            granted.close();
        });
    return Object.assign(connection, {
        clientOf: granted.clientOf,
        /** Each second's count at the URL the connection serves, and the rate after it. */
        load$: servedUrl$.pipe(switchMap((url) => granted.loadOf(url))),
    });
}

/** A running HTTP connection machine, its states typed with the use's health, its clients the transport's. */
export type HttpConnection<THealth, TClient> = ReturnType<typeof httpConnection<THealth, TClient>>;
