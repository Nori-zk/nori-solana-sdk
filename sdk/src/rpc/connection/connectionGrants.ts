import { type RunningMachine } from '@yaw-rx/ystate';
import {
    BehaviorSubject,
    catchError,
    defer,
    distinctUntilChanged,
    EMPTY,
    finalize,
    interval,
    map,
    type Observable,
    of,
    share,
    Subject,
    Subscription,
    switchMap,
    take,
    throwError,
    timeout,
    timer,
} from 'rxjs';

/** The HTTP status an endpoint turns a request away with when it gets too many. */
export const TOO_MANY_REQUESTS = 429;

/** What went to an endpoint through its connection in one second, and the rate after it. */
export interface ConnectionLoad {
    /** The requests each subscriber sent, by subscriber. */
    perSubscriber: Map<RunningMachine, number>;
    /** The requests all subscribers sent, those turned away included. */
    total: number;
    /** The requests the endpoint turned away for too many requests. */
    tooMany: number;
    /** The requests still waiting for their turn at the end of the second. */
    queued: number;
    /** The rate the grants let requests through after this second; `Infinity` while unlimited. */
    ratePerSecond: number;
}

/** How many seconds in a row with nothing waiting and nothing turned away lift a limit. */
export const QUIET_SECONDS_TO_LIFT = 30;

/**
 * A request that was never sent to its endpoint: its connection left the
 * endpoint, or closed, while it waited for its turn. It never reached the
 * node, so the runner tells the connection and the read waits for it.
 */
export class RequestNotSentError extends Error {
    constructor(reason: string) {
        super(`The request was never sent: ${reason}`);
        this.name = 'RequestNotSentError';
    }
}

/**
 * How long an endpoint asked to be left alone, from a `Retry-After` value:
 * delay seconds or an HTTP date.
 *
 * @param value The header's value, if there is one.
 * @returns The wait in ms, or `undefined` when it gave none.
 */
export function retryAfterMsOf(value: string | null | undefined): number | undefined {
    if (value === null || value === undefined || value.trim() === '') return undefined;
    if (/^\d+$/.test(value.trim())) return Number(value.trim()) * 1000;
    const at = Date.parse(value);
    return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now());
}

/** How one request came back, as its transport tells it. */
export type RequestResult<T> = { answered: T } | { failed: unknown };

/** One request waiting for its subscriber's turn. */
interface Waiting {
    subscriber: RunningMachine;
    granted: Subject<void>;
}

/**
 * An http connection's grants: every request to its endpoint takes one,
 * whichever running machine sends it (its subscriber) and whatever the
 * request is for (a first read, a refresh, a retry, a poll, a send).
 *
 * The grants work out the rate the endpoint takes from what it answers,
 * counting each second what went and what it turned away for too many
 * requests (which the transport tells, from the endpoint's HTTP status):
 *
 * - Unlimited, a request goes at once.
 * - A second with requests turned away limits the rate: to what got through
 *   that second, or, already limited, to half the limit.
 * - A limited second with requests still waiting and none turned away raises
 *   the limit by one; `quietSecondsToLift` seconds in a row with none waiting
 *   and none turned away lift it.
 * - A request turned away with a `Retry-After` pauses every grant until it
 *   has passed.
 *
 * Limited, the requests go `ratePerSecond$` a second, one from each waiting
 * subscriber in turn (round robin). A request turned away waits for its
 * subscriber's next turn and is sent again, so its sender never sees it;
 * unlimited, those wait for the next second rather than going again at
 * once. Each second's count (`load$`) is what the connection's machine
 * reads.
 *
 * @param quietSecondsToLift How many seconds in a row with nothing waiting and nothing turned away lift a limit.
 * @returns
 *   - `send$(subscriber, request, turnedAway, retryAfterMs)`: the request, sent on the subscriber's turn and again
 *     each time `turnedAway` says the endpoint turned it away, after any wait it asked for; its answer, or its failure.
 *   - `load$`: each second's count, and the rate after it.
 *   - `ratePerSecond$`: the limit, as it changes; `Infinity` while unlimited.
 *   - `release(reason)`: fails every request still waiting (`RequestNotSentError`).
 *   - `close()`: stops granting, and releases every request still waiting.
 */
function connectionGrants(quietSecondsToLift: number) {
    // Each subscriber's waiting requests, oldest first; the map's order is the round robin's.
    const queues = new Map<RunningMachine, Waiting[]>();
    const rate$ = new BehaviorSubject(Infinity);
    let lastServed: RunningMachine | undefined;
    let second: { subscriber: RunningMachine; tooMany: boolean }[] = [];
    // Until when the endpoint asked for nothing more (its `Retry-After`), and whether the grants closed.
    let pausedUntil = 0;
    let closed = false;
    const resuming = new Subscription();
    const paused = () => Date.now() < pausedUntil;

    const enqueue = (waiting: Waiting, atFront: boolean) => {
        const queue = queues.get(waiting.subscriber) ?? [];
        queues.set(waiting.subscriber, atFront ? [waiting, ...queue] : [...queue, waiting]);
    };
    const remove = (waiting: Waiting) => {
        const queue = queues.get(waiting.subscriber)?.filter((other) => other !== waiting) ?? [];
        if (queue.length === 0) queues.delete(waiting.subscriber);
        else queues.set(waiting.subscriber, queue);
    };
    const queued = () => [...queues.values()].reduce((count, queue) => count + queue.length, 0);
    /** Grants the next subscriber after the one served last its oldest waiting request. */
    const grantNext = () => {
        const subscribers = [...queues.keys()];
        if (subscribers.length === 0 || paused()) return;
        const subscriber = subscribers[(lastServed === undefined ? 0 : subscribers.indexOf(lastServed) + 1) % subscribers.length];
        lastServed = subscriber;
        const [waiting] = queues.get(subscriber) ?? [];
        if (waiting === undefined) return;
        remove(waiting);
        waiting.granted.next();
    };

    // Seconds in a row, while limited, with nothing waiting and nothing turned away.
    let quietSeconds = 0;
    /**
     * The limit after a second: set by requests turned away, raised by one
     * each second requests wait and none is turned away, and lifted only once
     * `QUIET_SECONDS_TO_LIFT` seconds in a row have had nothing waiting and
     * nothing turned away, so a burst after a lull is paced at the rate
     * learned rather than turned away again.
     */
    const nextRate = (rate: number, sent: number, tooMany: number, waiting: number): number => {
        if (tooMany > 0) {
            quietSeconds = 0;
            return rate === Infinity ? Math.max(1, sent - tooMany) : Math.max(1, Math.floor(rate / 2));
        }
        if (rate === Infinity) return rate;
        if (waiting > 0) {
            quietSeconds = 0;
            return rate + 1;
        }
        quietSeconds++;
        return quietSeconds >= quietSecondsToLift ? Infinity : rate;
    };

    // Each second: count what went, work out the rate, and, unlimited, send again those turned away.
    const load$: Observable<ConnectionLoad> = interval(1000).pipe(
        map(() => {
            const counted = second;
            second = [];
            const tooMany = counted.filter((sent) => sent.tooMany).length;
            const waiting = queued();
            rate$.next(nextRate(rate$.value, counted.length, tooMany, waiting));
            if (rate$.value === Infinity) for (let turn = 0; turn < waiting; turn++) grantNext();
            return {
                perSubscriber: counted.reduce(
                    (counts, { subscriber }) => counts.set(subscriber, (counts.get(subscriber) ?? 0) + 1),
                    new Map<RunningMachine, number>()
                ),
                total: counted.length,
                tooMany,
                queued: waiting,
                ratePerSecond: rate$.value,
            };
        }),
        share()
    );
    const counting: Subscription = load$.subscribe();

    // Limited, one request each tick of the rate.
    const granting: Subscription = rate$
        .pipe(
            distinctUntilChanged(),
            switchMap((rate) => (rate === Infinity ? EMPTY : interval(1000 / rate)))
        )
        .subscribe(() => grantNext());

    /**
     * The subscriber's turn: at once while unlimited and not paused for a
     * request not turned away before, else when it comes; never once the
     * grants have closed.
     */
    const turn$ = (subscriber: RunningMachine, turnedAwayBefore: boolean): Observable<void> =>
        defer(() => {
            if (closed) return throwError(() => new RequestNotSentError('its connection closed.'));
            if (rate$.value === Infinity && !turnedAwayBefore && !paused()) return of(undefined);
            const waiting: Waiting = { subscriber, granted: new Subject<void>() };
            enqueue(waiting, turnedAwayBefore);
            return waiting.granted.pipe(
                take(1),
                finalize(() => remove(waiting))
            );
        });

    /**
     * Grants nothing until `waitMs` from now, as the endpoint asked; then,
     * unlimited, every waiting request goes, and limited, the ticks go on.
     */
    const pauseFor = (waitMs: number) => {
        const until = Date.now() + waitMs;
        if (until <= pausedUntil) return;
        pausedUntil = until;
        resuming.add(
            timer(waitMs).subscribe(() => {
                if (rate$.value !== Infinity) return;
                for (let turn = queued(); turn > 0; turn--) grantNext();
            })
        );
    };

    // Requests waiting or in flight, by what they ask: an identical one joins rather than takes its own turn.
    const sharedByKey = new Map<string, Observable<unknown>>();

    /**
     * The request, coalesced by `key` when given: while one asking the same
     * waits or is in flight, the same answer is shared, on the turns of the
     * subscriber that asked first.
     */
    const send$ = <T>(
        subscriber: RunningMachine,
        request: () => Observable<T>,
        turnedAway: (outcome: RequestResult<T>) => boolean,
        retryAfterMs: (outcome: RequestResult<T>) => number | undefined = () => undefined,
        key?: string
    ): Observable<T> => {
        if (key === undefined) return sendOnce$(subscriber, request, turnedAway, retryAfterMs);
        const joined = sharedByKey.get(key) as Observable<T> | undefined;
        if (joined !== undefined) return joined;
        const shared = sendOnce$(subscriber, request, turnedAway, retryAfterMs).pipe(
            finalize(() => {
                if (sharedByKey.get(key) === shared) sharedByKey.delete(key);
            }),
            share()
        );
        sharedByKey.set(key, shared);
        return shared;
    };

    const sendOnce$ = <T>(
        subscriber: RunningMachine,
        request: () => Observable<T>,
        turnedAway: (outcome: RequestResult<T>) => boolean,
        retryAfterMs: (outcome: RequestResult<T>) => number | undefined
    ): Observable<T> => {
        const attempt = (turnedAwayBefore: boolean): Observable<T> =>
            turn$(subscriber, turnedAwayBefore).pipe(
                switchMap(() =>
                    request().pipe(
                        map((answered): RequestResult<T> => ({ answered })),
                        catchError((failed: unknown) => of<RequestResult<T>>({ failed }))
                    )
                ),
                switchMap((outcome) => {
                    const tooMany = turnedAway(outcome);
                    second.push({ subscriber, tooMany });
                    if (!tooMany) return 'answered' in outcome ? of(outcome.answered) : throwError(() => outcome.failed);
                    const waitMs = retryAfterMs(outcome);
                    if (waitMs !== undefined && waitMs > 0) pauseFor(waitMs);
                    return attempt(true);
                })
            );
        return attempt(false);
    };

    /** Fails every request still waiting for its turn: it is never sent, and fails rather than hang. */
    const release = (reason: string) => {
        for (const queue of [...queues.values()])
            for (const waiting of queue) waiting.granted.error(new RequestNotSentError(reason));
    };

    return {
        send$,
        load$,
        ratePerSecond$: rate$.pipe(distinctUntilChanged()),
        release,
        close: () => {
            closed = true;
            counting.unsubscribe();
            granting.unsubscribe();
            resuming.unsubscribe();
            rate$.complete();
            release('its connection closed.');
        },
    };
}

/**
 * A request sent through the grants, on the turn of the subscriber its
 * client was made for. With `key` (what it asks: its method and params), an
 * identical request already waiting or in flight at the endpoint is joined
 * rather than sent again.
 */
export type GrantedSend = <T>(request: () => Observable<T>, key?: string) => Observable<T>;

/**
 * What a transport gives for its requests to reach an endpoint: the
 * contract every transport behind a connection fills.
 */
export interface GrantedTransport<TClient> {
    /** Its client for one URL, every request of which goes through `send$`. */
    clientOn: (url: string, send$: GrantedSend) => TClient;
    /** Whether the endpoint turned a request away for too many requests, from how it came back. */
    turnedAway: (outcome: RequestResult<unknown>) => boolean;
    /** How long the endpoint asked to be left alone when it turned a request away (its `Retry-After`), if it said. */
    retryAfterMs?: (outcome: RequestResult<unknown>) => number | undefined;
}

/**
 * A connection's grants, one per endpoint (URL), and its transport's clients
 * over them: one client per URL and subscriber, every request of which takes
 * that subscriber's turn at that URL. It is the only way to the grants, so
 * every request to an endpoint takes one, whatever the transport. Each URL
 * learns its own rate.
 *
 * @param transport The transport's client, and what too many requests look like to it.
 * @param quietSecondsToLift How many seconds in a row with nothing waiting and nothing turned away lift a limit.
 * @returns
 *   - `clientOf(url, subscriber, timeoutMs?)`: the transport's client for the URL, its requests on the
 *     subscriber's turns; with `timeoutMs`, each request fails once sent and unanswered for that long (the
 *     wait for its turn is not counted).
 *   - `loadOf(url)`: each second's count at the URL, and the rate after it.
 *   - `rateOf(url)`: the URL's limit, as it changes; `Infinity` while unlimited.
 *   - `release(url, reason)`: fails every request still waiting at the URL, never sent.
 *   - `close()`: stops granting at every URL, and releases what still waits.
 */
export function grantedClients<TClient>(
    transport: GrantedTransport<TClient>,
    quietSecondsToLift: number = QUIET_SECONDS_TO_LIFT
) {
    const grantsByUrl = new Map<string, ReturnType<typeof connectionGrants>>();
    let closed = false;
    const grantsOf = (url: string) => {
        const made = grantsByUrl.get(url);
        if (made !== undefined) return made;
        const grants = connectionGrants(quietSecondsToLift);
        grantsByUrl.set(url, grants);
        // Once closed, an endpoint asked for afterwards sends nothing either.
        if (closed) grants.close();
        return grants;
    };
    const clients = new Map<string, WeakMap<RunningMachine, TClient>>();
    const clientOf = (url: string, subscriber: RunningMachine, timeoutMs?: number): TClient => {
        const key = `${url} ${timeoutMs ?? ''}`;
        const byUrl = clients.get(key) ?? new WeakMap<RunningMachine, TClient>();
        clients.set(key, byUrl);
        const made = byUrl.get(subscriber);
        if (made !== undefined) return made;
        const grants = grantsOf(url);
        // A client with its own timeout shares only with clients with the same one.
        const send$: GrantedSend = (request, key) =>
            grants.send$(
                subscriber,
                timeoutMs === undefined ? request : () => request().pipe(timeout(timeoutMs)),
                transport.turnedAway,
                transport.retryAfterMs,
                key === undefined ? undefined : `${timeoutMs ?? ''} ${key}`
            );
        const client = transport.clientOn(url, send$);
        byUrl.set(subscriber, client);
        return client;
    };
    return {
        clientOf,
        loadOf: (url: string) => grantsOf(url).load$,
        rateOf: (url: string) => grantsOf(url).ratePerSecond$,
        release: (url: string, reason: string) => grantsByUrl.get(url)?.release(reason),
        close: () => {
            closed = true;
            for (const grants of grantsByUrl.values()) grants.close();
        },
    };
}

/** A connection's grants and its transport's clients over them. */
export type GrantedClients<TClient> = ReturnType<typeof grantedClients<TClient>>;
