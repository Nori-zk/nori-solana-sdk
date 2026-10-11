import {
    catchError,
    defer,
    distinctUntilChanged,
    EMPTY,
    filter,
    fromEvent,
    map,
    NEVER,
    type Observable,
    of,
    ReplaySubject,
    share,
    Subject,
    Subscription,
    switchMap,
    take,
    timer,
} from 'rxjs';
import { type ResolveNodeData, type RunningMachine } from '@yaw-rx/ystate';
import { BrowserProvider, toQuantity } from 'ethers';
import { messageOf } from '../../utils/messageOf.js';
import {
    atNode,
    type RequestOutcome,
    requestOnEntry$,
    requestOutcomes,
    stateOf$,
    type StartedMachine,
    withOutcome,
} from '../../utils/machines.js';
import { grantedClients, type GrantedSend } from '../connection/connectionGrants.js';
import {
    type HealthCheckTimings,
    resolveHealthCheckTimings,
} from '../connection/healthCheckTimings.js';
import { backgroundCheckedTransitions } from '../connection/backgroundChecked.impl.js';
import {
    connectionTransitions,
    type HealthCheck,
    settledHealthCheck$,
} from '../connection/connection.impl.js';
import { LIMIT_EXCEEDED, turnedAwayWith } from '../connection/jsonRpcTopic.js';
import { type NetworkMachine } from '../connection/network.impl.js';
import { pacedTransitions } from '../connection/pacedConnection.impl.js';
import {
    type Eip1193EventProvider,
    eip1193Event$,
    type Eip1193Events,
    requestErrorCode,
    USER_REJECTED_REQUEST,
} from './eip1193.js';
import { chainHealthCheck$, type EthereumHealth } from './ethereumHttp.js';
import {
    EthereumWalletGraph,
    type EthereumWalletState,
    type WalletInfo,
} from './ethereumWallet.js';

/** A wallet as it announces itself (EIP-6963): who it is, and its provider. */
export interface WalletAnnouncement {
    info: WalletInfo;
    provider: Eip1193EventProvider;
}

/** The legacy `window.ethereum`, offered when no wallet announces itself. */
const LEGACY_WALLET_INFO: WalletInfo = {
    uuid: 'window.ethereum',
    name: 'Browser wallet',
    icon: '',
    rdns: '',
};

export interface EthereumWalletOptions extends HealthCheckTimings {
    /** The chain the proof request queue lives on. */
    expectedChainId: bigint;
    /** How long to wait for wallets to announce themselves, in ms (default: 500). */
    walletSearchMs?: number;
    /** How long a request to switch chain may go unanswered before it counts as failed, in ms (default: 60000). */
    switchTimeoutMs?: number;
    /**
     * Where wallets announce themselves (EIP-6963); the window by default.
     * Outside a browser, with none given, no wallet can announce itself.
     */
    walletEvents?: EventTarget;
    /**
     * The legacy injected wallet, used when no wallet announces itself;
     * `window.ethereum` by default.
     */
    injectedProvider?: Eip1193EventProvider;
}

/** What one health check of the wallet found: its chain, then its latest block. */
type WalletHealthCheck = HealthCheck<EthereumHealth>;

/**
 * How a switch request ended, when it did not switch the chain: the edge
 * out of `askingToSwitchChain` it takes, with that node's data.
 */
type SwitchOutcome = RequestOutcome<typeof EthereumWalletGraph, 'userDeclinedSwitch' | 'switchRequestFailed'>;

/**
 * Whether an announcement event carries a wallet as EIP-6963 describes it:
 * its info as strings, and a provider that can make requests. Announcements
 * come from third-party extensions, so they are checked, not assumed.
 *
 * @param event An `eip6963:announceProvider` event.
 * @returns `true` when its `detail` is a wallet announcement.
 */
function isWalletAnnouncement(
    event: Event
): event is CustomEvent<WalletAnnouncement> {
    if (typeof CustomEvent === 'undefined' || !(event instanceof CustomEvent))
        return false;
    const detail: unknown = event.detail;
    if (typeof detail !== 'object' || detail === null) return false;
    if (!('info' in detail) || !('provider' in detail)) return false;
    const { info, provider } = detail;
    return (
        typeof info === 'object' &&
        info !== null &&
        'uuid' in info &&
        typeof info.uuid === 'string' &&
        'name' in info &&
        typeof info.name === 'string' &&
        'icon' in info &&
        typeof info.icon === 'string' &&
        'rdns' in info &&
        typeof info.rdns === 'string' &&
        typeof provider === 'object' &&
        provider !== null &&
        'request' in provider &&
        typeof provider.request === 'function'
    );
}

/**
 * The window, where wallets announce themselves, if there is one: in a
 * browser the global object is the window, an `EventTarget`; in Node it is not.
 *
 * @returns The window, or `undefined` outside a browser.
 */
export function browserWindow(): EventTarget | undefined {
    const target: unknown = globalThis;
    return typeof EventTarget !== 'undefined' && target instanceof EventTarget
        ? target
        : undefined;
}

/**
 * Wallets announcing themselves on `walletEvents` (EIP-6963
 * `eip6963:announceProvider`).
 *
 * @param walletEvents Where wallets announce themselves.
 * @returns Each valid announcement, as it arrives.
 */
function walletAnnouncement$(
    walletEvents: EventTarget
): Observable<WalletAnnouncement> {
    return fromEvent(walletEvents, 'eip6963:announceProvider').pipe(
        filter(isWalletAnnouncement),
        map((event) => event.detail)
    );
}

/**
 * A wallet's EIP-1193 provider whose every request goes through the grants
 * (`send$`); its events are the wallet's own. EIP-1193 requests return a
 * promise, so the grants' answer becomes one here.
 *
 * @param provider The wallet's provider.
 * @param send$ Sends a request on its subscriber's turn.
 * @returns The provider, its requests on the subscriber's turns.
 */
function grantedWalletProvider(provider: Eip1193EventProvider, send$: GrantedSend): Eip1193EventProvider {
    return {
        request: (request) =>
            new Promise((resolve, reject) => {
                send$(() => defer(() => provider.request(request))).subscribe({ next: resolve, error: reject });
            }),
        on: (event, listener) => provider.on?.(event, listener),
        removeListener: (event, listener) => provider.removeListener?.(event, listener),
    };
}

/** Whether the wallet turned a request away for too many requests: it rejects with EIP-1474's "Limit exceeded". */
const turnedAway = turnedAwayWith(LIMIT_EXCEEDED);

/**
 * Opens the user's wallet for reads and runs its machine: finds the user's
 * wallets, settles which one reads go through, follows which chain it is
 * on, and checks that reads through it work. Every request through the
 * wallet takes a turn from its grants (`grantedClients`). Ethers providers
 * over the wallet are made afresh each time the machine comes to serve
 * requests (ethers caches the network it detected and fails every call
 * after a change).
 *
 * @param options The expected chain, how long to search for wallets, and timings.
 * @param network The running network machine the connection follows.
 * @returns
 *   - `connection`: the running wallet machine.
 *   - `current(subscriber)`: the subscriber's ethers provider over the wallet, made afresh on each entry into `ready` or `overwhelmed`.
 *   - `currentWalletProvider(subscriber)`: the chosen wallet's EIP-1193 provider, its requests on the subscriber's turns, if one is chosen.
 *   - `chooseWallet(uuid)`: picks one of several wallets, while choosing.
 *   - `switchToExpectedChain()`: asks the wallet to switch, once, while on another chain.
 *   - `walletProvider$(subscriber)`: the chosen wallet's provider, its requests on the subscriber's turns, once there is one.
 *   - `walletEvents$`: the chosen wallet's events alone, once there is one.
 *   - `load$`: each second's count of the requests through the wallet, and the rate after it.
 *   - `reportReadFailed()`: tells the machine a read failed to reach the wallet.
 *   - `close()`: moves the machine to `closed`.
 */
export function ethereumWallet(
    options: EthereumWalletOptions,
    network: NetworkMachine
) {
    const {
        expectedChainId,
        walletSearchMs = 500,
        switchTimeoutMs = 60_000,
        walletEvents = browserWindow(),
        injectedProvider = (globalThis as { ethereum?: Eip1193EventProvider })
            .ethereum,
    } = options;
    const timings = resolveHealthCheckTimings(options);

    const chooseWallet$ = new Subject<string>();
    const switchToExpectedChain$ = new Subject<void>();
    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<StartedMachine<EthereumWalletState>>(1);
    const state$ = stateOf$(started$);

    // Every wallet that has announced itself, by uuid, and each one as it does.
    const wallets = new Map<string, WalletAnnouncement>();
    const announced$ = new Subject<WalletAnnouncement>();
    const subscriptions = new Subscription();
    subscriptions.add(
        (walletEvents ? walletAnnouncement$(walletEvents) : EMPTY).subscribe(
            (announcement) => {
                if (wallets.has(announcement.info.uuid)) return;
                wallets.set(announcement.info.uuid, announcement);
                announced$.next(announcement);
            }
        )
    );
    // Ask every installed wallet to announce itself (EIP-6963).
    walletEvents?.dispatchEvent(new Event('eip6963:requestProvider'));

    // Every request through a wallet takes a turn from its grants: one granted provider per wallet (by uuid) and subscriber.
    const granted = grantedClients<Eip1193EventProvider>({
        clientOn: (uuid, send$) => {
            const announcement = wallets.get(uuid);
            if (announcement === undefined) throw new Error(`No wallet announced itself as ${uuid}.`);
            return grantedWalletProvider(announcement.provider, send$);
        },
        turnedAway,
    }, options.quietSecondsToLift);
    // The running wallet machine, once started: the subscriber its own requests (health checks, switch requests) take their turns as.
    const running$ = new ReplaySubject<RunningMachine>(1);

    // The chosen wallet, by uuid, from the wallet the machine carries.
    const chosenUuid$ = state$.pipe(
        map(({ data }) => ('wallet' in data ? data.wallet.uuid : undefined)),
        filter((uuid): uuid is string => uuid !== undefined && wallets.has(uuid)),
        distinctUntilChanged(),
        share({
            connector: () => new ReplaySubject(1),
            resetOnRefCountZero: false,
        })
    );
    // The chosen wallet's provider: its events, and what its requests go to through the grants.
    const walletProvider$ = chosenUuid$.pipe(
        map((uuid) => wallets.get(uuid)?.provider),
        filter(
            (provider): provider is Eip1193EventProvider =>
                provider !== undefined
        )
    );
    /**
     * The chosen wallet's provider for one subscriber: every request takes
     * the subscriber's turn, its events are the wallet's own.
     *
     * @param subscriber The machine the requests are for.
     * @returns The provider, once a wallet is chosen, and each wallet chosen after.
     */
    const grantedWalletProvider$ = (subscriber: RunningMachine): Observable<Eip1193EventProvider> =>
        chosenUuid$.pipe(map((uuid) => granted.clientOf(uuid, subscriber)));

    // The search's three outcome edges share one look at what announced
    // itself in time; the legacy wallet counts only when none did.
    const search$ = timer(walletSearchMs).pipe(
        map(() => {
            const found = [...wallets.values()];
            const legacy =
                found.length === 0 && injectedProvider
                    ? { info: LEGACY_WALLET_INFO, provider: injectedProvider }
                    : undefined;
            if (legacy) wallets.set(legacy.info.uuid, legacy);
            return legacy ? [legacy.info] : found.map(({ info }) => info);
        }),
        share()
    );

    /**
     * One health check of the wallet: its chain, then its latest block, with
     * a failure as a result rather than an error. Its requests take the
     * machine's turns, each timing out from when it is sent.
     *
     * @param running The wallet machine, whose turns the check takes.
     * @param wallet The wallet, whose reverse-DNS id is the check's `url`.
     * @returns The check's result, once.
     */
    const runHealthCheck$ = (running: RunningMachine, wallet: WalletInfo): Observable<WalletHealthCheck> => {
        const provider = granted.clientOf(wallet.uuid, running, timings.healthCheckTimeoutMs);
        return settledHealthCheck$(
            chainHealthCheck$(
                (method) => defer(() => provider.request({ method })).pipe(map((result) => BigInt(result as string))),
                expectedChainId,
                wallet.rdns
            ),
            wallet.rdns
        );
    };

    // The check's three outcome edges share one check per entry into
    // `connecting`; a chain change restarts it through the `connecting` self-loop.
    const check$ = requestOnEntry$(state$, 'connecting', ({ wallet }) =>
        walletProvider$.pipe(
            take(1),
            switchMap(() => running$.pipe(take(1))),
            switchMap((running) => runHealthCheck$(running, wallet))
        )
    );
    // Background checks while ready, one interval after each entry.
    const backgroundCheck$ = requestOnEntry$(state$, 'ready', ({ wallet }) =>
            timer(timings.healthCheckIntervalMs).pipe(
                switchMap(() => walletProvider$.pipe(take(1))),
                switchMap(() => running$.pipe(take(1))),
                switchMap((running) => runHealthCheck$(running, wallet))
            )
    );

    // One switch request per entry into `askingToSwitchChain`. Accepting
    // arrives as a chain change, so a request that succeeds emits nothing.
    const takingSwitch = requestOutcomes(
        state$,
        'askingToSwitchChain',
        (asking: ResolveNodeData<typeof EthereumWalletGraph.nodes, 'askingToSwitchChain'>): Observable<SwitchOutcome> =>
        walletProvider$.pipe(
            take(1),
            switchMap(() => running$.pipe(take(1))),
            switchMap((running) =>
                defer(() =>
                    granted.clientOf(asking.wallet.uuid, running).request({
                        method: 'wallet_switchEthereumChain',
                        params: [{ chainId: toQuantity(expectedChainId) }],
                    })
                ).pipe(
                    switchMap(() => NEVER),
                    catchError((error: unknown) =>
                        of<SwitchOutcome>(
                            requestErrorCode(error) === USER_REJECTED_REQUEST
                                ? { transition: 'switchDeclined', data: asking }
                                : { transition: 'switchRequestFailed', data: { ...asking, lastSwitchError: messageOf(error) } }
                        )
                    )
                )
            )
        )
    );

    const machine = EthereumWalletGraph.implement({
        ...pacedTransitions<ResolveNodeData<typeof EthereumWalletGraph.nodes, 'ready'>>({
            granted,
            state$,
            // A wallet's requests are granted per wallet.
            endpointOf: ({ wallet }) => wallet.uuid,
        }),
        ...backgroundCheckedTransitions<EthereumHealth, { wallet: WalletInfo }>({
            backgroundCheck$,
            carry: ({ wallet }) => ({ wallet }),
        }),
        ...connectionTransitions<EthereumHealth, { wallet: WalletInfo }>({
            check$,
            readFailed$,
            networkWentOffline$: network.state$.pipe(filter(atNode('offline'))),
            networkCameOnline$: network.state$.pipe(filter(atNode('online'))),
            close$,
            state$,
            timings,
            carry: ({ wallet }) => ({ wallet }),
        }),
        foundOneWallet: {
            $: () => search$.pipe(filter((found) => found.length === 1)),
            next: ([wallet]) => ({ wallet, failedChecks: 0 }),
        },
        foundSeveralWallets: {
            $: () => search$.pipe(filter((found) => found.length > 1)),
            next: (found) => ({ wallets: found }),
        },
        foundNoWallet: {
            $: () => search$.pipe(filter((found) => found.length === 0)),
            next: () => ({}),
        },
        walletAnnounced: {
            $: () => announced$,
            next: ({ info }, _dest, source) =>
                'wallets' in source
                    ? { wallets: [...source.wallets, info] }
                    : { wallet: info, failedChecks: 0 },
        },
        chooseWallet: {
            $: () =>
                chooseWallet$.pipe(
                    map((uuid) => wallets.get(uuid)?.info),
                    filter((info): info is WalletInfo => info !== undefined)
                ),
            next: (wallet) => ({ wallet, failedChecks: 0 }),
        },
        checkFoundOtherNetwork: {
            $: () => withOutcome(check$, 'onOtherNetwork'),
            next: ({ url, found, expected }, _dest, source) => ({
                wallet: source.wallet,
                url,
                found,
                expected,
                failedChecks: 0,
                lastSwitchError: '',
            }),
        },
        backgroundCheckFoundOtherNetwork: {
            $: () => withOutcome(backgroundCheck$, 'onOtherNetwork'),
            next: ({ url, found, expected }, _dest, source) => ({
                wallet: source.wallet,
                url,
                found,
                expected,
                failedChecks: 0,
                lastSwitchError: '',
            }),
        },
        walletChangedChain: {
            $: () =>
                walletProvider$.pipe(
                    switchMap((provider) =>
                        eip1193Event$(provider, 'chainChanged')
                    )
                ),
            next: (_chainChanged, _dest, source) => ({
                wallet: source.wallet,
                failedChecks: 0,
            }),
        },
        switchToExpectedChain: {
            $: () => switchToExpectedChain$,
            next: (_request, _dest, source) => ({
                wallet: source.wallet,
                url: source.url,
                found: source.found,
                expected: source.expected,
                failedChecks: source.failedChecks,
            }),
        },
        switchDeclined: { $: () => takingSwitch('switchDeclined'), next: (data) => data },
        switchRequestFailed: { $: () => takingSwitch('switchRequestFailed'), next: (data) => data },
        // A switch request left unanswered goes back to `wrongNetwork`, from where the app may ask again.
        switchTimedOut: {
            $: () => timer(switchTimeoutMs),
            next: (_timedOut, _dest, source) => ({
                wallet: source.wallet,
                url: source.url,
                found: source.found,
                expected: source.expected,
                failedChecks: source.failedChecks,
                lastSwitchError: `The wallet did not answer the request to switch chain within ${switchTimeoutMs} ms.`,
            }),
        },
        walletDisconnected: {
            $: () =>
                walletProvider$.pipe(
                    switchMap((provider) => eip1193Event$(provider, 'disconnect'))
                ),
            next: (_disconnected, _dest, source) => ({
                wallet: source.wallet,
                url: source.wallet.rdns,
                failedChecks: 1,
                error: 'The wallet disconnected from every chain.',
            }),
        },
        walletConnected: {
            $: () =>
                walletProvider$.pipe(
                    switchMap((provider) => eip1193Event$(provider, 'connect'))
                ),
            next: (_connected, _dest, source) => ({
                wallet: source.wallet,
                failedChecks: 0,
            }),
        },
    });

    const connection = machine.close().start('lookingForWallets');
    started$.next(connection);
    running$.next(connection);

    // Fresh ethers providers over the wallet, one per subscriber, on each
    // entry into a node that serves requests (`ready`, `overwhelmed`) from
    // another node; `stillReady` and moving between the two keep them.
    let serving:
        | { uuid: string; made: BrowserProvider[]; bySubscriber: WeakMap<RunningMachine, BrowserProvider> }
        | undefined;
    let chosenUuid: string | undefined;
    subscriptions.add(
        chosenUuid$.subscribe((uuid) => {
            chosenUuid = uuid;
        })
    );
    subscriptions.add(
        connection.state$
            .pipe(
                map((state) =>
                    state.node === 'ready' || state.node === 'overwhelmed' ? state.data.wallet.uuid : undefined
                ),
                distinctUntilChanged(),
                filter((uuid): uuid is string => uuid !== undefined)
            )
            .subscribe((uuid) => {
                serving?.made.forEach((client) => client.destroy());
                serving = { uuid, made: [], bySubscriber: new WeakMap() };
            })
    );
    subscriptions.add(
        connection.status$
            .pipe(filter((status) => status !== 'running'))
            .subscribe(() => {
                serving?.made.forEach((client) => client.destroy());
                granted.close();
                subscriptions.unsubscribe();
            })
    );

    /**
     * The subscriber's ethers provider over the wallet, every request of
     * which takes the subscriber's turn; made afresh on each entry into a
     * node that serves requests.
     *
     * @param subscriber The machine the requests are for.
     * @returns The provider, or `undefined` before the wallet first serves requests.
     */
    const current = (subscriber: RunningMachine): BrowserProvider | undefined => {
        if (serving === undefined) return undefined;
        const made = serving.bySubscriber.get(subscriber);
        if (made !== undefined) return made;
        const client = new BrowserProvider(granted.clientOf(serving.uuid, subscriber));
        serving.made.push(client);
        serving.bySubscriber.set(subscriber, client);
        return client;
    };

    return {
        connection,
        current,
        currentWalletProvider: (subscriber: RunningMachine): Eip1193EventProvider | undefined =>
            chosenUuid === undefined ? undefined : granted.clientOf(chosenUuid, subscriber),
        chooseWallet: (uuid: string) => chooseWallet$.next(uuid),
        switchToExpectedChain: () => switchToExpectedChain$.next(),
        walletProvider$: grantedWalletProvider$,
        load$: chosenUuid$.pipe(switchMap((uuid) => granted.loadOf(uuid))),
        walletEvents$: walletProvider$.pipe(
            map(
                (provider): Eip1193Events => ({
                    on: (event, listener) => provider.on?.(event, listener),
                    removeListener: (event, listener) => provider.removeListener?.(event, listener),
                })
            )
        ),
        reportReadFailed: () => readFailed$.next(),
        close: () => close$.next(),
    };
}

/** The user's wallet with its running machine and its controls. */
export type EthereumWallet = ReturnType<typeof ethereumWallet>;
