import {
    catchError,
    defer,
    distinctUntilChanged,
    EMPTY,
    filter,
    from,
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
    timeout,
    timer,
} from 'rxjs';
import { toQuantity } from 'ethers';
import { messageOf } from '../../../messageOf.js';
import { dataOnEntry$, stateOf$, type StartedMachine } from '../../../ystate/dataOnEntry.js';
import {
    type HealthCheckTimings,
    resolveHealthCheckTimings,
    retryDelayMs,
} from '../../healthCheckTimings.js';
import {
    type Eip1193EventProvider,
    eip1193Event$,
    requestErrorCode,
    USER_REJECTED_REQUEST,
} from '../eip1193.js';
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

/** What asking the wallet for its chain found. */
type ChainCheck =
    | { outcome: 'expected'; chainId: bigint }
    | { outcome: 'other'; chainId: bigint }
    | { outcome: 'failed'; error: string };

/** How a switch request ended, when it did not switch the chain. */
type SwitchRequest =
    { outcome: 'declined' } | { outcome: 'failed'; error: string };

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
function browserWindow(): EventTarget | undefined {
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
 * Starts the wallet machine: finds the user's wallets, settles which one
 * reads go through, and follows which chain it is on.
 *
 * @param options The expected chain, how long to search for wallets, and
 *   the backoff for a wallet that does not answer.
 * @returns
 *   - `ethereumWallet`: the running machine.
 *   - `chooseWallet(uuid)`: picks one of several wallets, while choosing.
 *   - `switchToExpectedChain()`: asks the wallet to switch, once, while on another chain.
 *   - `walletProvider$`: the chosen wallet's provider, once there is one.
 *   - `close()`: moves the machine to `closed`.
 */
export function createEthereumWalletMachine(options: EthereumWalletOptions) {
    const {
        expectedChainId,
        walletSearchMs = 500,
        walletEvents = browserWindow(),
        injectedProvider = (globalThis as { ethereum?: Eip1193EventProvider })
            .ethereum,
    } = options;
    const timings = resolveHealthCheckTimings(options);

    const chooseWallet$ = new Subject<string>();
    const switchToExpectedChain$ = new Subject<void>();
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

    // The chosen wallet's provider, from the wallet the machine carries.
    const walletProvider$ = state$.pipe(
        map(({ data }) => ('wallet' in data ? data.wallet.uuid : undefined)),
        filter((uuid): uuid is string => uuid !== undefined),
        distinctUntilChanged(),
        map((uuid) => wallets.get(uuid)?.provider),
        filter(
            (provider): provider is Eip1193EventProvider =>
                provider !== undefined
        ),
        share({
            connector: () => new ReplaySubject(1),
            resetOnRefCountZero: false,
        })
    );

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

    // The chain check's three outcome edges share one request per entry
    // into `checkingChain`. A chain change restarts it through the
    // `checkingChain` self-loop: the replayed state is the previous
    // `checkingChain`, with the same wallet, so the request goes to the
    // right wallet either way.
    const chainCheck$ = state$.pipe(
        filter((state) => state.node === 'checkingChain'),
        take(1),
        switchMap(() => walletProvider$.pipe(take(1))),
        switchMap((provider) =>
            defer(() => from(provider.request({ method: 'eth_chainId' }))).pipe(
                timeout(timings.healthCheckTimeoutMs),
                map((result): ChainCheck => {
                    const chainId = BigInt(result as string);
                    return chainId === expectedChainId
                        ? { outcome: 'expected', chainId }
                        : { outcome: 'other', chainId };
                }),
                catchError((error: unknown) =>
                    of<ChainCheck>({
                        outcome: 'failed',
                        error: messageOf(error),
                    })
                )
            )
        ),
        share()
    );

    // One switch request per entry into `askingToSwitchChain`. Accepting
    // arrives as a chain change, so a request that succeeds emits nothing.
    const switchRequest$ = dataOnEntry$<
        EthereumWalletState,
        'askingToSwitchChain'
    >(state$, 'askingToSwitchChain').pipe(
        switchMap(() => walletProvider$.pipe(take(1))),
        switchMap((provider) =>
            defer(() =>
                from(
                    provider.request({
                        method: 'wallet_switchEthereumChain',
                        params: [{ chainId: toQuantity(expectedChainId) }],
                    })
                )
            ).pipe(
                switchMap(() => NEVER),
                catchError((error: unknown) =>
                    of<SwitchRequest>(
                        requestErrorCode(error) === USER_REJECTED_REQUEST
                            ? { outcome: 'declined' }
                            : { outcome: 'failed', error: messageOf(error) }
                    )
                )
            )
        ),
        share()
    );

    const machine = EthereumWalletGraph.implement({
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
        chainFoundExpected: {
            $: () =>
                chainCheck$.pipe(
                    filter(
                        (
                            check
                        ): check is Extract<
                            ChainCheck,
                            { outcome: 'expected' }
                        > => check.outcome === 'expected'
                    )
                ),
            next: ({ chainId }, _dest, source) => ({
                wallet: source.wallet,
                chainId,
            }),
        },
        chainFoundOther: {
            $: () =>
                chainCheck$.pipe(
                    filter(
                        (
                            check
                        ): check is Extract<ChainCheck, { outcome: 'other' }> =>
                            check.outcome === 'other'
                    )
                ),
            next: ({ chainId }, _dest, source) => ({
                wallet: source.wallet,
                chainId,
                expectedChainId,
                lastSwitchError: '',
            }),
        },
        chainCheckFailed: {
            $: () =>
                chainCheck$.pipe(
                    filter(
                        (
                            check
                        ): check is Extract<
                            ChainCheck,
                            { outcome: 'failed' }
                        > => check.outcome === 'failed'
                    )
                ),
            next: ({ error }, _dest, source) => ({
                wallet: source.wallet,
                failedChecks: source.failedChecks + 1,
                error,
            }),
        },
        retryDue: {
            $: () =>
                dataOnEntry$<EthereumWalletState, 'walletNotResponding'>(
                    state$,
                    'walletNotResponding'
                ).pipe(
                    switchMap(({ failedChecks }) =>
                        timer(retryDelayMs(failedChecks, timings))
                    )
                ),
            next: (_due, _dest, source) => ({
                wallet: source.wallet,
                failedChecks: source.failedChecks,
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
                chainId: source.chainId,
                expectedChainId: source.expectedChainId,
            }),
        },
        switchDeclined: {
            $: () =>
                switchRequest$.pipe(
                    filter(({ outcome }) => outcome === 'declined')
                ),
            next: (_declined, _dest, source) => ({
                wallet: source.wallet,
                chainId: source.chainId,
                expectedChainId: source.expectedChainId,
            }),
        },
        switchRequestFailed: {
            $: () =>
                switchRequest$.pipe(
                    filter(
                        (
                            request
                        ): request is Extract<
                            SwitchRequest,
                            { outcome: 'failed' }
                        > => request.outcome === 'failed'
                    )
                ),
            next: ({ error }, _dest, source) => ({
                wallet: source.wallet,
                chainId: source.chainId,
                expectedChainId: source.expectedChainId,
                lastSwitchError: error,
            }),
        },
        close: {
            $: () => close$,
            next: () => ({}),
        },
    });

    const ethereumWallet = machine.close().start('lookingForWallets');
    started$.next(ethereumWallet);
    ethereumWallet.status$
        .pipe(filter((status) => status !== 'running'))
        .subscribe(() => subscriptions.unsubscribe());

    return {
        ethereumWallet,
        chooseWallet: (uuid: string) => chooseWallet$.next(uuid),
        switchToExpectedChain: () => switchToExpectedChain$.next(),
        walletProvider$,
        close: () => close$.next(),
    };
}

/** The running wallet machine and its controls. */
export type EthereumWalletMachine = ReturnType<
    typeof createEthereumWalletMachine
>;
