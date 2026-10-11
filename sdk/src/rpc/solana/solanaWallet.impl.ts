import { type ResolveNodeData } from '@yaw-rx/ystate';
import { address, getTransactionEncoder, signatureBytes, type TransactionSendingSigner } from '@solana/kit';
import {
    catchError,
    defer,
    distinctUntilChanged,
    EMPTY,
    filter,
    map,
    Observable,
    of,
    ReplaySubject,
    share,
    Subject,
    Subscription,
    switchMap,
    take,
    timer,
} from 'rxjs';
import { messageOf } from '../../utils/messageOf.js';
import { type RequestOutcome, requestOutcomes, stateOf$, type StartedMachine } from '../../utils/machines.js';
import { requestErrorCode, USER_REJECTED_REQUEST } from '../eth/eip1193.js';
import { browserWindow } from '../eth/ethereumWallet.impl.js';
import { SolanaWalletGraph, type SolanaWalletInfo, type SolanaWalletState } from './solanaWallet.js';

/** An account a wallet shares (Wallet Standard `WalletAccount`). */
export interface StandardWalletAccount {
    address: string;
    publicKey: Uint8Array;
    chains: readonly string[];
    features: readonly string[];
}

/** A wallet as it registers itself (Wallet Standard `Wallet`). */
export interface StandardWallet {
    name: string;
    icon: string;
    chains: readonly string[];
    features: Readonly<Record<string, unknown>>;
    accounts: readonly StandardWalletAccount[];
}

/** `standard:connect`: the accounts the wallet shares, asking the user unless silent. */
interface ConnectFeature {
    connect(input?: { silent?: boolean }): Promise<{ accounts: readonly StandardWalletAccount[] }>;
}

/** `standard:disconnect`: stops sharing the accounts. */
interface DisconnectFeature {
    disconnect(): Promise<void>;
}

/** What a wallet's `change` event says changed. */
export interface WalletChange {
    chains?: readonly string[];
    features?: unknown;
    accounts?: readonly StandardWalletAccount[];
}

/** `standard:events`: the wallet's `change` event. */
interface EventsFeature {
    on(event: 'change', listener: (properties: WalletChange) => void): () => void;
}

/** `solana:signAndSendTransaction`: signs each transaction with an account and sends it on a chain. */
interface SignAndSendFeature {
    signAndSendTransaction(
        ...inputs: { account: StandardWalletAccount; chain: string; transaction: Uint8Array }[]
    ): Promise<readonly { signature: Uint8Array }[]>;
}

/** Whether a value is an object whose fields can be read. */
const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === 'object' && value !== null;

/**
 * Whether a registered wallet is one as the Wallet Standard describes it.
 * Wallets are third-party extensions, so they are checked, not assumed.
 *
 * @param value What registered itself.
 * @returns `true` for a wallet.
 */
const isStandardWallet = (value: unknown): value is StandardWallet =>
    isRecord(value) &&
    typeof value.name === 'string' &&
    typeof value.icon === 'string' &&
    Array.isArray(value.chains) &&
    isRecord(value.features) &&
    Array.isArray(value.accounts);

/**
 * Whether a wallet offers a feature with a method.
 *
 * @param wallet The wallet.
 * @param name The feature.
 * @param method The method the feature has.
 * @returns `true` when it does.
 */
const offers = (wallet: StandardWallet, name: string, method: string): boolean => {
    const feature = wallet.features[name];
    return isRecord(feature) && typeof feature[method] === 'function';
};

const isConnectFeature = (feature: unknown): feature is ConnectFeature =>
    isRecord(feature) && typeof feature.connect === 'function';
const isDisconnectFeature = (feature: unknown): feature is DisconnectFeature =>
    isRecord(feature) && typeof feature.disconnect === 'function';
const isEventsFeature = (feature: unknown): feature is EventsFeature => isRecord(feature) && typeof feature.on === 'function';
const isSignAndSendFeature = (feature: unknown): feature is SignAndSendFeature =>
    isRecord(feature) && typeof feature.signAndSendTransaction === 'function';

/**
 * Wallets registering themselves on `walletEvents` (Wallet Standard): each
 * one already loaded, answering `wallet-standard:app-ready`, and each one
 * loading later, through `wallet-standard:register-wallet`.
 *
 * @param walletEvents Where wallets register themselves.
 * @returns Each valid wallet, as it registers.
 */
function registeredWallet$(walletEvents: EventTarget): Observable<StandardWallet> {
    return new Observable<StandardWallet>((subscriber) => {
        // Registering returns how to unregister; a registered wallet stays known for the page's life.
        const register = (...wallets: unknown[]): (() => void) => {
            for (const wallet of wallets) if (isStandardWallet(wallet)) subscriber.next(wallet);
            return (): void => undefined;
        };
        const onRegister = (event: Event) => {
            if (typeof CustomEvent !== 'undefined' && event instanceof CustomEvent && typeof event.detail === 'function')
                event.detail({ register });
        };
        walletEvents.addEventListener('wallet-standard:register-wallet', onRegister);
        walletEvents.dispatchEvent(new CustomEvent('wallet-standard:app-ready', { detail: { register } }));
        return () => walletEvents.removeEventListener('wallet-standard:register-wallet', onRegister);
    });
}

/**
 * A wallet's `change` events.
 *
 * @param wallet The wallet.
 * @returns Each change, while subscribed; nothing from a wallet without events.
 */
const walletChange$ = (wallet: StandardWallet): Observable<WalletChange> => {
    const events = wallet.features['standard:events'];
    return isEventsFeature(events)
        ? new Observable<WalletChange>((subscriber) => events.on('change', (change) => subscriber.next(change)))
        : EMPTY;
};

export interface SolanaWalletOptions {
    /** The Wallet Standard chain the wallet sends on, e.g. `solana:devnet`. */
    chain: string;
    /** How long to wait for wallets to register themselves, in ms (default: 500). */
    walletSearchMs?: number;
    /**
     * Where wallets register themselves (Wallet Standard); the window by
     * default. Outside a browser, with none given, no wallet can register.
     */
    walletEvents?: EventTarget;
}

/** How the silent connect ended: the edge out of `connecting` it takes, with that node's data. */
type ConnectingOutcome = RequestOutcome<
    typeof SolanaWalletGraph,
    'alreadyConnected' | 'notYetConnected' | 'walletFoundUnsupported'
>;

/** How a request to connect ended: the edge out of `askingToConnect` it takes, with that node's data. */
type ConnectOutcome = RequestOutcome<typeof SolanaWalletGraph, 'connected' | 'userDeclinedConnect' | 'connectFailed'>;

/**
 * Opens the user's Solana wallet and runs its machine: finds the user's
 * wallets, settles which one signs, checks that it serves the chain and can
 * sign and send, and follows the account the user shares with the app. A
 * signer over the wallet's account is made on each entry into `ready`.
 *
 * @param options The chain, how long to search for wallets, and where they register.
 * @returns
 *   - `connection`: the running wallet machine.
 *   - `current()`: a sending signer over the shared account, while `ready`.
 *   - `chooseWallet(name)`: picks one of several wallets, while choosing.
 *   - `connect()`: asks the user to share an account, while not connected or after a decline.
 *   - `disconnect()`: stops sharing the account, while ready.
 *   - `close()`: moves the machine to `closed`.
 */
export function solanaWallet(options: SolanaWalletOptions) {
    const { chain, walletSearchMs = 500, walletEvents = browserWindow() } = options;
    const chooseWallet$ = new Subject<string>();
    const connect$ = new Subject<void>();
    const disconnect$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<StartedMachine<SolanaWalletState>>(1);
    const state$ = stateOf$(started$);

    // Every wallet that has registered itself, by name, and each one as it does.
    const wallets = new Map<string, StandardWallet>();
    const registered$ = new Subject<StandardWallet>();
    const subscriptions = new Subscription();
    subscriptions.add(
        (walletEvents ? registeredWallet$(walletEvents) : EMPTY).subscribe((wallet) => {
            if (wallets.has(wallet.name)) return;
            wallets.set(wallet.name, wallet);
            registered$.next(wallet);
        })
    );
    const infoOf = ({ name, icon }: StandardWallet): SolanaWalletInfo => ({ name, icon });

    // The chosen wallet, from the wallet the machine carries.
    const chosen$ = state$.pipe(
        map(({ data }) => ('wallet' in data ? data.wallet.name : undefined)),
        filter((name): name is string => name !== undefined),
        distinctUntilChanged(),
        map((name) => wallets.get(name)),
        filter((wallet): wallet is StandardWallet => wallet !== undefined),
        share({ connector: () => new ReplaySubject(1), resetOnRefCountZero: false })
    );
    // The chosen wallet's changes, shared by the edges that follow them.
    const change$ = chosen$.pipe(switchMap(walletChange$), share());
    // An account serving the chain, from those the wallet shares.
    const accountOn = (accounts: readonly StandardWalletAccount[]) => accounts.find(({ chains }) => chains.includes(chain));

    // The search's three outcome edges share one look at what registered in time.
    const search$ = timer(walletSearchMs).pipe(
        map(() => [...wallets.values()].map(infoOf)),
        share()
    );

    // One silent connect per entry into `connecting`, after checking what the wallet serves.
    const takingConnecting = requestOutcomes(
        state$,
        'connecting',
        ({ wallet }: ResolveNodeData<typeof SolanaWalletGraph.nodes, 'connecting'>): Observable<ConnectingOutcome> => {
            const standard = wallets.get(wallet.name);
            const unsupported = (reason: string): Observable<ConnectingOutcome> =>
                of({ transition: 'walletUnsupported', data: { wallet, reason } });
            if (standard === undefined || !standard.chains.includes(chain))
                return unsupported(`The wallet does not serve ${chain}.`);
            if (!offers(standard, 'solana:signAndSendTransaction', 'signAndSendTransaction'))
                return unsupported('The wallet cannot sign and send a transaction.');
            const connect = standard.features['standard:connect'];
            const shared$ = isConnectFeature(connect)
                ? defer(() => connect.connect({ silent: true })).pipe(
                      map(({ accounts }) => accounts),
                      // A wallet refusing a silent connect shares nothing yet.
                      catchError(() => of(standard.accounts))
                  )
                : of(standard.accounts);
            return shared$.pipe(
                map((accounts): ConnectingOutcome => {
                    const account = accountOn(accounts);
                    return account === undefined
                        ? { transition: 'noSharedAccount', data: { wallet, lastConnectError: '' } }
                        : { transition: 'sharedAccountFound', data: { wallet, account: account.address } };
                })
            );
        }
    );

    // One request to connect per entry into `askingToConnect`.
    const takingConnect = requestOutcomes(
        state$,
        'askingToConnect',
        ({ wallet }: ResolveNodeData<typeof SolanaWalletGraph.nodes, 'askingToConnect'>): Observable<ConnectOutcome> => {
            const connect = wallets.get(wallet.name)?.features['standard:connect'];
            if (!isConnectFeature(connect))
                return of({
                    transition: 'connectFailed',
                    data: { wallet, lastConnectError: 'The wallet cannot share an account.' },
                });
            return defer(() => connect.connect()).pipe(
                map(({ accounts }): ConnectOutcome => {
                    const account = accountOn(accounts);
                    return account === undefined
                        ? {
                              transition: 'connectFailed',
                              data: { wallet, lastConnectError: `The wallet shared no account on ${chain}.` },
                          }
                        : { transition: 'accountShared', data: { wallet, account: account.address } };
                }),
                catchError((error: unknown) =>
                    of<ConnectOutcome>(
                        requestErrorCode(error) === USER_REJECTED_REQUEST
                            ? { transition: 'connectDeclined', data: { wallet } }
                            : { transition: 'connectFailed', data: { wallet, lastConnectError: messageOf(error) } }
                    )
                )
            );
        }
    );

    const machine = SolanaWalletGraph.implement({
        foundOneWallet: {
            $: () => search$.pipe(filter((found) => found.length === 1)),
            next: ([wallet]) => ({ wallet }),
        },
        foundSeveralWallets: {
            $: () => search$.pipe(filter((found) => found.length > 1)),
            next: (found) => ({ wallets: found }),
        },
        foundNoWallet: {
            $: () => search$.pipe(filter((found) => found.length === 0)),
            next: () => ({}),
        },
        walletRegistered: {
            $: () => registered$.pipe(map(infoOf)),
            next: (info, _dest, source) => ('wallets' in source ? { wallets: [...source.wallets, info] } : { wallet: info }),
        },
        chooseWallet: {
            $: () =>
                chooseWallet$.pipe(
                    map((name) => wallets.get(name)),
                    filter((wallet): wallet is StandardWallet => wallet !== undefined),
                    map(infoOf)
                ),
            next: (wallet) => ({ wallet }),
        },
        sharedAccountFound: { $: () => takingConnecting('sharedAccountFound'), next: (data) => data },
        noSharedAccount: { $: () => takingConnecting('noSharedAccount'), next: (data) => data },
        walletUnsupported: { $: () => takingConnecting('walletUnsupported'), next: (data) => data },
        walletChanged: {
            $: () => change$.pipe(filter(({ chains, features }) => chains !== undefined || features !== undefined)),
            next: (_change, _dest, source) => ({ wallet: source.wallet }),
        },
        connect: {
            $: () => connect$,
            next: (_request, _dest, source) => ({ wallet: source.wallet }),
        },
        accountShared: { $: () => takingConnect('accountShared'), next: (data) => data },
        connectDeclined: { $: () => takingConnect('connectDeclined'), next: (data) => data },
        connectFailed: { $: () => takingConnect('connectFailed'), next: (data) => data },
        accountsChanged: {
            $: () =>
                change$.pipe(
                    map(({ accounts }) => (accounts === undefined ? undefined : accountOn(accounts))),
                    filter((account): account is StandardWalletAccount => account !== undefined)
                ),
            next: (account, _dest, source) => ({ wallet: source.wallet, account: account.address }),
        },
        accountsUnshared: {
            $: () =>
                change$.pipe(filter(({ accounts }) => accounts !== undefined && accountOn(accounts) === undefined)),
            next: (_change, _dest, source) => ({ wallet: source.wallet, lastConnectError: '' }),
        },
        disconnect: {
            $: () =>
                disconnect$.pipe(
                    switchMap(() => chosen$.pipe(take(1))),
                    switchMap((wallet) => {
                        const feature = wallet.features['standard:disconnect'];
                        // Stopping to share is the user's wish: it disconnects here whatever the wallet answers.
                        return isDisconnectFeature(feature)
                            ? defer(() => feature.disconnect()).pipe(catchError(() => of(undefined)))
                            : of(undefined);
                    })
                ),
            next: (_disconnected, _dest, source) => ({ wallet: source.wallet, lastConnectError: '' }),
        },
        close: { $: () => close$, next: () => ({}) },
    });

    const connection = machine.close().start('lookingForWallets');
    started$.next(connection);

    /**
     * A sending signer over the wallet's shared account: each transaction is
     * signed and sent by the wallet on the chain.
     *
     * @param wallet The wallet.
     * @param account The shared account's address.
     * @returns The signer, or `undefined` when the wallet does not share the account.
     */
    const signerOf = (wallet: StandardWallet, account: string): TransactionSendingSigner | undefined => {
        const shared = wallet.accounts.find((candidate) => candidate.address === account);
        const feature = wallet.features['solana:signAndSendTransaction'];
        if (shared === undefined || !isSignAndSendFeature(feature)) return undefined;
        return {
            address: address(account),
            signAndSendTransactions: (transactions) =>
                feature
                    .signAndSendTransaction(
                        ...transactions.map((transaction) => ({
                            account: shared,
                            chain,
                            transaction: new Uint8Array(getTransactionEncoder().encode(transaction)),
                        }))
                    )
                    .then((outputs) => outputs.map(({ signature }) => signatureBytes(signature))),
        };
    };

    // A signer over the shared account on each entry into `ready`.
    let signer: TransactionSendingSigner | undefined;
    subscriptions.add(
        connection.state$.subscribe((state) => {
            const wallet = state.node === 'ready' ? wallets.get(state.data.wallet.name) : undefined;
            signer = wallet !== undefined && state.node === 'ready' ? signerOf(wallet, state.data.account) : undefined;
        })
    );
    subscriptions.add(
        connection.status$.pipe(filter((status) => status !== 'running')).subscribe(() => subscriptions.unsubscribe())
    );

    return {
        connection,
        current: (): TransactionSendingSigner | undefined => signer,
        chooseWallet: (name: string) => chooseWallet$.next(name),
        connect: () => connect$.next(),
        disconnect: () => disconnect$.next(),
        close: () => close$.next(),
    };
}

/** The user's Solana wallet with its running machine and its controls. */
export type SolanaWallet = ReturnType<typeof solanaWallet>;
