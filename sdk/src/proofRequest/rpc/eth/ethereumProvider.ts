import {
    filter,
    NEVER,
    type Observable,
    ReplaySubject,
    Subject,
    Subscription,
    switchMap,
} from 'rxjs';
import { BrowserProvider, JsonRpcProvider, Network } from 'ethers';
import {
    type EthereumProvider,
    getRpcUrl,
    parseRpcUrl,
} from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { stateOf$, type StartedMachine } from '../../ystate/dataOnEntry.js';
import { type EthereumProviderConnection } from './ystate/ethereumProviderConnectivity.js';
import { type NetworkMachine } from '../ystate/network.impl.js';
import { type Eip1193EventProvider, eip1193Event$ } from './eip1193.js';
import { createEthereumProviderConnectivityMachine } from './ystate/ethereumProviderConnectivity.impl.js';
import {
    createEthereumWalletMachine,
    type EthereumWalletMachine,
    type EthereumWalletOptions,
} from './ystate/ethereumWallet.impl.js';

/**
 * The expected chain, how to find the user's wallet, and timings. Reads run
 * only while the provider is on the expected chain.
 */
export interface EthereumProviderConnectivityOptions extends EthereumWalletOptions {
    /**
     * An HTTP(S) RPC URL to read through (or `ETH_RPC_URL`). When there is
     * none, reads go through the user's wallet.
     */
    rpcUrl?: string;
}

/**
 * Whether a running machine's state is at `node`.
 *
 * @param node The node.
 * @returns A predicate on states.
 */
const atNode =
    (node: string) =>
    (state: { node: string }): boolean =>
        state.node === node;

/**
 * Opens the Ethereum provider reads go through and runs its connectivity
 * machine.
 *
 * - With an RPC URL, reads go through it.
 * - Without one, they go through the user's wallet: the wallet machine
 *   finds it and follows its chain, and an ethers provider over the wallet
 *   is made afresh each time the wallet reaches the expected chain (ethers
 *   caches the network it detected and fails every call after a change).
 *
 * @param options The expected chain, an optional RPC URL, and timings.
 * @param network The running network machine both connections follow.
 * @returns
 *   - `ethereumProviderConnectivity`: the running connectivity machine.
 *   - `wallet`: the wallet machine and its controls, when reading through the wallet.
 *   - `currentProvider()`: the ethers provider reads go through, while `ready`.
 *   - `reportReadFailed()`: tells the machine a read failed to reach the provider.
 *   - `close()`: closes the connectivity and wallet machines.
 */
export function getEthereumProviderWithConnectivity$(
    options: EthereumProviderConnectivityOptions,
    network: NetworkMachine['network']
) {
    const { expectedChainId } = options;
    const rpcUrl = options.rpcUrl ?? getRpcUrl();
    const readFailed$ = new Subject<void>();
    const close$ = new Subject<void>();
    const started$ = new ReplaySubject<StartedMachine<EthereumProviderConnection>>(1);
    const subscriptions = new Subscription();

    const networkWentOffline$ = network.state$.pipe(filter(atNode('offline')));
    const networkCameOnline$ = network.state$.pipe(filter(atNode('online')));

    let provider: EthereumProvider | undefined;
    let wallet: EthereumWalletMachine | undefined;
    let request: (
        method: 'eth_chainId' | 'eth_blockNumber'
    ) => Promise<unknown>;
    let walletSignals: {
        walletOnExpectedChain$: Observable<unknown>;
        walletNotOnExpectedChain$: Observable<unknown>;
        walletConnected$: Observable<unknown>;
        walletDisconnected$: Observable<unknown>;
    };

    if (rpcUrl !== undefined) {
        // An RPC URL's chain is fixed, so ethers is given it up front and
        // never re-detects it; the health check reads it with a raw
        // `eth_chainId`.
        const rpcProvider = new JsonRpcProvider(
            parseRpcUrl(rpcUrl),
            undefined,
            {
                staticNetwork: Network.from(expectedChainId),
            }
        );
        provider = rpcProvider;
        request = (method) => rpcProvider.send(method, []);
        walletSignals = {
            walletOnExpectedChain$: NEVER,
            walletNotOnExpectedChain$: NEVER,
            walletConnected$: NEVER,
            walletDisconnected$: NEVER,
        };
    } else {
        const walletMachine = createEthereumWalletMachine(options);
        wallet = walletMachine;
        let walletProvider: Eip1193EventProvider | undefined;
        subscriptions.add(
            walletMachine.walletProvider$.subscribe((chosen) => {
                walletProvider = chosen;
            })
        );
        // Subscribed before the connectivity machine starts, so a fresh
        // provider is in place before the machine sees the wallet on the
        // expected chain.
        subscriptions.add(
            walletMachine.ethereumWallet.state$
                .pipe(filter(atNode('onExpectedChain')))
                .subscribe(() => {
                    if (!walletProvider) return;
                    provider?.destroy();
                    provider = new BrowserProvider(walletProvider);
                })
        );
        request = (method) => {
            if (!walletProvider)
                return Promise.reject(new Error('No wallet has been chosen.'));
            return walletProvider.request({ method });
        };
        const walletState$ = walletMachine.ethereumWallet.state$;
        walletSignals = {
            walletOnExpectedChain$: walletState$.pipe(
                filter(atNode('onExpectedChain'))
            ),
            walletNotOnExpectedChain$: walletState$.pipe(
                filter(
                    (state) =>
                        state.node !== 'onExpectedChain' &&
                        state.node !== 'closed'
                )
            ),
            walletConnected$: walletMachine.walletProvider$.pipe(
                switchMap((chosen) => eip1193Event$(chosen, 'connect'))
            ),
            walletDisconnected$: walletMachine.walletProvider$.pipe(
                switchMap((chosen) => eip1193Event$(chosen, 'disconnect'))
            ),
        };
    }

    const machine = createEthereumProviderConnectivityMachine({
        ...options,
        ...walletSignals,
        checkHealth: async () => {
            const chainId = BigInt((await request('eth_chainId')) as string);
            if (chainId !== expectedChainId)
                return { outcome: 'onOtherChain', chainId };
            const blockNumber = Number(
                BigInt((await request('eth_blockNumber')) as string)
            );
            return {
                outcome: 'onExpectedChain',
                chainId,
                blockNumber,
                checkedAt: Date.now(),
            };
        },
        networkWentOffline$,
        networkCameOnline$,
        readFailed$,
        close$,
        connection$: stateOf$(started$),
    });
    const ethereumProviderConnectivity = machine
        .close()
        .start(wallet ? 'waitingForWallet' : 'checking');
    started$.next(ethereumProviderConnectivity);

    subscriptions.add(
        ethereumProviderConnectivity.status$
            .pipe(filter((status) => status !== 'running'))
            .subscribe(() => {
                provider?.destroy();
                subscriptions.unsubscribe();
            })
    );

    return {
        ethereumProviderConnectivity,
        wallet,
        currentProvider: (): EthereumProvider => {
            if (!provider)
                throw new Error(
                    'There is no Ethereum provider to read through yet.'
                );
            return provider;
        },
        reportReadFailed: () => readFailed$.next(),
        close: () => {
            close$.next();
            wallet?.close();
        },
    };
}

/** An Ethereum provider with its running connectivity machine. */
export type EthereumProviderWithConnectivity = ReturnType<
    typeof getEthereumProviderWithConnectivity$
>;
