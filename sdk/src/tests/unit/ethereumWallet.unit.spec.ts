import { toQuantity } from 'ethers';
import { BehaviorSubject } from 'rxjs';
import {
    type Eip1193EventProvider,
    USER_REJECTED_REQUEST,
} from '../../rpc/eth/eip1193.js';
import { ethereumWallet } from '../../rpc/eth/ethereumWallet.impl.js';
import type { WalletInfo } from '../../rpc/eth/ethereumWallet.js';
import { type NetworkMachine } from '../../rpc/connection/network.impl.js';
import {
    EXPECTED_CHAIN_ID,
    FAST_TIMINGS,
    reach,
    recordNodes,
    sleep,
} from '../testUtils.js';

const OTHER_CHAIN_ID = 1n;

/**
 * A wallet as an extension provides it: answers `eth_chainId` and
 * `eth_blockNumber`, takes switch requests, and emits `chainChanged`,
 * `connect` and `disconnect`. `onSwitchRequest` decides what the user does
 * with a switch request.
 */
class FakeWallet implements Eip1193EventProvider {
    chainId = OTHER_CHAIN_ID;
    blockNumber = 1;
    answers = true;
    switchRequests = 0;
    onSwitchRequest: 'accept' | 'decline' | 'fail' = 'accept';
    private listeners = new Map<string, Set<(...args: unknown[]) => void>>();

    constructor(readonly info: WalletInfo) {}

    async request({
        method,
    }: {
        method: string;
        params?: unknown[] | Record<string, unknown>;
    }) {
        if (method === 'eth_chainId' || method === 'eth_blockNumber') {
            if (!this.answers) return new Promise(() => undefined);
            return toQuantity(
                method === 'eth_chainId' ? this.chainId : this.blockNumber
            );
        }
        if (method === 'wallet_switchEthereumChain') {
            this.switchRequests++;
            if (this.onSwitchRequest === 'decline') {
                throw Object.assign(new Error('User rejected the request.'), {
                    code: USER_REJECTED_REQUEST,
                });
            }
            if (this.onSwitchRequest === 'fail') {
                throw Object.assign(new Error('Unrecognized chain ID.'), {
                    code: 4902,
                });
            }
            this.changeChain(EXPECTED_CHAIN_ID);
            return null;
        }
        throw new Error(`Unexpected request ${method}`);
    }

    on(event: string, listener: (...args: unknown[]) => void) {
        if (!this.listeners.has(event)) this.listeners.set(event, new Set());
        this.listeners.get(event)?.add(listener);
    }

    removeListener(event: string, listener: (...args: unknown[]) => void) {
        this.listeners.get(event)?.delete(listener);
    }

    // Emits to a copy of the listeners, as an EventEmitter does: a listener
    // added while the event is being delivered does not receive it.
    emit(event: string, ...args: unknown[]) {
        setTimeout(() =>
            [...(this.listeners.get(event) ?? [])].forEach((listener) =>
                listener(...args)
            )
        );
    }

    changeChain(chainId: bigint) {
        this.chainId = chainId;
        this.emit('chainChanged', toQuantity(chainId));
    }

    announceOn(walletEvents: EventTarget) {
        walletEvents.dispatchEvent(
            new CustomEvent('eip6963:announceProvider', {
                detail: { info: this.info, provider: this },
            })
        );
    }
}

const walletInfo = (name: string): WalletInfo => ({
    uuid: `${name}-uuid`,
    name,
    icon: 'data:image/svg+xml,',
    rdns: `io.${name.toLowerCase()}`,
});

/** Starts the wallet machine over its own announcement target and network. */
function startWallet() {
    const walletEvents = new EventTarget();
    const network$ = new BehaviorSubject<{ node: 'online' | 'offline' }>({
        node: 'online',
    });
    const wallet = ethereumWallet(
        {
            ...FAST_TIMINGS,
            expectedChainId: EXPECTED_CHAIN_ID,
            walletSearchMs: 30,
            walletEvents,
            injectedProvider: undefined,
        },
        { state$: network$ } as unknown as NetworkMachine['network']
    );
    return { ...wallet, walletEvents, network$ };
}

describe('Ethereum wallet machine', () => {
    test('with no wallet it says so, and moves on when one is installed later', async () => {
        const { connection, walletEvents, close } = startWallet();
        await reach(connection, 'noWalletFound');

        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.chainId = EXPECTED_CHAIN_ID;
        metamask.announceOn(walletEvents);
        const ready = await reach(connection, 'ready');
        expect(ready.data).toEqual(
            expect.objectContaining({
                wallet: metamask.info,
                url: metamask.info.rdns,
                health: { blockNumber: 1 },
            })
        );
        close();
    });

    test('with several wallets it waits for the app to choose one', async () => {
        const { connection, walletEvents, chooseWallet, close } = startWallet();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        const rabby = new FakeWallet(walletInfo('Rabby'));
        rabby.chainId = EXPECTED_CHAIN_ID;
        metamask.announceOn(walletEvents);
        rabby.announceOn(walletEvents);

        const choosing = await reach(connection, 'choosingWallet');
        expect(choosing.data).toEqual({ wallets: [metamask.info, rabby.info] });

        chooseWallet('not-a-wallet');
        await sleep(20);
        chooseWallet(rabby.info.uuid);
        const ready = await reach(connection, 'ready');
        expect(ready.data).toEqual(
            expect.objectContaining({ wallet: rabby.info })
        );
        close();
    });

    test('on another chain the app can ask to switch, and an accepted switch is followed', async () => {
        const { connection, walletEvents, switchToExpectedChain, close } =
            startWallet();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.announceOn(walletEvents);

        const wrong = await reach(connection, 'wrongNetwork');
        expect(wrong.data).toEqual({
            wallet: metamask.info,
            url: metamask.info.rdns,
            found: OTHER_CHAIN_ID.toString(),
            expected: EXPECTED_CHAIN_ID.toString(),
            failedChecks: 0,
            lastSwitchError: '',
        });
        switchToExpectedChain();
        await reach(connection, 'ready');
        expect(metamask.switchRequests).toBe(1);
        close();
    });

    test('a declined switch is not asked again until the user changes chain', async () => {
        const { connection, walletEvents, switchToExpectedChain, close } =
            startWallet();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.onSwitchRequest = 'decline';
        metamask.announceOn(walletEvents);
        await reach(connection, 'wrongNetwork');

        switchToExpectedChain();
        await reach(connection, 'switchDeclined');
        switchToExpectedChain();
        switchToExpectedChain();
        await sleep(50);
        expect(metamask.switchRequests).toBe(1);

        metamask.changeChain(EXPECTED_CHAIN_ID);
        await reach(connection, 'ready');
        close();
    });

    test('a switch the wallet fails returns to the other chain with its error, and may be asked again', async () => {
        const { connection, walletEvents, switchToExpectedChain, close } =
            startWallet();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.onSwitchRequest = 'fail';
        metamask.announceOn(walletEvents);
        await reach(connection, 'wrongNetwork');

        const visited = recordNodes(connection);
        switchToExpectedChain();
        await sleep(50);
        const latest = await reach(connection, 'wrongNetwork');
        expect(latest.data).toEqual(
            expect.objectContaining({
                lastSwitchError: 'Unrecognized chain ID.',
            })
        );
        expect(visited).toContain('askingToSwitchChain');

        metamask.onSwitchRequest = 'accept';
        switchToExpectedChain();
        await reach(connection, 'ready');
        expect(metamask.switchRequests).toBe(2);
        close();
    });

    test('a wallet that does not answer is asked again with backoff', async () => {
        const { connection, walletEvents, close } = startWallet();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.chainId = EXPECTED_CHAIN_ID;
        metamask.answers = false;
        metamask.announceOn(walletEvents);

        const unreachable = await reach(connection, 'unreachable');
        expect(unreachable.data).toEqual(
            expect.objectContaining({ failedChecks: 1 })
        );
        await reach(connection, 'unreachable');
        metamask.answers = true;
        await reach(connection, 'ready');
        close();
    });

    test('a chain change while ready is followed to the other chain', async () => {
        const { connection, walletEvents, close } = startWallet();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.chainId = EXPECTED_CHAIN_ID;
        metamask.announceOn(walletEvents);
        await reach(connection, 'ready');

        metamask.changeChain(OTHER_CHAIN_ID);
        await reach(connection, 'wrongNetwork');
        close();
        await reach(connection, 'closed');
    });

    test('while ready it checks in the background, carrying the latest block', async () => {
        const { connection, walletEvents, close } = startWallet();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.chainId = EXPECTED_CHAIN_ID;
        metamask.announceOn(walletEvents);
        await reach(connection, 'ready');

        metamask.blockNumber = 2;
        await sleep(150);
        const ready = await reach(connection, 'ready');
        expect(ready.data).toEqual(
            expect.objectContaining({ health: { blockNumber: 2 } })
        );
        close();
    });

    test('a failed read checks at once, and a disconnect and reconnect recover', async () => {
        const { connection, walletEvents, reportReadFailed, close } =
            startWallet();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.chainId = EXPECTED_CHAIN_ID;
        metamask.announceOn(walletEvents);
        await reach(connection, 'ready');

        metamask.answers = false;
        reportReadFailed();
        await reach(connection, 'unreachable');

        metamask.answers = true;
        metamask.emit('connect', { chainId: toQuantity(EXPECTED_CHAIN_ID) });
        await reach(connection, 'ready');

        metamask.emit('disconnect');
        await reach(connection, 'unreachable');
        close();
    });

    test('going offline pauses it, and coming back online checks again', async () => {
        const { connection, walletEvents, network$, close } = startWallet();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.chainId = EXPECTED_CHAIN_ID;
        metamask.announceOn(walletEvents);
        await reach(connection, 'ready');

        network$.next({ node: 'offline' });
        const offline = await reach(connection, 'offline');
        expect(offline.data).toEqual({ wallet: metamask.info });
        network$.next({ node: 'online' });
        await reach(connection, 'ready');
        close();
    });
});
