import { toQuantity } from 'ethers';
import {
    type Eip1193EventProvider,
    USER_REJECTED_REQUEST,
} from '../../proofRequest/rpc/eth/eip1193.js';
import { createEthereumWalletMachine } from '../../proofRequest/rpc/eth/ystate/ethereumWallet.impl.js';
import type { WalletInfo } from '../../proofRequest/rpc/eth/ystate/ethereumWallet.js';
import {
    EXPECTED_CHAIN_ID,
    FAST_TIMINGS,
    reach,
    recordNodes,
    sleep,
} from '../testUtils.js';

const OTHER_CHAIN_ID = 1n;

/**
 * A wallet as an extension provides it: answers `eth_chainId`, takes switch
 * requests, and emits `chainChanged`. `onSwitchRequest` decides what the
 * user does with a switch request.
 */
class FakeWallet implements Eip1193EventProvider {
    chainId = OTHER_CHAIN_ID;
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
        if (method === 'eth_chainId') {
            if (!this.answers) return new Promise(() => undefined);
            return toQuantity(this.chainId);
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
    changeChain(chainId: bigint) {
        this.chainId = chainId;
        setTimeout(() =>
            [...(this.listeners.get('chainChanged') ?? [])].forEach(
                (listener) => listener(toQuantity(chainId))
            )
        );
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

/** Starts the wallet machine over its own announcement target. */
function startWalletMachine() {
    const walletEvents = new EventTarget();
    const wallet = createEthereumWalletMachine({
        ...FAST_TIMINGS,
        expectedChainId: EXPECTED_CHAIN_ID,
        walletSearchMs: 30,
        walletEvents,
        injectedProvider: undefined,
    });
    return { ...wallet, walletEvents };
}

describe('Ethereum wallet machine', () => {
    test('with no wallet it says so, and moves on when one is installed later', async () => {
        const { ethereumWallet, walletEvents, close } = startWalletMachine();
        await reach(ethereumWallet, 'noWalletFound');

        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.chainId = EXPECTED_CHAIN_ID;
        metamask.announceOn(walletEvents);
        const onChain = await reach(ethereumWallet, 'onExpectedChain');
        expect(onChain.data).toEqual({
            wallet: metamask.info,
            chainId: EXPECTED_CHAIN_ID,
        });
        close();
    });

    test('with several wallets it waits for the app to choose one', async () => {
        const { ethereumWallet, walletEvents, chooseWallet, close } =
            startWalletMachine();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        const rabby = new FakeWallet(walletInfo('Rabby'));
        rabby.chainId = EXPECTED_CHAIN_ID;
        metamask.announceOn(walletEvents);
        rabby.announceOn(walletEvents);

        const choosing = await reach(ethereumWallet, 'choosingWallet');
        expect(choosing.data).toEqual({ wallets: [metamask.info, rabby.info] });

        chooseWallet('not-a-wallet');
        await sleep(20);
        chooseWallet(rabby.info.uuid);
        const onChain = await reach(ethereumWallet, 'onExpectedChain');
        expect(onChain.data).toEqual({
            wallet: rabby.info,
            chainId: EXPECTED_CHAIN_ID,
        });
        close();
    });

    test('on another chain the app can ask to switch, and an accepted switch is followed', async () => {
        const { ethereumWallet, walletEvents, switchToExpectedChain, close } =
            startWalletMachine();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.announceOn(walletEvents);

        const other = await reach(ethereumWallet, 'onOtherChain');
        expect(other.data).toEqual({
            wallet: metamask.info,
            chainId: OTHER_CHAIN_ID,
            expectedChainId: EXPECTED_CHAIN_ID,
            lastSwitchError: '',
        });
        switchToExpectedChain();
        await reach(ethereumWallet, 'onExpectedChain');
        expect(metamask.switchRequests).toBe(1);
        close();
    });

    test('a declined switch is not asked again until the user changes chain', async () => {
        const { ethereumWallet, walletEvents, switchToExpectedChain, close } =
            startWalletMachine();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.onSwitchRequest = 'decline';
        metamask.announceOn(walletEvents);
        await reach(ethereumWallet, 'onOtherChain');

        switchToExpectedChain();
        await reach(ethereumWallet, 'switchDeclined');
        switchToExpectedChain();
        switchToExpectedChain();
        await sleep(50);
        expect(metamask.switchRequests).toBe(1);

        metamask.changeChain(EXPECTED_CHAIN_ID);
        await reach(ethereumWallet, 'onExpectedChain');
        close();
    });

    test('a switch the wallet fails returns to the other chain with its error, and may be asked again', async () => {
        const { ethereumWallet, walletEvents, switchToExpectedChain, close } =
            startWalletMachine();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.onSwitchRequest = 'fail';
        metamask.announceOn(walletEvents);
        await reach(ethereumWallet, 'onOtherChain');

        const visited = recordNodes(ethereumWallet);
        switchToExpectedChain();
        await sleep(50);
        const latest = await reach(ethereumWallet, 'onOtherChain');
        expect(latest.data).toEqual(
            expect.objectContaining({
                lastSwitchError: 'Unrecognized chain ID.',
            })
        );
        expect(visited).toContain('askingToSwitchChain');

        metamask.onSwitchRequest = 'accept';
        switchToExpectedChain();
        await reach(ethereumWallet, 'onExpectedChain');
        expect(metamask.switchRequests).toBe(2);
        close();
    });

    test('a wallet that does not answer is asked again with backoff', async () => {
        const { ethereumWallet, walletEvents, close } = startWalletMachine();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.chainId = EXPECTED_CHAIN_ID;
        metamask.answers = false;
        metamask.announceOn(walletEvents);

        const notResponding = await reach(
            ethereumWallet,
            'walletNotResponding'
        );
        expect(notResponding.data).toEqual(
            expect.objectContaining({ failedChecks: 1 })
        );
        await reach(ethereumWallet, 'walletNotResponding');
        metamask.answers = true;
        await reach(ethereumWallet, 'onExpectedChain');
        close();
    });

    test('a chain change while on the expected chain is followed to the other chain', async () => {
        const { ethereumWallet, walletEvents, close } = startWalletMachine();
        const metamask = new FakeWallet(walletInfo('MetaMask'));
        metamask.chainId = EXPECTED_CHAIN_ID;
        metamask.announceOn(walletEvents);
        await reach(ethereumWallet, 'onExpectedChain');

        metamask.changeChain(OTHER_CHAIN_ID);
        await reach(ethereumWallet, 'onOtherChain');
        close();
        await reach(ethereumWallet, 'closed');
    });
});
