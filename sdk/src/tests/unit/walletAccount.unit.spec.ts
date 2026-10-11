import { BrowserProvider, Network } from 'ethers';
import { BehaviorSubject, of } from 'rxjs';
import { type EthereumTransports } from '../../rpc/connection/connections.js';
import { createWalletAccountMachine } from '../../rpc/eth/walletAccount.impl.js';
import {
    createFakeEthereumProvider,
    createFakeSolanaRpc,
    createTestConnections,
    EXPECTED_CHAIN_ID,
    FAST_TIMINGS,
    nodesUntil,
    TARGET_A,
    TARGET_B,
    waitForNode,
} from '../testUtils.js';
import { type GraphState } from '../../utils/machines.js';

/** How the fake wallet answers `eth_requestAccounts`. */
type ShareAnswer = 'accept' | 'decline' | 'alreadyPending' | 'never';

/**
 * A wallet whose answers the test sets: the accounts it shares
 * (`eth_accounts`), how it answers a request to share one, and its
 * machine's node. `emit` fires one of its EIP-1193 events.
 */
function createAccountWallet() {
    const state$ = new BehaviorSubject<GraphState>({ node: 'ready', data: {} });
    const answers = { shared: [] as string[], answer: 'accept' as ShareAnswer };
    const listeners = new Map<string, Set<(value: unknown) => void>>();
    const walletProvider = {
        request: async ({ method }: { method: string }) => {
            if (method === 'eth_accounts') return answers.shared;
            if (answers.answer === 'decline') throw Object.assign(new Error('User rejected.'), { code: 4001 });
            if (answers.answer === 'alreadyPending')
                throw Object.assign(new Error('A request is already pending.'), { code: -32002 });
            if (answers.answer === 'never') return new Promise<never>(() => undefined);
            answers.shared = [TARGET_A];
            return answers.shared;
        },
        on: (event: string, listener: (value: unknown) => void) => {
            listeners.set(event, (listeners.get(event) ?? new Set()).add(listener));
        },
        removeListener: (event: string, listener: (value: unknown) => void) => {
            listeners.get(event)?.delete(listener);
        },
    };
    const network = Network.from(EXPECTED_CHAIN_ID);
    const provider = new BrowserProvider(walletProvider, network, { staticNetwork: network });
    const wallet = {
        connection: { state$ },
        current: () => provider,
        currentWalletProvider: () => walletProvider,
        walletProvider$: of(walletProvider),
        chooseWallet: (): void => undefined,
        switchToExpectedChain: (): void => undefined,
        reportReadFailed: (): void => undefined,
        close: (): void => undefined,
    } as unknown as NonNullable<EthereumTransports['wallet']>;
    const emit = (event: string, value: unknown) => listeners.get(event)?.forEach((listener) => listener(value));
    return { wallet, state$, answers, emit };
}

/** The connections with the account wallet on Ethereum, and its account machine. */
async function setUp() {
    const ethereum = createFakeEthereumProvider([], { latestBlock: 100 });
    const solana = await createFakeSolanaRpc([]);
    const accountWallet = createAccountWallet();
    const test = createTestConnections(ethereum.provider, solana.rpc, accountWallet.wallet);
    await waitForNode(test.connections.ethereum.http.connection, 'ready');
    const account = createWalletAccountMachine(test.connections, FAST_TIMINGS);
    return { ...accountWallet, ...test, account };
}

describe('wallet account machine', () => {
    test('reads no account, then shares one when the user accepts', async () => {
        const { account, close } = await setUp();
        const current = await waitForNode(account, 'current');
        expect(current.data.account).toBeUndefined();

        const moves = nodesUntil(account, (state) => state.node === 'current');
        account.shareAccount();
        expect(await moves).toEqual(['askingToShareAccount', 'current']);
        expect((await waitForNode(account, 'current')).data.account).toBe(TARGET_A);
        account.close();
        close();
    });

    test('a decline waits for a fresh ask, and connecting from inside the wallet reads again', async () => {
        const { account, answers, emit, close } = await setUp();
        answers.answer = 'decline';
        await waitForNode(account, 'current');

        const declined = nodesUntil(account, 'shareDeclined');
        account.shareAccount();
        expect(await declined).toEqual(['askingToShareAccount', 'shareDeclined']);

        answers.shared = [TARGET_B];
        const reread = nodesUntil(account, 'current');
        emit('accountsChanged', [TARGET_B]);
        expect(await reread).toEqual(['refreshing', 'current']);
        expect((await waitForNode(account, 'current')).data.account).toBe(TARGET_B);
        account.close();
        close();
    });

    test('any other failure goes back to current, with the error kept for logs', async () => {
        const { account, answers, close } = await setUp();
        answers.answer = 'alreadyPending';
        await waitForNode(account, 'current');

        const moves = nodesUntil(account, 'current');
        account.shareAccount();
        expect(await moves).toEqual(['askingToShareAccount', 'current']);
        const current = await waitForNode(account, 'current');
        expect(current.data.account).toBeUndefined();
        expect(current.data.lastShareError).toContain('already pending');
        account.close();
        close();
    });

    test('waits for the wallet, and the wallet leaving while asking waits for it again', async () => {
        const { account, answers, state$, close } = await setUp();
        answers.answer = 'never';
        await waitForNode(account, 'current');

        const asking = nodesUntil(account, 'askingToShareAccount');
        account.shareAccount();
        await asking;
        state$.next({ node: 'unreachable', data: {} });
        const waiting = await waitForNode(account, 'waitingForConnectionWhileRefreshing');
        expect(waiting.data.waitingOn).toEqual(['ethereum.wallet']);

        state$.next({ node: 'ready', data: {} });
        await waitForNode(account, 'current');
        account.close();
        close();
    });
});
