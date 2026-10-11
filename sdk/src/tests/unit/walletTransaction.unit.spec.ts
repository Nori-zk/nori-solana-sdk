import { type TransactionRequest } from 'ethers';
import { createWalletTransactionMachine } from '../../rpc/connection/walletTransaction.impl.js';
import {
    createFakeEthereumProvider,
    createFakeSolanaRpc,
    createFakeWallet,
    createTestConnections,
    type FakeSentTransaction,
    QUEUE_ADDRESS,
    statesDuring,
    waitForNode,
    TARGET_A,
    transactionHashOf,
} from '../testUtils.js';

/** A transaction the fake Ethereum node has mined, at block 100. */
const MINED = transactionHashOf(0n);

/** The call the machine is made for. */
const CALL = { to: QUEUE_ADDRESS, data: '0x1234', value: 0n };

/** The error a wallet gives when the user says no. */
const rejected = () => Object.assign(new Error('User rejected the request.'), { code: 'ACTION_REJECTED' });

/** The error ethers gives when the contract reverts a send at gas estimation with a named error. */
const reverted = (name: string) =>
    Object.assign(new Error(`execution reverted: ${name}()`), {
        code: 'CALL_EXCEPTION',
        revert: { name, signature: `${name}()`, args: [] },
        data: '0x',
    });

/** A send the wallet made: the transaction's hash, from the user's account. */
const sentAs = (hash: string) => (): Promise<FakeSentTransaction> => Promise.resolve({ hash, from: TARGET_A, nonce: 0 });

/**
 * Both chains, connected, with a wallet on Ethereum whose machine starts at
 * `walletNode` and which answers its sends in turn with `answers` (the last
 * one again once they run out); `asked` holds the transactions it was asked to send.
 */
async function setUp(walletNode: string, ...answers: (() => Promise<FakeSentTransaction>)[]) {
    const asked: TransactionRequest[] = [];
    const fake = createFakeWallet(walletNode, (transaction) => {
        asked.push(transaction);
        return answers[Math.min(asked.length, answers.length) - 1]();
    });
    const ethereum = createFakeEthereumProvider([{ requestId: 0n, blockNumber: 100, target: TARGET_A }], {
        latestBlock: 200,
    });
    const solana = await createFakeSolanaRpc([]);
    const test = createTestConnections(ethereum.provider, solana.rpc, fake.wallet);
    await waitForNode(test.connections.ethereum.http.connection, 'ready');
    return { ...fake, ...test, asked };
}

/** The nodes a machine is in while watched for 50 ms. */
const nodesDuring = async (machine: Parameters<typeof statesDuring>[0]) =>
    new Set((await statesDuring(machine, 50)).map((state) => (state as { node: string }).node));

describe('Ethereum wallet transaction machine', () => {
    test('waits in ready for a send request, then asks the wallet and follows the transaction until it is confirmed', async () => {
        const { connections, asked, close } = await setUp('ready', sentAs(MINED));
        const transaction = createWalletTransactionMachine(connections, CALL);

        expect(await nodesDuring(transaction)).toEqual(new Set(['ready']));
        expect(asked).toEqual([]);
        transaction.send();
        const confirmed = await waitForNode(transaction, 'confirmed');
        expect(confirmed.data).toEqual(
            expect.objectContaining({ transactionHash: MINED, receipt: expect.objectContaining({ status: 1 }) })
        );
        expect(asked).toEqual([expect.objectContaining(CALL)]);
        transaction.close();
        close();
    });

    test('a send request while the wallet is not ready is refused, naming the wallet, and never sent by itself', async () => {
        const { connections, state$, asked, close } = await setUp('wrongNetwork', sentAs(MINED));
        const transaction = createWalletTransactionMachine(connections, CALL);

        transaction.send();
        const notReady = await waitForNode(transaction, 'notReadyToSend');
        expect(notReady.data).toEqual({ transaction: expect.objectContaining(CALL), waitingOn: ['ethereum.wallet'] });
        state$.next({ node: 'ready', data: {} });
        await waitForNode(transaction, 'ready');
        expect(await nodesDuring(transaction)).toEqual(new Set(['ready']));
        expect(asked).toEqual([]);
        transaction.close();
        close();
    });

    test('a declined request is asked again only on a send request', async () => {
        const { connections, asked, close } = await setUp('ready', () => Promise.reject(rejected()), sentAs(MINED));
        const transaction = createWalletTransactionMachine(connections, CALL);

        transaction.send();
        await waitForNode(transaction, 'declined');
        expect(await nodesDuring(transaction)).toEqual(new Set(['declined']));
        expect(asked).toHaveLength(1);
        transaction.send();
        await waitForNode(transaction, 'confirmed');
        expect(asked).toHaveLength(2);
        transaction.close();
        close();
    });

    test('a request the contract reverts holds the revert name; one the wallet refuses holds its error', async () => {
        const refusedByContract = await setUp('ready', () => Promise.reject(reverted('QueueFull')));
        const refusing = createWalletTransactionMachine(refusedByContract.connections, CALL);
        refusing.send();
        const refused = await waitForNode(refusing, 'refused');
        expect(refused.data).toEqual({ transaction: expect.objectContaining(CALL), errorName: 'QueueFull' });
        refusing.close();
        refusedByContract.close();

        const refusedByWallet = await setUp('ready', () => Promise.reject(new Error('Internal JSON-RPC error.')));
        const failing = createWalletTransactionMachine(refusedByWallet.connections, CALL);
        failing.send();
        const failed = await waitForNode(failing, 'sendFailed');
        expect(failed.data).toEqual({
            transaction: expect.objectContaining(CALL),
            error: expect.stringContaining('Internal JSON-RPC error'),
        });
        failing.close();
        refusedByWallet.close();
    });

    test('the wallet dropping while asking fails the request, which is not sent again by itself', async () => {
        const { connections, state$, asked, close } = await setUp(
            'ready',
            () => new Promise<FakeSentTransaction>(() => undefined)
        );
        const transaction = createWalletTransactionMachine(connections, CALL);

        transaction.send();
        await waitForNode(transaction, 'askingToSign');
        state$.next({ node: 'waitingToRetry', data: {} });
        const failed = await waitForNode(transaction, 'sendFailed');
        expect(failed.data).toEqual({
            transaction: expect.objectContaining(CALL),
            error: expect.stringContaining('disconnected'),
        });
        state$.next({ node: 'ready', data: {} });
        expect(await nodesDuring(transaction)).toEqual(new Set(['sendFailed']));
        expect(asked).toHaveLength(1);
        transaction.close();
        close();
    });
});
