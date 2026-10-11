import { type Signer, type TransactionRequest } from 'ethers';
import { createSignerTransactionMachine } from '../../transaction/signerTransaction.impl.js';
import {
    createFakeEthereumProvider,
    createFakeSolanaRpc,
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
/** A transaction the fake Ethereum node has mined and reverted, at block 100. */
const REVERTED = transactionHashOf(1n);
/** A transaction the fake Ethereum node has not mined. */
const UNMINED = transactionHashOf(99n);

/** The call the machine is made for. */
const CALL = { to: QUEUE_ADDRESS, data: '0x1234', value: 0n };

/** The error ethers gives when the contract reverts a send at gas estimation with a named error. */
const reverted = (name: string) =>
    Object.assign(new Error(`execution reverted: ${name}()`), {
        code: 'CALL_EXCEPTION',
        revert: { name, signature: `${name}()`, args: [] },
        data: '0x',
    });

/** A send the node accepted: the transaction's hash, from the app's signer, with `nonce`. */
const sentAs =
    (hash: string, nonce = 5) =>
    (): Promise<FakeSentTransaction> =>
        Promise.resolve({ hash, from: TARGET_A, nonce });

/**
 * The app's own signer, answering its sends in turn with `answers` (the
 * last one again once they run out), and the transactions it was asked to send.
 */
function signerAnswering(...answers: (() => Promise<FakeSentTransaction>)[]) {
    const asked: TransactionRequest[] = [];
    const signer = {
        address: TARGET_A,
        sendTransaction: (transaction: TransactionRequest) => {
            asked.push(transaction);
            return answers[Math.min(asked.length, answers.length) - 1]();
        },
    } as unknown as Signer;
    return { signer, asked };
}

/** Both chains, connected. */
async function setUp() {
    const ethereum = createFakeEthereumProvider(
        [
            { requestId: 0n, blockNumber: 100, target: TARGET_A },
            { requestId: 1n, blockNumber: 100, target: TARGET_A, status: 0 },
        ],
        { latestBlock: 200 }
    );
    const solana = await createFakeSolanaRpc([]);
    const test = createTestConnections(ethereum.provider, solana.rpc);
    await waitForNode(test.connections.ethereum.http.connection, 'ready');
    return { ...test, ethereum: ethereum.state };
}

/** The nodes a machine is in while watched for 50 ms. */
const nodesDuring = async (machine: Parameters<typeof statesDuring>[0]) =>
    new Set((await statesDuring(machine, 50)).map((state) => (state as { node: string }).node));

describe('Ethereum signer transaction machine', () => {
    test('waits in ready for a send request, then sends and follows the transaction until it is confirmed', async () => {
        const { connections, close } = await setUp();
        const { signer, asked } = signerAnswering(sentAs(MINED));
        const transaction = createSignerTransactionMachine(connections, signer, CALL);

        expect(await nodesDuring(transaction)).toEqual(new Set(['ready']));
        expect(asked).toEqual([]);
        transaction.send();
        const confirmed = await waitForNode(transaction, 'confirmed');
        expect(confirmed.data).toEqual(
            expect.objectContaining({ transactionHash: MINED, receipt: expect.objectContaining({ status: 1 }) })
        );
        expect(asked).toEqual([expect.objectContaining({ ...CALL, nonce: undefined })]);
        transaction.close();
        close();
    });

    test('a transaction mined with a revert ends reverted', async () => {
        const { connections, close } = await setUp();
        const transaction = createSignerTransactionMachine(connections, signerAnswering(sentAs(REVERTED)).signer, CALL);

        transaction.send();
        const ended = await waitForNode(transaction, 'reverted');
        expect(ended.data).toEqual(expect.objectContaining({ receipt: expect.objectContaining({ status: 0 }) }));
        transaction.close();
        close();
    });

    test('a send the contract reverts holds the revert name, and is sent again only on a send request', async () => {
        const { connections, close } = await setUp();
        const { signer, asked } = signerAnswering(() => Promise.reject(reverted('QueueFull')), sentAs(MINED));
        const transaction = createSignerTransactionMachine(connections, signer, CALL);

        transaction.send();
        const refused = await waitForNode(transaction, 'refused');
        expect(refused.data).toEqual({ transaction: expect.objectContaining(CALL), errorName: 'QueueFull' });
        expect(await nodesDuring(transaction)).toEqual(new Set(['refused']));
        expect(asked).toHaveLength(1);
        transaction.send();
        await waitForNode(transaction, 'confirmed');
        expect(asked).toHaveLength(2);
        transaction.close();
        close();
    });

    test('any other failure to send holds the error, and is not sent again by itself', async () => {
        const { connections, close } = await setUp();
        const { signer, asked } = signerAnswering(() => Promise.reject(new Error('connect ECONNREFUSED')));
        const transaction = createSignerTransactionMachine(connections, signer, CALL);

        transaction.send();
        const failed = await waitForNode(transaction, 'sendFailed');
        expect(failed.data).toEqual({
            transaction: expect.objectContaining(CALL),
            error: expect.stringContaining('ECONNREFUSED'),
        });
        expect(await nodesDuring(transaction)).toEqual(new Set(['sendFailed']));
        expect(asked).toHaveLength(1);
        transaction.close();
        close();
    });

    test("a send request while Ethereum's connection is down is refused, naming the chain, and never sent by itself", async () => {
        const { connections, network$, close } = await setUp();
        const { signer, asked } = signerAnswering(sentAs(MINED));
        const transaction = createSignerTransactionMachine(connections, signer, CALL);

        network$.next('offline');
        await waitForNode(connections.ethereum.http.connection, 'offline');
        transaction.send();
        const notReady = await waitForNode(transaction, 'notReadyToSend');
        expect(notReady.data).toEqual({ transaction: expect.objectContaining(CALL), waitingOn: ['ethereum'] });
        network$.next('online');
        await waitForNode(transaction, 'ready');
        expect(await nodesDuring(transaction)).toEqual(new Set(['ready']));
        expect(asked).toEqual([]);
        transaction.close();
        close();
    });

    test("a transaction whose nonce the sender's mined nonce moved past, with no receipt, ends replaced", async () => {
        const { connections, ethereum, close } = await setUp();
        ethereum.pending.set(UNMINED, { from: TARGET_A, nonce: 5 });
        ethereum.minedNonces.set(TARGET_A, 5);
        const transaction = createSignerTransactionMachine(connections, signerAnswering(sentAs(UNMINED, 5)).signer, CALL);

        transaction.send();
        await waitForNode(transaction, 'current');
        ethereum.minedNonces.set(TARGET_A, 6);
        ethereum.latestBlock += 1;
        const replaced = await waitForNode(transaction, 'replaced');
        expect(replaced.data).toEqual(
            expect.objectContaining({ transactionHash: UNMINED, transaction: expect.objectContaining({ nonce: 5 }) })
        );
        transaction.close();
        close();
    });

    test('a transaction the node has not known for the blocks allowed ends dropped, and is sent again with its nonce', async () => {
        const { connections, ethereum, close } = await setUp();
        const { signer, asked } = signerAnswering(sentAs(UNMINED, 5), sentAs(MINED, 5));
        const transaction = createSignerTransactionMachine(connections, signer, CALL, { droppedAfterBlocks: 2 });

        transaction.send();
        await waitForNode(transaction, 'current');
        ethereum.latestBlock += 2;
        await waitForNode(transaction, 'dropped');
        expect(await nodesDuring(transaction)).toEqual(new Set(['dropped']));
        transaction.send();
        await waitForNode(transaction, 'confirmed');
        expect(asked.map(({ nonce }) => nonce)).toEqual([undefined, 5]);
        transaction.close();
        close();
    });
});
