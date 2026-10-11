import {
    AccountRole,
    address,
    type Address,
    generateKeyPairSigner,
    getAddressEncoder,
    getBase58Decoder,
    getBase58Encoder,
    getBase64EncodedWireTransaction,
    getSignatureFromTransaction,
    getTransactionDecoder,
    getU32Encoder,
    getU64Encoder,
    type Instruction,
    type KeyPairSigner,
    partiallySignTransaction,
    signature as signatureOf,
    type TransactionSigner,
} from '@solana/kit';
import { airdrop, connectTo, freshAddress, landed, validator, WAIT_MS, WS_URL } from './localValidator.js';
import { createSolanaSignerTransactionMachine } from '../../transaction/solanaSignerTransaction.impl.js';
import { createSolanaWalletTransactionMachine } from '../../transaction/solanaWalletTransaction.impl.js';
import { createSolanaChangesMachine } from '../../rpc/solana/solanaChanges.impl.js';
import { createSolanaAccountMachine } from '../../solana/account.impl.js';
import { createTransactionLogsMachine } from '../../solana/transactionLogs.impl.js';
import { createSignatureStatusMachine } from '../../transaction/signatureStatus.impl.js';
import { nodesUntil, statesDuring, waitForNode, waitForState } from '../testUtils.js';

const SYSTEM_PROGRAM_ADDRESS = address('11111111111111111111111111111111');

/**
 * The system program's transfer of `lamports` from `from` to `to`.
 *
 * @param from The signer the lamports leave.
 * @param to The receiving account.
 * @param lamports How many.
 * @returns The instruction.
 */
const transferInstruction = (from: TransactionSigner, to: Address, lamports: bigint): Instruction => ({
    programAddress: SYSTEM_PROGRAM_ADDRESS,
    accounts: [
        { address: from.address, role: AccountRole.WRITABLE_SIGNER },
        { address: to, role: AccountRole.WRITABLE },
    ],
    // The system program's `Transfer` (2), then the amount.
    data: new Uint8Array([...getU32Encoder().encode(2), ...getU64Encoder().encode(lamports)]),
});

/**
 * A Solana wallet as an extension offers one (Wallet Standard), holding
 * `keyPair`: it registers itself on `walletEvents`, shares its account only
 * once asked, and signs each transaction and sends it to the validator.
 * With `declines`, the user says no to every transaction.
 *
 * @param walletEvents Where it registers itself.
 * @param keyPair The account's key pair.
 * @param declines Whether the user declines every transaction.
 */
function registerWallet(walletEvents: EventTarget, keyPair: KeyPairSigner, declines = false) {
    const account = {
        address: keyPair.address,
        publicKey: new Uint8Array(getAddressEncoder().encode(keyPair.address)),
        chains: ['solana:localnet'],
        features: ['solana:signAndSendTransaction'],
    };
    const wallet = {
        version: '1.0.0',
        name: 'Test wallet',
        icon: 'data:image/svg+xml;base64,',
        chains: ['solana:localnet'],
        accounts: [] as (typeof account)[],
        features: {
            'standard:connect': {
                version: '1.0.0',
                connect: async ({ silent = false }: { silent?: boolean } = {}) => {
                    if (!silent) wallet.accounts = [account];
                    return { accounts: wallet.accounts };
                },
            },
            'solana:signAndSendTransaction': {
                version: '1.0.0',
                supportedTransactionVersions: [0],
                signAndSendTransaction: async (...inputs: { transaction: Uint8Array }[]) => {
                    if (declines) throw Object.assign(new Error('The user rejected the request.'), { code: 4001 });
                    const outputs = [];
                    for (const { transaction } of inputs) {
                        const signed = await partiallySignTransaction([keyPair.keyPair], getTransactionDecoder().decode(transaction));
                        await validator
                            .sendTransaction(getBase64EncodedWireTransaction(signed), {
                                encoding: 'base64',
                                preflightCommitment: 'confirmed',
                            })
                            .send();
                        outputs.push({ signature: new Uint8Array(getBase58Encoder().encode(getSignatureFromTransaction(signed))) });
                    }
                    return outputs;
                },
            },
        },
    };
    const register = (api: { register(...wallets: unknown[]): unknown }) => api.register(wallet);
    walletEvents.addEventListener('wallet-standard:app-ready', (event) => {
        if (event instanceof CustomEvent) register(event.detail);
    });
    walletEvents.dispatchEvent(new CustomEvent('wallet-standard:register-wallet', { detail: register }));
}

describe('the Solana changes machine, on a local validator', () => {
    test('subscribes for new roots: the node acknowledges, and each root is a change', async () => {
        const connections = await connectTo(WS_URL);
        const changes = createSolanaChangesMachine(connections.solana);
        await waitForState(changes, (state) => state.node === 'subscribed' && state.data.acknowledged, WAIT_MS);
        await waitForState(changes, (state) => state.node === 'subscribed' && state.data.changes >= 2, WAIT_MS);
        changes.close();
        connections.close();
    });

    test('an airdrop is pushed to a subscription on the account and on transactions mentioning it', async () => {
        const connections = await connectTo(WS_URL);
        const address = await freshAddress();
        const accountChanges = createSolanaChangesMachine(connections.solana, { account: address });
        const mentions = createSolanaChangesMachine(connections.solana, { mentions: address });
        await Promise.all(
            [accountChanges, mentions].map((machine) =>
                waitForState(machine, (state) => state.node === 'subscribed' && state.data.acknowledged, WAIT_MS)
            )
        );

        await airdrop(address);
        await Promise.all(
            [accountChanges, mentions].map((machine) =>
                waitForState(machine, (state) => state.node === 'subscribed' && state.data.changes >= 1, WAIT_MS)
            )
        );
        accountChanges.close();
        mentions.close();
        connections.close();
    });

    test('with no websocket it polls, and an airdrop to the account is a change', async () => {
        const connections = await connectTo('ws://127.0.0.1:1');
        const address = await freshAddress();
        const changes = createSolanaChangesMachine(connections.solana, { account: address });
        await waitForState(changes, (state) => state.node === 'polling' && state.data.lastPolled !== undefined, WAIT_MS);

        await airdrop(address);
        await waitForState(changes, (state) => state.node === 'polling' && state.data.changes >= 1, WAIT_MS);
        changes.close();
        connections.close();
    });
});

describe('the Solana value machines, on a local validator', () => {
    test('the signature status machine follows an airdrop until it is finalized', async () => {
        const connections = await connectTo(WS_URL);
        const signature = await airdrop(await freshAddress());
        const status = createSignatureStatusMachine(connections, signature);
        const finalized = await waitForState(
            status,
            (state) => state.node === 'current' && state.data.status?.confirmationStatus === 'finalized',
            WAIT_MS
        );
        expect(finalized.node === 'current' && finalized.data.status?.err).toBeNull();
        status.close();
        connections.close();
    });

    test('a transaction the cluster never sees ends expired once the block height passes its last valid one', async () => {
        const connections = await connectTo(WS_URL);
        const neverSent = signatureOf(getBase58Decoder().decode(crypto.getRandomValues(new Uint8Array(64))));
        const lastValidBlockHeight = Number(await validator.getBlockHeight({ commitment: 'finalized' }).send());
        const status = createSignatureStatusMachine(connections, neverSent, 'finalized', lastValidBlockHeight);
        const expired = await waitForNode(status, 'expired', WAIT_MS);
        expect(expired.data.blockHeight).toBeGreaterThan(lastValidBlockHeight);
        connections.close();
    });

    test('the account machine reads an account and reads it again when an airdrop changes it', async () => {
        const connections = await connectTo(WS_URL);
        const address = await freshAddress();
        await airdrop(address);
        const account = createSolanaAccountMachine(connections, address);
        const first = await waitForState(
            account,
            (state) => state.node === 'current' && state.data.account?.lamports === 1_000_000_000n,
            WAIT_MS
        );
        expect(first.node === 'current' && first.data.account?.owner).toBe('11111111111111111111111111111111');

        await airdrop(address, 2n);
        await waitForState(
            account,
            (state) => state.node === 'current' && state.data.account?.lamports === 3_000_000_000n,
            WAIT_MS
        );
        account.close();
        connections.close();
    });

    test('the signer transaction machine sends a transfer and follows it until it is finalized', async () => {
        const connections = await connectTo(WS_URL);
        const payer = await generateKeyPairSigner();
        await landed(connections, await airdrop(payer.address));
        const transfer = createSolanaSignerTransactionMachine(connections, payer, [
            transferInstruction(payer, await freshAddress(), 1_000_000n),
        ]);
        await waitForNode(transfer, 'ready', WAIT_MS);
        transfer.send();
        const confirmed = await waitForNode(transfer, 'confirmed', WAIT_MS);
        expect(confirmed.data.status.err).toBeNull();
        expect(confirmed.data.status.confirmationStatus).toBe('finalized');
        connections.close();
    });

    test('the signer transaction machine is refused, with the program error, when the program fails it in preflight', async () => {
        const connections = await connectTo(WS_URL);
        const payer = await generateKeyPairSigner();
        await landed(connections, await airdrop(payer.address));
        // More than the payer holds: the system program fails it with its custom error 1 (insufficient funds).
        const overdraft = createSolanaSignerTransactionMachine(connections, payer, [
            transferInstruction(payer, await freshAddress(), 5_000_000_000n),
        ]);
        await waitForNode(overdraft, 'ready', WAIT_MS);
        overdraft.send();
        const refused = await waitForNode(overdraft, 'refused', WAIT_MS);
        expect(refused.data).toEqual(
            expect.objectContaining({ programAddress: SYSTEM_PROGRAM_ADDRESS, errorCode: 1, errorName: 'custom error 1' })
        );
        expect(refused.data.logs.length).toBeGreaterThan(0);
        overdraft.close();
        connections.close();
    });

    test('the wallet transaction machine waits for the wallet, which the user connects, then sends and follows a transfer', async () => {
        const walletEvents = new EventTarget();
        const owner = await generateKeyPairSigner();
        registerWallet(walletEvents, owner);
        const connections = await connectTo(WS_URL, walletEvents);
        await landed(connections, await airdrop(owner.address));
        const wallet = connections.solana.wallet.connection;
        if (wallet === undefined) throw new Error('The connections have a Solana wallet.');
        await waitForNode(wallet, 'notConnected', WAIT_MS);

        const transfer = createSolanaWalletTransactionMachine(connections, [
            transferInstruction(owner, await freshAddress(), 1_000_000n),
        ]);
        transfer.send();
        const notReady = await waitForNode(transfer, 'notReadyToSend', WAIT_MS);
        expect(notReady.data.waitingOn).toEqual(['solana.wallet']);

        connections.solana.wallet.connect();
        const ready = await waitForNode(wallet, 'ready', WAIT_MS);
        expect(ready.data.account).toBe(owner.address);
        await waitForNode(transfer, 'ready', WAIT_MS);
        transfer.send();
        const confirmed = await waitForNode(transfer, 'confirmed', WAIT_MS);
        expect(confirmed.data.status.err).toBeNull();
        connections.close();
    });

    test('the wallet transaction machine is declined when the user says no, and can be sent again', async () => {
        const walletEvents = new EventTarget();
        const owner = await generateKeyPairSigner();
        registerWallet(walletEvents, owner, true);
        const connections = await connectTo(WS_URL, walletEvents);
        const wallet = connections.solana.wallet.connection;
        if (wallet === undefined) throw new Error('The connections have a Solana wallet.');
        await waitForNode(wallet, 'notConnected', WAIT_MS);
        connections.solana.wallet.connect();
        await waitForNode(wallet, 'ready', WAIT_MS);

        const transfer = createSolanaWalletTransactionMachine(connections, [
            transferInstruction(owner, await freshAddress(), 1_000_000n),
        ]);
        const declined = nodesUntil(transfer, 'declined', WAIT_MS);
        transfer.send();
        expect(await declined).toEqual(['askingToSign', 'declined']);
        const declinedAgain = nodesUntil(transfer, 'declined', WAIT_MS);
        transfer.send();
        expect(await declinedAgain).toEqual(['askingToSign', 'declined']);
        transfer.close();
        connections.close();
    });

    test('the transaction logs machine gives the logs of each new transaction mentioning an address', async () => {
        const connections = await connectTo(WS_URL);
        const address = await freshAddress();
        await airdrop(address);
        const logs = createTransactionLogsMachine(connections, address);
        // The first read only finds where to start: the airdrop before it does not arrive.
        const first = await waitForNode(logs, 'current', WAIT_MS);
        expect(first.data.transactions).toEqual([]);

        const signature = await airdrop(address, 2n);
        const found = await waitForState(
            logs,
            (state) => state.node === 'current' && state.data.transactions.some((logged) => logged.signature === signature),
            WAIT_MS
        );
        const logged = found.node === 'current' ? found.data.transactions.find((entry) => entry.signature === signature) : undefined;
        expect(logged?.err).toBeNull();
        expect(logged?.logs.length).toBeGreaterThan(0);
        logs.close();
        connections.close();
    });
});

describe('the Solana websocket on the local validator', () => {
    test('checks itself with a subscription the node acknowledges, and a quiet socket stays ready across heartbeats: the node answers the ping', async () => {
        const connections = await connectTo(WS_URL);
        const websocket = connections.solana.websocket.connection;
        await waitForNode(websocket, 'ready', WAIT_MS);

        // With checks every 500 ms, a socket is pinged after 500 ms of quiet and dropped after 1000 ms without a reply.
        const quiet = await statesDuring(websocket, 4_000);

        expect(new Set(quiet.map(({ node }) => node))).toEqual(new Set(['ready']));
        connections.close();
    }, WAIT_MS);
});
