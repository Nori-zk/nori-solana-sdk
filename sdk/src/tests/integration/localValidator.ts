import { type Address, createSolanaRpc, generateKeyPairSigner, lamports, type Signature } from '@solana/kit';
import { type Connections, createConnections } from '../../rpc/connection/connections.js';
import { createSignatureStatusMachine } from '../../transaction/signatureStatus.impl.js';
import { waitForState } from '../testUtils.js';

/** A running local validator (`surfpool start`): its RPC and websocket. */
export const RPC_URL = process.env.SOLANA_RPC_URL ?? 'http://127.0.0.1:8899';
export const WS_URL = process.env.SOLANA_WS_URL ?? 'ws://127.0.0.1:8900';

/** What a test does to the validator directly: airdrops, planting accounts, and reading where it is. */
export const validator = createSolanaRpc(RPC_URL);

/** How long a change on the validator may take to reach a machine, finality included. */
export const WAIT_MS = 60_000;

/**
 * The app's connections to the local validator alone: the network check
 * probes the validator, and Nori's websocket points at a closed port.
 *
 * @param websocketUrl Solana's websocket.
 * @param walletEvents Where a Solana wallet registers itself, for connections with one.
 * @returns The connections.
 */
export async function connectTo(websocketUrl: string, walletEvents?: EventTarget) {
    const expectedGenesisHash = await validator.getGenesisHash().send();
    return createConnections({
        ethereum: { expectedChainId: 1n },
        solana: {
            expectedGenesisHash,
            http: { rpcUrls: [RPC_URL] },
            websocket: { url: websocketUrl },
            ...(walletEvents && { wallet: true as const }),
        },
        nori: { websocket: { url: 'ws://127.0.0.1:1' } },
        network: { probeUrls: [RPC_URL] },
        healthChecks: { intervalMs: 500 },
        ...(walletEvents && { walletEvents }),
    });
}

/**
 * Airdrops `sol` SOL to `address`. Two airdrops of the same amount within
 * one blockhash are the same transaction, so a second airdrop to an address
 * takes another amount.
 *
 * @param address The receiving account.
 * @param sol The amount in SOL (default: 1).
 * @returns The airdrop's signature.
 */
export const airdrop = (address: Address, sol = 1n) =>
    validator.requestAirdrop(address, lamports(sol * 1_000_000_000n)).send();

/** A fresh address no transaction has touched yet. */
export const freshAddress = async () => (await generateKeyPairSigner()).address;

/**
 * Waits until a transaction is confirmed, read through the signature status machine.
 *
 * @param connections The connections.
 * @param signature The transaction.
 * @returns Once it is confirmed.
 */
export async function landed(connections: Connections, signature: Signature) {
    const status = createSignatureStatusMachine(connections, signature, 'confirmed');
    await waitForState(
        status,
        (state) =>
            state.node === 'current' &&
            state.data.status !== undefined &&
            state.data.status.confirmationStatus !== 'processed',
        WAIT_MS
    );
    status.close();
}

/**
 * Writes an account with surfpool's `surfnet_setAccount` cheatcode: `data`,
 * owned by `owner`, rent-exempt.
 *
 * @param address The account.
 * @param owner The program that owns it.
 * @param data Its data.
 * @param executable Whether it is a program (default: no).
 */
export async function setAccount(address: Address, owner: Address, data: Uint8Array, executable = false) {
    const rentExempt = await validator.getMinimumBalanceForRentExemption(BigInt(data.length)).send();
    const response = await fetch(RPC_URL, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            method: 'surfnet_setAccount',
            params: [
                address,
                { lamports: Number(rentExempt), data: Buffer.from(data).toString('hex'), owner, executable },
            ],
        }),
    });
    const answer = (await response.json()) as { error?: unknown };
    if (answer.error !== undefined) throw new Error(`surfnet_setAccount failed: ${JSON.stringify(answer.error)}`);
}
