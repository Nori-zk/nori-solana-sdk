import { readFileSync } from 'node:fs';
import { address, generateKeyPairSigner, getAddressEncoder } from '@solana/kit';
import { request_witness } from '@nori-zk/ethereum-solana-proof-queue-utils-glam';
import { getBytes, hexlify, sha256, toBeHex } from 'ethers';
import { firstValueFrom } from 'rxjs';
import { getNoriSolTokenBridgeDecoder } from '../../program/accounts/noriSolTokenBridge.js';
import { getProofRequestRootEntryEncoder } from '../../program/accounts/proofRequestRootEntry.js';
import { TOKEN_ERROR__ZERO_MINT_AMOUNT } from '../../program/errors/token.js';
import { getInitializeInstructionAsync } from '../../program/instructions/initialize.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';
import {
    type ProofAvailableProofRequestSnapshot,
    proofRequestWitnessesOf,
} from '../../proofRequest/getProofRequestStateSnapshot.js';
import { ProofRequestState } from '../../proofRequest/types.js';
import { type Connections } from '../../rpc/connection/connections.js';
import { proofQueueBatchAddress$, stateAddress$ } from '../../rpc/solana/programAddresses.js';
import { mintCall$ } from '../../rpc/solana/tokenBridgeInstructions.js';
import { createMintedSoFarMachine } from '../../tokenBridge/mintedSoFar.impl.js';
import { createTokenBalanceMachine } from '../../tokenBridge/tokenBalance.impl.js';
import { createSolanaSignerTransactionMachine } from '../../transaction/solanaSignerTransaction.impl.js';
import { waitForNode, waitForState } from '../testUtils.js';
import { airdrop, connectTo, landed, setAccount, validator, WAIT_MS, WS_URL } from './localValidator.js';

/** The Nori program, as `cargo build-sbf` builds it. */
const PROGRAM_SO = new URL('../../../../target/deploy/token.so', import.meta.url);

/** The loader a program planted as one executable account runs under. */
const BPF_LOADER_2 = address('BPFLoader2111111111111111111111111111111111');

/** What the deposit locked on Ethereum, in the bridged token's units. */
const LOCKED_SO_FAR = 1_000n;

/**
 * Makes sure the Nori program runs on the validator and its bridge is
 * initialized: the program is planted at its address, and `initialize` sent
 * through the signer transaction machine, only when they are not there yet.
 *
 * @param connections The connections.
 * @returns The Ethereum token bridge address the bridge was initialized with, 0x-prefixed.
 */
async function bridgeOn(connections: Connections): Promise<string> {
    const program = await validator.getAccountInfo(TOKEN_PROGRAM_ADDRESS, { encoding: 'base64' }).send();
    if (program.value === null) await setAccount(TOKEN_PROGRAM_ADDRESS, BPF_LOADER_2, readFileSync(PROGRAM_SO), true);

    const state = await firstValueFrom(stateAddress$());
    if ((await validator.getAccountInfo(state, { encoding: 'base64' }).send()).value === null) {
        const authority = await generateKeyPairSigner();
        await landed(connections, await airdrop(authority.address));
        const initialize = createSolanaSignerTransactionMachine(
            connections,
            authority,
            [
                await getInitializeInstructionAsync({
                    payer: authority,
                    verifiedStateRoot: [new Uint8Array(32).fill(1)],
                    latestHeliosStoreInputHash: [new Uint8Array(32).fill(3)],
                    ethProofQueueAddress: [new Uint8Array(20).fill(4)],
                    ethTokenBridgeAddress: [new Uint8Array(20).fill(5)],
                    latestHead: 42,
                    queueCursor: 0,
                }),
            ],
            { commitment: 'confirmed' }
        );
        await waitForNode(initialize, 'ready', WAIT_MS);
        initialize.send();
        await waitForNode(initialize, 'confirmed', WAIT_MS);
    }
    const { value } = await validator.getAccountInfo(state, { encoding: 'base64' }).send();
    if (value === null) throw new Error('The bridge is initialized.');
    const { ethTokenBridgeAddress } = getNoriSolTokenBridgeDecoder().decode(Buffer.from(value.data[0], 'base64'));
    return hexlify(new Uint8Array(ethTokenBridgeAddress));
}

describe('minting on a local validator', () => {
    test('a recipient mints with its verified witness: the signer transaction is confirmed, and its minted-so-far and balance follow', async () => {
        const connections = await connectTo(WS_URL);
        const ethTokenBridgeAddress = await bridgeOn(connections);
        const recipient = await generateKeyPairSigner();
        await landed(connections, await airdrop(recipient.address));

        // A one-request batch, committed at a fresh index as `update` lays it out, holding the recipient's deposit.
        const leaf = {
            target: ethTokenBridgeAddress,
            collectionKeysCount: 1,
            collectionKeys: [sha256(new Uint8Array(getAddressEncoder().encode(recipient.address)))],
            value: toBeHex(LOCKED_SO_FAR, 32),
        };
        const { root } = request_witness({ leaves: [leaf], index: 0 });
        const proofQueueBatchIndex = 1_000_000n + BigInt(Math.floor(Math.random() * 2 ** 32));
        const proofQueueBatchAddress = await firstValueFrom(proofQueueBatchAddress$(proofQueueBatchIndex));
        await setAccount(
            proofQueueBatchAddress,
            TOKEN_PROGRAM_ADDRESS,
            new Uint8Array(
                getProofRequestRootEntryEncoder().encode({
                    root: getBytes(root),
                    outputBlockNumber: 100n,
                    inputQueueCursor: 0n,
                    outputQueueCursor: 1n,
                })
            )
        );
        const proofAvailable: ProofAvailableProofRequestSnapshot = {
            state: ProofRequestState.ProofAvailable,
            requestId: 0n,
            requestBlockNumber: 100n,
            queueCursor: 1n,
            proofQueueBatchIndex,
            proofQueueBatchAddress,
            root,
            inputQueueCursor: 0n,
            outputQueueCursor: 1n,
            outputBlockNumber: 100n,
            previousOutputBlockNumber: -1n,
            indexInBatch: 0n,
        };
        const { verifiedWitness } = proofRequestWitnessesOf([leaf], proofAvailable);

        const minted = createMintedSoFarMachine(connections, recipient.address);
        const balance = createTokenBalanceMachine(connections, recipient.address);
        await waitForState(minted, (state) => state.node === 'current' && state.data.minted === 0n, WAIT_MS);
        await waitForState(balance, (state) => state.node === 'current' && state.data.balance === 0n, WAIT_MS);

        const mint = createSolanaSignerTransactionMachine(
            connections,
            recipient,
            [await firstValueFrom(mintCall$(recipient, verifiedWitness, proofQueueBatchAddress))],
            { commitment: 'confirmed' }
        );
        await waitForNode(mint, 'ready', WAIT_MS);
        mint.send();
        await waitForNode(mint, 'confirmed', WAIT_MS);
        await waitForState(minted, (state) => state.node === 'current' && state.data.minted === LOCKED_SO_FAR, WAIT_MS);
        await waitForState(balance, (state) => state.node === 'current' && state.data.balance === LOCKED_SO_FAR, WAIT_MS);

        // Nothing new is locked, so minting again is refused, with the program's error named.
        const mintAgain = createSolanaSignerTransactionMachine(connections, recipient, [
            await firstValueFrom(mintCall$(recipient, verifiedWitness, proofQueueBatchAddress)),
        ]);
        await waitForNode(mintAgain, 'ready', WAIT_MS);
        mintAgain.send();
        const refused = await waitForNode(mintAgain, 'refused', WAIT_MS);
        expect(refused.data).toEqual(
            expect.objectContaining({
                programAddress: TOKEN_PROGRAM_ADDRESS,
                errorCode: TOKEN_ERROR__ZERO_MINT_AMOUNT,
                errorName: 'ZERO_MINT_AMOUNT',
            })
        );
        minted.close();
        balance.close();
        mintAgain.close();
        connections.close();
    });
});
