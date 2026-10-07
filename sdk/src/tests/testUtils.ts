import { getBase64Decoder, type Address } from '@solana/kit';
import { type RunningMachine } from '@yaw-rx/ystate';
import { getAddress, Log, TransactionReceipt, zeroPadValue } from 'ethers';
import {
    BehaviorSubject,
    filter,
    firstValueFrom,
    NEVER,
    type Observable,
    ReplaySubject,
    Subject,
} from 'rxjs';
import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import type { EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { getNoriSolTokenBridgeEncoder } from '../program/accounts/noriSolTokenBridge.js';
import { getProofRequestRootEntryEncoder } from '../program/accounts/proofRequestRootEntry.js';
import { findStatePda } from '../program/pdas/state.js';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { findProofQueueBatchPda } from '../proofRequest/rpc/solana/findProofQueueBatchPda.js';
import { SolanaRpcTransportError } from '../proofRequest/rpc/solana/errors.js';
import type { SolanaRpc } from '../proofRequest/rpc/solana/solanaRpc.js';
import { createEthereumProviderConnectivityMachine } from '../proofRequest/rpc/eth/ystate/ethereumProviderConnectivity.impl.js';
import { createSolanaRpcConnectivityMachine } from '../proofRequest/rpc/solana/ystate/solanaRpcConnectivity.impl.js';
import { stateOf$, type StartedMachine } from '../proofRequest/ystate/dataOnEntry.js';
import { type EthereumProviderConnection } from '../proofRequest/rpc/eth/ystate/ethereumProviderConnectivity.js';
import { type SolanaRpcConnection } from '../proofRequest/rpc/solana/ystate/solanaRpcConnectivity.js';

export const QUEUE_ADDRESS = getAddress('0x' + '11'.repeat(20));
export const TARGET_A = getAddress('0x' + 'aa'.repeat(20));
export const TARGET_B = getAddress('0x' + 'bb'.repeat(20));
export const EXPECTED_CHAIN_ID = 11155111n;
export const EXPECTED_GENESIS_HASH = 'expected-genesis-hash';

/** Health check and retry timings short enough for tests. */
export const FAST_TIMINGS = {
    healthCheckIntervalMs: 100,
    healthCheckTimeoutMs: 200,
    retryBackoff: { initialDelayMs: 20, maxDelayMs: 80 },
};

export const sleep = (ms: number) =>
    new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Waits until a running machine reaches `node`, failing after `timeoutMs`.
 *
 * @param machine The running machine.
 * @param node The node to wait for.
 * @param timeoutMs How long to wait.
 * @returns The state at `node`.
 */
export function reach(
    machine: RunningMachine,
    node: string,
    timeoutMs = 30_000
): Promise<{ node: string; data: unknown }> {
    return Promise.race([
        firstValueFrom(
            machine.state$.pipe(filter((state) => state.node === node))
        ),
        sleep(timeoutMs).then(() => {
            throw new Error(`Did not reach ${node} within ${timeoutMs}ms.`);
        }),
    ]);
}

/**
 * Records every node a running machine visits.
 *
 * @param machine The running machine.
 * @returns The visited nodes, in order, as they happen.
 */
export function recordNodes(machine: {
    state$: Observable<{ node: string }>;
}): string[] {
    const nodes: string[] = [];
    machine.state$.subscribe(({ node }) => nodes.push(node));
    return nodes;
}

/** A proof request as the fake queue emits it. */
export interface FakeProofRequest {
    requestId: bigint;
    blockNumber: number;
    target: string;
}

/** The transaction hash the fake chain gives the transaction that enqueued `requestId`. */
export const transactionHashOf = (requestId: bigint) =>
    '0x' + requestId.toString(16).padStart(64, '0');

/**
 * A deterministic pseudo-random integer in `[0, n)`.
 *
 * @param seed The seed.
 * @returns A generator of integers below its argument.
 */
export function createRandom(seed: number) {
    let state = seed;
    return (n: number) => {
        state = (state * 1103515245 + 12345) % 2147483648;
        return Math.floor(state / 65536) % n;
    };
}

/**
 * An Ethereum provider serving `ProofRequested` logs and receipts for
 * `requests`, as a node would: logs filtered by block range and indexed
 * `target`, requests above `latestBlock` not yet mined. Log queries spanning
 * more than `maxBlockRange` blocks are rejected, as providers do;
 * `failNextReads` makes the next reads fail as an unreachable node does.
 */
export function createFakeEthereumProvider(
    requests: FakeProofRequest[],
    {
        latestBlock,
        maxBlockRange = 2000,
    }: { latestBlock: number; maxBlockRange?: number }
) {
    const queue = NoriProofRequestQueue__factory.createInterface();
    const proofRequested = queue.getEvent('ProofRequested');
    const state = { latestBlock, failNextReads: 0 };
    const failIfDown = () => {
        if (state.failNextReads > 0) {
            state.failNextReads--;
            throw new Error('The node did not answer.');
        }
    };
    const logOf = (request: FakeProofRequest, index: number) => {
        const { data, topics } = queue.encodeEventLog(proofRequested, [
            request.requestId,
            request.target,
            '0x' + '00'.repeat(32),
            [],
        ]);
        return {
            address: QUEUE_ADDRESS,
            data,
            topics,
            blockNumber: request.blockNumber,
            blockHash: '0x' + '00'.repeat(32),
            transactionHash: transactionHashOf(request.requestId),
            transactionIndex: 0,
            index,
            removed: false,
        };
    };

    const provider = {
        getBlockNumber: async () => {
            failIfDown();
            return state.latestBlock;
        },
        getNetwork: async () => ({ chainId: EXPECTED_CHAIN_ID }),
        async getLogs(filter: {
            fromBlock: number;
            toBlock: number;
            topics?: (string | null)[];
        }) {
            failIfDown();
            const fromBlock = Number(filter.fromBlock);
            const toBlock = Number(filter.toBlock);
            if (toBlock - fromBlock + 1 > maxBlockRange)
                throw new Error('block range too large');
            const targetTopic = filter.topics?.[2]?.toLowerCase();
            return requests
                .filter(
                    (request) =>
                        request.blockNumber >= fromBlock &&
                        request.blockNumber <=
                            Math.min(toBlock, state.latestBlock) &&
                        (targetTopic === undefined ||
                            zeroPadValue(request.target, 32).toLowerCase() ===
                                targetTopic)
                )
                .map(
                    (request, index) =>
                        new Log(logOf(request, index), fakeProvider)
                );
        },
        async getTransactionReceipt(hash: string) {
            failIfDown();
            const request = requests.find(
                (candidate) =>
                    transactionHashOf(candidate.requestId) === hash &&
                    candidate.blockNumber <= state.latestBlock
            );
            if (!request) return null;
            return new TransactionReceipt(
                {
                    to: QUEUE_ADDRESS,
                    from: request.target,
                    contractAddress: null,
                    hash,
                    index: 0,
                    blockHash: '0x' + '00'.repeat(32),
                    blockNumber: request.blockNumber,
                    logsBloom: '0x' + '00'.repeat(256),
                    logs: [logOf(request, 0)],
                    gasUsed: 0n,
                    blobGasUsed: null,
                    cumulativeGasUsed: 0n,
                    gasPrice: 0n,
                    blobGasPrice: null,
                    type: 2,
                    status: 1,
                    root: null,
                },
                fakeProvider
            );
        },
    };
    Object.assign(provider, { provider });
    const fakeProvider = provider as unknown as EthereumProvider;
    return { provider: fakeProvider, state };
}

/** A committed proof queue batch: `[inputQueueCursor, outputQueueCursor)`. */
export interface FakeProofQueueBatch {
    inputQueueCursor: bigint;
    outputQueueCursor: bigint;
}

/**
 * Contiguous batches, each resuming at the previous output cursor.
 *
 * @param sizes How many requests each batch holds.
 * @returns The batches, from cursor 0.
 */
export function createContiguousBatches(
    sizes: number[]
): FakeProofQueueBatch[] {
    let cursor = 0n;
    return sizes.map((size) => {
        const batch = {
            inputQueueCursor: cursor,
            outputQueueCursor: cursor + BigInt(size),
        };
        cursor += BigInt(size);
        return batch;
    });
}

/**
 * A Solana RPC serving the bridge state and its proof queue batches as the
 * bridge program owns them. `setBatches` replaces them (and the queue cursor,
 * the last batch's output); `failNextReads` makes the next reads fail as a
 * node that never answers does.
 */
export async function createFakeSolanaRpc(
    initialBatches: FakeProofQueueBatch[]
) {
    const accounts = new Map<string, Uint8Array>();
    const base64 = getBase64Decoder();
    const [statePda] = await findStatePda({
        programAddress: TOKEN_PROGRAM_ADDRESS,
    });
    const state = { failNextReads: 0, multipleAccountsCalls: 0 };

    async function setBatches(batches: FakeProofQueueBatch[]) {
        accounts.clear();
        for (const [index, batch] of batches.entries()) {
            const [pda] = await findProofQueueBatchPda(
                BigInt(index),
                TOKEN_PROGRAM_ADDRESS
            );
            accounts.set(
                pda,
                new Uint8Array(
                    getProofRequestRootEntryEncoder().encode({
                        root: new Uint8Array(32).fill(index % 256),
                        outputBlockNumber: BigInt(1000 + index),
                        ...batch,
                    })
                )
            );
        }
        accounts.set(
            statePda,
            new Uint8Array(
                getNoriSolTokenBridgeEncoder().encode({
                    authority: TOKEN_PROGRAM_ADDRESS,
                    verifiedStateRoot: new Uint8Array(32),
                    latestHead: 0n,
                    noriBridgeVk: new Uint8Array(32),
                    latestHeliosStoreInputHash: new Uint8Array(32),
                    ethProofQueueAddress: new Uint8Array(20),
                    ethTokenBridgeAddress: new Uint8Array(20),
                    queueCursor: batches.at(-1)?.outputQueueCursor ?? 0n,
                    proofQueueBatchCount: BigInt(batches.length),
                })
            )
        );
    }
    await setBatches(initialBatches);

    const toAccount = (address: Address) => {
        const data = accounts.get(address);
        return data === undefined
            ? null
            : {
                  data: [base64.decode(data), 'base64'],
                  executable: false,
                  lamports: 1n,
                  owner: TOKEN_PROGRAM_ADDRESS,
                  space: BigInt(data.length),
                  rentEpoch: 0n,
              };
    };
    const failIfDown = () => {
        if (state.failNextReads > 0) {
            state.failNextReads--;
            throw new SolanaRpcTransportError(
                'The request got no response.',
                new TypeError('fetch failed')
            );
        }
    };
    const rpc = {
        getAccountInfo: (address: Address) => ({
            send: async () => {
                failIfDown();
                return { context: { slot: 1n }, value: toAccount(address) };
            },
        }),
        getMultipleAccounts: (addresses: Address[]) => ({
            send: async () => {
                failIfDown();
                state.multipleAccountsCalls++;
                if (addresses.length > 100)
                    throw new Error('more than 100 accounts');
                return {
                    context: { slot: 1n },
                    value: addresses.map(toAccount),
                };
            },
        }),
    };
    return { rpc: rpc as unknown as SolanaRpc, state, setBatches };
}

/**
 * Both connections, each a real connectivity machine over a controllable
 * world: whether each endpoint answers its health checks, and whether the
 * network is online. Reads go through `provider` and `rpc`.
 *
 * @param provider The Ethereum provider reads go through.
 * @param rpc The Solana RPC reads go through.
 * @returns The connections, and the switches that drive them.
 */
export function createTestConnections(
    provider: EthereumProvider,
    rpc: SolanaRpc
) {
    // `…GoesDownOnReadFailure`: the endpoint stops answering at the moment a
    // read reports failing to reach it, so the re-check finds it down.
    const world = {
        ethereumAnswers: true,
        solanaAnswers: true,
        ethereumGoesDownOnReadFailure: false,
        solanaGoesDownOnReadFailure: false,
    };
    const network$ = new BehaviorSubject<'online' | 'offline'>('online');
    const ethereumReadFailures = { count: 0 };
    const solanaReadFailures = { count: 0 };

    const ethereumReadFailed$ = new Subject<void>();
    const ethereumClose$ = new Subject<void>();
    const ethereumStarted$ = new ReplaySubject<StartedMachine<EthereumProviderConnection>>(1);
    const ethereumProviderConnectivity =
        createEthereumProviderConnectivityMachine({
            ...FAST_TIMINGS,
            expectedChainId: EXPECTED_CHAIN_ID,
            checkHealth: async () => {
                if (!world.ethereumAnswers)
                    throw new Error('The Ethereum node did not answer.');
                return {
                    outcome: 'onExpectedChain',
                    chainId: EXPECTED_CHAIN_ID,
                    blockNumber: 1,
                    checkedAt: 0,
                };
            },
            walletOnExpectedChain$: NEVER,
            walletNotOnExpectedChain$: NEVER,
            walletConnected$: NEVER,
            walletDisconnected$: NEVER,
            networkWentOffline$: network$.pipe(
                filter((status) => status === 'offline')
            ),
            networkCameOnline$: network$.pipe(
                filter((status) => status === 'online')
            ),
            readFailed$: ethereumReadFailed$,
            close$: ethereumClose$,
            connection$: stateOf$(ethereumStarted$),
        })
            .close()
            .start('checking');
    ethereumStarted$.next(ethereumProviderConnectivity);

    const solanaReadFailed$ = new Subject<void>();
    const solanaClose$ = new Subject<void>();
    const solanaStarted$ = new ReplaySubject<StartedMachine<SolanaRpcConnection>>(1);
    const solanaRpcConnectivity = createSolanaRpcConnectivityMachine({
        ...FAST_TIMINGS,
        expectedGenesisHash: EXPECTED_GENESIS_HASH,
        rpcUrls: ['https://solana.test'],
        checkHealth: async (rpcUrl) => {
            if (!world.solanaAnswers)
                throw new Error('The Solana node did not answer.');
            return {
                outcome: 'onExpectedCluster',
                rpcUrl,
                slot: 1n,
                checkedAt: 0,
            };
        },
        networkWentOffline$: network$.pipe(
            filter((status) => status === 'offline')
        ),
        networkCameOnline$: network$.pipe(
            filter((status) => status === 'online')
        ),
        readFailed$: solanaReadFailed$,
        close$: solanaClose$,
        connection$: stateOf$(solanaStarted$),
    })
        .close()
        .start('checking');
    solanaStarted$.next(solanaRpcConnectivity);

    const connections: ProofRequestConnections = {
        ethereum: {
            ethereumProviderConnectivity,
            wallet: undefined,
            currentProvider: () => provider,
            reportReadFailed: () => {
                ethereumReadFailures.count++;
                if (world.ethereumGoesDownOnReadFailure)
                    world.ethereumAnswers = false;
                ethereumReadFailed$.next();
            },
            close: () => ethereumClose$.next(),
        },
        solana: {
            solanaRpcConnectivity,
            currentRpc: () => rpc,
            reportReadFailed: () => {
                solanaReadFailures.count++;
                if (world.solanaGoesDownOnReadFailure)
                    world.solanaAnswers = false;
                solanaReadFailed$.next();
            },
            close: () => solanaClose$.next(),
        },
    };
    return {
        connections,
        world,
        network$,
        ethereumReadFailures,
        solanaReadFailures,
        close: () => {
            ethereumClose$.next();
            solanaClose$.next();
        },
    };
}
