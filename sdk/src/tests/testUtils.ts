import { getBase64Decoder, type Address } from '@solana/kit';
import { type RunningMachine } from '@yaw-rx/ystate';
import {
    getAddress,
    type JsonRpcProvider,
    type JsonRpcSigner,
    Log,
    TransactionReceipt,
    type TransactionRequest,
    zeroPadValue,
} from 'ethers';
import { type GraphState } from '../utils/machines.js';
import {
    asapScheduler,
    BehaviorSubject,
    defer,
    filter,
    firstValueFrom,
    forkJoin,
    map,
    NEVER,
    type Observable,
    of,
    skip,
    Subject,
    subscribeOn,
    takeUntil,
    takeWhile,
    throwError,
    timeout,
    timer,
    toArray,
} from 'rxjs';
import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import type { EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { getNoriSolTokenBridgeEncoder } from '../program/accounts/noriSolTokenBridge.js';
import { getProofRequestRootEntryEncoder } from '../program/accounts/proofRequestRootEntry.js';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { proofQueueBatchAddress$, stateAddress$ } from '../rpc/solana/programAddresses.js';
import { SolanaRpcTransportError } from '../rpc/solana/errors.js';
import type { SolanaHealth, SolanaRpc } from '../rpc/solana/solanaHttp.js';
import type { EthereumHealth } from '../rpc/eth/ethereumHttp.js';
import { type GrantedSend, type RequestResult } from '../rpc/connection/connectionGrants.js';
import { httpConnection } from '../rpc/connection/httpConnection.impl.js';
import {
    ethereumChain,
    type EthereumTransports,
    solanaChain,
    type SolanaTransports,
} from '../rpc/connection/connections.js';

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

/** The URL of the test connections' Ethereum endpoint. */
export const ETHEREUM_URL = 'https://ethereum.test';

/** How often the test chains poll while no transport can subscribe. */
export const FAST_POLL_INTERVAL_MS = 50;

/**
 * A running machine with no states of its own, for a test that runs
 * requests directly to take their turns as.
 *
 * @returns The machine.
 */
export function testSubscriber(): RunningMachine {
    return { status$: NEVER, state$: NEVER, event$: NEVER };
}

/**
 * Waits until a running machine is in `node`, failing after `timeoutMs`.
 *
 * @param machine The running machine.
 * @param node The node to wait for.
 * @param timeoutMs How long to wait.
 * @returns The machine's state in `node`, typed by its graph.
 */
export function waitForNode<TState extends { node: string }, TNode extends TState['node']>(
    machine: { state$: Observable<TState> },
    node: TNode,
    timeoutMs = 30_000
): Promise<Extract<TState, { node: TNode }>> {
    return firstValueFrom(
        machine.state$.pipe(
            filter((state): state is Extract<TState, { node: TNode }> => state.node === node),
            timeout({
                first: timeoutMs,
                with: () => throwError(() => new Error(`Was not in ${node} within ${timeoutMs}ms.`)),
            })
        )
    );
}

/**
 * Waits until a running machine's state matches `matches`, failing after `timeoutMs`.
 *
 * @param machine The running machine.
 * @param matches The test on its state.
 * @param timeoutMs How long to wait.
 * @returns The first state that matches.
 */
export function waitForState<TState>(
    machine: { state$: Observable<TState> },
    matches: (state: TState) => boolean,
    timeoutMs = 30_000
): Promise<TState> {
    return firstValueFrom(
        machine.state$.pipe(
            filter(matches),
            timeout({
                first: timeoutMs,
                with: () => throwError(() => new Error(`No matching state within ${timeoutMs}ms.`)),
            })
        )
    );
}

/**
 * The nodes a machine moves to from now on, until it reaches `until`
 * (included). Call it before what makes the machine move.
 *
 * @param machine The running machine.
 * @param until The node to stop at, or a test on the state to stop at.
 * @param timeoutMs How long the moves may take.
 * @returns The nodes it moved to, in order, once it reaches `until`.
 */
export function nodesUntil<TState extends { node: string }>(
    machine: { state$: Observable<TState> },
    until: TState['node'] | ((state: TState) => boolean),
    timeoutMs = 30_000
): Promise<string[]> {
    const reached = typeof until === 'string' ? (state: TState) => state.node === until : until;
    return firstValueFrom(
        machine.state$.pipe(
            skip(1),
            takeWhile((state) => !reached(state), true),
            map(({ node }) => node),
            toArray(),
            timeout(timeoutMs)
        )
    );
}

/**
 * The states a machine is in over the next `ms`, starting with where it is now.
 *
 * @param machine The running machine.
 * @param ms How long to watch it.
 * @returns Its states, in order, once the time is up.
 */
export function statesDuring<TState>(machine: { state$: Observable<TState> }, ms: number): Promise<TState[]> {
    return firstValueFrom(machine.state$.pipe(takeUntil(timer(ms)), toArray()));
}

/** A proof request as the fake queue emits it. */
export interface FakeProofRequest {
    requestId: bigint;
    blockNumber: number;
    target: string;
    /** The transaction's receipt status: 1 for success (default), 0 for a revert. */
    status?: number;
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
 * An Ethereum provider serving `ProofRequested` logs, receipts, transactions,
 * nonces and the blocks up to `latestBlock` (block `n` produced at `12n` s) for `requests`, as a node
 * would: logs filtered by block range and indexed `target`, requests above
 * `latestBlock` not yet mined. Log queries spanning more than
 * `maxBlockRange` blocks are rejected, as providers do; `failNextReads`
 * makes the next reads fail as an unreachable node does. `minedNonces` holds
 * each sender's mined nonce, and `pending` the transactions the node knows
 * and has not mined.
 */
export function createFakeEthereumProvider(
    requests: FakeProofRequest[],
    {
        latestBlock,
        finalizedBlock = latestBlock,
        maxBlockRange = 2000,
    }: { latestBlock: number; finalizedBlock?: number; maxBlockRange?: number }
) {
    const queue = NoriProofRequestQueue__factory.createInterface();
    const proofRequested = queue.getEvent('ProofRequested');
    const state = {
        latestBlock,
        finalizedBlock,
        failNextReads: 0,
        minedNonces: new Map<string, number>(),
        pending: new Map<string, { from: string; nonce: number }>(),
    };
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
        async getBlock(tag: 'latest' | 'finalized' | number) {
            failIfDown();
            const number =
                tag === 'finalized' ? state.finalizedBlock : tag === 'latest' ? state.latestBlock : tag;
            if (number > state.latestBlock) return null;
            return {
                number,
                hash: '0x' + number.toString(16).padStart(64, '0'),
                parentHash: '0x' + Math.max(0, number - 1).toString(16).padStart(64, '0'),
                timestamp: number * 12,
            };
        },
        getNetwork: async () => ({ chainId: EXPECTED_CHAIN_ID }),
        async getTransactionCount(address: string) {
            failIfDown();
            return state.minedNonces.get(address) ?? 0;
        },
        async getTransaction(hash: string) {
            failIfDown();
            const pending = state.pending.get(hash);
            if (pending) return { hash, ...pending };
            const mined = requests.find(
                (request) => transactionHashOf(request.requestId) === hash && request.blockNumber <= state.latestBlock
            );
            return mined ? { hash, from: mined.target, nonce: Number(mined.requestId) } : null;
        },
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
                    status: request.status ?? 1,
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
 * bridge program owns them, its finalized slot, and the program's newest
 * transaction. `setBatches` replaces the batches (and the queue cursor, the
 * last batch's output) in a new slot, as one program transaction does;
 * `outputBlockOf` gives each batch's Ethereum output block; `failNextReads`
 * makes the next reads fail as a node that never answers does, and
 * `failNextAccountReads` the next account reads only.
 */
export async function createFakeSolanaRpc(
    initialBatches: FakeProofQueueBatch[],
    { outputBlockOf = (index: number) => 1000 + index }: { outputBlockOf?: (index: number) => number } = {}
) {
    const accounts = new Map<string, Uint8Array>();
    const base64 = getBase64Decoder();
    const statePda = await firstValueFrom(stateAddress$(TOKEN_PROGRAM_ADDRESS));
    // `slot`: the latest finalized slot; `programSlot`: the slot of the program's newest transaction.
    const state = {
        failNextReads: 0,
        failNextAccountReads: 0,
        multipleAccountsCalls: 0,
        slot: 1,
        programSlot: undefined as number | undefined,
    };

    async function setBatches(batches: FakeProofQueueBatch[]) {
        const pdas = await firstValueFrom(
            batches.length === 0
                ? of([])
                : forkJoin(batches.map((_, index) => proofQueueBatchAddress$(BigInt(index), TOKEN_PROGRAM_ADDRESS)))
        );
        accounts.clear();
        for (const [index, batch] of batches.entries()) {
            accounts.set(
                pdas[index],
                new Uint8Array(
                    getProofRequestRootEntryEncoder().encode({
                        root: new Uint8Array(32).fill(index % 256),
                        outputBlockNumber: BigInt(outputBlockOf(index)),
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
        state.slot++;
        state.programSlot = state.slot;
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
    const failIfDown = (accountRead = false) => {
        if (state.failNextReads > 0 || (accountRead && state.failNextAccountReads > 0)) {
            if (state.failNextReads > 0) state.failNextReads--;
            else state.failNextAccountReads--;
            throw new SolanaRpcTransportError(
                'The request got no response.',
                new TypeError('fetch failed')
            );
        }
    };
    const rpc = {
        getAccountInfo: (address: Address) => ({
            send: async () => {
                failIfDown(true);
                return { context: { slot: BigInt(state.slot) }, value: toAccount(address) };
            },
        }),
        getMultipleAccounts: (addresses: Address[]) => ({
            send: async () => {
                failIfDown(true);
                state.multipleAccountsCalls++;
                if (addresses.length > 100)
                    throw new Error('more than 100 accounts');
                return {
                    context: { slot: BigInt(state.slot) },
                    value: addresses.map(toAccount),
                };
            },
        }),
        getSlot: () => ({
            send: async () => {
                failIfDown();
                return BigInt(state.slot);
            },
        }),
        getSignaturesForAddress: (address: Address) => ({
            send: async () => {
                failIfDown();
                return (address === TOKEN_PROGRAM_ADDRESS || address === statePda) && state.programSlot !== undefined
                    ? [{ signature: `program-transaction-${state.programSlot}`, slot: BigInt(state.programSlot) }]
                    : [];
            },
        }),
    };
    return { rpc: rpc as unknown as SolanaRpc, state, setBatches };
}

/** What a test endpoint over its rate throws: it turned the request away for too many requests. */
export class TooManyRequestsError extends Error {
    constructor() {
        super('Too many requests.');
        this.name = 'TooManyRequestsError';
    }
}

/**
 * An endpoint's rate: `admit()` lets a request through while fewer than
 * `perSecond()` were let through in the last second, and throws
 * `TooManyRequestsError` otherwise, as a public RPC answers 429.
 *
 * @param perSecond How many requests a second it takes, read on each request.
 * @returns `admit()`, and how many requests it turned away.
 */
export function rateOf(perSecond: () => number) {
    const admittedAt: number[] = [];
    const counts = { turnedAway: 0 };
    const admit = () => {
        const now = Date.now();
        while (admittedAt.length > 0 && now - admittedAt[0] >= 1000) admittedAt.shift();
        if (admittedAt.length >= perSecond()) {
            counts.turnedAway++;
            throw new TooManyRequestsError();
        }
        admittedAt.push(now);
    };
    return { admit, counts };
}

/** Whether a test endpoint turned a request away: it threw `TooManyRequestsError`. */
const turnedAwayByTestEndpoint = (result: RequestResult<unknown>) =>
    'failed' in result && result.failed instanceof TooManyRequestsError;

/**
 * The fake Ethereum provider as a client over a connection's grants: every
 * call takes its subscriber's turn and passes the endpoint's rate.
 *
 * @param provider The fake provider.
 * @param send$ Sends a request on its subscriber's turn.
 * @param admit The endpoint's rate.
 * @returns The client.
 */
function grantedFakeProvider(provider: EthereumProvider, send$: GrantedSend, admit: () => void): JsonRpcProvider {
    return new Proxy(provider, {
        get: (target, key) => {
            const value: unknown = Reflect.get(target, key);
            return typeof value === 'function'
                ? (...args: unknown[]) =>
                      firstValueFrom(
                          send$(() =>
                              defer(() => {
                                  admit();
                                  return value.apply(target, args) as Promise<unknown>;
                              })
                          )
                      )
                : value;
        },
    }) as unknown as JsonRpcProvider;
}

/**
 * The fake Solana RPC as a client over a connection's grants: every
 * request's `send()` takes its subscriber's turn and passes the endpoint's rate.
 *
 * @param rpc The fake RPC.
 * @param send$ Sends a request on its subscriber's turn.
 * @param admit The endpoint's rate.
 * @returns The client.
 */
function grantedFakeRpc(rpc: SolanaRpc, send$: GrantedSend, admit: () => void): SolanaRpc {
    return new Proxy(rpc, {
        get: (target, key) => {
            const method: unknown = Reflect.get(target, key);
            if (typeof method !== 'function') return method;
            return (...args: unknown[]) => {
                const pending = method.apply(target, args) as { send: (...sendArgs: unknown[]) => Promise<unknown> };
                return {
                    send: (...sendArgs: unknown[]) =>
                        firstValueFrom(
                            send$(() =>
                                defer(() => {
                                    admit();
                                    return pending.send(...sendArgs);
                                })
                            )
                        ),
                };
            };
        },
    });
}

/** What a fake signer's `sendTransaction` resolves to: the parts of ethers' response the sdk reads. */
export interface FakeSentTransaction {
    hash: string;
    from: string;
    nonce: number;
}

/**
 * An Ethereum wallet transport whose machine's node the test sets
 * (`state$`): a stand-in for the user's wallet, which only signs. Its
 * signer is `signer`, which sends a transaction with `sendTransaction`.
 *
 * @param node The node its machine starts in (default: `ready`).
 * @param sendTransaction What the wallet does with a transaction it is asked to send (default: refuses it).
 * @returns The transport, its states and its signer.
 */
export function createFakeWallet(
    node = 'ready',
    sendTransaction: (transaction: TransactionRequest) => Promise<FakeSentTransaction> = () =>
        Promise.reject(new Error('The fake wallet sends nothing.'))
) {
    const state$ = new BehaviorSubject<GraphState>({ node, data: {} });
    const signer = { address: TARGET_A, sendTransaction } as unknown as JsonRpcSigner;
    const wallet = {
        connection: { state$ },
        current: () => ({ getSigner: () => Promise.resolve(signer) }),
        currentWalletProvider: (): undefined => undefined,
        walletProvider$: () => NEVER,
        walletEvents$: NEVER,
        load$: NEVER,
        chooseWallet: (): void => undefined,
        switchToExpectedChain: (): void => undefined,
        reportReadFailed: (): void => undefined,
        close: (): void => undefined,
    } as unknown as NonNullable<EthereumTransports['wallet']>;
    return { wallet, state$, signer };
}

/**
 * Both connections, each a real connectivity machine over a controllable
 * world: whether each endpoint answers its health checks, how many requests
 * a second it takes, and whether the network is online. Reads go through
 * `provider` and `rpc`, as clients over each connection's grants, so every
 * request takes its machine's turn; Solana's websocket stays closed, so the
 * Solana changes machine polls `rpc`.
 *
 * @param provider The Ethereum provider reads go through.
 * @param rpc The Solana RPC reads go through.
 * @param wallet An Ethereum wallet transport; reads still go through `provider`.
 * @returns The connections, the switches that drive them, and each endpoint's rate.
 */
export function createTestConnections(
    provider: EthereumProvider,
    rpc: SolanaRpc,
    wallet?: EthereumTransports['wallet']
) {
    // `…GoesDownOnReadFailure`: the endpoint stops answering at the moment a
    // read reports failing to reach it, so the re-check finds it down.
    // `…PerSecond`: how many requests a second the endpoint takes; the rest it turns away.
    const world = {
        ethereumAnswers: true,
        solanaAnswers: true,
        ethereumGoesDownOnReadFailure: false,
        solanaGoesDownOnReadFailure: false,
        ethereumPerSecond: Infinity,
        solanaPerSecond: Infinity,
    };
    const ethereumRate = rateOf(() => world.ethereumPerSecond);
    const solanaRate = rateOf(() => world.solanaPerSecond);
    const network$ = new BehaviorSubject<'online' | 'offline'>('online');
    const ethereumReadFailures = { count: 0 };
    const solanaReadFailures = { count: 0 };

    const ethereumReadFailed$ = new Subject<void>();
    const ethereumClose$ = new Subject<void>();
    const ethereumConnection = httpConnection<EthereumHealth, JsonRpcProvider>({
        ...FAST_TIMINGS,
        urls: [ETHEREUM_URL],
        transport: {
            clientOn: (_url, send$) => grantedFakeProvider(provider, send$, ethereumRate.admit),
            turnedAway: turnedAwayByTestEndpoint,
        },
        checkHealth: (_client, url) =>
            defer(() =>
                world.ethereumAnswers
                    ? of({ outcome: 'onExpectedNetwork' as const, url, health: { blockNumber: 1 }, checkedAt: 0 })
                    : throwError(() => new Error('The Ethereum node did not answer.'))
            ).pipe(subscribeOn(asapScheduler)),
        networkWentOffline$: network$.pipe(
            filter((status) => status === 'offline')
        ),
        networkCameOnline$: network$.pipe(
            filter((status) => status === 'online')
        ),
        readFailed$: ethereumReadFailed$,
        close$: ethereumClose$,
    });

    const solanaReadFailed$ = new Subject<void>();
    const solanaClose$ = new Subject<void>();
    const solanaConnection = httpConnection<SolanaHealth, SolanaRpc>({
        ...FAST_TIMINGS,
        urls: ['https://solana.test'],
        transport: {
            clientOn: (_url, send$) => grantedFakeRpc(rpc, send$, solanaRate.admit),
            turnedAway: turnedAwayByTestEndpoint,
        },
        checkHealth: (_client, url) =>
            defer(() =>
                world.solanaAnswers
                    ? of({ outcome: 'onExpectedNetwork' as const, url, health: { slot: 1n }, checkedAt: 0 })
                    : throwError(() => new Error('The Solana node did not answer.'))
            ).pipe(subscribeOn(asapScheduler)),
        networkWentOffline$: network$.pipe(
            filter((status) => status === 'offline')
        ),
        networkCameOnline$: network$.pipe(
            filter((status) => status === 'online')
        ),
        readFailed$: solanaReadFailed$,
        close$: solanaClose$,
    });

    const connections: ProofRequestConnections = {
        ethereum: ethereumChain(
            {
                http: {
                    connection: ethereumConnection,
                    current: (subscriber: RunningMachine) => ethereumConnection.clientOf(ETHEREUM_URL, subscriber),
                    load$: ethereumConnection.load$,
                    reportReadFailed: () => {
                        ethereumReadFailures.count++;
                        if (world.ethereumGoesDownOnReadFailure)
                            world.ethereumAnswers = false;
                        ethereumReadFailed$.next();
                    },
                    close: () => ethereumClose$.next(),
                },
                wallet,
            },
            // Reads go through http; the wallet only signs.
            wallet && { calls: ['http'], logs: ['http'] },
            FAST_POLL_INTERVAL_MS
        ),
        solana: solanaChain(
            {
                http: {
                    connection: solanaConnection,
                    rpcOf: solanaConnection.clientOf,
                    load$: solanaConnection.load$,
                    reportReadFailed: () => {
                        solanaReadFailures.count++;
                        if (world.solanaGoesDownOnReadFailure) world.solanaAnswers = false;
                        solanaReadFailed$.next();
                    },
                    close: () => solanaClose$.next(),
                },
                websocket: {
                    connection: { state$: new BehaviorSubject({ node: 'closed', data: {} }) },
                    socket: new Subject(),
                    close: (): void => undefined,
                } as unknown as SolanaTransports['websocket'],
            },
            FAST_POLL_INTERVAL_MS
        ),
    };
    return {
        connections,
        world,
        network$,
        ethereumReadFailures,
        solanaReadFailures,
        /** How many requests each endpoint turned away for too many. */
        turnedAway: { ethereum: ethereumRate.counts, solana: solanaRate.counts },
        /** The Ethereum client a machine's requests go through. */
        ethereumClientOf: (machine: RunningMachine) => ethereumConnection.clientOf(ETHEREUM_URL, machine),
        close: () => {
            ethereumClose$.next();
            solanaClose$.next();
        },
    };
}
