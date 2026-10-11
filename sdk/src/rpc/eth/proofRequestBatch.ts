import { Contract } from 'ethers';
import { NoriProofRequestQueue__factory } from '@nori-zk/ethereum-solana-bridge';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { concatMap, defer, from, map, type Observable, of, reduce, switchMap, toArray } from 'rxjs';
import { chunk } from '../../utils/arrays.js';
import { blockRanges, MAX_BLOCK_RANGE_PER_QUERY } from './blockRanges.js';
import { ethRpcRead$ } from './ethRpcRead.js';

export interface ProofRequestBatchEntry {
    target: string;
    collectionKeysCount: number;
    collectionKeys: string[];
    value: string;
}

export interface ProofRequestRecord {
    id: bigint;
    target: string;
    slotKey: string;
    collectionKeysCount: number;
    collectionKeys: string[];
}

/**
 * Canonical Multicall3 deployment address, identical across virtually every
 * EVM chain. See https://www.multicall3.com/
 */
const MULTICALL3_ADDRESS = '0xcA11bde05977b3631167028862bE2a173976CA11';

const MULTICALL3_ABI = [
    'function aggregate3(tuple(address target, bool allowFailure, bytes callData)[] calls) payable returns (tuple(bool success, bytes returnData)[] returnData)',
];

interface Multicall3Result {
    success: boolean;
    returnData: string;
}

/** Max entries per `aggregate3` call, kept under typical `eth_call` gas caps. */
const RECORDS_PER_MULTICALL = 200;

/**
 * Every `NoriProofRequestQueue` record with id in `[inputQueueCursor,
 * outputQueueCursor)`, from `ProofRequested` logs over `[fromBlock, toBlock]`.
 *
 * @param provider The Ethereum provider used for every read.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param inputQueueCursor Inclusive lower bound of the batch (queue request id).
 * @param outputQueueCursor Exclusive upper bound of the batch.
 * @param fromBlock Block to start the log search from.
 * @param toBlock Block to end the log search at.
 * @returns One record per request in the batch, in queue order, once.
 */
function recordsByLogs$(
    provider: EthereumProvider,
    proofQueueAddress: string,
    inputQueueCursor: bigint,
    outputQueueCursor: bigint,
    fromBlock: number,
    toBlock: number
): Observable<ProofRequestRecord[]> {
    const queue = NoriProofRequestQueue__factory.connect(proofQueueAddress, provider);
    return from(blockRanges(fromBlock, toBlock, MAX_BLOCK_RANGE_PER_QUERY, 'asc')).pipe(
        concatMap(([chunkFrom, chunkTo]) =>
            ethRpcRead$(
                () => queue.queryFilter(queue.filters.ProofRequested(), chunkFrom, chunkTo),
                `ProofRequested log query over blocks ${chunkFrom}-${chunkTo} failed.`
            )
        ),
        map((logs) =>
            logs
                .filter(({ args }) => args.requestId >= inputQueueCursor && args.requestId < outputQueueCursor)
                .map(({ args: { requestId, target, slotKey, collectionKeys } }) => ({
                    id: requestId,
                    target,
                    slotKey,
                    collectionKeysCount: collectionKeys.length,
                    collectionKeys: [...collectionKeys],
                }))
        ),
        reduce((records, chunkRecords) => [...records, ...chunkRecords], [] as ProofRequestRecord[])
    );
}

/**
 * Every `NoriProofRequestQueue` record with id in `[inputQueueCursor,
 * outputQueueCursor)`, by calling `requests(id)` at `blockNumber`, batched
 * through Multicall3. Used only for the queue's first-ever batch, which has
 * no previous settlement job to give a log search a lower bound.
 *
 * @param provider The Ethereum provider used for every read.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param inputQueueCursor Inclusive lower bound of the batch (queue request id).
 * @param outputQueueCursor Exclusive upper bound of the batch.
 * @param blockNumber The Ethereum block to read every record at.
 * @returns One record per request in the batch, in queue order, once.
 */
function recordsByCall$(
    provider: EthereumProvider,
    proofQueueAddress: string,
    inputQueueCursor: bigint,
    outputQueueCursor: bigint,
    blockNumber: number
): Observable<ProofRequestRecord[]> {
    const queue = NoriProofRequestQueue__factory.connect(proofQueueAddress, provider);
    const multicall = new Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, provider);
    const ids = Array.from(
        { length: Number(outputQueueCursor - inputQueueCursor) },
        (_unused, k) => inputQueueCursor + BigInt(k)
    );
    return from(chunk(ids, RECORDS_PER_MULTICALL)).pipe(
        concatMap((idChunk) =>
            ethRpcRead$(
                () =>
                    multicall.aggregate3.staticCall(
                        idChunk.map((id) => ({
                            target: proofQueueAddress,
                            allowFailure: false,
                            callData: queue.interface.encodeFunctionData('requests', [id]),
                        })),
                        { blockTag: blockNumber }
                    ) as Promise<Multicall3Result[]>,
                'Failed to read the proof requests through Multicall3.'
            ).pipe(
                map((aggregated) =>
                    aggregated.map(({ returnData }, i): ProofRequestRecord => {
                        const [request] = queue.interface.decodeFunctionResult('requests', returnData);
                        const collectionKeysCount = Number(request.collectionKeysCount);
                        return {
                            id: idChunk[i],
                            target: request.target as string,
                            slotKey: request.slotKey as string,
                            collectionKeysCount,
                            collectionKeys: [...(request.collectionKeys as string[])].slice(0, collectionKeysCount),
                        };
                    })
                )
            )
        ),
        reduce((records, chunkRecords) => [...records, ...chunkRecords], [] as ProofRequestRecord[])
    );
}

/**
 * Every `NoriProofRequestQueue` entry with id in `[inputQueueCursor,
 * outputQueueCursor)`, plus the raw storage word each one's `slotKey` points
 * at on its own `target`, at `outputBlockNumber`.
 *
 * @param provider The Ethereum provider used for every read.
 * @param proofQueueAddress The `NoriProofRequestQueue` address.
 * @param inputQueueCursor Inclusive lower bound of the batch (queue request id).
 * @param outputQueueCursor Exclusive upper bound of the batch.
 * @param previousOutputBlockNumber The output block of the settlement job
 *   immediately before this one, i.e. the point before which no request in
 *   this batch could have been enqueued. -1 is a sentinel: there is no
 *   previous job because this batch is the first the queue ever produced.
 * @param outputBlockNumber The Ethereum block to read every value at.
 * @returns One entry per request in the batch, in queue order, once.
 */
export const proofRequestBatch$ = (
    provider: EthereumProvider,
    proofQueueAddress: string,
    inputQueueCursor: bigint,
    outputQueueCursor: bigint,
    previousOutputBlockNumber: number,
    outputBlockNumber: number
): Observable<ProofRequestBatchEntry[]> =>
    defer(() => {
        if (outputQueueCursor - inputQueueCursor <= 0n) return of([]);
        const records$ =
            previousOutputBlockNumber !== -1 // sentinel: no previous job (first-ever batch)
                ? recordsByLogs$(
                      provider,
                      proofQueueAddress,
                      inputQueueCursor,
                      outputQueueCursor,
                      previousOutputBlockNumber,
                      outputBlockNumber
                  )
                : recordsByCall$(provider, proofQueueAddress, inputQueueCursor, outputQueueCursor, outputBlockNumber);
        return records$.pipe(
            switchMap((records) =>
                from(records).pipe(
                    concatMap((record) =>
                        ethRpcRead$(
                            () => provider.getStorage(record.target, record.slotKey, outputBlockNumber),
                            `Failed to read the storage of request ${record.id}.`
                        ).pipe(
                            map((value) => ({
                                target: record.target,
                                collectionKeysCount: record.collectionKeysCount,
                                collectionKeys: record.collectionKeys,
                                value,
                            }))
                        )
                    ),
                    toArray()
                )
            )
        );
    });
