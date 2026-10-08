import {
    type Address,
    type GetMultipleAccountsApi,
    type Rpc,
} from '@solana/kit';
import { type ProofRequestRootEntry } from '../../program/accounts/proofRequestRootEntry.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';
import { ProofQueueBatchSearchError } from './errors.js';
import {
    fetchProofQueueBatches,
    MAX_ACCOUNTS_PER_CALL,
} from './fetchProofQueueBatches.js';
import { type FoundProofQueueBatch } from './findProofQueueBatch.js';

/** A batch index interval `[low, high)` and the request ids it must cover. */
interface SearchInterval {
    low: bigint;
    high: bigint;
    requestIds: bigint[];
}

function range(from: bigint, to: bigint, step: bigint): bigint[] {
    const values: bigint[] = [];
    for (let value = from; value < to; value += step) values.push(value);
    return values;
}

/**
 * Reads the batches at `indices` once each, at most `MAX_ACCOUNTS_PER_CALL`
 * per RPC call, keyed by index.
 */
async function fetchProofQueueBatchesByIndex(
    rpc: Rpc<GetMultipleAccountsApi>,
    indices: bigint[],
    programAddress: Address
): Promise<Map<bigint, ProofRequestRootEntry>> {
    const unique = [...new Set(indices)];
    const batches = await fetchProofQueueBatches(rpc, unique, programAddress);
    return new Map(unique.map((index, i) => [index, batches[i]]));
}

/**
 * Finds the committed proof queue batch covering each of `requestIds`.
 *
 * The same k-ary search as `findProofQueueBatch`, run for every id at once:
 * each round reads the evenly spaced probes of every open interval in shared
 * calls, then splits each interval's ids by the last probe starting at or
 * before them, until every interval fits in one call. Ids covered by the same
 * batch resolve to the same read.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param requestIds The proof request ids, each below the bridge's queue cursor.
 * @param proofQueueBatchCount The bridge's `proofQueueBatchCount`.
 * @param programAddress The Nori Solana bridge program address.
 * @returns The covering batch and its index, keyed by request id.
 * @throws ProofQueueBatchSearchError When no committed batch covers one of the ids.
 */
export async function findProofQueueBatchesForRequests(
    rpc: Rpc<GetMultipleAccountsApi>,
    requestIds: bigint[],
    proofQueueBatchCount: bigint,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<Map<bigint, FoundProofQueueBatch>> {
    const maxPerCall = BigInt(MAX_ACCOUNTS_PER_CALL);
    const found = new Map<bigint, FoundProofQueueBatch>();
    const sortedIds = [...new Set(requestIds)].sort((a, b) =>
        a < b ? -1 : a > b ? 1 : 0
    );
    if (sortedIds.length === 0) return found;

    let intervals: SearchInterval[] = [
        { low: 0n, high: proofQueueBatchCount, requestIds: sortedIds },
    ];

    while (intervals.length > 0) {
        const narrow = intervals.filter(
            (interval) => interval.high - interval.low <= maxPerCall
        );
        const wide = intervals.filter(
            (interval) => interval.high - interval.low > maxPerCall
        );

        const narrowIndices = narrow.flatMap((interval) =>
            range(interval.low, interval.high, 1n)
        );
        const wideProbes = wide.map((interval) =>
            range(
                interval.low,
                interval.high,
                (interval.high - interval.low + maxPerCall - 1n) / maxPerCall
            )
        );
        const batches = await fetchProofQueueBatchesByIndex(
            rpc,
            [...narrowIndices, ...wideProbes.flat()],
            programAddress
        );

        for (const interval of narrow) {
            const indices = range(interval.low, interval.high, 1n);
            for (const requestId of interval.requestIds) {
                let covering: FoundProofQueueBatch | undefined;
                for (const index of indices) {
                    const batch = batches.get(index);
                    if (
                        batch !== undefined &&
                        batch.inputQueueCursor <= requestId &&
                        requestId < batch.outputQueueCursor
                    ) {
                        covering = {
                            proofQueueBatchIndex: index,
                            proofQueueBatch: batch,
                        };
                        break;
                    }
                }
                if (covering === undefined) {
                    throw new ProofQueueBatchSearchError(
                        requestId,
                        `No committed proof queue batch covers request ${requestId}.`
                    );
                }
                found.set(requestId, covering);
            }
        }

        const next: SearchInterval[] = [];
        wide.forEach((interval, w) => {
            const probes = wideProbes[w];
            const byProbe = new Map<number, bigint[]>();
            for (const requestId of interval.requestIds) {
                let lastAtOrBefore = -1;
                for (let i = 0; i < probes.length; i++) {
                    const probe = batches.get(probes[i]);
                    if (
                        probe === undefined ||
                        probe.inputQueueCursor > requestId
                    )
                        break;
                    lastAtOrBefore = i;
                }
                if (lastAtOrBefore === -1) {
                    throw new ProofQueueBatchSearchError(
                        requestId,
                        `Request ${requestId} precedes the first committed proof queue batch.`
                    );
                }
                byProbe.set(lastAtOrBefore, [
                    ...(byProbe.get(lastAtOrBefore) ?? []),
                    requestId,
                ]);
            }
            for (const [i, ids] of byProbe) {
                next.push({
                    low: probes[i],
                    high: i + 1 < probes.length ? probes[i + 1] : interval.high,
                    requestIds: ids,
                });
            }
        });
        intervals = next;
    }

    return found;
}
