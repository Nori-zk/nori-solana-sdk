import { type Address } from '@solana/kit';
import { defer, EMPTY, expand, last, map, type Observable, of } from 'rxjs';
import { type ProofRequestRootEntry } from '../../program/accounts/proofRequestRootEntry.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';
import { range } from '../../utils/arrays.js';
import { ProofQueueBatchSearchError } from './errors.js';
import { MAX_ACCOUNTS_PER_CALL, proofQueueBatchesByIndex$ } from './proofQueueBatches.js';
import { type SolanaRpc } from './solanaHttp.js';

export interface FoundProofQueueBatch {
    proofQueueBatchIndex: bigint;
    /** The batch's account address. */
    proofQueueBatchAddress: Address;
    proofQueueBatch: ProofRequestRootEntry;
}

/** A batch index interval `[low, high)` and the request ids it must cover. */
interface SearchInterval {
    low: bigint;
    high: bigint;
    requestIds: bigint[];
}

/** Where a search has got to: the intervals still open, and the batches found so far. */
interface Search {
    intervals: SearchInterval[];
    found: Map<bigint, FoundProofQueueBatch>;
}

/** The most batches one round reads per interval. */
const MAX_PER_CALL = BigInt(MAX_ACCOUNTS_PER_CALL);

/**
 * One round of the search: the batches of every interval that fits in one
 * call, and the evenly spaced probes of every wider one, read in shared
 * calls; each narrow interval's ids are resolved, and each wide interval's
 * ids are split by the last probe starting at or before them.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param search The intervals still open, and the batches found so far.
 * @param programAddress The Nori Solana bridge program address.
 * @returns The intervals left open, and the batches found, once.
 */
function searchRound$(rpc: SolanaRpc, { intervals, found }: Search, programAddress: Address): Observable<Search> {
    const narrow = intervals.filter((interval) => interval.high - interval.low <= MAX_PER_CALL);
    const wide = intervals.filter((interval) => interval.high - interval.low > MAX_PER_CALL);
    const wideProbes = wide.map((interval) =>
        range(interval.low, interval.high, (interval.high - interval.low + MAX_PER_CALL - 1n) / MAX_PER_CALL)
    );
    return proofQueueBatchesByIndex$(
        rpc,
        [...narrow.flatMap((interval) => range(interval.low, interval.high, 1n)), ...wideProbes.flat()],
        programAddress
    ).pipe(
        map((reads) => {
            const foundNow = new Map(found);
            for (const interval of narrow) {
                const indices = range(interval.low, interval.high, 1n);
                for (const requestId of interval.requestIds) {
                    const index = indices.find((candidate) => {
                        const batch = reads.get(candidate)?.batch;
                        return (
                            batch !== undefined &&
                            batch.inputQueueCursor <= requestId &&
                            requestId < batch.outputQueueCursor
                        );
                    });
                    const read = index === undefined ? undefined : reads.get(index);
                    if (index === undefined || read === undefined)
                        throw new ProofQueueBatchSearchError(
                            requestId,
                            `No committed proof queue batch covers request ${requestId}.`
                        );
                    foundNow.set(requestId, {
                        proofQueueBatchIndex: index,
                        proofQueueBatchAddress: read.address,
                        proofQueueBatch: read.batch,
                    });
                }
            }
            const next: SearchInterval[] = [];
            wide.forEach((interval, w) => {
                const probes = wideProbes[w];
                const byProbe = new Map<number, bigint[]>();
                for (const requestId of interval.requestIds) {
                    let lastAtOrBefore = -1;
                    for (let i = 0; i < probes.length; i++) {
                        const probe = reads.get(probes[i])?.batch;
                        if (probe === undefined || probe.inputQueueCursor > requestId) break;
                        lastAtOrBefore = i;
                    }
                    if (lastAtOrBefore === -1)
                        throw new ProofQueueBatchSearchError(
                            requestId,
                            `Request ${requestId} precedes the first committed proof queue batch.`
                        );
                    byProbe.set(lastAtOrBefore, [...(byProbe.get(lastAtOrBefore) ?? []), requestId]);
                }
                for (const [i, requestIds] of byProbe)
                    next.push({
                        low: probes[i],
                        high: i + 1 < probes.length ? probes[i + 1] : interval.high,
                        requestIds,
                    });
            });
            return { intervals: next, found: foundNow };
        })
    );
}

/**
 * The committed proof queue batch covering each of `requestIds`.
 *
 * Batch indices `0..proofQueueBatchCount` are contiguous and their cursor
 * ranges increase (each batch resumes at the previous batch's output
 * cursor), so the batches are found by a k-ary search run for every id at
 * once: each round reads up to `MAX_ACCOUNTS_PER_CALL` evenly spaced batches
 * of every open interval in shared calls and keeps, for each id, the
 * interval between the last batch starting at or before it and the next
 * one, until every interval fits in one call. Ids covered by the same batch
 * resolve to the same read.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param requestIds The proof request ids, each below the bridge's queue cursor.
 * @param proofQueueBatchCount The bridge's `proofQueueBatchCount`.
 * @param programAddress The Nori Solana bridge program address.
 * @returns The covering batch and its index, keyed by request id, once;
 *   errors with `ProofQueueBatchSearchError` when no committed batch covers one of the ids.
 */
export const proofQueueBatchesCovering$ = (
    rpc: SolanaRpc,
    requestIds: bigint[],
    proofQueueBatchCount: bigint,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Observable<Map<bigint, FoundProofQueueBatch>> =>
    defer(() => {
        const sortedIds = [...new Set(requestIds)].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
        const start: Search = {
            intervals: [{ low: 0n, high: proofQueueBatchCount, requestIds: sortedIds }],
            found: new Map(),
        };
        return sortedIds.length === 0
            ? of(start.found)
            : searchRound$(rpc, start, programAddress).pipe(
                  expand((search) => (search.intervals.length === 0 ? EMPTY : searchRound$(rpc, search, programAddress))),
                  last(),
                  map(({ found }) => found)
              );
    });
