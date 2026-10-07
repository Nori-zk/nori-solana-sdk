import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import { classifyProofRequests } from './classifyProofRequests.js';
import {
    type ProofRequestStateSnapshot,
    type ProofRequestStateSnapshotRequest,
} from './getProofRequestStateSnapshot.js';
import { EthRpcTransportError } from './rpc/eth/errors.js';
import { type ProofRequest } from './rpc/eth/fetchProofRequest.js';
import {
    fetchProofRequestsByTarget,
    type ProofRequestHistoryCursor,
    type ProofRequestsByTargetQuery,
} from './rpc/eth/fetchProofRequestsByTarget.js';
import { fetchBridgeState } from './rpc/solana/fetchBridgeState.js';
import { withBackoff } from './rpc/withBackoff.js';

/** The clients and addresses a history read needs: no single enqueuing transaction. */
export type ProofRequestHistoryRequest = Omit<
    ProofRequestStateSnapshotRequest,
    'proofRequestTxHash'
>;

/** The queue and program addresses a history read needs besides its clients. */
export type ProofRequestHistoryAddresses = Pick<
    ProofRequestHistoryRequest,
    'proofQueueAddress' | 'programAddress'
>;

/** A request a submitting address enqueued, with where it is now. */
export interface ProofRequestHistoryEntry extends ProofRequest {
    /** `unprocessed`, or `proofAvailable` with the committed batch covering it (what `fetchProofRequestWitness` takes). */
    snapshot: ProofRequestStateSnapshot;
}

export interface ProofRequestHistoryPage {
    entries: ProofRequestHistoryEntry[];
    /** Pass as `after` for the next page. */
    cursor?: ProofRequestHistoryCursor;
    /** Whether the scan reached the end of the block range. */
    done: boolean;
}

export interface ProofRequestCounts {
    total: number;
    proofAvailable: number;
    unprocessed: number;
}

/** Requests read per log page while counting; counting keeps no entries, only ids. */
const COUNTING_PAGE_SIZE = 1000;

/**
 * Reads one page of a submitting address's proof requests from Ethereum and
 * classifies them against one read of the bridge state on Solana.
 *
 * @param request The Ethereum provider, Solana RPC, queue and program addresses.
 * @param query The submitting address, block range, order, page size and cursor.
 * @returns The page's entries, its continuation cursor, and whether the range is exhausted.
 * @throws EthRpcTransportError When an Ethereum read still fails after its retries.
 * @throws SolanaRpcTransportError When a Solana read still fails after its retries.
 */
export async function fetchProofRequestHistoryPage(
    request: ProofRequestHistoryRequest,
    query: ProofRequestsByTargetQuery
): Promise<ProofRequestHistoryPage> {
    const page = await fetchProofRequestsByTarget(
        request.proofQueueAddress,
        query,
        request.provider
    );
    if (page.requests.length === 0) {
        return { entries: [], cursor: page.cursor, done: page.done };
    }
    const snapshots = await classifyProofRequests(
        request.rpc,
        page.requests.map((proofRequest) => ({
            requestId: proofRequest.requestId,
            requestBlockNumber: BigInt(proofRequest.blockNumber),
        })),
        request.programAddress
    );
    return {
        entries: page.requests.map((proofRequest, i) => ({
            ...proofRequest,
            snapshot: snapshots[i],
        })),
        cursor: page.cursor,
        done: page.done,
    };
}

/**
 * Counts a submitting address's proof requests over a block range, and how
 * many have a proof available: the queue drains in order, so every id below
 * the bridge's queue cursor is proven.
 *
 * @param request The Ethereum provider, Solana RPC, queue and program addresses.
 * @param query The submitting address and block range.
 * @returns The total, proven and unprocessed counts.
 * @throws EthRpcTransportError When an Ethereum read still fails after its retries.
 * @throws SolanaRpcTransportError When a Solana read still fails after its retries.
 */
export async function fetchProofRequestCountsByTarget(
    request: ProofRequestHistoryRequest,
    query: Pick<
        ProofRequestsByTargetQuery,
        'target' | 'fromBlock' | 'toBlock' | 'maxBlockRangePerQuery'
    >
): Promise<ProofRequestCounts> {
    // Fixed up front so every page reads the same range.
    const toBlock =
        query.toBlock ??
        (await withBackoff(() => request.provider.getBlockNumber()).catch(
            (error: unknown) => {
                throw new EthRpcTransportError(
                    'Failed to read the latest block number.',
                    error
                );
            }
        ));

    const requestIds: bigint[] = [];
    let after: ProofRequestHistoryCursor | undefined;
    for (;;) {
        const page = await fetchProofRequestsByTarget(
            request.proofQueueAddress,
            {
                ...query,
                toBlock,
                order: 'asc',
                pageSize: COUNTING_PAGE_SIZE,
                after,
            },
            request.provider
        );
        requestIds.push(
            ...page.requests.map((proofRequest) => proofRequest.requestId)
        );
        after = page.cursor;
        if (page.done) break;
    }

    const { queueCursor } = await fetchBridgeState(
        request.rpc,
        request.programAddress ?? TOKEN_PROGRAM_ADDRESS
    );
    const proofAvailable = requestIds.filter(
        (requestId) => requestId < queueCursor
    ).length;
    return {
        total: requestIds.length,
        proofAvailable,
        unprocessed: requestIds.length - proofAvailable,
    };
}
