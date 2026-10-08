import { type Block, type Log, toQuantity } from 'ethers';
import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { type Observable } from 'rxjs';
import { type EthereumSubscriptionSocket } from '../connection/connections.js';

/** A new block's header, as `eth_subscribe` `newHeads` pushes it (hex quantities). */
export interface EthereumNewHeadNotification {
    number: string;
    hash: string;
    parentHash: string;
    timestamp: string;
}

/** One log, as `eth_subscribe` `logs` pushes it (hex quantities). */
export interface EthereumLogNotification {
    address: string;
    topics: string[];
    data: string;
    blockNumber: string;
    blockHash: string;
    transactionHash: string;
    logIndex: string;
    removed: boolean;
}

/** Which logs to receive: by emitting contract and by topics. */
export interface EthereumLogsFilter {
    address?: string | string[];
    topics?: (string | string[] | null)[];
}

/**
 * Each new block's header (`eth_subscribe` `newHeads`).
 *
 * @param socket Where `eth_subscribe` goes: the websocket or the wallet.
 * @returns Each new head.
 */
export function newHeadsFrom(
    socket: EthereumSubscriptionSocket
): Observable<EthereumNewHeadNotification> {
    return socket.ethSubscribe<EthereumNewHeadNotification>(['newHeads']);
}

/**
 * The logs matching a filter (`eth_subscribe` `logs`); a log removed by a
 * reorg comes again with `removed: true`.
 *
 * @param socket Where `eth_subscribe` goes: the websocket or the wallet.
 * @param filter The emitting contracts and topics.
 * @returns Each matching log.
 */
export function logsFrom(
    socket: EthereumSubscriptionSocket,
    filter: EthereumLogsFilter
): Observable<EthereumLogNotification> {
    return socket.ethSubscribe<EthereumLogNotification>(['logs', filter]);
}

/**
 * The latest block's header, as `newHeads` would have pushed it: what a
 * new heads subscription polls while it cannot subscribe.
 *
 * @param provider The Ethereum provider.
 * @returns The latest head, or nothing when there is no block.
 */
export async function latestHeadFrom(provider: EthereumProvider): Promise<EthereumNewHeadNotification[]> {
    const block = await provider.getBlock('latest');
    return block === null ? [] : [headOf(block)];
}

/** Where a polled logs subscription has got to: the last block it read. */
export interface LogsCursor {
    lastBlock?: number;
}

/**
 * The logs matching a filter in the blocks since the cursor, as `logs`
 * would have pushed them: what a logs subscription polls while it cannot
 * subscribe. The first poll only sets the cursor to the latest block.
 *
 * @param provider The Ethereum provider.
 * @param filter The emitting contracts and topics.
 * @param cursor The last block read, moved on by each poll.
 * @returns The new logs, oldest first.
 */
export async function logsSinceFrom(
    provider: EthereumProvider,
    filter: EthereumLogsFilter,
    cursor: LogsCursor
): Promise<EthereumLogNotification[]> {
    const latest = await provider.getBlockNumber();
    if (cursor.lastBlock === undefined || latest <= cursor.lastBlock) {
        cursor.lastBlock ??= latest;
        return [];
    }
    const logs = await provider.getLogs({ ...filter, fromBlock: cursor.lastBlock + 1, toBlock: latest });
    cursor.lastBlock = latest;
    return logs.map(logOf);
}

/**
 * A block, as `newHeads` would have pushed its header.
 *
 * @param block A block ethers read.
 * @returns Its header, in the pushed shape.
 */
export function headOf(block: Block): EthereumNewHeadNotification {
    return {
        number: toQuantity(block.number),
        hash: block.hash ?? '',
        parentHash: block.parentHash,
        timestamp: toQuantity(block.timestamp),
    };
}

/**
 * A log, as `logs` would have pushed it.
 *
 * @param log A log ethers read.
 * @returns The log, in the pushed shape.
 */
export function logOf(log: Log): EthereumLogNotification {
    return {
        address: log.address,
        topics: [...log.topics],
        data: log.data,
        blockNumber: toQuantity(log.blockNumber),
        blockHash: log.blockHash,
        transactionHash: log.transactionHash,
        logIndex: toQuantity(log.index),
        removed: log.removed,
    };
}
