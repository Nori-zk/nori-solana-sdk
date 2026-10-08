import { distinctUntilKeyChanged, type Observable } from 'rxjs';
import { type Ethereum, forCalls, forSubscriptions } from '../rpc/connection/connections.js';
import { fetchFinalizedBlockNumber as fetchFinalizedBlockNumberFrom } from '../rpc/eth/fetchFinalizedBlockNumber.js';
import { fetchLatestBlockHeight as fetchLatestBlockHeightFrom } from '../rpc/eth/fetchLatestBlockHeight.js';
import {
    type EthereumLogNotification,
    type EthereumLogsFilter,
    type EthereumNewHeadNotification,
    latestHeadFrom,
    logsFrom,
    logsSinceFrom,
    newHeadsFrom,
} from '../rpc/eth/topics.js';

export type { EthereumLogNotification, EthereumLogsFilter, EthereumNewHeadNotification };

/**
 * The latest Ethereum block's number.
 *
 * @param ethereum The Ethereum chain; calls go in its calls order.
 * @returns The latest block number.
 */
export function getLatestBlockHeight(ethereum: Ethereum): Promise<number> {
    return forCalls(ethereum, fetchLatestBlockHeightFrom);
}

/**
 * The latest finalized Ethereum block's number.
 *
 * @param ethereum The Ethereum chain; calls go in its calls order.
 * @returns The latest finalized block number.
 */
export function getFinalizedBlockNumber(ethereum: Ethereum): Promise<number> {
    return forCalls(ethereum, fetchFinalizedBlockNumberFrom);
}

/**
 * Each new block's header: subscribed in the chain's subscriptions order,
 * and while none of those is ready, polled through its calls order, in the
 * same shape.
 *
 * @param ethereum The Ethereum chain.
 * @returns Each new head.
 */
export function getNewHeads$(ethereum: Ethereum): Observable<EthereumNewHeadNotification> {
    return forSubscriptions(ethereum, newHeadsFrom, latestHeadFrom)
        .pipe(distinctUntilKeyChanged('hash')); // a poll finds the same head until a new block
}

/**
 * The logs matching a filter: subscribed in the chain's subscriptions
 * order, and while none of those is ready, the logs of each new block
 * polled through its calls order, in the same shape.
 *
 * @param ethereum The Ethereum chain.
 * @param filter The emitting contracts and topics.
 * @returns Each matching log.
 */
export function getEthereumLogs$(
    ethereum: Ethereum,
    filter: EthereumLogsFilter
): Observable<EthereumLogNotification> {
    return forSubscriptions(ethereum, logsFrom, logsSinceFrom, filter, {});
}
