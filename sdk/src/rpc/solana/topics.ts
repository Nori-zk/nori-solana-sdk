import { type Address, type Commitment, type Signature } from '@solana/kit';
import { shareReplay } from 'rxjs';
import { getJsonRpcTopic$ } from '../connection/jsonRpcTopic.js';
import { type ReconnectingWebSocketSubject } from '../connection/websocket.js';
import { type SolanaRpc } from './solanaHttp.js';

/** One change of an account, as `accountSubscribe` pushes it (base64 data). */
export interface SolanaAccountNotification {
    context: { slot: number };
    value: {
        data: [string, 'base64'];
        executable: boolean;
        lamports: number;
        owner: string;
        rentEpoch: number;
        space: number;
    };
}

/** One transaction's logs, as `logsSubscribe` pushes them. */
export interface SolanaLogsNotification {
    context: { slot: number };
    value: { signature: string; err: unknown; logs: string[] };
}

/** A transaction reaching the commitment asked for, as `signatureSubscribe` pushes it. */
export interface SolanaSignatureNotification {
    context: { slot: number };
    value: { err: unknown };
}

/**
 * Returns an observable emitting an account's changes (`accountSubscribe`).
 *
 * @param solanaSocket A Solana RPC node's reconnecting websocket.
 * @param address The account.
 * @param commitment The commitment (default: `finalized`).
 * @returns Observable emitting the account each time it changes; replays the latest to every subscriber.
 */
export const getAccountTopic$ = (
    solanaSocket: ReconnectingWebSocketSubject<unknown>,
    address: Address,
    commitment: Commitment = 'finalized'
) =>
    getJsonRpcTopic$<SolanaAccountNotification>(
        solanaSocket,
        'accountSubscribe',
        [address, { encoding: 'base64', commitment }],
        'accountUnsubscribe'
    ).pipe(shareReplay({ bufferSize: 1, refCount: true }));

/**
 * Returns an observable emitting the logs of transactions mentioning an
 * address (`logsSubscribe`).
 *
 * @param solanaSocket A Solana RPC node's reconnecting websocket.
 * @param address The address, such as a program.
 * @param commitment The commitment (default: `finalized`).
 * @returns Observable emitting each transaction's logs.
 */
export const getSolanaLogsTopic$ = (
    solanaSocket: ReconnectingWebSocketSubject<unknown>,
    address: Address,
    commitment: Commitment = 'finalized'
) =>
    getJsonRpcTopic$<SolanaLogsNotification>(
        solanaSocket,
        'logsSubscribe',
        [{ mentions: [address] }, { commitment }],
        'logsUnsubscribe'
    );

/**
 * Returns an observable emitting when a transaction reaches a commitment
 * (`signatureSubscribe`).
 *
 * @param solanaSocket A Solana RPC node's reconnecting websocket.
 * @param signature The transaction's signature.
 * @param commitment The commitment (default: `finalized`).
 * @returns Observable emitting once the transaction reaches the commitment.
 */
export const getSignatureTopic$ = (
    solanaSocket: ReconnectingWebSocketSubject<unknown>,
    signature: Signature,
    commitment: Commitment = 'finalized'
) =>
    getJsonRpcTopic$<SolanaSignatureNotification>(
        solanaSocket,
        'signatureSubscribe',
        [signature, { commitment }],
        'signatureUnsubscribe'
    );

/** Commitment levels, weakest first. */
const COMMITMENTS: Commitment[] = ['processed', 'confirmed', 'finalized'];

/**
 * An account now, as `accountSubscribe` would have pushed it: what an
 * account subscription polls while the websocket is not open. The rent epoch
 * is not returned over http for base64 data, so it is 0.
 *
 * @param rpc The Solana RPC.
 * @param address The account.
 * @param commitment The commitment.
 * @returns The account, or nothing when it does not exist.
 */
export async function accountInfoFrom(
    rpc: SolanaRpc,
    address: Address,
    commitment: Commitment
): Promise<SolanaAccountNotification[]> {
    const { context, value } = await rpc
        .getAccountInfo(address, { encoding: 'base64', commitment })
        .send();
    if (value === null) return [];
    return [
        {
            context: { slot: Number(context.slot) },
            value: {
                data: value.data as [string, 'base64'],
                executable: value.executable,
                lamports: Number(value.lamports),
                owner: value.owner,
                rentEpoch: 0,
                space: Number(value.space),
            },
        },
    ];
}

/** Where a polled logs subscription has got to: the newest transaction it has seen. */
export interface SolanaLogsCursor {
    lastSignature?: Signature;
}

/**
 * The logs of transactions mentioning an address since the cursor, as
 * `logsSubscribe` would have pushed them: what a logs subscription polls
 * while the websocket is not open. The first poll only sets the cursor.
 *
 * @param rpc The Solana RPC.
 * @param address The address, such as a program.
 * @param commitment The commitment; `processed` reads as `confirmed`, the least `getTransaction` serves.
 * @param cursor The newest transaction seen, moved on by each poll.
 * @returns Each new transaction's logs, oldest first.
 */
export async function solanaLogsSinceFrom(
    rpc: SolanaRpc,
    address: Address,
    commitment: Commitment,
    cursor: SolanaLogsCursor
): Promise<SolanaLogsNotification[]> {
    const transactionCommitment = commitment === 'processed' ? 'confirmed' : commitment;
    const newest = await rpc
        .getSignaturesForAddress(address, {
            commitment: transactionCommitment,
            until: cursor.lastSignature,
        })
        .send();
    if (newest.length === 0) return [];
    const first = cursor.lastSignature === undefined;
    cursor.lastSignature = newest[0].signature;
    if (first) return [];
    const notifications: SolanaLogsNotification[] = [];
    for (const { signature, slot } of [...newest].reverse()) {
        const transaction = await rpc
            .getTransaction(signature, {
                commitment: transactionCommitment,
                encoding: 'json',
                maxSupportedTransactionVersion: 0,
            })
            .send();
        notifications.push({
            context: { slot: Number(slot) },
            value: {
                signature,
                err: transaction?.meta?.err ?? null,
                logs: [...(transaction?.meta?.logMessages ?? [])],
            },
        });
    }
    return notifications;
}

/**
 * A transaction's status, once it has reached a commitment, as
 * `signatureSubscribe` would have pushed it: what a signature subscription
 * polls while the websocket is not open.
 *
 * @param rpc The Solana RPC.
 * @param signature The transaction's signature.
 * @param commitment The commitment to reach.
 * @returns The status once reached, otherwise nothing.
 */
export async function signatureStatusFrom(
    rpc: SolanaRpc,
    signature: Signature,
    commitment: Commitment
): Promise<SolanaSignatureNotification[]> {
    const {
        value: [status],
    } = await rpc.getSignatureStatuses([signature]).send();
    if (
        status === null ||
        status.confirmationStatus === null ||
        COMMITMENTS.indexOf(status.confirmationStatus) < COMMITMENTS.indexOf(commitment)
    )
        return [];
    return [{ context: { slot: Number(status.slot) }, value: { err: status.err } }];
}
