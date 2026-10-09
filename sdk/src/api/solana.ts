import { type Address, type Commitment, type Signature } from '@solana/kit';
import {
    distinctUntilChanged,
    map,
    type Observable,
    shareReplay,
    switchMap,
    take,
} from 'rxjs';
import { type Solana } from '../rpc/connection/connections.js';
import {
    accountInfoFrom,
    getAccountTopic$ as accountFrom,
    getSignatureTopic$ as signatureFrom,
    getSolanaLogsTopic$ as solanaLogsFrom,
    signatureStatusFrom,
    type SolanaAccountNotification,
    type SolanaLogsCursor,
    type SolanaLogsNotification,
    solanaLogsSinceFrom,
    type SolanaSignatureNotification,
} from '../rpc/solana/topics.js';
import { poll$ } from '../utils/poll.js';

export type { SolanaAccountNotification, SolanaLogsNotification, SolanaSignatureNotification };

/** How often a subscription polls over http while the websocket is not open, in ms. */
const POLL_INTERVAL_MS = 15_000;

/**
 * An account's changes: pushed over the websocket while it is open,
 * otherwise polled over http, in the same shape. Replays the latest.
 *
 * @param solana The Solana chain.
 * @param address The account.
 * @param commitment The commitment (default: `finalized`).
 * @returns The account each time it changes.
 */
export function getAccount$(
    solana: Solana,
    address: Address,
    commitment: Commitment = 'finalized'
): Observable<SolanaAccountNotification> {
    return solana.websocket.connection.state$.pipe(
        map((state) => state.node === 'open'),
        distinctUntilChanged(),
        switchMap((open) =>
            open
                ? accountFrom(solana.websocket.socket, address, commitment)
                : poll$(
                      async () => accountInfoFrom(await solana.http.ready(), address, commitment),
                      POLL_INTERVAL_MS
                  )
        ),
        // A poll finds the same account until it changes.
        distinctUntilChanged(
            (previous, current) =>
                previous.value.lamports === current.value.lamports &&
                previous.value.data[0] === current.value.data[0]
        ),
        shareReplay({ bufferSize: 1, refCount: true })
    );
}

/**
 * The logs of transactions mentioning an address: pushed over the websocket
 * while it is open, otherwise the address's new transactions polled over
 * http, in the same shape.
 *
 * @param solana The Solana chain.
 * @param address The address, such as a program.
 * @param commitment The commitment (default: `finalized`).
 * @returns Each transaction's logs.
 */
export function getSolanaLogs$(
    solana: Solana,
    address: Address,
    commitment: Commitment = 'finalized'
): Observable<SolanaLogsNotification> {
    const cursor: SolanaLogsCursor = {};
    return solana.websocket.connection.state$.pipe(
        map((state) => state.node === 'open'),
        distinctUntilChanged(),
        switchMap((open) =>
            open
                ? solanaLogsFrom(solana.websocket.socket, address, commitment)
                : poll$(
                      async () =>
                          solanaLogsSinceFrom(await solana.http.ready(), address, commitment, cursor),
                      POLL_INTERVAL_MS
                  )
        )
    );
}

/**
 * A transaction reaching a commitment, once: pushed over the websocket
 * while it is open, otherwise polled over http, in the same shape.
 *
 * @param solana The Solana chain.
 * @param signature The transaction's signature.
 * @param commitment The commitment (default: `finalized`).
 * @returns One notification when the transaction reaches the commitment.
 */
export function getSignatureStatus$(
    solana: Solana,
    signature: Signature,
    commitment: Commitment = 'finalized'
): Observable<SolanaSignatureNotification> {
    return solana.websocket.connection.state$.pipe(
        map((state) => state.node === 'open'),
        distinctUntilChanged(),
        switchMap((open) =>
            open
                ? signatureFrom(solana.websocket.socket, signature, commitment)
                : poll$(
                      async () => signatureStatusFrom(await solana.http.ready(), signature, commitment),
                      POLL_INTERVAL_MS
                  )
        ),
        take(1)
    );
}
