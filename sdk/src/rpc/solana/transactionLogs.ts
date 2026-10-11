import { type Address, type Signature } from '@solana/kit';
import { concatMap, from, map, type Observable, of, switchMap, toArray } from 'rxjs';
import { type SolanaRpc } from './solanaHttp.js';
import { solanaRpcRead$ } from './solanaRpcRead.js';

/** One finalized transaction's logs: its signature and slot, its error if it failed, and its log lines. */
export interface SolanaTransactionLogs {
    signature: Signature;
    slot: number;
    /** `null` when the transaction succeeded. */
    err: unknown;
    logs: string[];
}

/**
 * The finalized transactions mentioning `address` after `until`, oldest
 * first, each with its logs.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param address The account or program.
 * @param until The newest transaction already read; the newest transactions there are when omitted.
 * @returns The transactions and their logs, once.
 */
export const transactionLogsSince$ = (
    rpc: SolanaRpc,
    address: Address,
    until: Signature | undefined
): Observable<SolanaTransactionLogs[]> =>
    solanaRpcRead$(
        (abortSignal) =>
            rpc.getSignaturesForAddress(address, { commitment: 'finalized', until }).send({ abortSignal }),
        `Failed to read the transactions of ${address}.`
    ).pipe(
        switchMap((newestFirst) =>
            newestFirst.length === 0
                ? of([])
                : from([...newestFirst].reverse()).pipe(
                      concatMap(({ signature, slot }) =>
                          solanaRpcRead$(
                              (abortSignal) =>
                                  rpc
                                      .getTransaction(signature, {
                                          commitment: 'finalized',
                                          encoding: 'json',
                                          maxSupportedTransactionVersion: 0,
                                      })
                                      .send({ abortSignal }),
                              `Failed to read the transaction ${signature}.`
                          ).pipe(
                              map(
                                  (transaction): SolanaTransactionLogs => ({
                                      signature,
                                      slot: Number(slot),
                                      err: transaction?.meta?.err ?? null,
                                      logs: [...(transaction?.meta?.logMessages ?? [])],
                                  })
                              )
                          )
                      ),
                      toArray()
                  )
        )
    );

/**
 * The newest finalized transaction mentioning `address`.
 *
 * @param rpc The Solana RPC used for the read.
 * @param address The account or program.
 * @returns Its signature, or `undefined` when none mentions it, once.
 */
export const newestSignature$ = (rpc: SolanaRpc, address: Address): Observable<Signature | undefined> =>
    solanaRpcRead$(
        (abortSignal) =>
            rpc.getSignaturesForAddress(address, { limit: 1, commitment: 'finalized' }).send({ abortSignal }),
        `Failed to read the transactions of ${address}.`
    ).pipe(map(([newest]) => newest?.signature));

/** What one read of an address's transaction logs found, and where it got to. */
export interface TransactionLogsRead {
    /** The transactions found, oldest first, each with its logs. */
    transactions: SolanaTransactionLogs[];
    /** The newest transaction read, where the next read starts after. */
    lastSignature: Signature | undefined;
}

/** A read that found no transactions. */
const NO_TRANSACTIONS: SolanaTransactionLogs[] = [];

/**
 * One read of the finalized transactions mentioning `address`, each with its
 * logs. A first read only finds where to start: the newest transaction
 * there is. A later read gives the transactions after `lastSignature`.
 *
 * @param rpc The Solana RPC used for the reads.
 * @param address The account or program.
 * @param lastSignature The newest transaction already read.
 * @param first Whether this is the first read.
 * @returns What the read found, and where it got to, once.
 */
export const transactionLogsRead$ = (
    rpc: SolanaRpc,
    address: Address,
    lastSignature: Signature | undefined,
    first: boolean
): Observable<TransactionLogsRead> =>
    first
        ? newestSignature$(rpc, address).pipe(
              map((newest) => ({ transactions: NO_TRANSACTIONS, lastSignature: newest }))
          )
        : transactionLogsSince$(rpc, address, lastSignature).pipe(
              map((transactions) => ({
                  transactions,
                  lastSignature: transactions.at(-1)?.signature ?? lastSignature,
              }))
          );
