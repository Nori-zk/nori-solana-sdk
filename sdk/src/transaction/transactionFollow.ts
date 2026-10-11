import { type EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
import { forkJoin, map, type Observable, of, switchMap } from 'rxjs';
import { blockNumber$ } from '../rpc/eth/blockNumber.js';
import { ethRpcRead$ } from '../rpc/eth/ethRpcRead.js';
import { type EthereumTransactionReceipt, transactionReceipt$ } from '../rpc/eth/transactionReceipt.js';

/**
 * A transaction followed on Ethereum by its hash, as the last read found
 * it: its sender and nonce (`''` and `undefined` until the node has known
 * it), its receipt once mined, the sender's mined nonce, the block the read
 * was made at, and the block since which the node has not known it.
 */
export type TransactionFollow = {
    transactionHash: string;
    from: string;
    nonce: number | undefined;
    receipt: EthereumTransactionReceipt | undefined;
    minedNonce: number;
    readAtBlock: number;
    unknownSinceBlock: number | undefined;
};

/** A transaction known only by its hash, before its first read. */
export const transactionFollowOf = (transactionHash: string): TransactionFollow => ({
    transactionHash,
    from: '',
    nonce: undefined,
    receipt: undefined,
    minedNonce: 0,
    readAtBlock: 0,
    unknownSinceBlock: undefined,
});

/** Blocks the node may not know a sent transaction before it counts as dropped, unless given. */
export const DROPPED_AFTER_BLOCKS = 12;

/**
 * One read of a transaction followed by its hash. Without its sender yet,
 * it first asks the node for the transaction, which gives its sender and
 * nonce. Then it reads the sender's mined nonce, then the receipt and the
 * latest block, then, with no receipt, whether the node knows the
 * transaction: a transaction mined between two reads is never taken for
 * replaced.
 *
 * @param provider The provider read through.
 * @param follow The transaction as the last read found it.
 * @returns The transaction as this read finds it, once.
 */
export function transactionFollow$(provider: EthereumProvider, follow: TransactionFollow): Observable<TransactionFollow> {
    const known$ = () =>
        ethRpcRead$(() => provider.getTransaction(follow.transactionHash), 'Failed to read the transaction.');
    const sender$: Observable<Pick<TransactionFollow, 'from' | 'nonce'>> =
        follow.from !== ''
            ? of(follow)
            : known$().pipe(map((known) => (known === null ? follow : { from: known.from, nonce: known.nonce })));
    return sender$.pipe(
        switchMap(({ from, nonce }) =>
            (from === ''
                ? of(follow.minedNonce)
                : ethRpcRead$(() => provider.getTransactionCount(from, 'latest'), "Failed to read the sender's mined nonce.")
            ).pipe(
                switchMap((minedNonce) =>
                    forkJoin([transactionReceipt$(provider, follow.transactionHash), blockNumber$(provider, 'latest')]).pipe(
                        switchMap(([receipt, readAtBlock]) =>
                            receipt !== undefined
                                ? of({ ...follow, from, nonce, minedNonce, readAtBlock, receipt, unknownSinceBlock: undefined })
                                : known$().pipe(
                                      map(
                                          (known): TransactionFollow => ({
                                              ...follow,
                                              from,
                                              nonce,
                                              minedNonce,
                                              readAtBlock,
                                              receipt: undefined,
                                              unknownSinceBlock:
                                                  known === null ? (follow.unknownSinceBlock ?? readAtBlock) : undefined,
                                          })
                                      )
                                  )
                        )
                    )
                )
            )
        )
    );
}

/** Another transaction took its nonce: the sender's mined nonce moved past its own with no receipt for it. */
export const replaced = (follow: Pick<TransactionFollow, 'receipt' | 'nonce' | 'minedNonce'>) =>
    follow.receipt === undefined && follow.nonce !== undefined && follow.minedNonce > follow.nonce;

/**
 * The node has not known it for `droppedAfterBlocks` blocks.
 *
 * @param follow The transaction as last read.
 * @param droppedAfterBlocks The blocks it may stay unknown.
 * @returns `true` once it counts as dropped.
 */
export const dropped = (
    follow: Pick<TransactionFollow, 'receipt' | 'nonce' | 'minedNonce' | 'readAtBlock' | 'unknownSinceBlock'>,
    droppedAfterBlocks: number
) =>
    follow.receipt === undefined &&
    !replaced(follow) &&
    follow.unknownSinceBlock !== undefined &&
    follow.readAtBlock - follow.unknownSinceBlock >= droppedAfterBlocks;
