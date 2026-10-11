import { type ResolveNodeData } from '@yaw-rx/ystate';
import { dataLength, type Interface, isError, type Signer } from 'ethers';
import { catchError, defer, filter, map, type Observable, of, switchMap, take, withLatestFrom } from 'rxjs';
import { type ConnectedReadClients, type ProofRequestConnections, type ReadNeed } from '../proofRequest/connectedRead.js';
import { changesOf$, createChainChangesMachine } from '../rpc/connection/chainChanges.impl.js';
import { type ReadThroughConnectionsRead } from '../rpc/connection/readThroughConnections.impl.js';
import { type EthereumTransactionReceipt } from '../rpc/eth/transactionReceipt.js';
import { messageOf } from '../utils/messageOf.js';
import { withOutcome } from '../utils/machines.js';
import { type sentTransactionOf, type TransactionToSend } from './sentTransaction.js';
import { DROPPED_AFTER_BLOCKS, dropped, replaced, type TransactionFollow, transactionFollow$ } from './transactionFollow.js';

/** The Ethereum transaction a machine is made for: the contract, the call's data and the value. */
export type TransactionCall = Omit<TransactionToSend, 'nonce'>;

/** A sent transaction as it is followed on the chain: what `current` holds. */
export type SentTransaction = ResolveNodeData<ReturnType<typeof sentTransactionOf<'sending'>>['nodes'], 'current'>;

/** A mined transaction: one with its receipt. */
type Mined = SentTransaction & { receipt: EthereumTransactionReceipt };

/** Thrown in a send request's gate when what sends the transaction is not usable. */
export class NotReadyToSendError extends Error {
    constructor(readonly waitingOn: ReadNeed[]) {
        super(`Not ready to send: waiting on ${waitingOn.join(', ')}.`);
        this.name = 'NotReadyToSendError';
    }
}

/**
 * The transaction a machine starts `ready` with: the call, no nonce pinned.
 *
 * @param call The contract, the call's data and the value.
 * @returns The transaction to send.
 */
export const transactionToSendOf = (call: TransactionCall): TransactionToSend => ({ ...call, nonce: undefined });

/**
 * Whether the node mined a transaction (with its receipt).
 *
 * @param sent The transaction as last read.
 * @returns `true` once it has a receipt.
 */
const mined = (sent: SentTransaction): sent is Mined => sent.receipt !== undefined;

/** A sent transaction, as `transactionFollow$` reads it: its nonce is the one it was sent with. */
const followOf = (sent: SentTransaction): TransactionFollow => ({ ...sent, nonce: sent.transaction.nonce });

/**
 * How an Ethereum transaction machine follows its sent transaction: through
 * Ethereum's connection, on each new block, until it ends
 * (`transactionFollow$`).
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param droppedAfterBlocks Blocks the node may not know it before it counts as dropped.
 * @returns The `read`, `refreshOn`, `arrivedElsewhere`, `needs` and `owns` of `startReadThroughConnectionsMachine`.
 */
export const sentTransactionRead = (connections: ProofRequestConnections, droppedAfterBlocks = DROPPED_AFTER_BLOCKS) => {
    const newBlocks = createChainChangesMachine(connections.ethereum);
    return {
        read: (clients: ConnectedReadClients, sent: SentTransaction) =>
            clients.ethereum((provider) => transactionFollow$(provider, followOf(sent))).pipe(
                map(
                    ({ minedNonce, readAtBlock, receipt, unknownSinceBlock }): SentTransaction => ({
                        ...sent,
                        minedNonce,
                        readAtBlock,
                        receipt,
                        unknownSinceBlock,
                    })
                )
            ),
        refreshOn: () => changesOf$(newBlocks),
        // A transaction that ended moves to the node it ended in, not `current`.
        arrivedElsewhere: (sent: SentTransaction) =>
            mined(sent) || replaced(followOf(sent)) || dropped(followOf(sent), droppedAfterBlocks),
        needs: ['ethereum' as const],
        owns: [newBlocks],
        droppedAfterBlocks,
    };
};

/** What the transitions every Ethereum transaction machine shares react to. */
export interface SentTransactionTransitionsOptions {
    /** The shared read's outcomes, from `ownTransitions`. */
    read$: Observable<ReadThroughConnectionsRead<SentTransaction>>;
    /** The send requests from outside. */
    send$: Observable<unknown>;
    /** The requests to leave `notReadyToSend` without sending. */
    dismiss$: Observable<unknown>;
    /** Whether what sends the transaction (the wallet, or Ethereum's connection) is usable, as it changes. */
    usable$: Observable<boolean>;
    /** What a send request waits on while it is not usable. */
    waitingOn: ReadNeed[];
    /** Blocks the node may not know a sent transaction before it counts as dropped. */
    droppedAfterBlocks: number;
}

/**
 * The transitions every Ethereum transaction machine shares
 * (`sentTransactionOf`): the gated send request, leaving `notReadyToSend`,
 * and how the followed transaction ends. Spread into the machine's own
 * transitions.
 *
 * @param options The read's outcomes, the requests, the gate and the drop threshold.
 * @returns The transitions, by name.
 */
export function sentTransactionTransitions(options: SentTransactionTransitionsOptions) {
    const { read$, usable$, waitingOn, droppedAfterBlocks } = options;
    const read = withOutcome(read$, 'succeeded').pipe(map(({ value }) => value));
    const keep = (_event: unknown, _dest: unknown, { transaction }: { transaction: TransactionToSend }) => ({
        transaction,
    });
    const ended = ({ transaction, transactionHash, from }: SentTransaction) => ({ transaction, transactionHash, from });
    const withReceipt = ({ transaction, transactionHash, from, receipt }: Mined) => ({
        transaction,
        transactionHash,
        from,
        receipt,
    });
    return {
        // The gate: a request while what sends it is not usable errors into `notReadyToSend`.
        send: {
            $: () =>
                options.send$.pipe(
                    withLatestFrom(usable$),
                    map(([, usable]) => {
                        if (!usable) throw new NotReadyToSendError(waitingOn);
                    })
                ),
            next: keep,
            error: (_notReady: unknown, _dest: unknown, { transaction }: { transaction: TransactionToSend }) => ({
                transaction,
                waitingOn,
            }),
        },
        readyToSend: { $: () => usable$.pipe(filter(Boolean)), next: keep },
        dismiss: { $: () => options.dismiss$, next: keep },
        transactionConfirmed: {
            $: () => read.pipe(filter((sent): sent is Mined => mined(sent) && sent.receipt.status === 1)),
            next: withReceipt,
        },
        transactionReverted: {
            $: () => read.pipe(filter((sent): sent is Mined => mined(sent) && sent.receipt.status !== 1)),
            next: withReceipt,
        },
        transactionReplaced: { $: () => read.pipe(filter((sent) => replaced(followOf(sent)))), next: ended },
        transactionDropped: {
            $: () => read.pipe(filter((sent) => dropped(followOf(sent), droppedAfterBlocks))),
            next: ended,
        },
    };
}

/** How one send ended: sent, with what `loading` follows; or why it was not. */
export type SendResult =
    | { outcome: 'sent'; sent: SentTransaction & { failedReads: number } }
    | { outcome: 'declined' }
    | { outcome: 'refused'; errorName: string }
    | { outcome: 'failed'; error: string };

/**
 * The name of the contract error a refusal's revert data encodes.
 *
 * @param data The revert data, if any.
 * @param errors The contract's error ABI, if given.
 * @returns The error's name, or what is known without it.
 */
const errorNameOf = (data: string | null | undefined, errors: Interface | undefined): string => {
    // An error's data starts with its 4-byte selector; `0x` (a bare `revert()`, or none) carries no error.
    if (!data || dataLength(data) < 4) return 'revert without data';
    return errors?.parseError(data)?.name ?? `revert ${data}`;
};

/**
 * Sends one Ethereum transaction with a signer, once: its nonce when one is
 * pinned. A revert at gas estimation is `refused`, with the contract error's
 * name; the user saying no in their wallet is `declined`; anything else is
 * `failed`, a nonce clash told as one.
 *
 * @param signer$ The signer, once.
 * @param transaction The transaction.
 * @param errors The contract's error ABI, to name a revert.
 * @returns How the send ended, once.
 */
export function sendTransaction$(
    signer$: Observable<Signer>,
    transaction: TransactionToSend,
    errors: Interface | undefined
): Observable<SendResult> {
    const { to, data, value, nonce } = transaction;
    return signer$.pipe(
        take(1),
        switchMap((signer) => defer(() => signer.sendTransaction({ to, data, value, nonce }))),
        map(
            (response): SendResult => ({
                outcome: 'sent',
                sent: {
                    transaction: { ...transaction, nonce: response.nonce },
                    transactionHash: response.hash,
                    from: response.from,
                    receipt: undefined,
                    minedNonce: response.nonce,
                    readAtBlock: 0,
                    unknownSinceBlock: undefined,
                    failedReads: 0,
                },
            })
        ),
        catchError((error: unknown) =>
            of<SendResult>(
                isError(error, 'ACTION_REJECTED')
                    ? { outcome: 'declined' }
                    : isError(error, 'CALL_EXCEPTION')
                      ? { outcome: 'refused', errorName: error.revert?.name ?? errorNameOf(error.data, errors) }
                      : isError(error, 'REPLACEMENT_UNDERPRICED')
                        ? { outcome: 'failed', error: 'The transaction sent before with this nonce is still pending.' }
                        : isError(error, 'NONCE_EXPIRED')
                          ? { outcome: 'failed', error: 'This nonce is used: the transaction sent before with it may have landed.' }
                          : { outcome: 'failed', error: messageOf(error) }
            )
        )
    );
}
