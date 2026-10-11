import { type ReadNeed } from '../proofRequest/connectedRead.js';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { type EthereumTransactionReceipt } from '../rpc/eth/transactionReceipt.js';

/**
 * An Ethereum transaction to send: the contract, the call's data and the
 * value, and the nonce once one is pinned (a send again after a drop reuses
 * the dropped transaction's).
 */
export type TransactionToSend = {
    to: string;
    data: string;
    value: bigint;
    nonce: number | undefined;
};

/** The defaults the graph's nodes are typed by. */
const noTransaction: TransactionToSend = { to: '', data: '0x', value: 0n, nonce: undefined };
const noReceipt: EthereumTransactionReceipt = { transactionHash: '', blockNumber: 0n, status: null };

/**
 * One Ethereum transaction, made for one call and sent only on a send
 * request, then followed on the chain until it ends. Every Ethereum
 * transaction machine spreads these nodes and edges and adds its own
 * `sending` node (the node that sends it, named by the machine) and that
 * node's outcomes.
 *
 * - `ready` holds the transaction until a send request. The request is
 *   gated on what sends it (the wallet, or Ethereum's connection): usable,
 *   it moves to `sending`; not usable, to `notReadyToSend`, naming what it
 *   waits on in `waitingOn`. That node goes back to `ready` once it is
 *   usable, or on `dismiss`; it never sends by itself.
 * - `refused`: the contract reverted it at gas estimation (`errorName`).
 *   `sendFailed`: anything else kept it from being sent (`error`). Both
 *   accept a send request again, through the same gate.
 * - From `loading` on, it is followed through Ethereum's connection
 *   (`readThroughConnectionsOf`) on each new block: `current` holds it sent
 *   and not mined. Each read reads the sender's mined nonce first, then the
 *   receipt, then whether the node knows the transaction.
 * - It ends `confirmed` or `reverted` once mined, with its receipt;
 *   `replaced` once the sender's mined nonce moved past its own with no
 *   receipt for it (another transaction took its nonce); or `dropped` once
 *   the node has not known it for a number of blocks (`unknownSinceBlock`).
 *   `dropped` accepts a send request again, with its nonce pinned, so only
 *   one of the two can land.
 * - `confirmed`, `reverted`, `replaced` and `closed` are terminal.
 *
 * @param sending The machine's node that sends the transaction.
 * @returns The nodes and edges.
 */
export const sentTransactionOf = <TSending extends string>(sending: TSending) => {
    const followed = readThroughConnectionsOf({
        transaction: noTransaction,
        transactionHash: '',
        from: '',
        receipt: undefined as EthereumTransactionReceipt | undefined,
        minedNonce: 0,
        readAtBlock: 0,
        unknownSinceBlock: undefined as number | undefined,
    });
    const sent = { transaction: noTransaction, transactionHash: '', from: '' };
    return {
        nodes: {
            ready: { transaction: noTransaction },
            notReadyToSend: { transaction: noTransaction, waitingOn: [] as ReadNeed[] },
            refused: { transaction: noTransaction, errorName: '' },
            sendFailed: { transaction: noTransaction, error: '' },
            ...followed.nodes,
            confirmed: { ...sent, receipt: noReceipt },
            reverted: { ...sent, receipt: noReceipt },
            replaced: { ...sent },
            dropped: { ...sent },
        },
        edges: {
            sendRequested: { from: 'ready', to: sending, on: 'send.next' },
            notReadyWhenRequested: { from: 'ready', to: 'notReadyToSend', on: 'send.error' },
            sendAgainAfterRefusal: { from: 'refused', to: sending, on: 'send.next' },
            notReadyAfterRefusal: { from: 'refused', to: 'notReadyToSend', on: 'send.error' },
            sendAgainAfterFailure: { from: 'sendFailed', to: sending, on: 'send.next' },
            notReadyAfterFailure: { from: 'sendFailed', to: 'notReadyToSend', on: 'send.error' },
            sendAgainAfterDrop: { from: 'dropped', to: sending, on: 'send.next' },
            notReadyAfterDrop: { from: 'dropped', to: 'notReadyToSend', on: 'send.error' },
            readyToSend: { from: 'notReadyToSend', to: 'ready', on: 'readyToSend.next' },
            dismissed: { from: 'notReadyToSend', to: 'ready', on: 'dismiss.next' },

            ...followed.edges,
            confirmedOnFirstRead: { from: 'loading', to: 'confirmed', on: 'transactionConfirmed.next' },
            confirmedOnRefresh: { from: 'refreshing', to: 'confirmed', on: 'transactionConfirmed.next' },
            revertedOnFirstRead: { from: 'loading', to: 'reverted', on: 'transactionReverted.next' },
            revertedOnRefresh: { from: 'refreshing', to: 'reverted', on: 'transactionReverted.next' },
            replacedOnFirstRead: { from: 'loading', to: 'replaced', on: 'transactionReplaced.next' },
            replacedOnRefresh: { from: 'refreshing', to: 'replaced', on: 'transactionReplaced.next' },
            droppedOnFirstRead: { from: 'loading', to: 'dropped', on: 'transactionDropped.next' },
            droppedOnRefresh: { from: 'refreshing', to: 'dropped', on: 'transactionDropped.next' },

            closedWhileReady: { from: 'ready', to: 'closed', on: 'close.next' },
            closedWhileNotReadyToSend: { from: 'notReadyToSend', to: 'closed', on: 'close.next' },
            closedAfterRefusal: { from: 'refused', to: 'closed', on: 'close.next' },
            closedAfterSendFailed: { from: 'sendFailed', to: 'closed', on: 'close.next' },
            closedAfterDrop: { from: 'dropped', to: 'closed', on: 'close.next' },
        } as const,
    };
};
