import { type ResolveNodeData } from '@yaw-rx/ystate';
import { defer, filter, map, type Observable, Subject, switchMap, take } from 'rxjs';
import { type ProofRequestConnections } from '../../proofRequest/connectedRead.js';
import { type TransactionToSend } from '../../transaction/sentTransaction.js';
import {
    sendTransaction$,
    sentTransactionRead,
    sentTransactionTransitions,
    type TransactionCall,
    transactionToSendOf,
} from '../../transaction/sentTransaction.impl.js';
import { type TransactionMachineOptions } from '../../transaction/signerTransaction.impl.js';
import { type RequestOutcome, requestOutcomes } from '../../utils/machines.js';
import { transportUsable$ } from './connections.js';
import { startReadThroughConnectionsMachine } from './readThroughConnections.impl.js';
import { WalletTransactionGraph } from './walletTransaction.js';

/** How one request to the wallet ended: the edge out of `askingToSign` it takes, with that node's data. */
type SendOutcome = RequestOutcome<
    typeof WalletTransactionGraph,
    'sent' | 'userDeclined' | 'contractRefused' | 'walletRefused'
>;

/**
 * Starts an Ethereum transaction made for `call` and sent through the
 * user's wallet on each send request, then followed until it ends. It
 * starts `ready`; a send request while the wallet is not ready moves to
 * `notReadyToSend`.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param call The contract, the call's data and the value.
 * @param options The contract's error ABI, the drop threshold and the read backoff.
 * @returns The running machine. Its controls:
 *   - `send()`: asks the wallet to send it, from `ready`, `declined`, `refused`, `sendFailed` or `dropped`.
 *   - `dismiss()`: leaves `notReadyToSend` for `ready` without sending.
 *   - `retry()`: reads it again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export function createWalletTransactionMachine(
    connections: ProofRequestConnections,
    call: TransactionCall,
    options: TransactionMachineOptions = {}
) {
    const transaction = transactionToSendOf(call);
    const wallet = connections.ethereum.wallet;
    const send$ = new Subject<void>();
    const dismiss$ = new Subject<void>();
    const walletUsable$ = transportUsable$(connections.ethereum, 'wallet');
    const { droppedAfterBlocks, ...following } = sentTransactionRead(connections, options.droppedAfterBlocks);

    const machine = startReadThroughConnectionsMachine(WalletTransactionGraph, {
        connections,
        ...following,
        backoff: options.backoff,
        start: { node: 'ready', data: { transaction } },
        ownTransitions: (read$, state$, started$) => {
            // One request to the wallet per entry into `askingToSign`, on this machine's turns.
            const taking = requestOutcomes(
                state$,
                'askingToSign',
                ({ transaction }: ResolveNodeData<typeof WalletTransactionGraph.nodes, 'askingToSign'>): Observable<SendOutcome> =>
                    sendTransaction$(
                        started$.pipe(
                            take(1),
                            switchMap((machine) => wallet.ready$(machine)),
                            switchMap((provider) => defer(() => provider.getSigner()))
                        ),
                        transaction,
                        options.errors
                    ).pipe(
                        map((result): SendOutcome => {
                            if (result.outcome === 'sent') return { transition: 'sent', data: result.sent };
                            if (result.outcome === 'declined') return { transition: 'declined', data: { transaction } };
                            if (result.outcome === 'refused')
                                return { transition: 'refused', data: { transaction, errorName: result.errorName } };
                            return { transition: 'sendFailed', data: { transaction, error: result.error } };
                        })
                    )
            );
            const enter = <TData>(data: TData) => data;
            return {
                ...sentTransactionTransitions({
                    read$,
                    send$,
                    dismiss$,
                    usable$: walletUsable$,
                    waitingOn: ['ethereum.wallet'],
                    droppedAfterBlocks,
                }),
                sent: { $: () => taking('sent'), next: enter },
                declined: { $: () => taking('declined'), next: enter },
                refused: { $: () => taking('refused'), next: enter },
                sendFailed: { $: () => taking('sendFailed'), next: enter },
                walletLost: {
                    $: () => walletUsable$.pipe(filter((usable) => !usable)),
                    next: (_lost: unknown, _dest: unknown, { transaction }: { transaction: TransactionToSend }) => ({
                        transaction,
                        error: 'The wallet disconnected before the transaction was sent.',
                    }),
                },
            };
        },
    });

    return Object.assign(machine, { send: () => send$.next(), dismiss: () => dismiss$.next() });
}
