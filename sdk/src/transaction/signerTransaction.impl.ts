import { type ResolveNodeData } from '@yaw-rx/ystate';
import { type Interface, type Provider, type Signer } from 'ethers';
import { catchError, defer, map, type Observable, of, Subject, switchMap, take } from 'rxjs';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { ethereumCallsUsable$, forCalls$ } from '../rpc/connection/connections.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine } from '../rpc/connection/readThroughConnections.impl.js';
import { type RequestOutcome, requestOutcomes } from '../utils/machines.js';
import { messageOf } from '../utils/messageOf.js';
import {
    type SendResult,
    sendTransaction$,
    sentTransactionRead,
    sentTransactionTransitions,
    type TransactionCall,
    transactionToSendOf,
} from './sentTransaction.impl.js';
import { SignerTransactionGraph } from './signerTransaction.js';

/** How an Ethereum transaction machine names a refusal, tells a drop, and retries a failed read. */
export interface TransactionMachineOptions {
    /** The contract's error ABI, to name a revert at gas estimation. */
    errors?: Interface;
    /** Blocks the node may not know a sent transaction before it counts as dropped (default: 12). */
    droppedAfterBlocks?: number;
    /** How long a failed read waits before reading again. */
    backoff?: ReadRetryBackoff;
}

/** How one send ended: the edge out of `sending` it takes, with that node's data. */
type SendOutcome = RequestOutcome<typeof SignerTransactionGraph, 'sent' | 'contractRefused' | 'failedToSend'>;

/**
 * Starts an Ethereum transaction made for `call` and sent with a signer of
 * the app's own on each send request, then followed until it ends. It
 * starts `ready`; a send request while Ethereum's connection cannot take a
 * call moves to `notReadyToSend`.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param signer The app's Ethereum signer, e.g. an ethers `Wallet`; it sends connected to the machine's own provider
 *   (`signer.connect`), so its requests take the machine's turns. One bound to a provider of the app's own
 *   (`JsonRpcSigner`) sends through that provider.
 * @param call The contract, the call's data and the value.
 * @param options The contract's error ABI, the drop threshold and the read backoff.
 * @returns The running machine. Its controls:
 *   - `send()`: sends it, from `ready`, `refused`, `sendFailed` or `dropped`.
 *   - `dismiss()`: leaves `notReadyToSend` for `ready` without sending.
 *   - `retry()`: reads it again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export function createSignerTransactionMachine(
    connections: ProofRequestConnections,
    signer: Signer,
    call: TransactionCall,
    options: TransactionMachineOptions = {}
) {
    const transaction = transactionToSendOf(call);
    /**
     * The signer on the machine's provider, so its requests take the
     * machine's turns. A signer bound to a provider of the app's own, which
     * cannot be moved (ethers' `JsonRpcSigner`), sends through that provider
     * as it is.
     */
    const signerOn = (provider: Provider): Signer => {
        try {
            return signer.connect(provider);
        } catch {
            return signer;
        }
    };
    const send$ = new Subject<void>();
    const dismiss$ = new Subject<void>();
    const { droppedAfterBlocks, ...following } = sentTransactionRead(connections, options.droppedAfterBlocks);

    const machine = startReadThroughConnectionsMachine(SignerTransactionGraph, {
        connections,
        ...following,
        backoff: options.backoff,
        start: { node: 'ready', data: { transaction } },
        ownTransitions: (read$, state$, started$) => {
            // One send per entry into `sending`, the signer on this machine's provider, so it takes this machine's turns.
            const taking = requestOutcomes(
                state$,
                'sending',
                ({ transaction }: ResolveNodeData<typeof SignerTransactionGraph.nodes, 'sending'>): Observable<SendOutcome> =>
                    started$.pipe(
                        take(1),
                        switchMap((machine) =>
                            forCalls$(connections.ethereum, machine, (provider) =>
                                sendTransaction$(defer(() => of(signerOn(provider))), transaction, options.errors)
                            )
                        ),
                        catchError((error: unknown) => of<SendResult>({ outcome: 'failed', error: messageOf(error) }))
                    ).pipe(
                        map((result): SendOutcome => {
                            if (result.outcome === 'sent') return { transition: 'sent', data: result.sent };
                            if (result.outcome === 'refused')
                                return { transition: 'refused', data: { transaction, errorName: result.errorName } };
                            return {
                                transition: 'sendFailed',
                                data: {
                                    transaction,
                                    error: result.outcome === 'failed' ? result.error : 'The signer declined to sign.',
                                },
                            };
                        })
                    )
            );
            const enter = <TData>(data: TData) => data;
            return {
                ...sentTransactionTransitions({
                    read$,
                    send$,
                    dismiss$,
                    usable$: ethereumCallsUsable$(connections.ethereum),
                    waitingOn: ['ethereum'],
                    droppedAfterBlocks,
                }),
                sent: { $: () => taking('sent'), next: enter },
                refused: { $: () => taking('refused'), next: enter },
                sendFailed: { $: () => taking('sendFailed'), next: enter },
            };
        },
    });

    return Object.assign(machine, { send: () => send$.next(), dismiss: () => dismiss$.next() });
}
