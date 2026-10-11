import { type ResolveNodeData } from '@yaw-rx/ystate';
import { type Instruction } from '@solana/kit';
import { catchError, filter, map, type Observable, of, Subject, switchMap, take } from 'rxjs';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { solanaWalletUsable$ } from '../rpc/connection/connections.js';
import { startReadThroughConnectionsMachine } from '../rpc/connection/readThroughConnections.impl.js';
import { messageOf } from '../utils/messageOf.js';
import { type RequestOutcome, requestOutcomes } from '../utils/machines.js';
import { instructionsToSendOf, type InstructionToSend } from './solanaTransaction.js';
import {
    sendSolanaTransaction$,
    sentSolanaTransactionRead,
    sentSolanaTransactionTransitions,
    type SolanaTransactionMachineOptions,
    TOKEN_PROGRAM_ERROR_NAMES,
} from './solanaTransaction.impl.js';
import { SolanaWalletTransactionGraph } from './solanaWalletTransaction.js';

/** How one request to the wallet ended: the edge out of `askingToSign` it takes, with that node's data. */
type SendOutcome = RequestOutcome<
    typeof SolanaWalletTransactionGraph,
    'sent' | 'userDeclined' | 'programRefused' | 'walletRefused'
>;

/**
 * Starts a Solana transaction made for `instructions`, signed, fee-paid and
 * sent by the user's Solana wallet on each send request, then followed until
 * it ends. It starts `ready`; a send request while the wallet is not ready
 * moves to `notReadyToSend`.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param instructions The instructions, e.g. from `mintCall$`; the wallet's account signs every account at its address.
 * @param options The programs' error names, the commitment and the read backoff.
 * @returns The running machine. Its controls:
 *   - `send()`: asks the wallet to send it, from `ready`, `declined`, `refused`, `sendFailed` or `expired`.
 *   - `dismiss()`: leaves `notReadyToSend` for `ready` without sending.
 *   - `retry()`: reads it again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export function createSolanaWalletTransactionMachine(
    connections: ProofRequestConnections,
    instructions: readonly Instruction[],
    options: SolanaTransactionMachineOptions = {}
) {
    const { errorNames = TOKEN_PROGRAM_ERROR_NAMES, commitment = 'finalized' } = options;
    const send$ = new Subject<void>();
    const dismiss$ = new Subject<void>();
    const walletUsable$ = solanaWalletUsable$(connections.solana);
    const { startedAs, ...following } = sentSolanaTransactionRead(connections, commitment);

    const machine = startReadThroughConnectionsMachine(SolanaWalletTransactionGraph, {
        connections,
        ...following,
        backoff: options.backoff,
        // The instructions it is made for are its initial state.
        start: { node: 'ready', data: { instructions: instructionsToSendOf(instructions) } },
        ownTransitions: (read$, state$, started$) => {
            // One request to the wallet per entry into `askingToSign`, on this machine's turns.
            const taking = requestOutcomes(
                state$,
                'askingToSign',
                ({ instructions }: ResolveNodeData<typeof SolanaWalletTransactionGraph.nodes, 'askingToSign'>): Observable<SendOutcome> =>
                    started$.pipe(
                        take(1),
                        switchMap((machine) =>
                            connections.solana.wallet.ready$(machine).pipe(
                                take(1),
                                switchMap((signer) =>
                                    sendSolanaTransaction$(connections.solana, machine, signer, instructions, errorNames)
                                )
                            )
                        ),
                        map((result): SendOutcome => {
                            if (result.outcome === 'sent') return { transition: 'sent', data: result.sent };
                            if (result.outcome === 'declined') return { transition: 'declined', data: { instructions } };
                            if (result.outcome === 'refused') {
                                const { outcome: _refused, ...refusal } = result;
                                return { transition: 'refused', data: { instructions, ...refusal } };
                            }
                            return { transition: 'sendFailed', data: { instructions, error: result.error } };
                        }),
                        // The wallet stopped being ready between the gate and the request.
                        catchError((error: unknown) =>
                            of<SendOutcome>({ transition: 'sendFailed', data: { instructions, error: messageOf(error) } })
                        )
                    )
            );
            const enter = <TData>(data: TData) => data;
            return {
                ...sentSolanaTransactionTransitions({
                    read$,
                    send$,
                    dismiss$,
                    usable$: walletUsable$,
                    waitingOn: ['solana.wallet'],
                    commitment,
                    errorNames,
                }),
                sent: { $: () => taking('sent'), next: enter },
                declined: { $: () => taking('declined'), next: enter },
                refused: { $: () => taking('refused'), next: enter },
                sendFailed: { $: () => taking('sendFailed'), next: enter },
                walletLost: {
                    $: () => walletUsable$.pipe(filter((usable) => !usable)),
                    next: (_lost: unknown, _dest: unknown, { instructions }: { instructions: InstructionToSend[] }) => ({
                        instructions,
                        error: 'The wallet disconnected before the transaction was sent.',
                    }),
                },
            };
        },
    });
    startedAs(machine);

    return Object.assign(machine, { send: () => send$.next(), dismiss: () => dismiss$.next() });
}
