import { type ResolveNodeData } from '@yaw-rx/ystate';
import { type Instruction, type TransactionSigner } from '@solana/kit';
import { map, type Observable, Subject } from 'rxjs';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { solanaCallsUsable$ } from '../rpc/connection/connections.js';
import { startReadThroughConnectionsMachine } from '../rpc/connection/readThroughConnections.impl.js';
import { type RequestOutcome, requestOutcomes } from '../utils/machines.js';
import { SolanaSignerTransactionGraph } from './solanaSignerTransaction.js';
import { instructionsToSendOf } from './solanaTransaction.js';
import {
    sendSolanaTransaction$,
    sentSolanaTransactionRead,
    sentSolanaTransactionTransitions,
    type SolanaTransactionMachineOptions,
    TOKEN_PROGRAM_ERROR_NAMES,
} from './solanaTransaction.impl.js';

/** How one send ended: the edge out of `sending` it takes, with that node's data. */
type SendOutcome = RequestOutcome<typeof SolanaSignerTransactionGraph, 'sent' | 'programRefused' | 'failedToSend'>;

/**
 * Starts a Solana transaction made for `instructions`, signed and fee-paid
 * by a signer of the app's own and sent on each send request, then followed
 * until it ends. It starts `ready`; a send request while Solana's connection
 * cannot take a call moves to `notReadyToSend`.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param signer The app's signer, e.g. from `createKeyPairSignerFromBytes`; it signs every account at its address.
 * @param instructions The instructions, e.g. from the program's generated builders.
 * @param options The programs' error names, the commitment and the read backoff.
 * @returns The running machine. Its controls:
 *   - `send()`: sends it, from `ready`, `refused`, `sendFailed` or `expired`.
 *   - `dismiss()`: leaves `notReadyToSend` for `ready` without sending.
 *   - `retry()`: reads it again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export function createSolanaSignerTransactionMachine(
    connections: ProofRequestConnections,
    signer: TransactionSigner,
    instructions: readonly Instruction[],
    options: SolanaTransactionMachineOptions = {}
) {
    const { errorNames = TOKEN_PROGRAM_ERROR_NAMES, commitment = 'finalized' } = options;
    const send$ = new Subject<void>();
    const dismiss$ = new Subject<void>();
    const { startedAs, ...following } = sentSolanaTransactionRead(connections, commitment);

    const machine = startReadThroughConnectionsMachine(SolanaSignerTransactionGraph, {
        connections,
        ...following,
        backoff: options.backoff,
        // The instructions it is made for are its initial state.
        start: { node: 'ready', data: { instructions: instructionsToSendOf(instructions) } },
        ownTransitions: (read$, state$) => {
            // One send per entry into `sending`.
            const taking = requestOutcomes(
                state$,
                'sending',
                ({ instructions }: ResolveNodeData<typeof SolanaSignerTransactionGraph.nodes, 'sending'>): Observable<SendOutcome> =>
                    sendSolanaTransaction$(connections.solana, signer, instructions, errorNames).pipe(
                        map((result): SendOutcome => {
                            if (result.outcome === 'sent') return { transition: 'sent', data: result.sent };
                            if (result.outcome === 'refused') {
                                const { outcome: _refused, ...refusal } = result;
                                return { transition: 'refused', data: { instructions, ...refusal } };
                            }
                            return {
                                transition: 'sendFailed',
                                data: {
                                    instructions,
                                    error: result.outcome === 'failed' ? result.error : 'The signer declined to sign.',
                                },
                            };
                        })
                    )
            );
            const enter = <TData>(data: TData) => data;
            return {
                ...sentSolanaTransactionTransitions({
                    read$,
                    send$,
                    dismiss$,
                    usable$: solanaCallsUsable$(connections.solana),
                    waitingOn: ['solana'],
                    commitment,
                    errorNames,
                }),
                sent: { $: () => taking('sent'), next: enter },
                refused: { $: () => taking('refused'), next: enter },
                sendFailed: { $: () => taking('sendFailed'), next: enter },
            };
        },
    });
    startedAs(machine);

    return Object.assign(machine, { send: () => send$.next(), dismiss: () => dismiss$.next() });
}
