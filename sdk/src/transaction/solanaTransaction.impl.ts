import { type ResolveNodeData, type RunningMachine } from '@yaw-rx/ystate';
import {
    address,
    appendTransactionMessageInstructions,
    type Commitment,
    createTransactionMessage,
    getBase58Decoder,
    getBase64EncodedWireTransaction,
    getBase64Encoder,
    getSignatureFromTransaction,
    getSolanaErrorFromTransactionError,
    type Instruction,
    isSignerRole,
    isSolanaError,
    isTransactionSendingSigner,
    pipe,
    setTransactionMessageFeePayerSigner,
    setTransactionMessageLifetimeUsingBlockhash,
    signAndSendTransactionMessageWithSigners,
    signature as signatureOf,
    signTransactionMessageWithSigners,
    SOLANA_ERROR__INSTRUCTION_ERROR__CUSTOM,
    SOLANA_ERROR__JSON_RPC__SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE,
    type TransactionSigner,
} from '@solana/kit';
import {
    catchError,
    defer,
    distinctUntilChanged,
    filter,
    map,
    type Observable,
    of,
    ReplaySubject,
    switchMap,
    throwError,
    withLatestFrom,
} from 'rxjs';
import { TOKEN_PROGRAM_ADDRESS } from '../program/programs/token.js';
import * as tokenErrors from '../program/errors/token.js';
import { type ConnectedReadClients, type ProofRequestConnections, type ReadNeed } from '../proofRequest/connectedRead.js';
import { ConnectionNotReadyError } from '../rpc/connection/connectionNotReady.js';
import { forSolana$, type Solana, solanaSubscriptionsOf } from '../rpc/connection/connections.js';
import { type ReadRetryBackoff, type ReadThroughConnectionsRead } from '../rpc/connection/readThroughConnections.impl.js';
import { requestErrorCode, USER_REJECTED_REQUEST } from '../rpc/eth/eip1193.js';
import { expired, reached, signatureFollow$ } from '../rpc/solana/signatureStatus.js';
import { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';
import { solanaRpcRead$ } from '../rpc/solana/solanaRpcRead.js';
import { messageOf } from '../utils/messageOf.js';
import { withOutcome } from '../utils/machines.js';
import { NotReadyToSendError } from './sentTransaction.impl.js';
import { signatureDue$ } from './signatureStatus.impl.js';
import { type InstructionToSend, type sentSolanaTransactionOf } from './solanaTransaction.js';

/** A sent transaction as it is followed on the chain: what `current` holds. */
export type SentSolanaTransaction = ResolveNodeData<ReturnType<typeof sentSolanaTransactionOf<'sending'>>['nodes'], 'current'>;

/** A program's custom error names by error code, for each program by address. */
export type ProgramErrorNames = Record<string, Record<number, string>>;

/**
 * The custom error names of a program, from its generated client's error
 * constants: each constant named `<prefix><NAME>` gives `NAME` to its code.
 *
 * @param programAddress The program.
 * @param errors The generated errors module, e.g. `import * as errors from '…/errors/token.js'`.
 * @param prefix The constants' prefix, e.g. `TOKEN_ERROR__`.
 * @returns The program's error names.
 */
export const programErrorNamesOf = (
    programAddress: string,
    errors: Record<string, unknown>,
    prefix: string
): ProgramErrorNames => ({
    [programAddress]: Object.fromEntries(
        Object.entries(errors).flatMap(([constant, code]) =>
            constant.startsWith(prefix) && typeof code === 'number' ? [[code, constant.slice(prefix.length)]] : []
        )
    ),
});

/** The Nori program's custom error names (`ZERO_MINT_AMOUNT`, …). */
export const TOKEN_PROGRAM_ERROR_NAMES: ProgramErrorNames = programErrorNamesOf(
    TOKEN_PROGRAM_ADDRESS,
    tokenErrors,
    'TOKEN_ERROR__'
);

/** How a Solana transaction machine names a program's refusal, which commitment it waits for, and retries a failed read. */
export interface SolanaTransactionMachineOptions {
    /** The programs' custom error names (default: the Nori program's). */
    errorNames?: ProgramErrorNames;
    /** The commitment the transaction is followed to (default: `finalized`). */
    commitment?: Commitment;
    /** How long a failed read waits before reading again. */
    backoff?: ReadRetryBackoff;
}

/**
 * The instructions to send, for the signer that signs them: each account
 * at the signer's address with a signer role is signed by it.
 *
 * @param instructions The instructions as the machine's state holds them.
 * @param signer The signer that pays the fee and signs.
 * @returns The instructions.
 */
const instructionsOf = (instructions: InstructionToSend[], signer: TransactionSigner): Instruction[] =>
    instructions.map(({ programAddress, accounts, data }) => ({
        programAddress: address(programAddress),
        accounts: accounts.map(({ address: account, role }) =>
            isSignerRole(role) && account === signer.address
                ? { address: signer.address, role, signer }
                : { address: address(account), role }
        ),
        data: getBase64Encoder().encode(data),
    }));

/** Whether a value is an object whose fields can be read. */
const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === 'object' && value !== null;

/** A program's refusal of one instruction: the program, its custom error code and name when it gave one. */
type ProgramRefusal = { programAddress: string; errorCode: number | undefined; errorName: string };

/**
 * The program that failed a transaction, and its error.
 *
 * @param error The transaction's error, as kit raises it.
 * @param instructions The transaction's instructions.
 * @param errorNames The programs' custom error names.
 * @returns The program, its error code when custom, and the error's name or message.
 */
const refusalOf = (error: unknown, instructions: InstructionToSend[], errorNames: ProgramErrorNames): ProgramRefusal => {
    if (!isSolanaError(error, SOLANA_ERROR__INSTRUCTION_ERROR__CUSTOM))
        return { programAddress: '', errorCode: undefined, errorName: messageOf(error) };
    const { code, index } = error.context;
    const programAddress = instructions[index]?.programAddress ?? '';
    return { programAddress, errorCode: code, errorName: errorNames[programAddress]?.[code] ?? `custom error ${code}` };
};

/**
 * How a Solana transaction machine follows its sent transaction: through
 * Solana's connection by its signature, read again once it reaches
 * `commitment` (`signatureSubscribe`) and every poll interval meanwhile,
 * until it lands or expires (`signatureFollow$`). The signature watched is
 * the one the machine enters `loading` with, so `startedAs` is given the
 * running machine once it is started.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param commitment The commitment it is followed to.
 * @returns The `read`, `refreshOn`, `arrivedElsewhere`, `needs` and `owns` of `startReadThroughConnectionsMachine`, and `startedAs`.
 */
export const sentSolanaTransactionRead = (connections: ProofRequestConnections, commitment: Commitment) => {
    const started$ = new ReplaySubject<{ state$: Observable<{ node: string; data: unknown }> }>(1);
    // The signature the machine sent last, as it enters `loading` with it.
    const sentSignature$ = started$.pipe(
        switchMap(({ state$ }) => state$),
        filter(({ node }) => node === 'loading'),
        map(({ data }) => (isRecord(data) && typeof data.signature === 'string' ? data.signature : '')),
        filter((signature) => signature !== ''),
        distinctUntilChanged(),
        map(signatureOf)
    );
    const signatureReached = createSolanaChangesMachine(connections.solana, { signature: sentSignature$, commitment });
    const { pollIntervalMs } = solanaSubscriptionsOf(connections.solana);
    return {
        read: (clients: ConnectedReadClients, sent: SentSolanaTransaction) =>
            clients
                .solana((rpc) => signatureFollow$(rpc, signatureOf(sent.signature), sent))
                .pipe(map((follow): SentSolanaTransaction => ({ ...sent, ...follow }))),
        refreshOn: () => signatureDue$(signatureReached, pollIntervalMs, true),
        // A transaction that landed or expired moves to the node it ended in, not `current`.
        arrivedElsewhere: (sent: SentSolanaTransaction) =>
            (sent.status !== undefined && reached(sent.status, commitment)) || expired(sent),
        needs: ['solana' as const],
        owns: [signatureReached],
        startedAs: (machine: { state$: Observable<{ node: string; data: unknown }> }) => started$.next(machine),
    };
};

/** A landed transaction: one with its status. */
type Landed = SentSolanaTransaction & { status: NonNullable<SentSolanaTransaction['status']> };

/** What the transitions every Solana transaction machine shares react to. */
export interface SentSolanaTransactionTransitionsOptions {
    /** The shared read's outcomes, from `ownTransitions`. */
    read$: Observable<ReadThroughConnectionsRead<SentSolanaTransaction>>;
    /** The send requests from outside. */
    send$: Observable<unknown>;
    /** The requests to leave `notReadyToSend` without sending. */
    dismiss$: Observable<unknown>;
    /** Whether what sends the transaction (the wallet, or Solana's connection) is usable, as it changes. */
    usable$: Observable<boolean>;
    /** What a send request waits on while it is not usable. */
    waitingOn: ReadNeed[];
    /** The commitment it is followed to. */
    commitment: Commitment;
    /** The programs' custom error names. */
    errorNames: ProgramErrorNames;
}

/**
 * The transitions every Solana transaction machine shares
 * (`sentSolanaTransactionOf`): the gated send request, leaving
 * `notReadyToSend`, and how the followed transaction ends. Spread into the
 * machine's own transitions.
 *
 * @param options The read's outcomes, the requests, the gate, the commitment and the error names.
 * @returns The transitions, by name.
 */
export function sentSolanaTransactionTransitions(options: SentSolanaTransactionTransitionsOptions) {
    const { read$, usable$, waitingOn, commitment, errorNames } = options;
    const read = withOutcome(read$, 'succeeded').pipe(map(({ value }) => value));
    const landed = (sent: SentSolanaTransaction): sent is Landed =>
        sent.status !== undefined && reached(sent.status, commitment);
    const keep = (_event: unknown, _dest: unknown, { instructions }: { instructions: InstructionToSend[] }) => ({
        instructions,
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
            error: (_notReady: unknown, _dest: unknown, { instructions }: { instructions: InstructionToSend[] }) => ({
                instructions,
                waitingOn,
            }),
        },
        readyToSend: { $: () => usable$.pipe(filter(Boolean)), next: keep },
        dismiss: { $: () => options.dismiss$, next: keep },
        transactionConfirmed: {
            $: () => read.pipe(filter((sent): sent is Landed => landed(sent) && sent.status.err === null)),
            next: ({ instructions, signature, status }: Landed) => ({ instructions, signature, status }),
        },
        transactionReverted: {
            $: () => read.pipe(filter((sent): sent is Landed => landed(sent) && sent.status.err !== null)),
            next: ({ instructions, signature, status }: Landed) => {
                const { err } = status;
                const error = typeof err === 'string' || isRecord(err) ? getSolanaErrorFromTransactionError(err) : err;
                return { instructions, signature, status, errorName: refusalOf(error, instructions, errorNames).errorName };
            },
        },
        blockhashExpired: {
            $: () => read.pipe(filter(expired)),
            next: ({ instructions, signature, lastValidBlockHeight, blockHeight }: SentSolanaTransaction) => ({
                instructions,
                signature,
                lastValidBlockHeight: lastValidBlockHeight ?? 0,
                blockHeight: blockHeight ?? 0,
            }),
        },
    };
}

/** How one send ended: sent, with what `loading` follows; or why it was not. */
export type SolanaSendResult =
    | { outcome: 'sent'; sent: SentSolanaTransaction & { failedReads: number } }
    | { outcome: 'declined' }
    | ({ outcome: 'refused'; logs: string[] } & ProgramRefusal)
    | { outcome: 'failed'; error: string };

/**
 * Sends one Solana transaction, once: its instructions with a recent
 * blockhash, signed and fee-paid by `signer`. A signer that sends (a
 * wallet) sends it itself; any other signs it, and it is sent through
 * Solana's connection. A preflight simulation the program fails is
 * `refused`, with the program's error; the user saying no in their wallet
 * is `declined`; a signed transaction whose send got no answer is `sent`,
 * as it may have landed, and following it tells; anything else is
 * `failed`.
 *
 * @param solana The Solana chain.
 * @param subscriber The machine sending it, whose turns its requests take.
 * @param signer The signer that pays the fee and signs.
 * @param instructions The instructions.
 * @param errorNames The programs' custom error names.
 * @returns How the send ended, once.
 */
export function sendSolanaTransaction$(
    solana: Solana,
    subscriber: RunningMachine,
    signer: TransactionSigner,
    instructions: InstructionToSend[],
    errorNames: ProgramErrorNames
): Observable<SolanaSendResult> {
    const sentWith =
        (lastValidBlockHeight: bigint) =>
        (signature: string): SolanaSendResult => ({
            outcome: 'sent',
            sent: {
                instructions,
                signature,
                lastValidBlockHeight: Number(lastValidBlockHeight),
                status: undefined,
                blockHeight: undefined,
                failedReads: 0,
            },
        });
    return forSolana$(solana, subscriber, (rpc) =>
        solanaRpcRead$(
            (abortSignal) => rpc.getLatestBlockhash({ commitment: 'confirmed' }).send({ abortSignal }),
            'Failed to read the latest blockhash.'
        )
    ).pipe(
        switchMap(({ value: blockhash }) => {
            const message = pipe(
                createTransactionMessage({ version: 0 }),
                (m) => setTransactionMessageFeePayerSigner(signer, m),
                (m) => setTransactionMessageLifetimeUsingBlockhash(blockhash, m),
                (m) => appendTransactionMessageInstructions(instructionsOf(instructions, signer), m)
            );
            const sent = sentWith(blockhash.lastValidBlockHeight);
            if (isTransactionSendingSigner(signer))
                return defer(() => signAndSendTransactionMessageWithSigners(message)).pipe(
                    map((signatureBytes) => sent(getBase58Decoder().decode(signatureBytes)))
                );
            return defer(() => signTransactionMessageWithSigners(message)).pipe(
                switchMap((transaction) =>
                    forSolana$(solana, subscriber, (rpc) =>
                        solanaRpcRead$(
                            (abortSignal) =>
                                rpc
                                    .sendTransaction(getBase64EncodedWireTransaction(transaction), {
                                        encoding: 'base64',
                                        preflightCommitment: 'confirmed',
                                    })
                                    .send({ abortSignal }),
                            'Failed to send the transaction.'
                        )
                    ).pipe(
                        map(() => sent(getSignatureFromTransaction(transaction))),
                        // Sent with no answer: it may have landed, so it is followed.
                        catchError((error: unknown) =>
                            error instanceof ConnectionNotReadyError
                                ? of(sent(getSignatureFromTransaction(transaction)))
                                : throwError(() => error)
                        )
                    )
                )
            );
        }),
        catchError((error: unknown) => of(sendOutcomeOf(error, instructions, errorNames)))
    );
}

/**
 * Why a send did not send: the user declined, the program refused it in
 * preflight, or it failed.
 *
 * @param error What the send raised.
 * @param instructions The transaction's instructions.
 * @param errorNames The programs' custom error names.
 * @returns The outcome.
 */
function sendOutcomeOf(error: unknown, instructions: InstructionToSend[], errorNames: ProgramErrorNames): SolanaSendResult {
    if (requestErrorCode(error) === USER_REJECTED_REQUEST) return { outcome: 'declined' };
    if (isSolanaError(error, SOLANA_ERROR__JSON_RPC__SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE))
        return {
            outcome: 'refused',
            ...refusalOf(error.cause, instructions, errorNames),
            logs: [...(error.context.logs ?? [])],
        };
    return { outcome: 'failed', error: messageOf(error) };
}
