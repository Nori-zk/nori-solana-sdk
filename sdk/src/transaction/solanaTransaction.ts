import { getBase64Decoder, type Instruction } from '@solana/kit';
import { type ReadNeed } from '../proofRequest/connectedRead.js';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { type SolanaSignatureStatus } from '../rpc/solana/signatureStatus.js';

/**
 * One instruction of a Solana transaction to send, as the machine's state
 * shows it: the program it calls, its accounts with their roles
 * (`AccountRole`), and its data in base64.
 */
export type InstructionToSend = {
    programAddress: string;
    accounts: { address: string; role: number }[];
    data: string;
};

/**
 * The instructions a Solana transaction machine is made for, as its state
 * shows them.
 *
 * @param instructions The instructions, e.g. from the program's generated builders.
 * @returns The instructions to send.
 */
export const instructionsToSendOf = (instructions: readonly Instruction[]): InstructionToSend[] =>
    instructions.map(({ programAddress, accounts = [], data = new Uint8Array() }) => ({
        programAddress,
        accounts: accounts.map(({ address, role }) => ({ address, role })),
        data: getBase64Decoder().decode(data),
    }));

/** The defaults the graph's nodes are typed by. */
const noInstructions: InstructionToSend[] = [];
const noStatus: SolanaSignatureStatus = { slot: 0, confirmationStatus: null, err: null };

/**
 * One Solana transaction, made for its instructions and sent only on a send
 * request, then followed on the chain until it ends. Every Solana
 * transaction machine spreads these nodes and edges and adds its own
 * `sending` node (the node that signs and sends it, named by the machine)
 * and that node's outcomes. Its signer pays the fee.
 *
 * - `ready` holds the instructions until a send request. The request is
 *   gated on what sends it (the wallet, or Solana's connection): usable, it
 *   moves to `sending`; not usable, to `notReadyToSend`, naming what it
 *   waits on in `waitingOn`. That node goes back to `ready` once it is
 *   usable, or on `dismiss`; it never sends by itself.
 * - `refused`: the cluster's preflight simulation failed it: the program
 *   that refused it (`programAddress`), its custom error code and name when
 *   it gave one (`errorCode`, `errorName`), and the simulation's `logs`.
 *   `sendFailed`: anything else kept it from being sent (`error`). Both
 *   accept a send request again, through the same gate.
 * - From `loading` on, it is followed through Solana's connection
 *   (`readThroughConnectionsOf`) by its `signature` until it reaches the
 *   commitment asked for, and every poll interval meanwhile: `current`
 *   holds it sent and not there yet (`status`, `undefined` while the
 *   cluster does not know it; `blockHeight`, the finalized block height the
 *   last read saw).
 * - It ends `confirmed` once it reaches the commitment and succeeded;
 *   `reverted` once it reaches it and failed, with the program error's
 *   name when it gave one (`errorName`); or `expired` once the finalized
 *   block height passed its `lastValidBlockHeight` while the cluster never
 *   knew it, so it never will land. `expired` accepts a send request again:
 *   a new blockhash makes it a new transaction, and the expired one cannot
 *   land, so only one of the two can.
 * - `confirmed`, `reverted` and `closed` are terminal.
 *
 * @param sending The machine's node that signs and sends the transaction.
 * @returns The nodes and edges.
 */
export const sentSolanaTransactionOf = <TSending extends string>(sending: TSending) => {
    const followed = readThroughConnectionsOf({
        instructions: noInstructions,
        signature: '',
        lastValidBlockHeight: undefined as number | undefined,
        status: undefined as SolanaSignatureStatus | undefined,
        blockHeight: undefined as number | undefined,
    });
    const landed = { instructions: noInstructions, signature: '', status: noStatus };
    return {
        nodes: {
            ready: { instructions: noInstructions },
            notReadyToSend: { instructions: noInstructions, waitingOn: [] as ReadNeed[] },
            refused: {
                instructions: noInstructions,
                programAddress: '',
                errorCode: undefined as number | undefined,
                errorName: '',
                logs: [] as string[],
            },
            sendFailed: { instructions: noInstructions, error: '' },
            ...followed.nodes,
            confirmed: { ...landed },
            reverted: { ...landed, errorName: '' },
            expired: { instructions: noInstructions, signature: '', lastValidBlockHeight: 0, blockHeight: 0 },
        },
        edges: {
            sendRequested: { from: 'ready', to: sending, on: 'send.next' },
            notReadyWhenRequested: { from: 'ready', to: 'notReadyToSend', on: 'send.error' },
            sendAgainAfterRefusal: { from: 'refused', to: sending, on: 'send.next' },
            notReadyAfterRefusal: { from: 'refused', to: 'notReadyToSend', on: 'send.error' },
            sendAgainAfterFailure: { from: 'sendFailed', to: sending, on: 'send.next' },
            notReadyAfterFailure: { from: 'sendFailed', to: 'notReadyToSend', on: 'send.error' },
            sendAgainAfterExpiry: { from: 'expired', to: sending, on: 'send.next' },
            notReadyAfterExpiry: { from: 'expired', to: 'notReadyToSend', on: 'send.error' },
            readyToSend: { from: 'notReadyToSend', to: 'ready', on: 'readyToSend.next' },
            dismissed: { from: 'notReadyToSend', to: 'ready', on: 'dismiss.next' },

            ...followed.edges,
            confirmedOnFirstRead: { from: 'loading', to: 'confirmed', on: 'transactionConfirmed.next' },
            confirmedOnRefresh: { from: 'refreshing', to: 'confirmed', on: 'transactionConfirmed.next' },
            revertedOnFirstRead: { from: 'loading', to: 'reverted', on: 'transactionReverted.next' },
            revertedOnRefresh: { from: 'refreshing', to: 'reverted', on: 'transactionReverted.next' },
            expiredOnFirstRead: { from: 'loading', to: 'expired', on: 'blockhashExpired.next' },
            expiredOnRefresh: { from: 'refreshing', to: 'expired', on: 'blockhashExpired.next' },

            closedWhileReady: { from: 'ready', to: 'closed', on: 'close.next' },
            closedWhileNotReadyToSend: { from: 'notReadyToSend', to: 'closed', on: 'close.next' },
            closedAfterRefusal: { from: 'refused', to: 'closed', on: 'close.next' },
            closedAfterSendFailed: { from: 'sendFailed', to: 'closed', on: 'close.next' },
            closedAfterExpiry: { from: 'expired', to: 'closed', on: 'close.next' },
        } as const,
    };
};
