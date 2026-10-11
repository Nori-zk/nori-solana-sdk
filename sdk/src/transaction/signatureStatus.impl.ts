import { type ResolveNodeData } from '@yaw-rx/ystate';
import { type Commitment, type Signature, signature as signatureOf } from '@solana/kit';
import { filter, interval, map, merge, NEVER, type Observable, switchMap, take } from 'rxjs';
import { type ProofRequestConnections } from '../proofRequest/connectedRead.js';
import { type ChainChanges, changesOf$ } from '../rpc/connection/chainChanges.impl.js';
import { solanaSubscriptionsOf } from '../rpc/connection/connections.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine, dueOn } from '../rpc/connection/readThroughConnections.impl.js';
import { expired, reached, signatureFollow$ } from '../rpc/solana/signatureStatus.js';
import { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';
import { withOutcome } from '../utils/machines.js';
import { SignatureStatusGraph } from './signatureStatus.js';

/** The transaction as the last read found it: what `current` holds. */
type FollowedSignature = ResolveNodeData<typeof SignatureStatusGraph.nodes, 'current'>;

/**
 * When a transaction followed by its signature is due to be read again:
 * once it reaches the commitment its changes machine watches for; while it
 * may still expire, also every poll interval once that is followed.
 *
 * @param signatureReached The Solana changes machine watching the signature.
 * @param pollIntervalMs The interval between reads while it may expire.
 * @param mayExpire Whether it has a `lastValidBlockHeight` to pass.
 * @returns The trigger, as `refreshOn` takes it.
 */
export const signatureDue$ = (
    signatureReached: ChainChanges,
    pollIntervalMs: number,
    mayExpire: boolean
): Observable<unknown> =>
    mayExpire
        ? merge(
              changesOf$(signatureReached),
              changesOf$(signatureReached).pipe(
                  take(1),
                  switchMap(() => interval(pollIntervalMs))
              )
          )
        : changesOf$(signatureReached);

/**
 * Starts reading a Solana transaction's status through the Solana
 * connection, and reading it again once it reaches `commitment`; with a
 * `lastValidBlockHeight`, also every poll interval until it lands or
 * expires.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param signature The transaction's signature.
 * @param commitment The commitment it is followed to (default: `finalized`).
 * @param lastValidBlockHeight The block height after which the transaction's blockhash no longer lands, if known.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries the status, and `expired` ends it. Its controls:
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export const createSignatureStatusMachine = (
    connections: ProofRequestConnections,
    signature: Signature,
    commitment: Commitment = 'finalized',
    lastValidBlockHeight?: number,
    backoff: ReadRetryBackoff = {}
) => {
    const { pollIntervalMs } = solanaSubscriptionsOf(connections.solana);
    const signatureReached = createSolanaChangesMachine(connections.solana, { signature, commitment });

    return startReadThroughConnectionsMachine(SignatureStatusGraph, {
        connections,
        read: (clients, followed) =>
            clients.solana((rpc) =>
                signatureFollow$(rpc, signatureOf(followed.signature), followed).pipe(
                    map((follow) => ({ ...followed, ...follow }))
                )
            ),
        refreshOn: (followed) =>
            followed.status !== undefined && reached(followed.status, commitment)
                ? dueOn(NEVER)
                : signatureDue$(signatureReached, pollIntervalMs, followed.lastValidBlockHeight !== undefined),
        // A read that finds the transaction expired moves there, not to `current`.
        arrivedElsewhere: expired,
        needs: ['solana'],
        backoff,
        owns: [signatureReached],
        ownTransitions: (read$) => ({
            blockhashExpired: {
                $: () =>
                    withOutcome(read$, 'succeeded').pipe(
                        map(({ value }) => value),
                        filter(expired)
                    ),
                next: (value: FollowedSignature) => ({
                    signature: value.signature,
                    lastValidBlockHeight: value.lastValidBlockHeight ?? 0,
                    blockHeight: value.blockHeight ?? 0,
                }),
            },
        }),
        // The transaction it is made for, and when its blockhash stops landing, are its initial state.
        start: {
            node: 'loading',
            data: { signature, lastValidBlockHeight, status: undefined, blockHeight: undefined, failedReads: 0 },
        },
    });
};
