import { type Address, type Commitment, type Signature } from '@solana/kit';
import { distinctUntilChanged, isObservable, map, type Observable, of, switchMap, take } from 'rxjs';
import { heldAcrossMoves } from '../../utils/machines.js';
import { startChainChangesMachine } from '../connection/chainChanges.impl.js';
import { forSolana$, type Solana, solanaCallsUsable$, solanaSubscriptionsOf } from '../connection/connections.js';
import { type SubscriptionEvent } from '../connection/jsonRpcTopic.js';
import { finalizedSlot$, newestTransactionSlot$ } from './slots.js';

/**
 * What counts as a change on Solana, besides every new finalized root:
 *
 * - `mentions`: a finalized transaction that mentions an address, such as
 *   one that calls a program (`logsSubscribe`);
 * - `account`: an account's finalized state changing (`accountSubscribe`);
 *   the address may arrive later, as a program address does;
 * - `signature`: a transaction reaching a commitment (`signatureSubscribe`);
 *   the signature may arrive later, as a sent transaction's does.
 */
export type SolanaWatch =
    | { mentions: Address }
    | { account: Address | Observable<Address> }
    | { signature: Signature | Observable<Signature>; commitment: Commitment };

/**
 * The subscription that pushes the watched changes on Solana's websocket:
 * its acknowledgement, then each push.
 *
 * @param subscribe$ A subscription on the websocket, on the machine's turns.
 * @param watch What counts as a change; every new root when omitted.
 * @returns The subscription.
 */
function pushes$(
    subscribe$: <T>(subscribeMethod: string, params: unknown[], unsubscribeMethod: string) => Observable<SubscriptionEvent<T>>,
    watch: SolanaWatch | undefined
): Observable<SubscriptionEvent<unknown>> {
    if (watch === undefined)
        // Every node serves `slotSubscribe` (not every one `rootSubscribe`); each slot is pushed with
        // the root, and a push is a change only when the root moved.
        return subscribe$<{ root: number }>('slotSubscribe', [], 'slotUnsubscribe').pipe(
            distinctUntilChanged(
                (previous, push) =>
                    previous.kind === 'notification' &&
                    push.kind === 'notification' &&
                    previous.result.root === push.result.root
            )
        );
    if ('mentions' in watch)
        return subscribe$('logsSubscribe', [{ mentions: [watch.mentions] }, { commitment: 'finalized' }], 'logsUnsubscribe');
    if ('signature' in watch) {
        const { commitment } = watch;
        const signature$ = isObservable(watch.signature) ? watch.signature : of(watch.signature);
        return signature$.pipe(
            switchMap((signature) =>
                subscribe$('signatureSubscribe', [signature, { commitment }], 'signatureUnsubscribe')
            )
        );
    }
    const address$ = isObservable(watch.account) ? watch.account : of(watch.account);
    return address$.pipe(
        switchMap((address) =>
            subscribe$('accountSubscribe', [address, { encoding: 'base64', commitment: 'finalized' }], 'accountUnsubscribe')
        )
    );
}

/**
 * The address whose transactions a poll reads for a watch: the mentioned
 * address, or the account; none for every root or a signature, which a new
 * slot stands for.
 *
 * @param watch What counts as a change.
 * @returns The address, once; or `undefined`.
 */
function polledAddress$(watch: SolanaWatch | undefined): Observable<Address> | undefined {
    if (watch === undefined || 'signature' in watch) return undefined;
    if ('mentions' in watch) return of(watch.mentions);
    return isObservable(watch.account) ? watch.account : of(watch.account);
}

/**
 * Starts following whether Solana changes, as the chain changes graph
 * (`ChainChangesGraph`) describes: subscribed on Solana's websocket while it
 * serves, else polling through its RPC. Without a watch, every new
 * finalized root is a change; with one, what it names (`SolanaWatch`). A
 * poll compares the latest finalized slot with the last one polled, and for
 * an address, the slot of its newest finalized transaction: a transaction
 * that only reads an account counts while polling.
 *
 * @param solana The Solana chain.
 * @param watch What counts as a change; every new root when omitted.
 * @returns The running machine; `changes` counts the changes. Its control:
 *   - `close()`: moves the machine to `closed`.
 */
export function createSolanaChangesMachine(solana: Solana, watch?: SolanaWatch) {
    const { subscribable$, subscribe$, pollIntervalMs } = solanaSubscriptionsOf(solana);
    const address$ = polledAddress$(watch);
    return startChainChangesMachine({
        subscribable$: subscribable$.pipe(map((open) => (open ? ['websocket' as const] : []))),
        // One subscription, kept while a push loops back to `subscribed`.
        pushesOn: (started$) => ({
            websocket: started$
                .pipe(
                    take(1),
                    switchMap((machine) =>
                        pushes$(
                            <T>(subscribeMethod: string, params: unknown[], unsubscribeMethod: string) =>
                                subscribe$<T>(subscribeMethod, params, unsubscribeMethod, machine),
                            watch
                        )
                    )
                )
                .pipe(heldAcrossMoves()),
        }),
        poll$: (lastSlot, machine) =>
            forSolana$(solana, machine, (rpc) => finalizedSlot$(rpc)).pipe(
                switchMap((latest) => {
                    if (lastSlot === undefined) return of({ lastPolled: latest, changed: false });
                    if (latest <= lastSlot) return of(undefined);
                    if (address$ === undefined) return of({ lastPolled: latest, changed: true });
                    return address$.pipe(
                        switchMap((address) =>
                            forSolana$(solana, machine, (rpc) => newestTransactionSlot$(rpc, address))
                        ),
                        map((slot) => ({ lastPolled: latest, changed: slot !== undefined && slot > lastSlot }))
                    );
                })
            ),
        callsUsable$: solanaCallsUsable$(solana),
        pollIntervalMs,
    });
}
