import { type ResolveNodeData } from '@yaw-rx/ystate';
import { BrowserProvider } from 'ethers';
import { catchError, defer, filter, map, NEVER, type Observable, of, Subject, switchMap, take, throwError } from 'rxjs';
import { type ProofRequestConnections } from '../../proofRequest/connectedRead.js';
import { type RequestOutcome, requestOutcomes, stateOnEntry$ } from '../../utils/machines.js';
import { messageOf } from '../../utils/messageOf.js';
import { walletProviderOf$ } from '../connection/connections.js';
import { type ReadRetryBackoff, startReadThroughConnectionsMachine, dueOn } from '../connection/readThroughConnections.impl.js';
import { eip1193Event$, requestErrorCode, USER_REJECTED_REQUEST } from './eip1193.js';
import { NoWalletConfiguredError } from './errors.js';
import { ethRpcRead$ } from './ethRpcRead.js';
import { WalletAccountGraph, type WalletAccountState } from './walletAccount.js';

/** How one ask to share an account ended: the edge out of `askingToShareAccount` it takes, with that node's data. */
type ShareOutcome = RequestOutcome<typeof WalletAccountGraph, 'shareAccepted' | 'userDeclinedShare' | 'shareRequestFailed'>;

/**
 * Starts reading the account the user's Ethereum wallet shares, through the
 * wallet alone, and reading it again on the wallet's `accountsChanged`.
 *
 * @param connections The Ethereum and Solana chains, with their running connectivity machines.
 * @param backoff How long a failed read waits before reading again.
 * @returns The running machine; `current` carries `account`. Its controls:
 *   - `shareAccount()`: asks the user to share an account, while none is shared or after a decline.
 *   - `retry()`: reads again now, without waiting, while failed.
 *   - `close()`: moves the machine to `closed`.
 */
export function createWalletAccountMachine(connections: ProofRequestConnections, backoff: ReadRetryBackoff = {}) {
    const walletNeed = 'ethereum.wallet' as const;
    const shareAccount$ = new Subject<void>();
    const walletProvider$ = walletProviderOf$(connections.ethereum);
    const accountsChanged$ = walletProvider$.pipe(switchMap((provider) => eip1193Event$(provider, 'accountsChanged')));
    const walletLeftReady$ = (connections.ethereum.wallet.connection?.state$ ?? NEVER).pipe(
        filter(({ node }) => node !== 'ready')
    );

    const machine = startReadThroughConnectionsMachine(WalletAccountGraph, {
        connections,
        // The wallet transport's provider is an ethers `BrowserProvider` over the wallet.
        read: (clients, { lastShareError }) =>
            clients
                .transport((provider) =>
                    provider instanceof BrowserProvider
                        ? ethRpcRead$<string[]>(() => provider.send('eth_accounts', []), "Failed to read the wallet's accounts.")
                        : throwError(() => new NoWalletConfiguredError())
                )
                .pipe(map(([account]) => ({ account, lastShareError }))),
        refreshOn: () => dueOn(accountsChanged$),
        needs: [walletNeed],
        backoff,
        ownTransitions: (_read$, state$) => {
            const entered$ = stateOnEntry$(state$);
            // The outcome edges of `askingToShareAccount` share one request per entry into it.
            const taking = requestOutcomes(
                state$,
                'askingToShareAccount',
                (data: ResolveNodeData<typeof WalletAccountGraph.nodes, 'askingToShareAccount'>): Observable<ShareOutcome> =>
                    walletProvider$.pipe(
                        take(1),
                        switchMap((provider) => defer(() => provider.request({ method: 'eth_requestAccounts' }))),
                        map(
                            (accounts): ShareOutcome => ({
                                transition: 'shareAccepted',
                                data: { account: (accounts as string[])[0], lastShareError: data.lastShareError },
                            })
                        ),
                        catchError((error: unknown) =>
                            of<ShareOutcome>(
                                requestErrorCode(error) === USER_REJECTED_REQUEST
                                    ? { transition: 'shareDeclined', data }
                                    : { transition: 'shareRequestFailed', data: { ...data, lastShareError: messageOf(error) } }
                            )
                        )
                    )
            );
            /** The data of `askingToShareAccount` or `shareDeclined`, on entry into either. */
            const ownNodeData$ = entered$.pipe(
                filter(
                    (state): state is Extract<WalletAccountState, { node: 'askingToShareAccount' | 'shareDeclined' }> =>
                        state.node === 'askingToShareAccount' || state.node === 'shareDeclined'
                ),
                map(({ data }) => data)
            );
            const enter = <TData>(data: TData) => data;
            return {
                // Offered from `current` while no account is shared, and after a decline.
                shareAccount: {
                    $: () =>
                        entered$.pipe(
                            switchMap(({ node, data }) =>
                                node === 'shareDeclined' || (node === 'current' && data.account === undefined)
                                    ? shareAccount$.pipe(map(() => data))
                                    : NEVER
                            )
                        ),
                    next: enter,
                },
                shareAccepted: { $: () => taking('shareAccepted'), next: enter },
                shareDeclined: { $: () => taking('shareDeclined'), next: enter },
                shareRequestFailed: { $: () => taking('shareRequestFailed'), next: enter },
                accountsChanged: {
                    $: () =>
                        ownNodeData$.pipe(
                            switchMap((data) => accountsChanged$.pipe(map(() => ({ ...data, failedReads: 0 }))))
                        ),
                    next: enter,
                },
                walletLeftReady: {
                    $: () =>
                        ownNodeData$.pipe(
                            switchMap((data) =>
                                walletLeftReady$.pipe(map(() => ({ ...data, failedReads: 0, waitingOn: [walletNeed] })))
                            )
                        ),
                    next: enter,
                },
            };
        },
    });

    return Object.assign(machine, { shareAccount: () => shareAccount$.next() });
}

/** A running wallet account machine, with `shareAccount()`, `retry()` and `close()`. */
export type WalletAccount = ReturnType<typeof createWalletAccountMachine>;
