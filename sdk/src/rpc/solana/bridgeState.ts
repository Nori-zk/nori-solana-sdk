import { type Address } from '@solana/kit';
import { map, type Observable, switchMap } from 'rxjs';
import {
    fetchNoriSolTokenBridge,
    NORI_SOL_TOKEN_BRIDGE_DISCRIMINATOR,
    type NoriSolTokenBridge,
} from '../../program/accounts/noriSolTokenBridge.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../program/programs/token.js';
import { assertProgramAccount } from './accountChecks.js';
import { stateAddress$ } from './programAddresses.js';
import { type SolanaRpc } from './solanaHttp.js';
import { solanaRpcRead$ } from './solanaRpcRead.js';

/**
 * The bridge program's state account, at `finalized` commitment.
 *
 * @param rpc The Solana RPC used for the read.
 * @param programAddress The Nori Solana bridge program address.
 * @returns The decoded bridge state, including its queue cursor and proof queue batch count, once.
 */
export const bridgeState$ = (
    rpc: SolanaRpc,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Observable<NoriSolTokenBridge> =>
    stateAddress$(programAddress).pipe(
        switchMap((address) =>
            solanaRpcRead$(
                (abortSignal) => fetchNoriSolTokenBridge(rpc, address, { commitment: 'finalized', abortSignal }),
                'Failed to read the bridge state.'
            )
        ),
        map((account) => {
            assertProgramAccount(account, programAddress, NORI_SOL_TOKEN_BRIDGE_DISCRIMINATOR);
            return account.data;
        })
    );
