import { type Address, type GetAccountInfoApi, type Rpc } from '@solana/kit';
import {
    fetchNoriSolTokenBridge,
    NORI_SOL_TOKEN_BRIDGE_DISCRIMINATOR,
    type NoriSolTokenBridge,
} from '../../../program/accounts/noriSolTokenBridge.js';
import { findStatePda } from '../../../program/pdas/state.js';
import { TOKEN_PROGRAM_ADDRESS } from '../../../program/programs/token.js';
import { assertProgramAccount } from './accountChecks.js';

/**
 * Reads the bridge state account at `finalized` commitment.
 *
 * @param rpc The Solana RPC used for the read.
 * @param programAddress The Nori Solana bridge program address.
 * @returns The decoded bridge state, including its queue cursor and proof queue batch count.
 */
export async function fetchBridgeState(
    rpc: Rpc<GetAccountInfoApi>,
    programAddress: Address = TOKEN_PROGRAM_ADDRESS
): Promise<NoriSolTokenBridge> {
    const [address] = await findStatePda({ programAddress });
    const account = await fetchNoriSolTokenBridge(rpc, address, { commitment: 'finalized' });
    assertProgramAccount(account, programAddress, NORI_SOL_TOKEN_BRIDGE_DISCRIMINATOR);
    return account.data;
}
