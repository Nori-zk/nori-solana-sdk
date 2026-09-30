use anchor_lang::prelude::*;
use anchor_spl::{token_interface::Mint as Token, token_interface::TokenInterface};

use crate::{constants::*, state::*};

#[derive(Accounts)]
pub struct Initialize<'info> {
    #[account(mut)]
    pub payer: Signer<'info>,
    // init via CPI is fine here: 5 576 bytes is under the 10 KiB CPI
    // creation limit; the loader then initializes the zero-copy state in
    // place (see state.rs).
    #[account(
        init,
        payer = payer,
        space = 8 + std::mem::size_of::<NoriSolTokenBridge>(),
        seeds = [NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
        bump
    )]
    pub state: AccountLoader<'info, NoriSolTokenBridge>,
    #[account(
        init,
        payer = payer,
        // Mint authority is the state PDA, not a user key;
        // no key can mint unbacked tokens directly, and
        // claims are permissionless
        mint::authority = state,
        mint::freeze_authority = state,
        mint::decimals = TOKEN_DECIMALS,
        seeds = [NORI_SOL_TOKEN_BRIDGE_SEED],
        bump
    )]
    pub token: Box<InterfaceAccount<'info, Token>>,
    pub system_program: Program<'info, System>,
    pub token_program: Interface<'info, TokenInterface>,
}

pub fn handle_initialize(
    ctx: Context<Initialize>,
    init_values: NoriSolTokenBridgeInit,
) -> Result<()> {
    let mut state = ctx.accounts.state.load_init()?;
    state.authority = ctx.accounts.payer.key();
    state.latest_head = init_values.latest_head;
    state.verified_state_root = init_values.verified_state_root.into();
    state.nori_bridge_vk = init_values.nori_bridge_vk.into();
    state.latest_helios_store_input_hash = init_values.latest_helios_store_input_hash.into();
    state.eth_proof_queue_address = init_values.eth_proof_queue_address.into();
    state.eth_token_bridge_address = init_values.eth_token_bridge_address.into();
    state.queue_cursor = init_values.queue_cursor;
    state.window_index = 0;

    msg!("NoriSolTokenBridge initialized");

    Ok(())
}
