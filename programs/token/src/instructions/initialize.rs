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
    ctx.accounts
        .state
        .load_init()?
        .apply_init((init_values, ctx.accounts.payer.key()));

    msg!("NoriSolTokenBridge initialized");

    Ok(())
}
