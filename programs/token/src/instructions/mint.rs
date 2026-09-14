use anchor_lang::prelude::*;
use anchor_spl::associated_token::{self, AssociatedToken};
use anchor_spl::token_interface::{Mint as Token, TokenAccount, TokenInterface};

use crate::{constants::*, error::ErrorCode, state::NoriSolTokenBridge};

#[derive(Accounts)]
pub struct Mint<'info> {
    #[account(mut)]
    pub payer: Signer<'info>,
    #[account(mut)]
    pub authority: Signer<'info>,
    #[account(mut)]
    pub recipient: Signer<'info>,

    #[account(mut, seeds = [NORI_SOL_TOKEN_BRIDGE_STATE_SEED], bump)]
    pub state: Account<'info, NoriSolTokenBridge>,
    #[account(mut, seeds = [NORI_SOL_TOKEN_BRIDGE_SEED], bump)]
    pub token: InterfaceAccount<'info, Token>,
    #[account(mut)]
    pub token_account: UncheckedAccount<'info>,

    pub system_program: Program<'info, System>,
    pub token_program: Interface<'info, TokenInterface>,
    pub associated_token_program: Program<'info, AssociatedToken>,
}

pub fn handle_mint(ctx: Context<Mint>) -> Result<()> {
    // Create an account for this user if it does not exist
    associated_token::create_idempotent(CpiContext::new(
        ctx.accounts.associated_token_program.key(),
        associated_token::Create {
            payer: ctx.accounts.payer.to_account_info(),
            associated_token: ctx.accounts.token_account.to_account_info(),
            authority: ctx.accounts.recipient.to_account_info(),
            mint: ctx.accounts.token.to_account_info(),
            system_program: ctx.accounts.system_program.to_account_info(),
            token_program: ctx.accounts.token_program.to_account_info(),
        },
    ))?;

    Ok(())
}
