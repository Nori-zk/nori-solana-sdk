use anchor_lang::prelude::*;
use anchor_spl::token_interface::{
    TokenInterface,
    TokenAccount,
    Mint as Token,
};
use anchor_spl::associated_token::AssociatedToken;


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
    #[account(init, payer = payer, associated_token::mint = token, associated_token::authority = recipient)]
    pub token_account: InterfaceAccount<'info, TokenAccount>,
    
    pub system_program: Program<'info, System>,
    pub token_program: Interface<'info, TokenInterface>,
    pub associated_token_program: Program<'info, AssociatedToken>,
}

pub fn handle_hint() -> Result<()> {
    Ok(())
}
