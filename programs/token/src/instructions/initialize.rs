use anchor_lang::prelude::*;
use anchor_spl::{
    token_interface::Mint as Token,
    token_interface::TokenInterface
};

use crate::{constants::*,state::*};

#[derive(Accounts)]
pub struct Initialize<'info> {
    #[account(mut)]
    pub payer: Signer<'info>,
    #[account(
        init,
        payer = payer,
        mint::authority = payer,
        mint::decimals = TOKEN_DECIMALS,
        seeds = [NORI_SOL_TOKEN_BRIDGE_SEED],
        bump
    )]
    pub token: InterfaceAccount<'info, Token>,
    #[account(
        init,
        payer = payer,
        space = 8 + NoriSolTokenBridge::INIT_SPACE,
        seeds = [NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
        bump
    )]
    pub state: Account<'info, NoriSolTokenBridge>,
    pub system_program: Program<'info, System>,
    pub token_program: Interface<'info, TokenInterface>,
}

pub fn handle_initialize(ctx: Context<Initialize>) -> Result<()> {
    // Initalize state
    ctx.accounts.state.authority = ctx.accounts.payer.key();
    
    // also todo
    //ctx.accounts.token.supply;
    //ctx.accounts.token.mint_authority;
    //ctx.accounts.token.freeze_authority = ctx.accounts.payer.key();

    // Todo
    /*let cpi_accounts = anchor_lang::system_program::Transfer {
        from: ctx.accounts.payer.to_account_info(),
        to: ctx.accounts.counter.to_account_info(),
    };
    let cpi_ctx = CpiContext::new(anchor_lang::system_program::ID, cpi_accounts);
    anchor_lang::system_program::transfer(cpi_ctx, HELLO_WORLD_LAMPORTS)?;

    msg!("Hello, world! Counter initialized");
    */

    Ok(())
}
