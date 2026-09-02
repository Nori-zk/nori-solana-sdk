use anchor_lang::prelude::*;

use crate::{constants::*, error::ErrorCode, state::Counter};

#[derive(Accounts)]
pub struct Mint<'info> {
    #[account(mut, seeds = [NORI_SOL_TOKEN_BRIDGE_SEED], bump)]
    pub counter: Account<'info, Counter>,
    pub authority: Signer<'info>,
}

pub fn handle_hint() -> Result<()> {
    
}