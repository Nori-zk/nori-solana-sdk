pub mod constants;
pub mod error;
pub mod instructions;
pub mod state;

use anchor_lang::prelude::*;

pub use constants::*;
pub use instructions::*;
pub use state::*;

declare_id!("2J24QuEjM9HgPgmgV2SURCUyfsTrkGiwGsAFeH5tnuL8");

#[program]
pub mod token {
    use super::*;

    pub fn initialize(ctx: Context<Initialize>, init_values: NoriSolTokenBridgeInit) -> Result<()> {
        crate::instructions::initialize::handle_initialize(ctx, init_values)
    }
}
