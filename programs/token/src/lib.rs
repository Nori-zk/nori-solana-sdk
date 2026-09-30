pub mod constants;
pub mod deposit_witness;
pub mod error;
pub mod instructions;
pub mod request_leaf_hash;
pub mod state;

use anchor_lang::prelude::*;

pub use constants::*;
pub use instructions::*;
pub use state::*;

declare_id!("5xS32i7XNRk7JCjYR7RgfHVxdLepVSnnJ1gdHkmfD8W1");

#[program]
pub mod token {
    use super::*;

    pub fn initialize(ctx: Context<Initialize>, init_values: NoriSolTokenBridgeInit) -> Result<()> {
        crate::instructions::initialize::handle_initialize(ctx, init_values)
    }

    /// Permissionless state transition: advance the verified Ethereum
    /// light-client state by one SP1 Groth16 proof batch.
    pub fn update(ctx: Context<Update>, proof: sp1_solana::SP1Groth16Proof) -> Result<()> {
        crate::instructions::update::handle_update(ctx, proof)
    }

    /// Mint bridged tokens against a proven deposit (Merkle witness). The
    /// deposit's first collection key commits to sha256(recipient_pubkey);
    /// the recipient claims by signing the transaction.
    pub fn mint(
        ctx: Context<Mint>,
        deposit_witness: deposit_witness::VerifiedRequestWitnessInput,
    ) -> Result<()> {
        crate::instructions::mint::handle_mint(ctx, deposit_witness)
    }
}
