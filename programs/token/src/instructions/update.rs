use crate::{constants::*, state::*};
use alloy_primitives::hex;
use anchor_lang::prelude::*;

#[error_code]
pub enum UpdateError {
    #[msg("SP1 Groth16 proof verification failed")]
    ProofVerificationFailed,
    #[msg("Failed to decode proof bytes")]
    DecodingProofFailed,
    #[msg("ETH proof queue address mismatch")]
    ETHProofQueueAddressMismatch,
    #[msg("Queue cursor mismatch")]
    QueueCursorMismatch,
    #[msg("Input slot does not match latest verified head")]
    InputSlotMismatch,
    #[msg("Input store hash does not match latest verified store hash")]
    InputStoreHashMismatch,
    #[msg("Output slot is not greater than input slot")]
    InvalidOutputSlot,
    #[msg("Next sync committee hash is zero")]
    ZeroSyncCommitteeHash,
}

#[derive(Accounts)]
pub struct Update<'info> {
    #[account(mut)]
    pub payer: Signer<'info>,
    #[account(
        init,
        payer = payer,
        space = 8 + NoriSolTokenBridge::INIT_SPACE,
        seeds = [NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
        bump
    )]
    pub state: Account<'info, NoriSolTokenBridge>,
    pub system_program: Program<'info, System>,
}
use nori_sp1_helios_primitives::types::ProofOutputs;
use sp1_solana::{verify_proof, SP1Groth16Proof};

pub fn handle_update(ctx: Context<Update>, proof: SP1Groth16Proof) -> Result<()> {
    // Hex encode the vkey_hash
    ctx.accounts.state.nori_bridge_vk;
    let vkey_hash = format!("0x{}", hex::encode(ctx.accounts.state.nori_bridge_vk));

    // Verify the proof
    verify_proof(&proof.proof, proof.sp1_public_inputs.as_slice(), &vkey_hash).map_err(|e| {
        msg!("Proof verification failed: {}", e);
        error!(UpdateError::ProofVerificationFailed)
    })?;

    // Decode the verified proof
    let proof_outputs = ProofOutputs::from_bytes(&proof.sp1_public_inputs).map_err(|e| {
        msg!("Failed to decode proof: {}", e);
        error!(UpdateError::DecodingProofFailed)
    })?;

    // ================================================================
    // Bridge state transition validation
    // ================================================================

    // Verify the proof anchors its storage witnesses on the expected queue
    (ctx.accounts.state.eth_proof_queue_address == proof_outputs.proof_request_queue_address)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "ETH proof queue address mismatch, proof contained address: {}, on chain state is: 0x{}",
                proof_outputs.proof_request_queue_address,
                hex::encode(ctx.accounts.state.eth_proof_queue_address)
            );
            error!(UpdateError::ETHProofQueueAddressMismatch)
        })?;

    // Cursor continuity: the proof must resume exactly where the last one settled
    (ctx.accounts.state.queue_cursor == proof_outputs.input_queue_cursor)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Queue cursor mismatch, proof resumes from: {}, on chain state is: {}",
                proof_outputs.input_queue_cursor,
                ctx.accounts.state.queue_cursor
            );
            error!(UpdateError::QueueCursorMismatch)
        })?;

    // Input slot must pick up exactly where the last verified head left off
    (proof_outputs.input_slot == ctx.accounts.state.latest_head)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Input slot mismatch, proof input slot: {}, on chain latest head is: {}",
                proof_outputs.input_slot,
                ctx.accounts.state.latest_head
            );
            error!(UpdateError::InputSlotMismatch)
        })?;

    // Input store hash must chain from the last verified store hash
    (proof_outputs.input_store_hash == ctx.accounts.state.latest_helios_store_input_hash)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Input store hash mismatch, proof input store hash: {}, on chain state is: 0x{}",
                proof_outputs.input_store_hash,
                hex::encode(ctx.accounts.state.latest_helios_store_input_hash)
            );
            error!(UpdateError::InputStoreHashMismatch)
        })?;

    // Proof must make forward progress
    (proof_outputs.output_slot > proof_outputs.input_slot)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Output slot not greater than input slot, input slot: {}, output slot: {}",
                proof_outputs.input_slot,
                proof_outputs.output_slot
            );
            error!(UpdateError::InvalidOutputSlot)
        })?;

    // Next sync committee hash must be populated
    (proof_outputs.next_sync_committee_hash != alloy_primitives::B256::ZERO)
        .then_some(())
        .ok_or_else(|| {
            msg!("Next sync committee hash is zero");
            error!(UpdateError::ZeroSyncCommitteeHash)
        })?;

    // ================================================================
    // Commit the update to bridge state
    // ================================================================

    ctx.accounts.state.apply_update(&proof_outputs);

    // TODO need to consider what to do for windowing

    Ok(())
}
