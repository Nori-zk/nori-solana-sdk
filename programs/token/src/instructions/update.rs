use crate::{constants::*, pda::create_program_owned_pda, state::*};
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
    #[msg("Proof queue batch account is not the PDA for the next proof queue batch index")]
    ProofQueueBatchAccountMismatch,
}

#[event]
pub struct UpdateApplied {
    pub output_slot: u64,
    pub queue_cursor: u64,
    pub verified_state_root: [u8; 32],
    pub proof_queue_batch_count: u64,
}

#[derive(Accounts)]
pub struct Update<'info> {
    #[account(mut)]
    pub payer: Signer<'info>,
    #[account(
        mut,
        seeds = [NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
        bump
    )]
    pub state: AccountLoader<'info, NoriSolTokenBridge>,
    /// CHECK: the PDA for the next proof queue batch index
    /// (`state.proof_queue_batch_count`). Its address can only be checked
    /// against state inside the handler, so seeds are derived and compared
    /// there; it is created and written in handle_update only when the
    /// update's batch drained at least one request, and ignored otherwise.
    #[account(mut)]
    pub proof_queue_batch: UncheckedAccount<'info>,
    pub system_program: Program<'info, System>,
}
use nori_sp1_helios_primitives::types::ProofOutputs;
use sp1_solana::{verify_proof, SP1Groth16Proof};

/// Advances the bridge's verified Ethereum light-client state by one proof batch.
///
/// This is the bridge's permissionless state-transition entrypoint: anyone may
/// call it, and the only credential that matters is a valid SP1 Groth16 proof
/// for the batch. The stored vkey anchors which proving program is trusted;
/// the signer only pays rent for the proof queue batch account.
///
/// Each accepted call moves `latest_head`, the execution state root, the
/// store-hash chain, and the proof-request queue cursor forward together,
/// atomically. Beyond proof validity, the checks enforce that the batch is a
/// strict continuation of the bridge's current state — it must resume from the
/// settled queue cursor, chain from the latest verified head and store hash,
/// and make forward progress — so no caller can skip, replay, or fork history.
///
/// On success, when the batch drained at least one request, its root and
/// cursor range are recorded append-only in a new proof queue batch account
/// at the next index; an [`UpdateApplied`] event is emitted either way.
///
/// # Errors
///
/// Returns an [`UpdateError`] if proof verification or decoding fails, if
/// any value the proof commits to breaks continuity with the bridge's current
/// state, or if a non-empty batch is submitted with an account that is not
/// the next proof queue batch PDA.
pub fn handle_update(ctx: Context<Update>, proof: SP1Groth16Proof) -> Result<()> {
    let mut state = ctx.accounts.state.load_mut()?;

    // Hex encode the vkey_hash
    let vkey_hash = format!("0x{}", hex::encode(state.nori_bridge_vk));

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
    (state.eth_proof_queue_address == proof_outputs.proof_request_queue_address)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "ETH proof queue address mismatch, proof contained address: {}, on chain state is: 0x{}",
                proof_outputs.proof_request_queue_address,
                hex::encode(state.eth_proof_queue_address)
            );
            error!(UpdateError::ETHProofQueueAddressMismatch)
        })?;

    // Cursor continuity: the proof must resume exactly where the last one settled
    (state.queue_cursor == proof_outputs.input_queue_cursor)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Queue cursor mismatch, proof resumes from: {}, on chain state is: {}",
                proof_outputs.input_queue_cursor,
                state.queue_cursor
            );
            error!(UpdateError::QueueCursorMismatch)
        })?;

    // Input slot must pick up exactly where the last verified head left off
    (proof_outputs.input_slot == state.latest_head)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Input slot mismatch, proof input slot: {}, on chain latest head is: {}",
                proof_outputs.input_slot,
                state.latest_head
            );
            error!(UpdateError::InputSlotMismatch)
        })?;

    // Input store hash must chain from the last verified store hash
    (proof_outputs.input_store_hash == state.latest_helios_store_input_hash)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Input store hash mismatch, proof input store hash: {}, on chain state is: 0x{}",
                proof_outputs.input_store_hash,
                hex::encode(state.latest_helios_store_input_hash)
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

    let proof_queue_batch = state.apply_update(&proof_outputs);
    let verified_state_root = state.verified_state_root;
    let queue_cursor = state.queue_cursor;
    let proof_queue_batch_count = state.proof_queue_batch_count;
    drop(state);

    // ================================================================
    // Record the proof queue batch
    // ================================================================

    if let Some((proof_queue_batch_index, proof_request_root_entry)) = proof_queue_batch {
        create_proof_queue_batch(&ctx, proof_queue_batch_index, &proof_request_root_entry)?;
    }

    // ================================================================
    // Emit success event / message
    // ================================================================

    emit!(UpdateApplied {
        output_slot: proof_outputs.output_slot,
        queue_cursor: proof_outputs.output_queue_cursor,
        verified_state_root,
        proof_queue_batch_count,
    });

    msg!(
        "Update applied: slot {}, cursor {}, proof queue batches {}",
        proof_outputs.output_slot,
        queue_cursor,
        proof_queue_batch_count
    );
    Ok(())
}

/// Creates the proof queue batch PDA for `proof_queue_batch_index` and writes
/// `proof_request_root_entry` into it. The next PDA address is predictable
/// from state, so creation goes through [`create_program_owned_pda`], which
/// tolerates the address having been pre-funded.
fn create_proof_queue_batch(
    ctx: &Context<Update>,
    proof_queue_batch_index: u64,
    proof_request_root_entry: &ProofRequestRootEntry,
) -> Result<()> {
    let proof_queue_batch_index_bytes = proof_queue_batch_index.to_le_bytes();
    let (expected_proof_queue_batch, bump) = Pubkey::find_program_address(
        &[
            NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED,
            &proof_queue_batch_index_bytes,
        ],
        &crate::ID,
    );

    (ctx.accounts.proof_queue_batch.key() == expected_proof_queue_batch)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Proof queue batch account mismatch, passed: {}, expected PDA for index {}: {}",
                ctx.accounts.proof_queue_batch.key(),
                proof_queue_batch_index,
                expected_proof_queue_batch
            );
            error!(UpdateError::ProofQueueBatchAccountMismatch)
        })?;

    let seeds: &[&[u8]] = &[
        NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED,
        &proof_queue_batch_index_bytes,
        &[bump],
    ];
    let proof_queue_batch = ctx.accounts.proof_queue_batch.to_account_info();
    create_program_owned_pda(
        ctx.accounts.payer.to_account_info(),
        proof_queue_batch.clone(),
        ctx.accounts.system_program.key(),
        seeds,
        8 + ProofRequestRootEntry::INIT_SPACE,
    )?;

    let mut data = proof_queue_batch.try_borrow_mut_data()?;
    proof_request_root_entry.try_serialize(&mut &mut data[..])?;
    Ok(())
}
