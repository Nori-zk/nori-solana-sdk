//! `NoriSolTokenBridge::apply_update` proof queue batch bookkeeping: only
//! updates whose batch drained at least one request reserve a proof queue
//! batch index, and indices are contiguous from 0. The example proofs all
//! carry empty batches, so the non-empty path is driven directly here.

use {
    alloy_primitives::{Address, B256},
    bytemuck::Zeroable,
    nori_sp1_helios_primitives::types::ProofOutputs,
    token::state::NoriSolTokenBridge,
};

fn outputs(
    input_queue_cursor: u64,
    output_queue_cursor: u64,
    output_slot: u64,
    output_block_number: u64,
) -> ProofOutputs {
    ProofOutputs {
        input_slot: output_slot - 32,
        input_store_hash: B256::repeat_byte(0x01),
        output_slot,
        output_store_hash: B256::repeat_byte(0x02),
        execution_state_root: B256::repeat_byte(0x03),
        verified_requests_root: B256::repeat_byte(output_queue_cursor as u8),
        next_sync_committee_hash: B256::repeat_byte(0x04),
        proof_request_queue_address: Address::repeat_byte(0x05),
        input_queue_cursor,
        output_queue_cursor,
        output_block_number,
    }
}

#[test]
fn empty_batch_reserves_no_proof_queue_batch() {
    let mut state = NoriSolTokenBridge::zeroed();
    let out = outputs(0, 0, 64, 100);

    assert!(state.apply_update(&out).is_none());
    assert_eq!(state.proof_queue_batch_count, 0);
    assert_eq!(state.latest_head, 64);
    assert_eq!(state.queue_cursor, 0);
}

#[test]
fn non_empty_batch_reserves_next_proof_queue_batch() {
    let mut state = NoriSolTokenBridge::zeroed();
    let out = outputs(0, 5, 64, 100);

    let (proof_queue_batch_index, entry) = state
        .apply_update(&out)
        .expect("non-empty batch reserves an index");
    assert_eq!(proof_queue_batch_index, 0);
    assert_eq!(state.proof_queue_batch_count, 1);
    assert_eq!(state.queue_cursor, 5);
    assert_eq!(entry.root, <[u8; 32]>::from(out.verified_requests_root));
    assert_eq!(entry.output_block_number, 100);
    assert_eq!(entry.input_queue_cursor, 0);
    assert_eq!(entry.output_queue_cursor, 5);
}

#[test]
fn proof_queue_batch_indices_are_contiguous_across_empty_batches() {
    let mut state = NoriSolTokenBridge::zeroed();

    let (first, _) = state.apply_update(&outputs(0, 5, 64, 100)).unwrap();
    assert!(state.apply_update(&outputs(5, 5, 96, 101)).is_none());
    assert!(state.apply_update(&outputs(5, 5, 128, 102)).is_none());
    let (second, entry) = state.apply_update(&outputs(5, 9, 160, 103)).unwrap();

    assert_eq!(first, 0);
    assert_eq!(second, 1);
    assert_eq!(state.proof_queue_batch_count, 2);
    assert_eq!(entry.input_queue_cursor, 5);
    assert_eq!(entry.output_queue_cursor, 9);
    assert_eq!(state.queue_cursor, 9);
    assert_eq!(state.latest_head, 160);
}
