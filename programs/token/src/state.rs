use alloy_primitives::{Address, B256};
use anchor_lang::prelude::*;
use nori_sp1_helios_primitives::types::ProofOutputs;

/// One committed proof queue batch, stored append-only in its own PDA at
/// `[NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED, index.to_le_bytes()]`.
/// Indices are contiguous from 0 and only updates whose batch drained at
/// least one request create an entry, so clients can search entries by index
/// for the batch whose cursor range covers a request id.
#[account]
#[derive(InitSpace)]
pub struct ProofRequestRootEntry {
    pub root: [u8; 32],           // 32 bytes
    pub output_block_number: u64, // 8 bytes
    pub input_queue_cursor: u64,  // 8 bytes
    pub output_queue_cursor: u64, // 8 bytes
}

// Zero-copy layout: handlers access the state by reference directly in
// account memory. All fields are fixed-size and ordered so the struct is
// padding-free (a bytemuck::Pod requirement).
#[account(zero_copy)]
#[repr(C)]
pub struct NoriSolTokenBridge {
    pub authority: Pubkey,
    pub verified_state_root: [u8; 32],
    pub latest_head: u64,
    pub nori_bridge_vk: [u8; 32],
    pub latest_helios_store_input_hash: [u8; 32],
    pub eth_proof_queue_address: [u8; 20],
    pub eth_token_bridge_address: [u8; 20],
    pub queue_cursor: u64,
    pub proof_queue_batch_count: u64,
}

/// Instruction arguments for `initialize` (~120 bytes — fine on stack).
/// Written into the zeroed state account field by field.
#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct NoriSolTokenBridgeInit {
    pub verified_state_root: B256,
    pub latest_helios_store_input_hash: B256,
    pub eth_proof_queue_address: Address,
    pub eth_token_bridge_address: Address,
    /// Beacon slot of the state the bridge starts from. The first accepted
    /// `update` must have `input_slot == latest_head`, so this pins where the
    /// proven chain resumes.
    pub latest_head: u64,
    pub queue_cursor: u64,
}

impl NoriSolTokenBridge {
    pub fn apply_init(&mut self, (init, authority): (NoriSolTokenBridgeInit, Pubkey)) {
        self.authority = authority;
        self.latest_head = init.latest_head;
        self.verified_state_root = init.verified_state_root.into();
        self.nori_bridge_vk = nori_elf::NORI_SP1_HELIOS_PROGRAM_VK;
        self.latest_helios_store_input_hash = init.latest_helios_store_input_hash.into();
        self.eth_proof_queue_address = init.eth_proof_queue_address.into();
        self.eth_token_bridge_address = init.eth_token_bridge_address.into();
        self.queue_cursor = init.queue_cursor;
        self.proof_queue_batch_count = 0;
    }

    /// Applies the update's commitments. When the update's batch drained at
    /// least one request, reserves the next proof queue batch index and
    /// returns it with the entry the caller must store at that index's PDA;
    /// an update with an empty batch only advances the head and returns `None`.
    pub fn apply_update(&mut self, outputs: &ProofOutputs) -> Option<(u64, ProofRequestRootEntry)> {
        // Update commitments
        self.latest_head = outputs.output_slot;
        self.latest_helios_store_input_hash = outputs.output_store_hash.into();
        self.verified_state_root = outputs.execution_state_root.into();
        self.queue_cursor = outputs.output_queue_cursor;

        // Reserve the next proof queue batch index for a non-empty batch
        (outputs.output_queue_cursor != outputs.input_queue_cursor).then(|| {
            let proof_queue_batch_index = self.proof_queue_batch_count;
            self.proof_queue_batch_count += 1;
            let proof_request_root_entry = ProofRequestRootEntry {
                root: outputs.verified_requests_root.into(),
                input_queue_cursor: outputs.input_queue_cursor,
                output_queue_cursor: outputs.output_queue_cursor,
                output_block_number: outputs.output_block_number,
            };
            (proof_queue_batch_index, proof_request_root_entry)
        })
    }
}

#[account]
#[derive(InitSpace)]
pub struct NoriSolTokenAccountStorage {
    pub minted_so_far: u64,
}
