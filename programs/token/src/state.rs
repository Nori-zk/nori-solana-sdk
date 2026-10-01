use crate::constants::MAX_PROOF_USAGE_WINDOW;
use alloy_primitives::{Address, B256};
use anchor_lang::prelude::*;
use bytemuck::{Pod, Zeroable};
use nori_sp1_helios_primitives::types::ProofOutputs;

// Zero-copy layout: the state account is ~5.6 KB, far past the 4 KB SBF
// stack frame, so the struct is never instantiated on stack — handlers
// access it by reference directly in account memory. All fields are
// fixed-size and the explicit padding keeps the struct padding-free
// (a bytemuck::Pod requirement).
#[derive(Copy, Clone, Default, Pod, Zeroable)]
#[repr(C)]
pub struct ProofRequestRootEntry {
    pub root: [u8; 32],           // 32 bytes
    pub output_block_number: u64, // 8 bytes
    pub input_queue_cursor: u64,  // 8 bytes
    pub output_queue_cursor: u64, // 8 bytes
}

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
    pub window_index: u8,
    pub _padding: [u8; 7],
    pub window_buffer: [ProofRequestRootEntry; MAX_PROOF_USAGE_WINDOW],
}

/// Instruction arguments for `initialize` (~120 bytes — fine on stack).
/// Written into the zeroed state account field by field; building the whole
/// state struct on stack first would blow the 4 KB frame.
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
        self.window_index = 0u8;
    }

    pub fn apply_update(&mut self, outputs: &ProofOutputs) {
        // Update commitments
        self.latest_head = outputs.output_slot;
        self.latest_helios_store_input_hash = outputs.output_store_hash.into();
        self.verified_state_root = outputs.execution_state_root.into();
        self.queue_cursor = outputs.output_queue_cursor;

        // Add the proof request entry to the contract ring buffer
        let proof_request_root_entry = ProofRequestRootEntry {
            root: outputs.verified_requests_root.into(),
            input_queue_cursor: outputs.input_queue_cursor,
            output_queue_cursor: outputs.output_queue_cursor,
            output_block_number: outputs.output_block_number,
        };
        self.window_buffer[self.window_index as usize] = proof_request_root_entry;

        // Advance for the next update, wrapping at the end
        self.window_index = ((self.window_index as usize + 1) % MAX_PROOF_USAGE_WINDOW) as u8;
    }
}

#[account]
#[derive(InitSpace)]
pub struct NoriSolTokenAccountStorage {
    pub minted_so_far: u64,
}
