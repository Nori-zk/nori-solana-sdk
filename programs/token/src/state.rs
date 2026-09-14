use alloy_primitives::{Address, B256};
use anchor_lang::prelude::*;
use nori_sp1_helios_primitives::types::ProofOutputs;

#[account]
#[derive(InitSpace)]
pub struct NoriSolTokenBridge {
    pub authority: Pubkey,
    pub verified_state_root: [u8; 32],
    pub latest_head: u64,
    pub nori_bridge_vk: [u8; 32],
    pub latest_helios_store_input_hash: [u8; 32],
    pub eth_proof_queue_address: [u8; 20],
    pub queue_cursor: u64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct NoriSolTokenBridgeInit {
    pub verified_state_root: B256,
    pub nori_bridge_vk: B256,
    pub latest_helios_store_input_hash: B256,
    pub eth_proof_queue_address: Address,
    pub queue_cursor: u64,
}

impl From<(NoriSolTokenBridgeInit, Pubkey)> for NoriSolTokenBridge {
    fn from((init, authority): (NoriSolTokenBridgeInit, Pubkey)) -> Self {
        Self {
            authority,
            latest_head: 0,
            verified_state_root: init.verified_state_root.into(),
            nori_bridge_vk: init.nori_bridge_vk.into(),
            latest_helios_store_input_hash: init.latest_helios_store_input_hash.into(),
            eth_proof_queue_address: init.eth_proof_queue_address.into(),
            queue_cursor: init.queue_cursor,
        }
    }
}

impl NoriSolTokenBridge {
    pub fn apply_update(&mut self, outputs: &ProofOutputs) {
        self.latest_head = outputs.output_slot;
        self.latest_helios_store_input_hash = outputs.output_store_hash.into();
        self.verified_state_root = outputs.execution_state_root.into();
        self.queue_cursor = outputs.output_queue_cursor;
    }
}

