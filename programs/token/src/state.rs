use alloy_primitives::{Address, B256};
use anchor_lang::prelude::*;

#[account]
#[derive(InitSpace)]
pub struct NoriSolTokenBridge {
    pub authority: Pubkey,
    pub verified_state_root: [u8; 32],
    pub latest_head: u64,
    pub nori_bridge_vk: [u8; 32],
    pub latest_helios_store_input_hash: [u8; 32],
    pub eth_token_bridge_address: [u8; 20]
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct NoriSolTokenBridgeInit {
    pub verified_state_root: B256,
    pub nori_bridge_vk: B256,
    pub latest_helios_store_input_hash: B256,
    pub eth_token_bridge_address: Address
}

impl From<(NoriSolTokenBridgeInit, Pubkey)> for NoriSolTokenBridge {
    fn from((init, authority): (NoriSolTokenBridgeInit, Pubkey)) -> Self {
        Self {
            authority,
            latest_head: 0,
            verified_state_root: init.verified_state_root.into(),
            nori_bridge_vk: init.nori_bridge_vk.into(),
            latest_helios_store_input_hash: init.latest_helios_store_input_hash.into(),
            eth_token_bridge_address: init.eth_token_bridge_address.into(),
        }
    }
}

