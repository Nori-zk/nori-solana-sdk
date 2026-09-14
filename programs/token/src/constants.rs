use anchor_lang::prelude::*;

#[constant]
pub const NORI_SOL_TOKEN_BRIDGE_SEED: &[u8] = b"NETH";

#[constant]
pub const NORI_SOL_TOKEN_BRIDGE_STATE_SEED: &[u8] = b"STATE";

#[constant]
pub const TOKEN_DECIMALS: u8 = 12;

#[constant]
pub const TOKEN_MAX_MAGNITUDE: u64 = ((1u128 << 64) - 1) as u64;

#[constant]
pub const TOKEN_WEI_PER_BRIDGE_UNIT: u64 = 10u64.pow(18 - (TOKEN_DECIMALS as u32));

#[constant]
pub const MAX_PROOF_USAGE_WINDOW: usize = 96;

#[constant]
pub const PROOF_REQUEST_ROOT_ENTRY_SIZE: usize = 56;

#[constant]
pub const WINDOW_BUFFER_SIZE: usize = MAX_PROOF_USAGE_WINDOW * PROOF_REQUEST_ROOT_ENTRY_SIZE;