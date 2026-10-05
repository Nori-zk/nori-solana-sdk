use anchor_lang::prelude::*;

#[constant]
pub const NORI_SOL_TOKEN_BRIDGE_SEED: &[u8] = b"NETH";

#[constant]
pub const NORI_SOL_TOKEN_BRIDGE_STATE_SEED: &[u8] = b"STATE";

#[constant]
pub const NORI_SOL_TOKEN_ACCOUNT_STORAGE_SEED: &[u8] = b"STORAGE";

#[constant]
pub const NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED: &[u8] = b"PROOF_QUEUE_BATCH";

// Matches the Ethereum bridge's DECIMALS: 1 bridge unit (10^-6 ETH, the
// proven `lockedTokens` word) mints exactly 1 SPL base unit, so NETH is
// 1:1 with locked ETH and the u64 mint cast is capped by MAX_MAGNITUDE there.
#[constant]
pub const TOKEN_DECIMALS: u8 = 6;

// https://github.com/Nori-zk/nori-bridge-head/blob/FEAT/solana-bridge/nori-hash/src/merkle_sha256_fixed.rs
// Not #[constant]: usize has no IDL type.
pub const MAX_TREE_DEPTH: usize = 16;

// https://github.com/Nori-zk/nori-bridge-head/blob/FEAT/solana-bridge/nori-hash/src/merkle_sha256_fixed.rs
// Not #[constant]: usize has no IDL type.
pub const MAX_BATCH: usize = 1 << MAX_TREE_DEPTH;
