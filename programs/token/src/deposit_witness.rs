use std::fmt;
use alloy_primitives::{Address, B256, U256};
use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use nori_sp1_helios_primitives::storage_layout::MAX_COLLECTION_KEYS;

use crate::constants::{MAX_BATCH, MAX_TREE_DEPTH};
use solana_sha256_hasher::hashv;

#[derive(Debug, AnchorSerialize, AnchorDeserialize, Clone)]
pub struct CollectionKeysError {
    count: u8,
    max: usize,
}

impl fmt::Display for CollectionKeysError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "collection keys count {} exceeds MAX_COLLECTION_KEYS {}",
            self.count, self.max
        )
    }
}

impl std::error::Error for CollectionKeysError {}

#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct VerifiedRequest {
    pub target: Address,
    pub collection_keys_count: u8,
    pub collection_keys: [B256; MAX_COLLECTION_KEYS],
    pub value: U256,
}

impl VerifiedRequest {
    pub fn validate(&self) -> Result<(), CollectionKeysError> {
        if self.collection_keys_count as usize > MAX_COLLECTION_KEYS {
            return Err(CollectionKeysError {
                count: self.collection_keys_count,
                max: MAX_COLLECTION_KEYS,
            });
        }
        Ok(())
    }

    pub fn leaf_hash(&self) -> B256 {
        crate::request_leaf_hash::request_leaf_hash(
            &self.target,
            self.collection_keys_count,
            &self.collection_keys[0],
            &self.collection_keys[1],
            &self.value,
        )
    }
}

#[derive(Debug, AnchorSerialize, AnchorDeserialize, Clone)]
pub enum WitnessInputError {
    PathTooLong { len: usize, max: usize },
    IndexOutOfBounds { index: u64, max: usize },
    InvalidValue(CollectionKeysError),
}

impl fmt::Display for WitnessInputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PathTooLong { len, max } => {
                write!(f, "witness path length {len} exceeds MAX_TREE_DEPTH {max}")
            }
            Self::IndexOutOfBounds { index, max } => {
                write!(f, "witness index {index} is outside of MAX_BATCH {max}")
            }
            Self::InvalidValue(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for WitnessInputError {}

#[derive(AnchorSerialize, AnchorDeserialize, Clone)]

pub struct VerifiedRequestWitnessInput {
    pub path: Vec<B256>,
    pub index: u64,
    pub value: VerifiedRequest,
}

impl VerifiedRequestWitnessInput {
    pub fn validate(&self) -> Result<(), WitnessInputError> {
        if self.index as usize > MAX_BATCH - 1usize {
            return Err(WitnessInputError::IndexOutOfBounds {
                index: self.index,
                max: MAX_BATCH,
            });
        }
        if self.path.len() > MAX_TREE_DEPTH {
            return Err(WitnessInputError::PathTooLong {
                len: self.path.len(),
                max: MAX_TREE_DEPTH,
            });
        }
        self.value.validate().map_err(WitnessInputError::InvalidValue)?;
        Ok(())
    }

    pub fn root(&self) -> B256 {
        let mut current_hash = self.value.leaf_hash();
        for (level, sibling) in self.path.iter().enumerate() {
            let bit = (self.index >> level) & 1;
            let (left, right) = if bit == 1 {
                (sibling, &current_hash)
            } else {
                (&current_hash, sibling)
            };
            current_hash = B256::from(hashv(&[left.as_slice(), right.as_slice()]).to_bytes());
        }
        current_hash
    }
}