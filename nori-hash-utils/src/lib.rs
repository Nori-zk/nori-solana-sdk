//! Proof queue request leaf and Merkle witness hashing for TypeScript.
//!
//! Every hash here is computed by `nori_hash::merkle_sha256_fixed`, the same
//! code the SP1 guest runs to build each proof queue batch's
//! `verified_requests_root`, so a witness built from these functions verifies
//! against the batch root the Solana program stores.

use std::str::FromStr;

use alloy_primitives::{Address, B256, U256};
use nori_hash::merkle_sha256_fixed::{
    build_merkle_tree, compute_merkle_root_from_path, compute_merkle_tree_depth_and_size,
    get_merkle_path_from_tree, get_merkle_zeros, hash_request_leaf, MAX_BATCH,
};
use serde::{Deserialize, Serialize};
#[cfg(feature = "wasm")]
use tsify::Tsify;

#[cfg(feature = "wasm")]
pub mod wasm;

/// Collection keys hashed per leaf (`MAX_COLLECTION_KEYS` in nori-sp1-helios-primitives).
const MAX_COLLECTION_KEYS: usize = 2;

/// One proof request queue entry, as the bridge hashes it into a Merkle leaf.
///
/// Every value is 0x-prefixed hex:
/// - `target`: the 20-byte address whose storage the request proves.
/// - `collectionKeysCount`: how many collection keys the request supplied.
/// - `collectionKeys`: up to two 32-byte keys; absent keys hash as zero.
/// - `value`: the 32-byte big-endian storage word read at the batch's output block.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "wasm", derive(Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
#[serde(rename_all = "camelCase")]
pub struct RequestLeaf {
    pub target: String,
    pub collection_keys_count: u8,
    pub collection_keys: Vec<String>,
    pub value: String,
}

/// Every request in one proof queue batch, in queue order.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "wasm", derive(Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct RequestBatch {
    pub leaves: Vec<RequestLeaf>,
}

/// Every request in one proof queue batch, in queue order, and the index of
/// the request to build a witness for (its request id minus the batch's
/// `inputQueueCursor`).
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "wasm", derive(Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct RequestWitnessInput {
    pub leaves: Vec<RequestLeaf>,
    pub index: u32,
}

/// The Merkle witness for one request in a proof queue batch, as 0x-prefixed hex.
///
/// - `root`: the batch root; it must equal the root committed on Solana.
/// - `index`: the request's leaf index in the batch.
/// - `leaf`: the request's leaf hash.
/// - `path`: the sibling hashes from the leaf up to the root, bottom-up.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "wasm", derive(Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct RequestWitness {
    pub root: String,
    pub index: u32,
    pub leaf: String,
    pub path: Vec<String>,
}

/// A leaf hash, its leaf index and its bottom-up sibling path, as 0x-prefixed hex.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "wasm", derive(Tsify))]
#[cfg_attr(feature = "wasm", tsify(into_wasm_abi, from_wasm_abi))]
pub struct MerkleRootFromPathInput {
    pub leaf: String,
    pub index: u32,
    pub path: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestLeafHashError {
    InvalidHex { field: &'static str, value: String },
    TooManyCollectionKeys { count: usize },
}

impl std::fmt::Display for RequestLeafHashError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidHex { field, value } => {
                write!(f, "request leaf {field} '{value}' is not valid hex of the expected length")
            }
            Self::TooManyCollectionKeys { count } => write!(
                f,
                "request leaf has {count} collection keys, at most {MAX_COLLECTION_KEYS} are hashed"
            ),
        }
    }
}

impl std::error::Error for RequestLeafHashError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestBatchRootError {
    EmptyBatch,
    BatchTooLarge { len: usize },
    Leaf { index: usize, error: RequestLeafHashError },
}

impl std::fmt::Display for RequestBatchRootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyBatch => write!(f, "a proof queue batch has at least one request"),
            Self::BatchTooLarge { len } => {
                write!(f, "batch of {len} requests exceeds MAX_BATCH {MAX_BATCH}")
            }
            Self::Leaf { index, error } => write!(f, "request {index}: {error}"),
        }
    }
}

impl std::error::Error for RequestBatchRootError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestWitnessError {
    Batch(RequestBatchRootError),
    IndexOutOfRange { index: u32, len: usize },
}

impl std::fmt::Display for RequestWitnessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batch(error) => write!(f, "{error}"),
            Self::IndexOutOfRange { index, len } => {
                write!(f, "index {index} is outside a batch of {len} requests")
            }
        }
    }
}

impl std::error::Error for RequestWitnessError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MerkleRootFromPathError {
    InvalidHex { field: &'static str, value: String },
}

impl std::fmt::Display for MerkleRootFromPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidHex { field, value } => {
                write!(f, "{field} '{value}' is not a 32-byte hex value")
            }
        }
    }
}

impl std::error::Error for MerkleRootFromPathError {}

fn to_hex(value: &B256) -> String {
    value.to_string()
}

/// Hashes one request into its Merkle leaf with `hash_request_leaf`.
pub fn request_leaf_hash(leaf: &RequestLeaf) -> Result<B256, RequestLeafHashError> {
    let target = Address::from_str(&leaf.target).map_err(|_| RequestLeafHashError::InvalidHex {
        field: "target",
        value: leaf.target.clone(),
    })?;
    if leaf.collection_keys.len() > MAX_COLLECTION_KEYS {
        return Err(RequestLeafHashError::TooManyCollectionKeys {
            count: leaf.collection_keys.len(),
        });
    }
    let mut keys = [B256::ZERO; MAX_COLLECTION_KEYS];
    for (slot, key) in keys.iter_mut().zip(&leaf.collection_keys) {
        *slot = B256::from_str(key).map_err(|_| RequestLeafHashError::InvalidHex {
            field: "collection key",
            value: key.clone(),
        })?;
    }
    let value = B256::from_str(&leaf.value).map_err(|_| RequestLeafHashError::InvalidHex {
        field: "value",
        value: leaf.value.clone(),
    })?;
    Ok(hash_request_leaf(
        &target,
        leaf.collection_keys_count,
        &keys[0],
        &keys[1],
        &U256::from_be_bytes(value.0),
    ))
}

fn batch_tree(leaves: &[RequestLeaf]) -> Result<Vec<Vec<B256>>, RequestBatchRootError> {
    if leaves.is_empty() {
        return Err(RequestBatchRootError::EmptyBatch);
    }
    if leaves.len() > MAX_BATCH {
        return Err(RequestBatchRootError::BatchTooLarge { len: leaves.len() });
    }
    let leaf_hashes = leaves
        .iter()
        .enumerate()
        .map(|(index, leaf)| {
            request_leaf_hash(leaf).map_err(|error| RequestBatchRootError::Leaf { index, error })
        })
        .collect::<Result<Vec<B256>, _>>()?;
    let (depth, padded_size) = compute_merkle_tree_depth_and_size(leaf_hashes.len());
    Ok(build_merkle_tree(
        leaf_hashes,
        padded_size,
        depth,
        &get_merkle_zeros(),
    ))
}

/// Computes a proof queue batch's root with `build_merkle_tree`.
pub fn request_batch_root(leaves: &[RequestLeaf]) -> Result<B256, RequestBatchRootError> {
    Ok(batch_tree(leaves)?[0][0])
}

/// Builds the Merkle witness for the request at `index` in a proof queue
/// batch with `build_merkle_tree` and `get_merkle_path_from_tree`.
pub fn request_witness(
    leaves: &[RequestLeaf],
    index: u32,
) -> Result<RequestWitness, RequestWitnessError> {
    let tree = batch_tree(leaves).map_err(RequestWitnessError::Batch)?;
    if index as usize >= leaves.len() {
        return Err(RequestWitnessError::IndexOutOfRange {
            index,
            len: leaves.len(),
        });
    }
    let depth = tree.len() - 1;
    Ok(RequestWitness {
        root: to_hex(&tree[0][0]),
        index,
        leaf: to_hex(&tree[depth][index as usize]),
        path: get_merkle_path_from_tree(&tree, index)
            .iter()
            .map(to_hex)
            .collect(),
    })
}

/// Recomputes a root from a leaf hash, its index and its bottom-up path with
/// `compute_merkle_root_from_path`.
pub fn merkle_root_from_path(
    input: &MerkleRootFromPathInput,
) -> Result<B256, MerkleRootFromPathError> {
    let parse = |field: &'static str, value: &String| {
        B256::from_str(value).map_err(|_| MerkleRootFromPathError::InvalidHex {
            field,
            value: value.clone(),
        })
    };
    let leaf = parse("leaf", &input.leaf)?;
    let path = input
        .path
        .iter()
        .map(|sibling| parse("path entry", sibling))
        .collect::<Result<Vec<B256>, _>>()?;
    Ok(compute_merkle_root_from_path(leaf, input.index as u64, &path))
}
