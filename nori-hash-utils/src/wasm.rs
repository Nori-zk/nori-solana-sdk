//! WebAssembly bindings for nori-hash-utils.
//!
//! This module provides wasm-bindgen exported functions for use from JavaScript.

use wasm_bindgen::{prelude::*, JsError};

use crate::{
    MerkleRootFromPathInput, RequestBatch, RequestLeaf, RequestWitness, RequestWitnessInput,
};

/// Hashes one proof request queue entry into its Merkle leaf, exactly as the
/// SP1 guest does (`hash_request_leaf`).
///
/// Returns the leaf hash as 0x-prefixed hex.
#[wasm_bindgen]
pub fn request_leaf_hash(leaf: RequestLeaf) -> Result<String, JsError> {
    crate::request_leaf_hash(&leaf)
        .map(|hash| hash.to_string())
        .map_err(|e| JsError::new(&e.to_string()))
}

/// Computes a proof queue batch's root from every request in the batch, in
/// queue order, exactly as the SP1 guest computes `verified_requests_root`.
///
/// Returns the root as 0x-prefixed hex.
#[wasm_bindgen]
pub fn request_batch_root(batch: RequestBatch) -> Result<String, JsError> {
    crate::request_batch_root(&batch.leaves)
        .map(|root| root.to_string())
        .map_err(|e| JsError::new(&e.to_string()))
}

/// Builds the Merkle witness for one request in a proof queue batch from
/// every request in the batch, in queue order. The witness's `root` must
/// equal the batch root committed on Solana.
#[wasm_bindgen]
pub fn request_witness(input: RequestWitnessInput) -> Result<RequestWitness, JsError> {
    crate::request_witness(&input.leaves, input.index).map_err(|e| JsError::new(&e.to_string()))
}

/// Recomputes a Merkle root from a leaf hash, its leaf index and its
/// bottom-up sibling path (`compute_merkle_root_from_path`).
///
/// Returns the root as 0x-prefixed hex.
#[wasm_bindgen]
pub fn merkle_root_from_path(input: MerkleRootFromPathInput) -> Result<String, JsError> {
    crate::merkle_root_from_path(&input)
        .map(|root| root.to_string())
        .map_err(|e| JsError::new(&e.to_string()))
}
