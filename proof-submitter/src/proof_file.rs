//! Loading of SP1 `SP1ProofWithPublicValues` JSON dumps into the on-chain
//! wire format.
//!
//! The bridge head (nori-bridge-head) serializes proofs with serde_json.
//! Only the fields the Solana program needs are decoded here:
//!
//! ```text
//! proof.Groth16.encoded_proof     352 B  [exit_code 32][vk_root 32][nonce 32][groth16 256]
//! proof.Groth16.groth16_vkey_hash  32 B  sha256(groth16_vk.bin); first 4 B prefix the wire proof
//! proof.Groth16.public_inputs[0]   dec   SP1 program vkey hash (pinned on-chain at initialize)
//! public_values.buffer.data       220 B  ProofOutputs layout (see nori-sp1-helios-primitives)
//! ```
//!
//! The on-chain verifier expects `SP1ProofWithPublicValues::bytes()`
//! (356 B = 4 B vkey-hash prefix ++ the 352 B encoded proof).

use alloy_primitives::{hex, U256};
use serde::Deserialize;
use sp1_solana::{SP1Groth16Proof, SP1_GROTH16_PROOF_LEN, VK_HASH_PREFIX_LEN};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum ProofFileError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("encoded_proof is not valid hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("encoded_proof is {got} bytes, expected {want}")]
    EncodedProofLength { got: usize, want: usize },
    #[error("public_values.buffer.data is empty")]
    EmptyPublicValues,
    #[error("public_inputs must have 5 entries, got {0}")]
    PublicInputsLength(usize),
    #[error("public_inputs[0] (program vkey) does not fit in 32 bytes: {0}")]
    ProgramVkey(String),
}

#[derive(Debug, Deserialize)]
struct ProofFile {
    proof: ProofSection,
    public_values: PublicValuesSection,
}

#[derive(Debug, Deserialize)]
struct ProofSection {
    #[serde(rename = "Groth16")]
    groth16: Groth16Section,
}

#[derive(Debug, Deserialize)]
struct Groth16Section {
    encoded_proof: String,
    groth16_vkey_hash: [u8; 32],
    public_inputs: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PublicValuesSection {
    buffer: BufferSection,
}

#[derive(Debug, Deserialize)]
struct BufferSection {
    data: Vec<u8>,
}

/// A proof JSON loaded from disk: the instruction argument plus the SP1
/// program vkey hash the proof was produced for.
pub struct LoadedProof {
    /// Instruction argument for the program's `update` entrypoint.
    pub wire: SP1Groth16Proof,
    /// `public_inputs[0]` — the SP1 program vkey hash (`vk.bytes32()`), to be
    /// pinned as `nori_bridge_vk` at `initialize`.
    pub program_vkey: [u8; 32],
}

fn parse_file(path: &Path) -> Result<LoadedProof, ProofFileError> {
    let text = std::fs::read_to_string(path)?;
    let file: ProofFile = serde_json::from_str(&text)?;

    let encoded = hex::decode(&file.proof.groth16.encoded_proof)?;
    if encoded.len() != SP1_GROTH16_PROOF_LEN - VK_HASH_PREFIX_LEN {
        return Err(ProofFileError::EncodedProofLength {
            got: encoded.len(),
            want: SP1_GROTH16_PROOF_LEN - VK_HASH_PREFIX_LEN,
        });
    }
    if file.public_values.buffer.data.is_empty() {
        return Err(ProofFileError::EmptyPublicValues);
    }
    if file.proof.groth16.public_inputs.len() != 5 {
        return Err(ProofFileError::PublicInputsLength(
            file.proof.groth16.public_inputs.len(),
        ));
    }
    let vkey_dec = &file.proof.groth16.public_inputs[0];
    let vkey_u256 = U256::from_str_radix(vkey_dec, 10)
        .map_err(|_| ProofFileError::ProgramVkey(vkey_dec.clone()))?;
    let program_vkey: [u8; 32] = vkey_u256.to_be_bytes::<32>();
    if U256::from_be_bytes::<32>(program_vkey).to_string() != *vkey_dec {
        return Err(ProofFileError::ProgramVkey(vkey_dec.clone()));
    }

    let mut proof = Vec::with_capacity(SP1_GROTH16_PROOF_LEN);
    proof.extend_from_slice(&file.proof.groth16.groth16_vkey_hash[..VK_HASH_PREFIX_LEN]);
    proof.extend_from_slice(&encoded);

    Ok(LoadedProof {
        wire: SP1Groth16Proof {
            proof,
            sp1_public_inputs: file.public_values.buffer.data,
        },
        program_vkey,
    })
}

/// Load one proof JSON file.
pub fn load_update_proof(path: impl AsRef<Path>) -> Result<LoadedProof, ProofFileError> {
    parse_file(path.as_ref())
}

/// Load every `*.json` proof in a directory, ordered by file name (the
/// `<slot>-v<x.y.z>.json` naming of the example proofs sorts into
/// chain-continuation order).
pub fn load_update_proofs_dir(dir: impl AsRef<Path>) -> Result<Vec<LoadedProof>, ProofFileError> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    paths.sort();
    paths.iter().map(|path| parse_file(path)).collect()
}
