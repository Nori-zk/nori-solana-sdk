//! SCRAM — Signature Commit-Reveal Authentication Mechanism
//!
//! Short:
//!   SCRAM is a scheme where a signature is hashed into a commitment. The
//!   commitment is stored on-chain within the Eth deposit smart contract as
//!   the key in the deposit map. Later, the claimant supplies the original
//!   signature and message as a witness, and the program verifies both that
//!   the signature is valid for the claimed (publicKey, message) pair AND
//!   that its hash equals the stored commitment.
//!
//! Definitions:
//!   - signature: an Ed25519 signature produced by signing a message with
//!     the signer's private key.
//!   - publicKey: the Ed25519 public key corresponding to the signer's
//!     private key.
//!   - message: the bytes that were signed.
//!   - commitment: `[u8; 32] = sha256(signature)` — the commitment stored
//!     on the Eth smart contract as the deposit key.
//!
//! Off-chain (committer):
//!   1. Sign a message with the signer's Ed25519 private key.
//!   2. Compute commitment = sha256(signature).
//!   3. Submit the commitment to the Eth smart contract's lock function,
//!      where it becomes the deposit key.
//!
//! On-chain Solana (claim):
//!   1. Claimant supplies the original signature and message as a witness.
//!      The publicKey is derived from the required signer of the claim
//!      instruction — not supplied as user input.
//!   2. Solana's native ed25519 program verifies the signature against
//!      publicKey and message, proving key ownership and message
//!      integrity.
//!   3. This program reads back that verification and computes
//!      sha256(signature), asserting equality with the stored commitment.
//!
//! Security:
//!   - Commitment is binding: only the holder of the original signature can
//!     produce a preimage that hashes to the stored commitment.
//!   - Identity is verified: the signature must be valid for the claimed
//!     publicKey and message, preventing substitution.
//!   - SHA-256 is collision-resistant, so forging a different signature
//!     with the same hash is computationally infeasible.
//!   - The signature is revealed once claimed; nothing in this scheme
//!     keeps it private, so the deposit-to-claim link becomes visible.
//!   - Note: SCRAM does not enforce any constraint on what the message is.
//!     Any domain separation or message binding is the caller's
//!     responsibility.

use anchor_lang::prelude::*;
use solana_ed25519_program::{
    DATA_START, PUBKEY_SERIALIZED_SIZE, SIGNATURE_OFFSETS_START, SIGNATURE_SERIALIZED_SIZE,
};
use solana_instructions_sysvar::load_instruction_at_checked;
use solana_sha256_hasher::hash;

#[error_code]
pub enum SCRAMError {
    #[msg("SCRAM commitment does not match signature hash")]
    CommitmentMismatch,
    #[msg("Ed25519 verification instruction missing or malformed")]
    MissingSignatureVerification,
    #[msg("Ed25519 verification instruction does not match claimed signature/publicKey/message")]
    SignatureVerificationMismatch,
}

/// The user-supplied witness for SCRAM verification.
///
/// Contains only the inputs that are not computed or derived on-chain:
///   - signature: the signature that was committed to.
///   - message: the message that was signed.
///
/// The commitment is read from on-chain state and the publicKey is derived
/// from the required signer of the claim instruction — neither is user
/// input.
#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct SCRAMWitness {
    pub signature: [u8; SIGNATURE_SERIALIZED_SIZE],
    pub message: Vec<u8>,
}

/// Computes the SCRAM commitment from a signature.
///
/// @param signature - signature to commit to.
/// @returns bytes representing the commitment.
///
/// Conceptually:
///   The signature is hashed via SHA-256 to produce a commitment. This
///   commitment is submitted to the Eth smart contract's lock function as
///   the deposit key. The original signature is required to open the
///   commitment later on the Solana side.
pub fn create_commitment(signature: &[u8; SIGNATURE_SERIALIZED_SIZE]) -> [u8; 32] {
    hash(signature).to_bytes()
}

/// Verifies a SCRAM commitment against a witnessed signature.
///
/// @param commitment - Stored commitment to verify against.
/// @param signature - The signature supplied as a witness.
/// @param publicKey - Public key of the original signer.
/// @param message - The message that was signed.
///
/// Conceptually:
///   Two independent checks are performed:
///   1. Signature validity — the signature must verify against the
///      supplied publicKey and message, proving key ownership and message
///      integrity.
///   2. Commitment match — the hash of the signature must equal the stored
///      commitment, proving this is the exact signature that was committed
///      to.
pub fn verify_commitment(
    commitment: [u8; 32],
    signature: &[u8; SIGNATURE_SERIALIZED_SIZE],
    public_key: &Pubkey,
    message: &[u8],
    instructions_sysvar: &AccountInfo,
    ed25519_instruction_index: u16,
) -> Result<()> {
    let ix = load_instruction_at_checked(ed25519_instruction_index as usize, instructions_sysvar)
        .map_err(|_| error!(SCRAMError::MissingSignatureVerification))?;

    (ix.program_id == solana_sdk_ids::ed25519_program::ID
        && ed25519_instruction_matches(&ix.data, signature, public_key, message))
    .then_some(())
    .ok_or_else(|| error!(SCRAMError::SignatureVerificationMismatch))?;

    (create_commitment(signature) == commitment)
        .then_some(())
        .ok_or_else(|| error!(SCRAMError::CommitmentMismatch))?;

    Ok(())
}

/// Converts a SCRAM commitment into a big-endian hex string.
///
/// @param commitment - The bytes representing the commitment.
/// @returns A 0x-prefixed hexadecimal string representing the commitment in
///          big-endian byte order, suitable for off-chain use or contract
///          arguments.
pub fn commitment_to_hex(commitment: [u8; 32]) -> String {
    format!("0x{}", hex::encode(commitment))
}

/// Checks whether Solana's native ed25519 precompile instruction, given as
/// its raw instruction data, verified this exact (signature, publicKey,
/// message) triple.
///
/// @param data - Raw instruction data of the ed25519 precompile instruction.
/// @param signature - The signature being claimed.
/// @param publicKey - The public key being claimed.
/// @param message - The message being claimed.
/// @returns Whether the precompile instruction covers this exact triple.
///
/// Conceptually:
///   The precompile instruction data encodes offsets pointing to where the
///   signature, public key, and message it verified live within that same
///   instruction's data. This function reads those offsets, extracts the
///   bytes they point to, and compares them byte-for-byte against the
///   claimed signature, publicKey, and message — confirming the runtime
///   verified this specific triple, not a different valid signature
///   attached to the same transaction.
fn ed25519_instruction_matches(
    data: &[u8],
    signature: &[u8; SIGNATURE_SERIALIZED_SIZE],
    public_key: &Pubkey,
    message: &[u8],
) -> bool {
    if data.len() < DATA_START || data[0] != 1 {
        return false;
    }

    let offsets_bytes = &data[SIGNATURE_OFFSETS_START..DATA_START];
    let mut fields = offsets_bytes
        .chunks_exact(2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]));

    let (
        Some(signature_offset),
        Some(signature_instruction_index),
        Some(public_key_offset),
        Some(public_key_instruction_index),
        Some(message_data_offset),
        Some(message_data_size),
        Some(message_instruction_index),
    ) = (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    )
    else {
        return false;
    };

    if signature_instruction_index != u16::MAX
        || public_key_instruction_index != u16::MAX
        || message_instruction_index != u16::MAX
    {
        return false;
    }
    if message_data_size as usize != message.len() {
        return false;
    }

    let sig_start = signature_offset as usize;
    let pk_start = public_key_offset as usize;
    let msg_start = message_data_offset as usize;
    let msg_len = message_data_size as usize;

    let Some(sig_bytes) = data.get(sig_start..sig_start + SIGNATURE_SERIALIZED_SIZE) else {
        return false;
    };
    let Some(pubkey_bytes) = data.get(pk_start..pk_start + PUBKEY_SERIALIZED_SIZE) else {
        return false;
    };
    let Some(message_bytes) = data.get(msg_start..msg_start + msg_len) else {
        return false;
    };

    sig_bytes == signature && pubkey_bytes == public_key.as_ref() && message_bytes == message
}
