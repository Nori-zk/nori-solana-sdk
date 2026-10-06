//! Client-side proof submission for the Nori Solana token bridge.
//!
//! Two halves:
//!
//! * [`proof_file`] — loads SP1 `SP1ProofWithPublicValues` JSON dumps (as
//!   produced by nori-bridge-head) into the on-chain wire format
//!   [`sp1_solana::SP1Groth16Proof`].
//! * [`submitter`] — builds and sends the `update` transaction against a
//!   Solana RPC endpoint ([`SolanaProofSubmitter`]).

pub mod proof_file;
pub mod submitter;

pub use proof_file::{load_update_proof, load_update_proofs_dir, LoadedProof, ProofFileError};
pub use submitter::{
    read_keypair_file, SolanaProofSubmitter, SolanaTransactionResult, SubmitterError,
};
