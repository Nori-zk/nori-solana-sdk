//! Instruction argument types Anchor's IDL can describe.
//!
//! alloy's `B256`, `Address` and `U256` and sp1-solana's `SP1Groth16Proof`
//! implement Borsh but not Anchor's `IdlBuild`, so they cannot appear in an
//! instruction argument and still produce an IDL. Each type here has the
//! same Borsh encoding as the type it stands in for, so instruction bytes are
//! unchanged; convert with `From` at the boundary and work with the alloy and
//! sp1 types inside the program.

use alloy_primitives::{Address, B256, U256};
use anchor_lang::prelude::*;
use sp1_solana::SP1Groth16Proof;

/// 32 bytes; Borsh-encoded exactly as alloy's `B256`.
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bytes32(pub [u8; 32]);

impl From<B256> for Bytes32 {
    fn from(value: B256) -> Self {
        Self(value.0)
    }
}

impl From<Bytes32> for B256 {
    fn from(value: Bytes32) -> Self {
        B256::from(value.0)
    }
}

impl From<Bytes32> for [u8; 32] {
    fn from(value: Bytes32) -> Self {
        value.0
    }
}

/// A 20-byte Ethereum address; Borsh-encoded exactly as alloy's `Address`.
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EthAddress(pub [u8; 20]);

impl From<Address> for EthAddress {
    fn from(value: Address) -> Self {
        Self(value.into_array())
    }
}

impl From<EthAddress> for Address {
    fn from(value: EthAddress) -> Self {
        Address::from(value.0)
    }
}

impl From<EthAddress> for [u8; 20] {
    fn from(value: EthAddress) -> Self {
        value.0
    }
}

/// A 256-bit unsigned integer as 32 little-endian bytes; Borsh-encoded
/// exactly as alloy's `U256`.
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct U256Le(pub [u8; 32]);

impl From<U256> for U256Le {
    fn from(value: U256) -> Self {
        Self(value.to_le_bytes::<32>())
    }
}

impl From<U256Le> for U256 {
    fn from(value: U256Le) -> Self {
        U256::from_le_bytes(value.0)
    }
}

/// An SP1 Groth16 proof; Borsh-encoded exactly as sp1-solana's `SP1Groth16Proof`.
///
/// * `proof` - `SP1ProofWithPublicValues::bytes()`
/// * `sp1_public_inputs` - `SP1ProofWithPublicValues::public_values.to_vec()`
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct UpdateProof {
    pub proof: Vec<u8>,
    pub sp1_public_inputs: Vec<u8>,
}

impl From<SP1Groth16Proof> for UpdateProof {
    fn from(value: SP1Groth16Proof) -> Self {
        Self {
            proof: value.proof,
            sp1_public_inputs: value.sp1_public_inputs,
        }
    }
}

impl From<UpdateProof> for SP1Groth16Proof {
    fn from(value: UpdateProof) -> Self {
        Self {
            proof: value.proof,
            sp1_public_inputs: value.sp1_public_inputs,
        }
    }
}
