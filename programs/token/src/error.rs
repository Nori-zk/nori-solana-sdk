use anchor_lang::prelude::*;

/// Every error the program returns.
///
/// Anchor's IDL allows a single `#[error_code]` enum per program, so the
/// instructions share this one, grouped by instruction below. Each variant's
/// code is 6000 plus its position in the enum, so add new variants only at the
/// end of the enum to keep every existing code stable.
#[error_code]
pub enum NoriSolTokenBridgeError {
    // ================================================================
    // update (6000-6008)
    // ================================================================
    #[msg("SP1 Groth16 proof verification failed")]
    ProofVerificationFailed,
    #[msg("Failed to decode proof bytes")]
    DecodingProofFailed,
    #[msg("ETH proof queue address mismatch")]
    ETHProofQueueAddressMismatch,
    #[msg("Queue cursor mismatch")]
    QueueCursorMismatch,
    #[msg("Input slot does not match latest verified head")]
    InputSlotMismatch,
    #[msg("Input store hash does not match latest verified store hash")]
    InputStoreHashMismatch,
    #[msg("Output slot is not greater than input slot")]
    InvalidOutputSlot,
    #[msg("Next sync committee hash is zero")]
    ZeroSyncCommitteeHash,
    #[msg("Proof queue batch account is not the PDA for the next proof queue batch index")]
    ProofQueueBatchAccountMismatch,

    // ================================================================
    // mint (6009-6016)
    // ================================================================
    #[msg("VerifiedRequest is not a proof of state for the token bridge contract")]
    NotTokenBridgeRequest,
    #[msg("locked_so_far is less than minted_so_far; this would cause a negative mint amount")]
    MintedExceedsLocked,
    #[msg("No new amount to mint: locked_so_far equals minted_so_far")]
    ZeroMintAmount,
    #[msg("Locked amount does not fit in a u64 token amount")]
    LockedAmountOverflow,
    #[msg("Recipient pubkey does not hash to the deposit commitment")]
    CommitmentMismatch,
    #[msg("Witness root does not match the committed proof queue batch root")]
    ProofQueueBatchRootMismatch,
    #[msg("Witness index is outside the committed proof queue batch")]
    WitnessIndexOutsideProofQueueBatch,
    #[msg("Deposit witness is malformed")]
    InvalidDepositWitness,
}
