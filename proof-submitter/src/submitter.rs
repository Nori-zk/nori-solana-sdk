//! Builds and submits `update` transactions for the Nori Solana token bridge.
//!
//! Interface mirrors the bridge head's other destination-chain submitters:
//! construct from env ([`SolanaProofSubmitter::from_env`]), then call
//! [`SolanaProofSubmitter::submit_update`] per proof batch.

use anchor_lang::{
    prelude::Pubkey, solana_program::instruction::Instruction, solana_program::system_program,
    InstructionData, ToAccountMetas,
};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_keypair::Keypair;
use solana_message::{Message, VersionedMessage};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_signer::Signer;
use solana_transaction::versioned::VersionedTransaction;
use sp1_solana::SP1Groth16Proof;
use std::str::FromStr;

/// Groth16 verification is the dominant cost; cap the budget so the
/// transaction is never CU-starved (unused units are not charged).
const UPDATE_COMPUTE_UNIT_LIMIT: u32 = 1_400_000;

#[derive(Debug, Clone)]
pub struct SolanaTransactionResult {
    pub tx_hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SubmitterError {
    #[error("env: {0}")]
    Env(#[from] std::env::VarError),
    #[error("invalid NORI_SOL_TOKEN_PROGRAM_ID: {0}")]
    ProgramId(String),
    #[error("failed to read payer keypair from {path}: {reason}")]
    KeypairRead { path: String, reason: String },
    #[error("payer keypair has wrong length ({0} bytes, expected 64)")]
    KeypairLength(usize),
    #[error("transaction build failed: {0}")]
    TransactionBuild(String),
    #[error("rpc: {0}")]
    Rpc(#[from] solana_rpc_client_api::client_error::Error),
}

pub struct SolanaProofSubmitter {
    rpc_url: String,
    payer: Keypair,
    program_id: Pubkey,
}

impl SolanaProofSubmitter {
    /// Env:
    /// * `SOLANA_RPC_NETWORK_URL` (required)
    /// * `SOLANA_PAYER_KEYPAIR_PATH` (required) — JSON-array keypair file (Solana CLI `id.json` format)
    /// * `NORI_SOL_TOKEN_PROGRAM_ID` (optional — defaults to the program's declared id)
    pub fn from_env() -> Result<Self, SubmitterError> {
        dotenvy::dotenv().ok();
        let rpc_url = std::env::var("SOLANA_RPC_NETWORK_URL")?;
        let payer = read_keypair_file(&std::env::var("SOLANA_PAYER_KEYPAIR_PATH")?)?;
        let program_id = match std::env::var("NORI_SOL_TOKEN_PROGRAM_ID") {
            Ok(raw) => Pubkey::from_str(&raw).map_err(|_| SubmitterError::ProgramId(raw))?,
            Err(_) => token::id(),
        };
        Ok(Self {
            rpc_url,
            payer,
            program_id,
        })
    }

    pub fn new(rpc_url: String, payer: Keypair, program_id: Pubkey) -> Self {
        Self {
            rpc_url,
            payer,
            program_id,
        }
    }

    pub fn payer_pubkey(&self) -> Pubkey {
        self.payer.pubkey()
    }

    /// The `initialize` instruction (one-off bridge setup; see DEPLOYMENT.md).
    pub fn build_initialize_instruction(
        &self,
        init_values: token::state::NoriSolTokenBridgeInit,
    ) -> Instruction {
        let (state, _bump) = Pubkey::find_program_address(
            &[token::constants::NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
            &self.program_id,
        );
        let (token_mint, _bump) = Pubkey::find_program_address(
            &[token::constants::NORI_SOL_TOKEN_BRIDGE_SEED],
            &self.program_id,
        );
        Instruction::new_with_bytes(
            self.program_id,
            &token::instruction::Initialize { init_values }.data(),
            token::accounts::Initialize {
                payer: self.payer.pubkey(),
                token: token_mint,
                state,
                system_program: system_program::ID,
                token_program: anchor_spl::token::ID,
            }
            .to_account_metas(None),
        )
    }

    /// The `update` instruction: state PDA + system program, plus a compute
    /// budget raise, as a two-instruction message payload.
    pub fn build_update_instructions(&self, proof: &SP1Groth16Proof) -> Vec<Instruction> {
        let (state, _bump) = Pubkey::find_program_address(
            &[token::constants::NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
            &self.program_id,
        );
        let update = Instruction::new_with_bytes(
            self.program_id,
            &token::instruction::Update {
                proof: proof.clone(),
            }
            .data(),
            token::accounts::Update {
                state,
                system_program: system_program::ID,
            }
            .to_account_metas(None),
        );
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(UPDATE_COMPUTE_UNIT_LIMIT),
            update,
        ]
    }

    /// Submit one proof batch as an `update` transaction.
    pub async fn submit_update(
        &self,
        proof: &SP1Groth16Proof,
    ) -> Result<SolanaTransactionResult, SubmitterError> {
        let client = RpcClient::new(self.rpc_url.clone());
        let instructions = self.build_update_instructions(proof);
        let blockhash = client.get_latest_blockhash().await?;
        let msg =
            Message::new_with_blockhash(&instructions, Some(&self.payer.pubkey()), &blockhash);
        let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[&self.payer])
            .map_err(|e| SubmitterError::TransactionBuild(e.to_string()))?;
        let signature = client.send_and_confirm_transaction(&tx).await?;
        log::info!(
            "submitted Nori update tx {} through {}",
            signature,
            self.rpc_url
        );
        Ok(SolanaTransactionResult {
            tx_hash: signature.to_string(),
        })
    }
}

/// Solana CLI keypair file: a JSON array of 64 bytes.
fn read_keypair_file(path: &str) -> Result<Keypair, SubmitterError> {
    let text = std::fs::read_to_string(path).map_err(|e| SubmitterError::KeypairRead {
        path: path.to_string(),
        reason: e.to_string(),
    })?;
    let bytes: Vec<u8> = serde_json::from_str(&text).map_err(|e| SubmitterError::KeypairRead {
        path: path.to_string(),
        reason: format!("expected a JSON array of 64 bytes: {e}"),
    })?;
    if bytes.len() != 64 {
        return Err(SubmitterError::KeypairLength(bytes.len()));
    }
    Keypair::try_from(&bytes[..]).map_err(|e| SubmitterError::KeypairRead {
        path: path.to_string(),
        reason: e.to_string(),
    })
}
