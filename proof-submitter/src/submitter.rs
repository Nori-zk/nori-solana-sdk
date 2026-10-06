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
use solana_loader_v3_interface::{instruction as loader_v3, state::UpgradeableLoaderState};
use solana_message::{Message, VersionedMessage};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_signature::Signature;
use solana_signer::Signer;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status_client_types::{
    option_serializer::OptionSerializer, UiTransactionEncoding,
};
use sp1_solana::SP1Groth16Proof;
use std::str::FromStr;

/// Groth16 verification is the dominant cost; cap the budget so the
/// transaction is never CU-starved (unused units are not charged). Measured
/// at ~103.4k CU per update (surfpool e2e, 2026-09); 200k is ~2x margin.
const UPDATE_COMPUTE_UNIT_LIMIT: u32 = 200_000;

/// Program bytes are written one chunk per transaction; sized so a write
/// plus its accounts fits a legacy transaction (same ballpark the CLI uses).
const BUFFER_WRITE_CHUNK_LEN: usize = 1_012;

/// Minimal result; mirrors the mock shape from the bridge-head side so
/// call sites can swap implementations.
#[derive(Debug, Clone)]
pub struct SolanaTransactionResult {
    /// Transaction signature.
    pub tx_hash: String,
    /// Compute units the transaction consumed (best-effort fetch from the
    /// confirmed transaction's meta; `None` when the RPC doesn't serve it).
    pub cu_consumed: Option<u64>,
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
    #[error("bridge state account data is {len} bytes, expected at least {expected}")]
    StateAccountData { len: usize, expected: usize },
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

    pub fn program_id(&self) -> Pubkey {
        self.program_id
    }

    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    /// Bridge state PDA (`[b"STATE"]`), created by `initialize`.
    pub fn state_address(&self) -> Pubkey {
        Pubkey::find_program_address(
            &[token::constants::NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
            &self.program_id,
        )
        .0
    }

    /// Bridged token mint PDA (`[b"NETH"]`), created by `initialize`.
    pub fn mint_address(&self) -> Pubkey {
        Pubkey::find_program_address(
            &[token::constants::NORI_SOL_TOKEN_BRIDGE_SEED],
            &self.program_id,
        )
        .0
    }

    /// The `initialize` instruction (one-off bridge setup; see DEPLOYMENT.md).
    pub fn build_initialize_instruction(
        &self,
        init_values: token::state::NoriSolTokenBridgeInit,
    ) -> Instruction {
        Instruction::new_with_bytes(
            self.program_id,
            &token::instruction::Initialize { init_values }.data(),
            token::accounts::Initialize {
                payer: self.payer.pubkey(),
                token: self.mint_address(),
                state: self.state_address(),
                system_program: system_program::ID,
                token_program: anchor_spl::token::ID,
            }
            .to_account_metas(None),
        )
    }

    /// The `update` instruction: payer, state PDA, the proof queue batch PDA
    /// for `proof_queue_batch_count` (the next index; the program only
    /// creates it when the proof's batch drained at least one request) and
    /// the system program, plus a compute budget raise, as a two-instruction
    /// message payload.
    pub fn build_update_instructions(
        &self,
        proof: &SP1Groth16Proof,
        proof_queue_batch_count: u64,
    ) -> Vec<Instruction> {
        let (proof_queue_batch, _bump) = Pubkey::find_program_address(
            &[
                token::constants::NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED,
                &proof_queue_batch_count.to_le_bytes(),
            ],
            &self.program_id,
        );
        let update = Instruction::new_with_bytes(
            self.program_id,
            &token::instruction::Update {
                proof: proof.clone().into(),
            }
            .data(),
            token::accounts::Update {
                payer: self.payer.pubkey(),
                state: self.state_address(),
                proof_queue_batch,
                system_program: system_program::ID,
            }
            .to_account_metas(None),
        );
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(UPDATE_COMPUTE_UNIT_LIMIT),
            update,
        ]
    }

    /// Submit the one-off `initialize` transaction that creates the bridge
    /// state PDA and the token mint (see DEPLOYMENT.md §6). Must land before
    /// any `update`; the init values come from the first proof's public
    /// values — see [`crate::LoadedProof::bridge_init`].
    pub async fn submit_initialize(
        &self,
        init_values: token::state::NoriSolTokenBridgeInit,
    ) -> Result<SolanaTransactionResult, SubmitterError> {
        let instructions = vec![self.build_initialize_instruction(init_values)];
        self.send_instructions_with_signers(instructions, &[], "initialize")
            .await
    }

    /// Submit one proof batch as an `update` transaction. Reads the bridge
    /// state first for the next proof queue batch index; if another update
    /// lands in between, the program's continuity checks reject this one.
    pub async fn submit_update(
        &self,
        proof: &SP1Groth16Proof,
    ) -> Result<SolanaTransactionResult, SubmitterError> {
        let proof_queue_batch_count = self.fetch_proof_queue_batch_count().await?;
        let instructions = self.build_update_instructions(proof, proof_queue_batch_count);
        self.send_instructions_with_signers(instructions, &[], "update")
            .await
    }

    /// Read `proof_queue_batch_count` from the bridge state account.
    async fn fetch_proof_queue_batch_count(&self) -> Result<u64, SubmitterError> {
        let client = RpcClient::new(self.rpc_url.clone());
        let account = client.get_account(&self.state_address()).await?;
        // 8-byte Anchor discriminator, then the zero-copy state struct.
        let expected = 8 + std::mem::size_of::<token::state::NoriSolTokenBridge>();
        let data = account
            .data
            .get(8..expected)
            .ok_or(SubmitterError::StateAccountData {
                len: account.data.len(),
                expected,
            })?;
        let state: token::state::NoriSolTokenBridge = bytemuck::pod_read_unaligned(data);
        Ok(state.proof_queue_batch_count)
    }

    /// Deploy a compiled program (raw `.so` bytes) to the configured RPC via
    /// the upgradeable loader: create buffer → write chunks → deploy. The
    /// payer covers fees and rent and becomes the upgrade authority; the
    /// program lands at `program_keypair`'s address.
    ///
    /// Dev/test convenience. Production deploys should use the Solana CLI
    /// (DEPLOYMENT.md §5): it batches writes in parallel and resumes
    /// interrupted uploads; this implementation is sequential and does not
    /// resume — a failure mid-write abandons the (recoverable) buffer.
    pub async fn deploy_program(
        &self,
        program_keypair: &Keypair,
        program_data: &[u8],
    ) -> Result<SolanaTransactionResult, SubmitterError> {
        let client = RpcClient::new(self.rpc_url.clone());
        let payer = self.payer.pubkey();

        let buffer = Keypair::new();
        let buffer_lamports = client
            .get_minimum_balance_for_rent_exemption(UpgradeableLoaderState::size_of_buffer(
                program_data.len(),
            ))
            .await?;
        let create = loader_v3::create_buffer(
            &payer,
            &buffer.pubkey(),
            &payer,
            buffer_lamports,
            program_data.len(),
        )
        .map_err(|e| SubmitterError::TransactionBuild(e.to_string()))?;
        self.send_instructions_with_signers(create, &[&buffer], "buffer create")
            .await?;

        // Fire the chunk writes without per-transaction confirmation
        // (hundreds of txs), then confirm the last one — the batching
        // strategy the Solana CLI uses. Local and RPC endpoints process
        // these in order; the final confirm proves the whole batch landed.
        let blockhash = client.get_latest_blockhash().await?;
        let mut last_signature = None;
        for (index, chunk) in program_data.chunks(BUFFER_WRITE_CHUNK_LEN).enumerate() {
            let write = loader_v3::write(
                &buffer.pubkey(),
                &payer,
                (index * BUFFER_WRITE_CHUNK_LEN) as u32,
                chunk.to_vec(),
            );
            let msg = Message::new_with_blockhash(&[write], Some(&payer), &blockhash);
            let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[&self.payer])
                .map_err(|e| SubmitterError::TransactionBuild(e.to_string()))?;
            last_signature = Some(client.send_transaction(&tx).await?);
        }
        if let Some(signature) = last_signature {
            client.confirm_transaction(&signature).await?;
        }

        let program_lamports = client
            .get_minimum_balance_for_rent_exemption(UpgradeableLoaderState::size_of_program())
            .await?;
        let deploy = loader_v3::deploy_with_max_program_len(
            &payer,
            &program_keypair.pubkey(),
            &buffer.pubkey(),
            &payer,
            program_lamports,
            program_data.len() * 2,
        )
        .map_err(|e| SubmitterError::TransactionBuild(e.to_string()))?;
        self.send_instructions_with_signers(deploy, &[program_keypair], "program deploy")
            .await
    }

    /// Shared send path: blockhash, sign with the payer plus any extra
    /// signers, send and confirm.
    async fn send_instructions_with_signers(
        &self,
        instructions: Vec<Instruction>,
        extra_signers: &[&Keypair],
        what: &str,
    ) -> Result<SolanaTransactionResult, SubmitterError> {
        let client = RpcClient::new(self.rpc_url.clone());
        let blockhash = client.get_latest_blockhash().await?;
        let msg =
            Message::new_with_blockhash(&instructions, Some(&self.payer.pubkey()), &blockhash);
        let mut signers: Vec<&Keypair> = vec![&self.payer];
        signers.extend_from_slice(extra_signers);
        let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &signers)
            .map_err(|e| SubmitterError::TransactionBuild(e.to_string()))?;
        let signature = client.send_and_confirm_transaction(&tx).await?;
        let cu_consumed = fetch_cu_consumed(&client, &signature).await;
        log::info!(
            "submitted Nori {what} tx {} through {} ({} CU)",
            signature,
            self.rpc_url,
            cu_consumed
                .map(|cu| cu.to_string())
                .unwrap_or_else(|| "?".into())
        );
        Ok(SolanaTransactionResult {
            tx_hash: signature.to_string(),
            cu_consumed,
        })
    }
}

/// Fetch the compute units a confirmed transaction consumed. Best-effort:
/// returns `None` if the meta is unavailable or the RPC errors.
async fn fetch_cu_consumed(client: &RpcClient, signature: &Signature) -> Option<u64> {
    let tx = client
        .get_transaction(signature, UiTransactionEncoding::Json)
        .await
        .ok()?;
    match tx.transaction.meta?.compute_units_consumed {
        OptionSerializer::Some(cu) => Some(cu),
        _ => None,
    }
}

/// Solana CLI keypair file: a JSON array of 64 bytes.
pub fn read_keypair_file(path: &str) -> Result<Keypair, SubmitterError> {
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
