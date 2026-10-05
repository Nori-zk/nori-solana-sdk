//! `mint` on a local surfpool validator: the deposit witness must resolve to
//! the root of a committed proof queue batch, with its index inside the
//! batch; and the per-recipient storage PDA must be created both on a fresh
//! address and on one someone pre-funded with lamports before the first mint
//! (which must not block the recipient from ever minting).
//!
//! The example proofs carry no deposits, so `update` never creates a proof
//! queue batch here; the batch account is written directly with surfpool's
//! `surfnet_setAccount` cheatcode, exactly as `update` would lay it out.

use {
    alloy_primitives::{hex, Address, B256, U256},
    anchor_lang::{
        prelude::Pubkey,
        solana_program::{instruction::Instruction, system_instruction, system_program},
        AccountDeserialize, AccountSerialize, InstructionData, ToAccountMetas,
    },
    anchor_spl::associated_token::get_associated_token_address,
    nori_sp1_helios_primitives::storage_layout::MAX_COLLECTION_KEYS,
    solana_keypair::Keypair,
    solana_rpc_client::{api::request::RpcRequest, nonblocking::rpc_client::RpcClient},
    solana_signer::Signer,
    test_utils::{custom_error_code, deploy_program_with_cli, funded_keypair, Surfpool},
    token::{
        deposit_witness::{VerifiedRequest, VerifiedRequestWitnessInput},
        state::{NoriSolTokenAccountStorage, NoriSolTokenBridgeInit, ProofRequestRootEntry},
    },
};

const PROGRAM_SO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/deploy/token.so");
const ETH_TOKEN_BRIDGE_ADDRESS: [u8; 20] = [5u8; 20];
const LOCKED_SO_FAR: u64 = 1_000;

// Anchor custom error codes: 6000 + MintError variant index; 3007 is
// Anchor's AccountOwnedByWrongProgram.
const ERR_PROOF_QUEUE_BATCH_ROOT_MISMATCH: u32 = 6005;
const ERR_WITNESS_INDEX_OUTSIDE_PROOF_QUEUE_BATCH: u32 = 6006;
const ERR_INVALID_DEPOSIT_WITNESS: u32 = 6007;
const ERR_ACCOUNT_OWNED_BY_WRONG_PROGRAM: u32 = 3007;

struct MintHarness {
    // Field exists only for its Drop; must outlive the test.
    _surfpool: Surfpool,
    client: RpcClient,
    payer: Keypair,
}

async fn try_send(
    client: &RpcClient,
    instructions: &[Instruction],
    signers: &[&Keypair],
) -> Result<(), String> {
    let blockhash = client.get_latest_blockhash().await.unwrap();
    let msg = solana_message::Message::new_with_blockhash(
        instructions,
        Some(&signers[0].pubkey()),
        &blockhash,
    );
    let tx = solana_transaction::versioned::VersionedTransaction::try_new(
        solana_message::VersionedMessage::Legacy(msg),
        signers,
    )
    .unwrap();
    client
        .send_and_confirm_transaction(&tx)
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

async fn send(client: &RpcClient, instructions: &[Instruction], signers: &[&Keypair]) {
    try_send(client, instructions, signers)
        .await
        .expect("transaction must succeed");
}

/// The custom program error code of a failed send.
fn custom_error(result: Result<(), String>) -> u32 {
    custom_error_code(&result.expect_err("transaction must fail"))
}

/// Boot surfpool, deploy the program and initialize the bridge.
async fn setup() -> MintHarness {
    let surfpool = Surfpool::start().await;
    let program_id = token::id();
    let client = surfpool.client();

    let payer = funded_keypair(&client, 5_000_000_000).await;
    deploy_program_with_cli(&surfpool, &payer, PROGRAM_SO);

    let init_values = NoriSolTokenBridgeInit {
        verified_state_root: B256::from([1u8; 32]),
        latest_helios_store_input_hash: B256::from([3u8; 32]),
        eth_proof_queue_address: Address::from([4u8; 20]),
        eth_token_bridge_address: Address::from(ETH_TOKEN_BRIDGE_ADDRESS),
        latest_head: 42,
        queue_cursor: 0,
    };
    let init_ix = Instruction::new_with_bytes(
        program_id,
        &token::instruction::Initialize { init_values }.data(),
        token::accounts::Initialize {
            payer: payer.pubkey(),
            token: token_mint(),
            state: state(),
            system_program: system_program::ID,
            token_program: anchor_spl::token::ID,
        }
        .to_account_metas(None),
    );
    send(&client, &[init_ix], &[&payer]).await;

    MintHarness {
        _surfpool: surfpool,
        client,
        payer,
    }
}

fn token_mint() -> Pubkey {
    Pubkey::find_program_address(
        &[token::constants::NORI_SOL_TOKEN_BRIDGE_SEED],
        &token::id(),
    )
    .0
}

fn state() -> Pubkey {
    Pubkey::find_program_address(
        &[token::constants::NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
        &token::id(),
    )
    .0
}

fn token_account_storage(recipient: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[
            token::constants::NORI_SOL_TOKEN_ACCOUNT_STORAGE_SEED,
            recipient.as_ref(),
        ],
        &token::id(),
    )
    .0
}

fn proof_queue_batch(proof_queue_batch_index: u64) -> Pubkey {
    Pubkey::find_program_address(
        &[
            token::constants::NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED,
            &proof_queue_batch_index.to_le_bytes(),
        ],
        &token::id(),
    )
    .0
}

/// Single-leaf witness (empty path) for a token bridge deposit committing to
/// sha256(recipient).
fn deposit_witness(recipient: &Pubkey, index: u64) -> VerifiedRequestWitnessInput {
    let mut collection_keys = [B256::ZERO; MAX_COLLECTION_KEYS];
    collection_keys[0] = B256::from(solana_sha256_hasher::hash(recipient.as_ref()).to_bytes());
    VerifiedRequestWitnessInput {
        path: vec![],
        index,
        value: VerifiedRequest {
            target: Address::from(ETH_TOKEN_BRIDGE_ADDRESS),
            collection_keys_count: 1,
            collection_keys,
            value: U256::from(LOCKED_SO_FAR),
        },
    }
}

/// Write `entry` at `address` with surfpool's `surfnet_setAccount`
/// cheatcode, serialized and owned by `owner`.
async fn set_proof_queue_batch(
    h: &MintHarness,
    address: Pubkey,
    entry: &ProofRequestRootEntry,
    owner: Pubkey,
) {
    let mut data = Vec::new();
    entry.try_serialize(&mut data).unwrap();
    let lamports = h
        .client
        .get_minimum_balance_for_rent_exemption(data.len())
        .await
        .unwrap();
    h.client
        .send::<serde_json::Value>(
            RpcRequest::Custom {
                method: "surfnet_setAccount",
            },
            serde_json::json!([
                address.to_string(),
                {
                    "lamports": lamports,
                    "data": hex::encode(&data),
                    "owner": owner.to_string(),
                    "executable": false,
                }
            ]),
        )
        .await
        .expect("surfnet_setAccount");
}

/// Commit a one-request proof queue batch at index 0 whose root is the
/// recipient's single-leaf deposit root.
async fn commit_proof_queue_batch_for(h: &MintHarness, recipient: &Pubkey) -> Pubkey {
    let entry = ProofRequestRootEntry {
        root: deposit_witness(recipient, 0).root().into(),
        output_block_number: 100,
        input_queue_cursor: 0,
        output_queue_cursor: 1,
    };
    set_proof_queue_batch(h, proof_queue_batch(0), &entry, token::id()).await;
    proof_queue_batch(0)
}

async fn try_mint(
    h: &MintHarness,
    recipient: &Keypair,
    deposit_witness: VerifiedRequestWitnessInput,
    proof_queue_batch: Pubkey,
) -> Result<(), String> {
    let mint_ix = Instruction::new_with_bytes(
        token::id(),
        &token::instruction::Mint { deposit_witness }.data(),
        token::accounts::Mint {
            payer: h.payer.pubkey(),
            recipient: recipient.pubkey(),
            state: state(),
            token: token_mint(),
            token_account: get_associated_token_address(&recipient.pubkey(), &token_mint()),
            token_account_storage: token_account_storage(&recipient.pubkey()),
            proof_queue_batch,
            system_program: system_program::ID,
            token_program: anchor_spl::token::ID,
            associated_token_program: anchor_spl::associated_token::ID,
        }
        .to_account_metas(None),
    );
    try_send(&h.client, &[mint_ix], &[&h.payer, recipient]).await
}

async fn mint(h: &MintHarness, recipient: &Keypair) {
    let proof_queue_batch = commit_proof_queue_batch_for(h, &recipient.pubkey()).await;
    try_mint(
        h,
        recipient,
        deposit_witness(&recipient.pubkey(), 0),
        proof_queue_batch,
    )
    .await
    .expect("mint must succeed");
}

async fn assert_minted(h: &MintHarness, recipient: &Pubkey) {
    let storage = h
        .client
        .get_account(&token_account_storage(recipient))
        .await
        .expect("storage account must exist");
    assert_eq!(storage.owner, token::id());
    let storage = NoriSolTokenAccountStorage::try_deserialize(&mut &storage.data[..])
        .expect("storage account deserializes");
    assert_eq!(storage.minted_so_far, LOCKED_SO_FAR);

    let balance = h
        .client
        .get_token_account_balance(&get_associated_token_address(recipient, &token_mint()))
        .await
        .expect("recipient token account must exist");
    assert_eq!(balance.amount, LOCKED_SO_FAR.to_string());
}

#[tokio::test]
async fn mint_creates_token_account_storage() {
    let h = setup().await;
    let recipient = Keypair::new();

    mint(&h, &recipient).await;
    assert_minted(&h, &recipient.pubkey()).await;
}

#[tokio::test]
async fn mint_creates_pre_funded_token_account_storage() {
    let h = setup().await;
    let recipient = Keypair::new();

    // Anyone can send lamports to the predictable storage PDA before the
    // recipient's first mint. The runtime only accepts a transfer that
    // leaves a new account rent-exempt at 0 bytes; that is still below the
    // storage account's rent exemption, so the program must also top it up.
    let pre_fund_lamports = h
        .client
        .get_minimum_balance_for_rent_exemption(0)
        .await
        .unwrap();
    let pre_fund_ix = system_instruction::transfer(
        &h.payer.pubkey(),
        &token_account_storage(&recipient.pubkey()),
        pre_fund_lamports,
    );
    send(&h.client, &[pre_fund_ix], &[&h.payer]).await;

    mint(&h, &recipient).await;
    assert_minted(&h, &recipient.pubkey()).await;
}

#[tokio::test]
async fn mint_rejects_witness_not_in_committed_proof_queue_batch() {
    let h = setup().await;
    let recipient = Keypair::new();
    let other = Keypair::new();

    // The committed batch holds another recipient's deposit root.
    let proof_queue_batch = commit_proof_queue_batch_for(&h, &other.pubkey()).await;
    let code = custom_error(
        try_mint(
            &h,
            &recipient,
            deposit_witness(&recipient.pubkey(), 0),
            proof_queue_batch,
        )
        .await,
    );
    assert_eq!(code, ERR_PROOF_QUEUE_BATCH_ROOT_MISMATCH);
}

#[tokio::test]
async fn mint_rejects_witness_index_outside_proof_queue_batch() {
    let h = setup().await;
    let recipient = Keypair::new();

    // One-request batch; an empty path resolves to the same root at any
    // index, so only the index check stands between it and index 1.
    let proof_queue_batch = commit_proof_queue_batch_for(&h, &recipient.pubkey()).await;
    let code = custom_error(
        try_mint(
            &h,
            &recipient,
            deposit_witness(&recipient.pubkey(), 1),
            proof_queue_batch,
        )
        .await,
    );
    assert_eq!(code, ERR_WITNESS_INDEX_OUTSIDE_PROOF_QUEUE_BATCH);
}

#[tokio::test]
async fn mint_rejects_malformed_deposit_witness() {
    let h = setup().await;
    let recipient = Keypair::new();

    // A path one level deeper than MAX_TREE_DEPTH, against a batch whose
    // root it resolves to, so only witness validation rejects it.
    let mut witness = deposit_witness(&recipient.pubkey(), 0);
    witness.path = vec![B256::ZERO; token::constants::MAX_TREE_DEPTH + 1];
    let entry = ProofRequestRootEntry {
        root: witness.root().into(),
        output_block_number: 100,
        input_queue_cursor: 0,
        output_queue_cursor: 1,
    };
    set_proof_queue_batch(&h, proof_queue_batch(0), &entry, token::id()).await;
    let code = custom_error(try_mint(&h, &recipient, witness, proof_queue_batch(0)).await);
    assert_eq!(code, ERR_INVALID_DEPOSIT_WITNESS);
}

#[tokio::test]
async fn mint_rejects_proof_queue_batch_not_owned_by_program() {
    let h = setup().await;
    let recipient = Keypair::new();

    // A forged batch account with a matching root but not created by the
    // program (owned by the system program).
    let forged = Pubkey::new_unique();
    let entry = ProofRequestRootEntry {
        root: deposit_witness(&recipient.pubkey(), 0).root().into(),
        output_block_number: 100,
        input_queue_cursor: 0,
        output_queue_cursor: 1,
    };
    set_proof_queue_batch(&h, forged, &entry, system_program::ID).await;
    let code = custom_error(
        try_mint(
            &h,
            &recipient,
            deposit_witness(&recipient.pubkey(), 0),
            forged,
        )
        .await,
    );
    assert_eq!(code, ERR_ACCOUNT_OWNED_BY_WRONG_PROGRAM);
}
