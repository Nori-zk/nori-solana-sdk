//! End-to-end `update` tests against a local Surfnet validator (surfpool),
//! driving the real RPC path: program deploy → initialize →
//! `SolanaProofSubmitter::submit_update` over JSON-RPC.
//!
//! Every test boots its own `surfpool start --offline --no-deploy` on a free
//! port (initialize is one-shot per program id, so scenarios can't share a
//! chain), deploys `target/deploy/token.so` through the submitter crate, and
//! tears the validator down on drop. Requires `surfpool` on PATH; the .so
//! must be built first (`cargo build-sbf`, see DEVELOPMENT_GUIDE.md).

use {
    alloy_primitives::{Address, B256},
    anchor_lang::prelude::Pubkey,
    nori_sp1_helios_primitives::types::ProofOutputs,
    proof_submitter::{load_update_proofs_dir, LoadedProof, SolanaProofSubmitter},
    solana_keypair::Keypair,
    solana_rpc_client::nonblocking::rpc_client::RpcClient,
    solana_signer::Signer,
    test_utils::{custom_error_code, funded_keypair, read_keypair_file, Surfpool},
    token::state::{NoriSolTokenBridge, NoriSolTokenBridgeInit},
};

const PROOFS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/example-proofs");
const PROGRAM_SO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/deploy/token.so");
const SLOT_1: u64 = 11_298_144;
const SLOT_2: u64 = 11_298_176;
const SLOT_3: u64 = 11_298_208;
const SLOT_4: u64 = 11_298_240;
const BLOCK_1: u64 = 11_857_617;

// Anchor custom error codes: 6000 + NoriSolTokenBridgeError variant index, as they
// appear in RPC error text ("custom program error: 0x1774").
const ERR_QUEUE_ADDRESS_MISMATCH: u32 = 6002;
const ERR_INPUT_SLOT_MISMATCH: u32 = 6004;

fn load_proofs() -> Vec<LoadedProof> {
    load_update_proofs_dir(PROOFS_DIR).expect("example proofs must parse")
}

fn outputs(proof: &LoadedProof) -> ProofOutputs {
    proof.outputs().expect("public values decode")
}

struct SurfpoolHarness {
    // Field exists only for its Drop; must outlive the test.
    _surfpool: Surfpool,
    rpc_url: String,
    submitter: SolanaProofSubmitter,
    state: Pubkey,
}

/// Deploy target/deploy/token.so through the submitter crate's own
/// upgradeable-loader path (create buffer → chunked writes → deploy), with
/// the payer as upgrade authority. The program id comes from
/// target/deploy/token-keypair.json, matching the program's declare_id.
async fn deploy_program(submitter: &SolanaProofSubmitter, program_keypair: &Keypair) {
    let program_data =
        std::fs::read(PROGRAM_SO).expect("token.so missing — build it first (see README)");
    submitter
        .deploy_program(program_keypair, &program_data)
        .await
        .expect("program deploy");
}

async fn setup() -> SurfpoolHarness {
    setup_with(|_| {}).await
}

async fn setup_with(f: impl FnOnce(&mut NoriSolTokenBridgeInit)) -> SurfpoolHarness {
    let surfpool = Surfpool::start().await;
    let rpc_url = surfpool.rpc_url().to_string();
    let payer = funded_keypair(&surfpool.client(), 20 * 1_000_000_000).await;

    let program_id = token::id();
    let submitter = SolanaProofSubmitter::new(
        rpc_url.clone(),
        Keypair::try_from(&payer.to_bytes()[..]).unwrap(),
        program_id,
    );

    // The program id comes from the build-sbf-generated keypair next to the
    // .so (the declare_id must match).
    let program_keypair = read_keypair_file(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../target/deploy/token-keypair.json"
    ));
    assert_eq!(program_keypair.pubkey(), program_id);
    deploy_program(&submitter, &program_keypair).await;

    // Initialize from the first proof's public values (verified_state_root
    // is not part of update-continuity checks; a real deployment uses the
    // execution state root at the start point).
    let proofs = load_proofs();
    let mut init_values = proofs[0]
        .bridge_init(B256::ZERO, Address::ZERO)
        .expect("init values from proof 0");
    f(&mut init_values);

    submitter
        .submit_initialize(init_values)
        .await
        .expect("initialize must succeed");

    let (state, _bump) = Pubkey::find_program_address(
        &[token::constants::NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
        &program_id,
    );

    SurfpoolHarness {
        _surfpool: surfpool,
        rpc_url,
        submitter,
        state,
    }
}

async fn read_state(h: &SurfpoolHarness) -> NoriSolTokenBridge {
    let client = RpcClient::new(h.rpc_url.clone());
    let account = client
        .get_account(&h.state)
        .await
        .expect("state account must exist");
    *bytemuck::from_bytes::<NoriSolTokenBridge>(
        &account.data[8..8 + std::mem::size_of::<NoriSolTokenBridge>()],
    )
}

async fn proof_queue_batch_exists(h: &SurfpoolHarness, proof_queue_batch_index: u64) -> bool {
    let (proof_queue_batch, _bump) = Pubkey::find_program_address(
        &[
            token::constants::NORI_SOL_TOKEN_BRIDGE_PROOF_QUEUE_BATCH_SEED,
            &proof_queue_batch_index.to_le_bytes(),
        ],
        &token::id(),
    );
    let client = RpcClient::new(h.rpc_url.clone());
    client
        .get_account(&proof_queue_batch)
        .await
        .is_ok_and(|account| !account.data.is_empty())
}

/// Extract the anchor custom error code from a failed submission's RPC error.
fn expect_custom_error(
    result: Result<proof_submitter::SolanaTransactionResult, proof_submitter::SubmitterError>,
) -> u32 {
    let err = result.expect_err("submission must fail");
    custom_error_code(&format!("{err:?}"))
}

#[tokio::test]
async fn submit_single_update_advances_state() {
    let h = setup().await;
    let proofs = load_proofs();
    let result = h
        .submitter
        .submit_update(&proofs[0].wire)
        .await
        .expect("first update must succeed");
    assert!(!result.tx_hash.is_empty());

    let state = read_state(&h).await;
    let out = outputs(&proofs[0]);
    assert_eq!(state.latest_head, SLOT_1);
    assert_eq!(
        state.verified_state_root,
        <[u8; 32]>::from(out.execution_state_root)
    );
    assert_eq!(
        state.latest_helios_store_input_hash,
        <[u8; 32]>::from(out.output_store_hash)
    );
    assert_eq!(state.queue_cursor, out.output_queue_cursor);
    assert_eq!(out.output_block_number, BLOCK_1);

    // The example proofs drain no requests (empty batch): the update only
    // advances the head, records no proof queue batch, and leaves the next
    // proof queue batch PDA uncreated.
    assert_eq!(out.input_queue_cursor, out.output_queue_cursor);
    assert_eq!(state.proof_queue_batch_count, 0);
    assert!(!proof_queue_batch_exists(&h, 0).await);
}

#[tokio::test]
async fn submit_update_series() {
    let h = setup().await;
    let proofs = load_proofs();
    let heads = [SLOT_1, SLOT_2, SLOT_3, SLOT_4];
    for (i, proof) in proofs.iter().enumerate() {
        let result = h
            .submitter
            .submit_update(&proof.wire)
            .await
            .unwrap_or_else(|e| panic!("update {i} failed: {e}"));
        // CU tripwire: measured ~103.4k; the submitter budgets 200k.
        let cu = result
            .cu_consumed
            .expect("local validator serves compute_units_consumed");
        println!("update[{i}]: {cu} CU");
        assert!(cu < 200_000, "update CU usage approaches the budget");
        let state = read_state(&h).await;
        assert_eq!(state.latest_head, heads[i]);
        assert_eq!(state.proof_queue_batch_count, 0);
    }
    assert!(!proof_queue_batch_exists(&h, 0).await);
}

#[tokio::test]
async fn skipping_a_transition_fails_continuity() {
    let h = setup().await;
    let proofs = load_proofs();
    h.submitter
        .submit_update(&proofs[0].wire)
        .await
        .expect("first update must succeed");
    let code = expect_custom_error(h.submitter.submit_update(&proofs[2].wire).await);
    assert_eq!(code, ERR_INPUT_SLOT_MISMATCH);

    // State is untouched by the failed transaction.
    let state = read_state(&h).await;
    assert_eq!(state.latest_head, SLOT_1);
    assert_eq!(state.proof_queue_batch_count, 0);
}

#[tokio::test]
async fn replaying_a_proof_fails() {
    let h = setup().await;
    let proofs = load_proofs();
    h.submitter
        .submit_update(&proofs[0].wire)
        .await
        .expect("first update must succeed");
    let code = expect_custom_error(h.submitter.submit_update(&proofs[0].wire).await);
    assert_eq!(code, ERR_INPUT_SLOT_MISMATCH);
}

#[tokio::test]
async fn update_with_wrong_queue_address_fails() {
    let h =
        setup_with(|init| init.eth_proof_queue_address = Address::from([0xEEu8; 20]).into()).await;
    let proofs = load_proofs();
    let code = expect_custom_error(h.submitter.submit_update(&proofs[0].wire).await);
    assert_eq!(code, ERR_QUEUE_ADDRESS_MISMATCH);
}
