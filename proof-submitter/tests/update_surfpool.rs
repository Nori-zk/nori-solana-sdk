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
    std::{
        net::TcpListener,
        process::{Child, Command, Stdio},
        time::{Duration, Instant},
    },
    token::state::{NoriSolTokenBridge, NoriSolTokenBridgeInit},
};

const PROOFS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/example-proofs");
const PROGRAM_SO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../target/deploy/token.so");
const SLOT_1: u64 = 11_247_360;
const SLOT_2: u64 = 11_247_392;
const SLOT_3: u64 = 11_247_424;
const SLOT_4: u64 = 11_247_456;
const BLOCK_1: u64 = 11_808_937;

// Anchor custom error codes: 6000 + UpdateError variant index, as they
// appear in RPC error text ("custom program error: 0x1774").
const ERR_PROOF_VERIFICATION_FAILED: u32 = 6000;
const ERR_QUEUE_ADDRESS_MISMATCH: u32 = 6002;
const ERR_INPUT_SLOT_MISMATCH: u32 = 6004;

fn load_proofs() -> Vec<LoadedProof> {
    load_update_proofs_dir(PROOFS_DIR).expect("example proofs must parse")
}

fn outputs(proof: &LoadedProof) -> ProofOutputs {
    proof.outputs().expect("public values decode")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Kill-on-drop guard for the surfpool validator. A bare `Child` does NOT
/// kill on drop, so the child must be wrapped in this guard immediately
/// after spawn — before any fallible setup step (rpc_ready / airdrop /
/// deploy / initialize). Then a panic on any of those paths still kills the
/// validator during unwind instead of leaking it (reparented to init).
struct Surfpool(Child);

impl Drop for Surfpool {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct SurfpoolHarness {
    // Field exists only for its Drop; must outlive every fallible setup step.
    _surfpool: Surfpool,
    rpc_url: String,
    submitter: SolanaProofSubmitter,
    state: Pubkey,
}

async fn rpc_ready(rpc_url: &str, timeout: Duration) -> bool {
    let client = RpcClient::new(rpc_url.to_string());
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if client.get_health().await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    false
}

async fn airdrop(client: &RpcClient, to: &Pubkey, sol: u64) {
    let sig = client
        .request_airdrop(to, sol * 1_000_000_000)
        .await
        .expect("airdrop request");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let balance = client.get_balance(to).await.unwrap_or(0);
        if balance >= sol * 1_000_000_000 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "airdrop {sig} not confirmed in time"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Read a Solana CLI keypair file (JSON array of 64 bytes).
fn read_keypair(path: &str) -> Keypair {
    let text = std::fs::read_to_string(path).expect("keypair file readable");
    let bytes: Vec<u8> = serde_json::from_str(&text).expect("keypair json");
    Keypair::try_from(&bytes[..]).expect("valid keypair")
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
    let port = free_port();
    let ws_port = free_port();
    let rpc_url = format!("http://127.0.0.1:{port}");
    let child = Command::new("surfpool")
        .args([
            "start",
            "--offline",
            "--no-deploy",
            "--ci",
            "-p",
            &port.to_string(),
            "--ws-port",
            &ws_port.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn surfpool (is it on PATH?)");
    // Wrap in the kill-on-drop guard IMMEDIATELY: every step below
    // (rpc_ready, airdrop, deploy, initialize) may panic, and an unwrapped
    // Child would leak the validator on unwind.
    let surfpool = Surfpool(child);
    assert!(
        rpc_ready(&rpc_url, Duration::from_secs(30)).await,
        "surfpool on port {port} did not come up"
    );

    let client = RpcClient::new(rpc_url.clone());
    let payer = Keypair::new();
    airdrop(&client, &payer.pubkey(), 20).await;

    let program_id = token::id();
    let submitter = SolanaProofSubmitter::new(
        rpc_url.clone(),
        Keypair::try_from(&payer.to_bytes()[..]).unwrap(),
        program_id,
    );

    // The program id comes from the build-sbf-generated keypair next to the
    // .so (the declare_id must match).
    let program_keypair = read_keypair(concat!(
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

/// Extract the anchor custom error code from a failed submission's RPC error.
fn expect_custom_error(
    result: Result<proof_submitter::SolanaTransactionResult, proof_submitter::SubmitterError>,
) -> u32 {
    let err = result.expect_err("submission must fail");
    let text = format!("{err:?}");
    let marker = "custom program error: 0x";
    let start = text
        .find(marker)
        .unwrap_or_else(|| panic!("no custom error in: {text}"));
    let hex = text[start + marker.len()..]
        .chars()
        .take_while(|c| c.is_ascii_hexdigit())
        .collect::<String>();
    u32::from_str_radix(&hex, 16).expect("hex error code")
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
    assert_eq!(state.window_index, 1);
    let entry = &state.window_buffer[0];
    assert_eq!(entry.root, <[u8; 32]>::from(out.verified_requests_root));
    assert_eq!(entry.output_block_number, BLOCK_1);
    assert_eq!(entry.input_queue_cursor, out.input_queue_cursor);
    assert_eq!(entry.output_queue_cursor, out.output_queue_cursor);
}

#[tokio::test]
async fn submit_update_series() {
    let h = setup().await;
    let proofs = load_proofs();
    let heads = [SLOT_1, SLOT_2, SLOT_3, SLOT_4];
    for (i, proof) in proofs.iter().enumerate() {
        h.submitter
            .submit_update(&proof.wire)
            .await
            .unwrap_or_else(|e| panic!("update {i} failed: {e}"));
        let state = read_state(&h).await;
        assert_eq!(state.latest_head, heads[i]);
        assert_eq!(state.window_index as usize, i + 1);
    }
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
    assert_eq!(state.window_index, 1);
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
    let h = setup_with(|init| init.eth_proof_queue_address = Address::from([0xEEu8; 20])).await;
    let proofs = load_proofs();
    let code = expect_custom_error(h.submitter.submit_update(&proofs[0].wire).await);
    assert_eq!(code, ERR_QUEUE_ADDRESS_MISMATCH);
}

#[tokio::test]
async fn update_with_wrong_program_vkey_fails() {
    let h = setup_with(|init| init.nori_bridge_vk = B256::from([0xAAu8; 32])).await;
    let proofs = load_proofs();
    let code = expect_custom_error(h.submitter.submit_update(&proofs[0].wire).await);
    assert_eq!(code, ERR_PROOF_VERIFICATION_FAILED);
}
