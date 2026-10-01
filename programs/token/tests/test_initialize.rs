//! `initialize` on a local surfpool validator: deploy, initialize, then check
//! the SPL mint and the bridge state were written as configured.

use {
    alloy_primitives::{Address, B256},
    anchor_lang::{
        prelude::Pubkey,
        solana_program::{instruction::Instruction, system_program},
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    anchor_spl::token_interface::Mint,
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

const PROGRAM_SO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/deploy/token.so");

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Surfpool(Child);
impl Drop for Surfpool {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
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

#[tokio::test]
async fn test_initialize() {
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
    // Wrap in the kill-on-drop guard IMMEDIATELY after spawn, before any
    // fallible step (rpc_ready/airdrop/deploy): a bare Child does not kill
    // on drop, so a panic on those paths would leak the validator.
    let _surfpool = Surfpool(child);
    assert!(
        rpc_ready(&rpc_url, Duration::from_secs(30)).await,
        "surfpool did not come up on {rpc_url}"
    );

    let program_id = token::id();
    let payer = Keypair::new();
    let client = RpcClient::new(rpc_url.clone());

    // Fund the payer, then deploy via the Solana CLI.
    let sig = client
        .request_airdrop(&payer.pubkey(), 5_000_000_000)
        .await
        .expect("airdrop request");
    client
        .confirm_transaction(&sig)
        .await
        .expect("airdrop confirm");

    let payer_path = std::env::temp_dir().join(format!(
        "nori-init-test-payer-{}-{}.json",
        std::process::id(),
        payer.pubkey()
    ));
    let bytes: Vec<u8> = payer.to_bytes().to_vec();
    std::fs::write(&payer_path, serde_json::to_string(&bytes).unwrap()).unwrap();
    let deploy = Command::new("solana")
        .args([
            "program",
            "deploy",
            PROGRAM_SO,
            "-k",
            payer_path.to_str().unwrap(),
            "--url",
            &rpc_url,
            "--ws",
            &format!("ws://127.0.0.1:{ws_port}"),
        ])
        .output()
        .expect("failed to run solana CLI");
    std::fs::remove_file(&payer_path).ok();
    assert!(
        deploy.status.success(),
        "program deploy failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&deploy.stdout),
        String::from_utf8_lossy(&deploy.stderr)
    );

    // Initialize with dummy values (update-continuity values are covered by
    // the proof-submitter suite).
    let init_values = NoriSolTokenBridgeInit {
        verified_state_root: B256::from([1u8; 32]),
        latest_helios_store_input_hash: B256::from([3u8; 32]),
        eth_proof_queue_address: Address::from([4u8; 20]),
        eth_token_bridge_address: Address::from([5u8; 20]),
        latest_head: 42,
        queue_cursor: 7,
    };

    let token_mint =
        Pubkey::find_program_address(&[token::constants::NORI_SOL_TOKEN_BRIDGE_SEED], &program_id)
            .0;
    let state = Pubkey::find_program_address(
        &[token::constants::NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
        &program_id,
    )
    .0;
    let init_ix = Instruction::new_with_bytes(
        program_id,
        &token::instruction::Initialize {
            init_values: init_values.clone(),
        }
        .data(),
        token::accounts::Initialize {
            payer: payer.pubkey(),
            token: token_mint,
            state,
            system_program: system_program::ID,
            token_program: anchor_spl::token::ID,
        }
        .to_account_metas(None),
    );
    let blockhash = client.get_latest_blockhash().await.unwrap();
    let msg =
        solana_message::Message::new_with_blockhash(&[init_ix], Some(&payer.pubkey()), &blockhash);
    let tx = solana_transaction::versioned::VersionedTransaction::try_new(
        solana_message::VersionedMessage::Legacy(msg),
        &[&payer],
    )
    .unwrap();
    client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("initialize must succeed");

    let token_account = client.get_account(&token_mint).await.unwrap();
    let mut data: &[u8] = &token_account.data;
    let token_state = Mint::try_deserialize(&mut data).unwrap();
    assert_eq!(token_state.decimals, token::constants::TOKEN_DECIMALS);
    assert_eq!(token_state.supply, 0);
    assert_eq!(
        token_state.mint_authority,
        anchor_lang::solana_program::program_option::COption::Some(state)
    );
    assert_eq!(
        token_state.freeze_authority,
        anchor_lang::solana_program::program_option::COption::Some(state)
    );

    let state_account = client.get_account(&state).await.unwrap();
    let data: &[u8] = &state_account.data;
    let state_state = *bytemuck::from_bytes::<NoriSolTokenBridge>(
        &data[8..8 + std::mem::size_of::<NoriSolTokenBridge>()],
    );
    assert_eq!(state_state.authority, payer.pubkey());
    assert_eq!(state_state.latest_head, 42);
    assert_eq!(state_state.queue_cursor, 7);
    assert_eq!(state_state.window_index, 0);
    assert_eq!(
        state_state.verified_state_root,
        <[u8; 32]>::from(init_values.verified_state_root)
    );
    assert_eq!(
        state_state.nori_bridge_vk,
        nori_elf::NORI_SP1_HELIOS_PROGRAM_VK
    );
    assert_eq!(
        state_state.latest_helios_store_input_hash,
        <[u8; 32]>::from(init_values.latest_helios_store_input_hash)
    );
    assert_eq!(
        state_state.eth_proof_queue_address,
        <[u8; 20]>::from(init_values.eth_proof_queue_address)
    );
    assert_eq!(
        state_state.eth_token_bridge_address,
        <[u8; 20]>::from(init_values.eth_token_bridge_address)
    );
}
