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
    solana_signer::Signer,
    test_utils::{deploy_program_with_cli, funded_keypair, Surfpool},
    token::state::{NoriSolTokenBridge, NoriSolTokenBridgeInit},
};

const PROGRAM_SO: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/deploy/token.so");

#[tokio::test]
async fn test_initialize() {
    let surfpool = Surfpool::start().await;
    let program_id = token::id();
    let client = surfpool.client();

    // Fund the payer, then deploy via the Solana CLI.
    let payer = funded_keypair(&client, 5_000_000_000).await;
    deploy_program_with_cli(&surfpool, &payer, PROGRAM_SO);

    // Initialize with dummy values (update-continuity values are covered by
    // the proof-submitter suite).
    let init_values = NoriSolTokenBridgeInit {
        verified_state_root: B256::from([1u8; 32]).into(),
        latest_helios_store_input_hash: B256::from([3u8; 32]).into(),
        eth_proof_queue_address: Address::from([4u8; 20]).into(),
        eth_token_bridge_address: Address::from([5u8; 20]).into(),
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
    assert_eq!(state_state.proof_queue_batch_count, 0);
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
