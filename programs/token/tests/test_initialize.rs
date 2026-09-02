
use {
    alloy_primitives::{Address, B256},
    anchor_lang::{
        prelude::Pubkey,
        solana_program::{instruction::Instruction, system_program},
        AccountDeserialize, InstructionData, ToAccountMetas,
    },
    anchor_spl::token_interface::Mint,
    litesvm::LiteSVM,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
    token::state::NoriSolTokenBridgeInit,
};

#[test]
fn test_initialize() {
    let program_id = token::id();
    let payer = Keypair::new();
    let token = Pubkey::find_program_address(
        &[token::constants::NORI_SOL_TOKEN_BRIDGE_SEED],
        &program_id,
    )
    .0;
    let state = Pubkey::find_program_address(
        &[token::constants::NORI_SOL_TOKEN_BRIDGE_STATE_SEED],
        &program_id,
    )
    .0;
    let mut svm = LiteSVM::new();
    let bytes = include_bytes!(concat!(
        env!("CARGO_TARGET_TMPDIR"),
        "/../deploy/token.so"
    ));
    svm.add_program(program_id, bytes).unwrap();
    svm.airdrop(&payer.pubkey(), 1_000_000_000).unwrap();

    let init_values = NoriSolTokenBridgeInit {
        verified_state_root: B256::from([1u8; 32]),
        nori_bridge_vk: B256::from([2u8; 32]),
        latest_helios_store_input_hash: B256::from([3u8; 32]),
        eth_token_bridge_address: Address::from([4u8; 20]),
    };

    let instruction = Instruction::new_with_bytes(
        program_id,
        &token::instruction::Initialize {
            init_values: init_values.clone(),
        }
        .data(),
        token::accounts::Initialize {
            payer: payer.pubkey(),
            token,
            state,
            system_program: system_program::ID,
            token_program: anchor_spl::token::ID,
        }
        .to_account_metas(None),
    );

    let blockhash = svm.latest_blockhash();
    let msg = Message::new_with_blockhash(&[instruction], Some(&payer.pubkey()), &blockhash);
    let tx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[&payer]).unwrap();

    let res = svm.send_transaction(tx);
    assert!(res.is_ok());

    let token_account = svm.get_account(&token).unwrap();
    let mut data: &[u8] = &token_account.data;
    let token_state = Mint::try_deserialize(&mut data).unwrap();
    assert_eq!(token_state.decimals, token::constants::TOKEN_DECIMALS);
    assert_eq!(token_state.supply, 0);
    assert_eq!(
        token_state.mint_authority,
        anchor_lang::solana_program::program_option::COption::Some(payer.pubkey())
    );

    let state_account = svm.get_account(&state).unwrap();
    let mut data: &[u8] = &state_account.data;
    let state_state = token::state::NoriSolTokenBridge::try_deserialize(&mut data).unwrap();
    assert_eq!(state_state.authority, payer.pubkey());
    assert_eq!(state_state.latest_head, 0);
    assert_eq!(
        state_state.verified_state_root,
        <[u8; 32]>::from(init_values.verified_state_root)
    );
    assert_eq!(
        state_state.nori_bridge_vk,
        <[u8; 32]>::from(init_values.nori_bridge_vk)
    );
    assert_eq!(
        state_state.latest_helios_store_input_hash,
        <[u8; 32]>::from(init_values.latest_helios_store_input_hash)
    );
    assert_eq!(
        state_state.eth_token_bridge_address,
        <[u8; 20]>::from(init_values.eth_token_bridge_address)
    );
}
