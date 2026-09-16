use alloy_primitives::{hex, Address};
use anchor_lang::prelude::*;
use anchor_spl::associated_token::{self, AssociatedToken};
use anchor_spl::token_interface::{mint_to, Mint as Token, MintTo, TokenInterface};

use crate::{
    constants::*, deposit_witness::VerifiedRequestWitnessInput, scram::verify_commitment,
    scram::SCRAMWitness, state::NoriSolTokenAccountStorage, state::NoriSolTokenBridge,
};

#[error_code]
pub enum MintError {
    #[msg("VerifiedRequest is not a proof of state for the token bridge contract")]
    NotTokenBridgeRequest,
    #[msg("locked_so_far is less than minted_so_far; this would cause a negative mint amount")]
    MintedExceedsLocked,
    #[msg("No new amount to mint: locked_so_far equals minted_so_far")]
    ZeroMintAmount,
    #[msg("Locked amount does not fit in a u64 token amount")]
    LockedAmountOverflow,
}

#[event]
pub struct MintApplied {
    pub recipient: Pubkey,
    pub deposit_root: [u8; 32],
    pub amount_minted: u64,
    pub minted_so_far: u64,
}

#[derive(Accounts)]
pub struct Mint<'info> {
    #[account(mut)]
    pub payer: Signer<'info>,
    #[account(mut)]
    pub recipient: Signer<'info>,

    #[account(mut, seeds = [NORI_SOL_TOKEN_BRIDGE_STATE_SEED], bump)]
    pub state: Account<'info, NoriSolTokenBridge>,
    #[account(mut, seeds = [NORI_SOL_TOKEN_BRIDGE_SEED], bump)]
    pub token: InterfaceAccount<'info, Token>,
    /// CHECK: address and type are validated by the associated_token program's
    /// create_idempotent CPI below, which derives the ATA address itself and
    /// verifies any pre-existing account at it is a valid token account for
    /// this mint/owner/token_program.
    #[account(mut)]
    pub token_account: UncheckedAccount<'info>,

    /// CHECK: seeds/bump validate the PDA address; the account is manually
    /// created and initialized in handle_mint on first use (not via Anchor's
    /// init_if_needed, which would skip re-running our init logic on an
    /// account an attacker got created ahead of time) and left untouched if
    /// it already exists.
    #[account(mut, seeds = [NORI_SOL_TOKEN_ACCOUNT_STORAGE_SEED, recipient.key().as_ref()], bump)]
    pub token_account_storage: UncheckedAccount<'info>,

    /// CHECK: validated via address constraint against the instructions sysvar id
    #[account(address = solana_instructions_sysvar::ID)]
    pub instructions_sysvar: UncheckedAccount<'info>,

    pub system_program: Program<'info, System>,
    pub token_program: Interface<'info, TokenInterface>,
    pub associated_token_program: Program<'info, AssociatedToken>,
}

/// Mints bridged tokens to `recipient` against a proven Ethereum-side deposit.
///
/// # Arguments
///
/// * `ctx` - Accounts for the mint: bridge `state`, the token mint, the
///   recipient's associated token account (created here if absent), and the
///   instructions sysvar used for Ed25519 introspection below.
/// * `deposit_witness` - The Merkle witness proving a `VerifiedRequest` leaf
///   (target, collection keys, locked value) is present at `index` in the
///   deposit tree, resolving to a root via [`VerifiedRequestWitnessInput::root`].
/// * `scram_witness` - The signature and message claimed to open the SCRAM
///   commitment stored in the deposit leaf's first collection key, checked by
///   [`verify_commitment`].
/// * `ed25519_instruction_index` - The position, within this transaction's own
///   instruction list, of the client-supplied Ed25519 signature-verification
///   instruction that `scram_witness` is checked against. Solana's Ed25519
///   program is not invoked via CPI and returns nothing a caller can read
///   directly; instead the client includes a separate Ed25519 instruction
///   alongside this one in the same transaction, and this program reads that
///   sibling instruction's raw data back through the instructions sysvar to
///   confirm it covers the exact `(signature, publicKey, message)` triple
///   being claimed here. The client places that instruction when building the
///   transaction and so knows its index directly; the program has no way to
///   discover it on its own short of an unbounded scan of every instruction
///   in the transaction.
///
/// # Errors
///
/// Returns a [`MintError`] if the deposit leaf's target does not match the
/// bridge's configured token bridge address, or an error from
/// [`verify_commitment`] if the Ed25519 verification is missing, mismatched,
/// or the signature does not hash to the claimed commitment.
pub fn handle_mint(
    ctx: Context<Mint>,
    deposit_witness: VerifiedRequestWitnessInput,
    scram_witness: SCRAMWitness,
    ed25519_instruction_index: u16,
) -> Result<()> {
    // ================================================================
    // Setup accounts
    // ================================================================

    // Create an account for this user if it does not exist
    associated_token::create_idempotent(CpiContext::new(
        ctx.accounts.associated_token_program.key(),
        associated_token::Create {
            payer: ctx.accounts.payer.to_account_info(),
            associated_token: ctx.accounts.token_account.to_account_info(),
            authority: ctx.accounts.recipient.to_account_info(),
            mint: ctx.accounts.token.to_account_info(),
            system_program: ctx.accounts.system_program.to_account_info(),
            token_program: ctx.accounts.token_program.to_account_info(),
        },
    ))?;

    // Create the per-recipient minted-so-far storage account if it does not
    // exist yet; leave it untouched if it does.
    if ctx.accounts.token_account_storage.lamports() == 0 {
        let recipient_key = ctx.accounts.recipient.key();
        let bump = ctx.bumps.token_account_storage;
        let seeds: &[&[u8]] = &[
            NORI_SOL_TOKEN_ACCOUNT_STORAGE_SEED,
            recipient_key.as_ref(),
            &[bump],
        ];

        let space = 8 + NoriSolTokenAccountStorage::INIT_SPACE;
        let lamports = Rent::get()?.minimum_balance(space);

        anchor_lang::system_program::create_account(
            CpiContext::new(
                ctx.accounts.system_program.key(),
                anchor_lang::system_program::CreateAccount {
                    from: ctx.accounts.payer.to_account_info(),
                    to: ctx.accounts.token_account_storage.to_account_info(),
                },
            )
            .with_signer(&[seeds]),
            lamports,
            space as u64,
            &crate::ID,
        )?;

        let storage = NoriSolTokenAccountStorage { minted_so_far: 0 };
        let mut data = ctx.accounts.token_account_storage.try_borrow_mut_data()?;
        storage.try_serialize(&mut *data)?;
    }

    // ================================================================
    // Deposit proof validation
    // ================================================================

    let root = deposit_witness.root();
    let request = &deposit_witness.value;

    (request.target == Address::from(ctx.accounts.state.eth_token_bridge_address))
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Deposit target mismatch, leaf target: {}, expected token bridge address: 0x{}",
                request.target,
                hex::encode(ctx.accounts.state.eth_token_bridge_address)
            );
            error!(MintError::NotTokenBridgeRequest)
        })?;

    verify_commitment(
        request.collection_keys[0].0,
        &scram_witness.signature,
        &ctx.accounts.recipient.key(),
        &scram_witness.message,
        &ctx.accounts.instructions_sysvar.to_account_info(),
        ed25519_instruction_index,
    )?;

    msg!("deposit slot root verified: {:?}", root);

    // ================================================================
    // Mint amount calculation
    // ================================================================

    let locked_so_far: u64 = request
        .value
        .try_into()
        .map_err(|_| error!(MintError::LockedAmountOverflow))?;

    // token_account_storage is an UncheckedAccount (not Account<'info, T>)
    // Anchor never auto-deserializes it for us, so we do it by here.
    let mut storage_data = ctx.accounts.token_account_storage.try_borrow_mut_data()?;
    let mut storage = NoriSolTokenAccountStorage::try_deserialize(&mut &storage_data[..])?;

    (locked_so_far >= storage.minted_so_far)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Underflow: locked_so_far {} is less than minted_so_far {}",
                locked_so_far,
                storage.minted_so_far
            );
            error!(MintError::MintedExceedsLocked)
        })?;

    let amount_to_mint = locked_so_far - storage.minted_so_far;

    (amount_to_mint > 0)
        .then_some(())
        .ok_or_else(|| {
            msg!("No new amount to mint");
            error!(MintError::ZeroMintAmount)
        })?;

    storage.minted_so_far = locked_so_far;
    storage.try_serialize(&mut *storage_data)?;

    msg!(
        "amount to mint: {}, new minted_so_far: {}",
        amount_to_mint,
        storage.minted_so_far
    );

    // ================================================================
    // Mint tokens
    // ================================================================

    mint_to(
        CpiContext::new(
            ctx.accounts.token_program.key(),
            MintTo {
                mint: ctx.accounts.token.to_account_info(),
                to: ctx.accounts.token_account.to_account_info(),
                // Mint authority is the state PDA (set at initialize): sign the
                // CPI with its seeds so only this instruction — after the
                // deposit and SCRAM checks above — can mint.
                authority: ctx.accounts.state.to_account_info(),
            },
        )
        .with_signer(&[&[
            NORI_SOL_TOKEN_BRIDGE_STATE_SEED,
            &[ctx.bumps.state],
        ]]),
        amount_to_mint,
    )?;

    // ================================================================
    // Emit success event / message
    // ================================================================

    emit!(MintApplied {
        recipient: ctx.accounts.recipient.key(),
        deposit_root: root.into(),
        amount_minted: amount_to_mint,
        minted_so_far: storage.minted_so_far,
    });

    msg!(
        "Mint applied: recipient {}, amount {}, minted_so_far {}",
        ctx.accounts.recipient.key(),
        amount_to_mint,
        storage.minted_so_far
    );

    Ok(())
}
