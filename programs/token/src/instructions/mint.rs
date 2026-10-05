use alloy_primitives::{hex, Address, B256};
use anchor_lang::prelude::*;
use anchor_spl::associated_token::{self, AssociatedToken};
use anchor_spl::token_interface::{mint_to, Mint as Token, MintTo, TokenInterface};
use solana_sha256_hasher::hash;

use crate::{
    constants::*, deposit_witness::VerifiedRequestWitnessInput, pda::create_program_owned_pda,
    state::NoriSolTokenAccountStorage, state::NoriSolTokenBridge, state::ProofRequestRootEntry,
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
    #[msg("Recipient pubkey does not hash to the deposit commitment")]
    CommitmentMismatch,
    #[msg("Witness root does not match the committed proof queue batch root")]
    ProofQueueBatchRootMismatch,
    #[msg("Witness index is outside the committed proof queue batch")]
    WitnessIndexOutsideProofQueueBatch,
    #[msg("Deposit witness is malformed")]
    InvalidDepositWitness,
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

    // Read-only: mint only reads state and signs as the mint authority with
    // the state PDA's seeds, so it does not write-lock state against update.
    #[account(seeds = [NORI_SOL_TOKEN_BRIDGE_STATE_SEED], bump)]
    pub state: AccountLoader<'info, NoriSolTokenBridge>,
    #[account(mut, seeds = [NORI_SOL_TOKEN_BRIDGE_SEED], bump)]
    pub token: Box<InterfaceAccount<'info, Token>>,
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

    /// The committed proof queue batch the deposit witness proves against.
    /// `Account` checks it is owned by this program and carries the
    /// `ProofRequestRootEntry` discriminator; only `update` creates such
    /// accounts, at the proof queue batch PDAs.
    pub proof_queue_batch: Account<'info, ProofRequestRootEntry>,

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
///   committed `proof_queue_batch` the witness proves against.
/// * `deposit_witness` - The Merkle witness proving a `VerifiedRequest` leaf
///   (target, collection keys, locked value) is present at `index` in the
///   deposit tree, resolving to a root via [`VerifiedRequestWitnessInput::root`]
///   that must equal the committed batch root, with `index` inside the
///   batch's cursor range.
///
/// The deposit leaf's first collection key is the commitment
/// `sha256(recipient_pubkey)` placed by the depositor on Ethereum. Claiming
/// requires no witness beyond the recipient's own signature on the
/// transaction: the program re-hashes the `recipient` signer key and compares
/// it against the committed key. The recipient stays hidden on Ethereum until
/// the first claim (hash preimage), and only the holder of the recipient key
/// can ever claim.
///
/// # Errors
///
/// Returns a [`MintError`] if the deposit witness is malformed (path longer
/// than `MAX_TREE_DEPTH`, index beyond `MAX_BATCH`, too many collection
/// keys), if the witness root does not match the committed
/// proof queue batch root or its index falls outside the batch, if the
/// deposit leaf's target does not match the bridge's configured token bridge
/// address, or if `sha256(recipient)` does not equal the leaf's first
/// collection key.
pub fn handle_mint(ctx: Context<Mint>, deposit_witness: VerifiedRequestWitnessInput) -> Result<()> {
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

    // Create the per-recipient minted-so-far storage account if this program
    // does not own it yet; leave it untouched if it does. Ownership, not a
    // zero balance, marks "not created": the address is predictable and may
    // already hold lamports someone sent to it.
    if *ctx.accounts.token_account_storage.owner != crate::ID {
        let recipient_key = ctx.accounts.recipient.key();
        let bump = ctx.bumps.token_account_storage;
        let seeds: &[&[u8]] = &[
            NORI_SOL_TOKEN_ACCOUNT_STORAGE_SEED,
            recipient_key.as_ref(),
            &[bump],
        ];

        create_program_owned_pda(
            ctx.accounts.payer.to_account_info(),
            ctx.accounts.token_account_storage.to_account_info(),
            ctx.accounts.system_program.key(),
            seeds,
            8 + NoriSolTokenAccountStorage::INIT_SPACE,
        )?;

        let storage = NoriSolTokenAccountStorage { minted_so_far: 0 };
        let mut data = ctx.accounts.token_account_storage.try_borrow_mut_data()?;
        storage.try_serialize(&mut &mut data[..])?;
    }

    // ================================================================
    // Deposit proof validation
    // ================================================================

    deposit_witness.validate().map_err(|e| {
        msg!("Invalid deposit witness: {}", e);
        error!(MintError::InvalidDepositWitness)
    })?;

    let root = deposit_witness.root();
    let request = &deposit_witness.value;
    let proof_queue_batch = &ctx.accounts.proof_queue_batch;

    (root == B256::from(proof_queue_batch.root))
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Proof queue batch root mismatch, witness root: {}, committed root: 0x{}",
                root,
                hex::encode(proof_queue_batch.root)
            );
            error!(MintError::ProofQueueBatchRootMismatch)
        })?;

    let proof_queue_batch_size = proof_queue_batch
        .output_queue_cursor
        .saturating_sub(proof_queue_batch.input_queue_cursor);
    (deposit_witness.index < proof_queue_batch_size)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Witness index {} outside proof queue batch of {} requests",
                deposit_witness.index,
                proof_queue_batch_size
            );
            error!(MintError::WitnessIndexOutsideProofQueueBatch)
        })?;

    let eth_token_bridge_address = {
        let state = ctx.accounts.state.load()?;
        state.eth_token_bridge_address
    };

    (request.target == Address::from(eth_token_bridge_address))
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Deposit target mismatch, leaf target: {}, expected token bridge address: 0x{}",
                request.target,
                hex::encode(eth_token_bridge_address)
            );
            error!(MintError::NotTokenBridgeRequest)
        })?;

    // The deposit committed to sha256(recipient_pubkey). The recipient is a
    // required signer, so a matching hash proves the claimant holds the key.
    let recipient_commitment = hash(ctx.accounts.recipient.key().as_ref()).to_bytes();
    (request.collection_keys[0].0 == recipient_commitment)
        .then_some(())
        .ok_or_else(|| {
            msg!(
                "Commitment mismatch: leaf commits to 0x{}, sha256(recipient) is 0x{}",
                hex::encode(request.collection_keys[0].0),
                hex::encode(recipient_commitment)
            );
            error!(MintError::CommitmentMismatch)
        })?;

    msg!(
        "deposit root verified against proof queue batch: {:?}",
        root
    );

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

    (amount_to_mint > 0).then_some(()).ok_or_else(|| {
        msg!("No new amount to mint");
        error!(MintError::ZeroMintAmount)
    })?;

    storage.minted_so_far = locked_so_far;
    storage.try_serialize(&mut &mut storage_data[..])?;

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
                // deposit and commitment checks above — can mint.
                authority: ctx.accounts.state.to_account_info(),
            },
        )
        .with_signer(&[&[NORI_SOL_TOKEN_BRIDGE_STATE_SEED, &[ctx.bumps.state]]]),
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
