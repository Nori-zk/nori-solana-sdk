use anchor_lang::prelude::*;
use anchor_lang::system_program::{
    allocate, assign, create_account, transfer, Allocate, Assign, CreateAccount, Transfer,
};

/// Creates `pda` as a rent-exempt account of `space` bytes owned by this
/// program, signing for it with `seeds` (which must include the bump).
///
/// PDA addresses are predictable, so anyone can send lamports to one before
/// the program creates it. `create_account` rejects an address that already
/// holds lamports, which would make creation fail forever; a pre-funded
/// address is instead topped up to rent exemption, allocated and assigned,
/// the same path Anchor's `init` takes. Only this program can sign for the
/// PDA, so a pre-funded address can carry lamports but no data or owner.
pub(crate) fn create_program_owned_pda<'info>(
    payer: AccountInfo<'info>,
    pda: AccountInfo<'info>,
    system_program: Pubkey,
    seeds: &[&[u8]],
    space: usize,
) -> Result<()> {
    let rent_exempt_lamports = Rent::get()?.minimum_balance(space);
    let current_lamports = pda.lamports();

    if current_lamports == 0 {
        create_account(
            CpiContext::new(
                system_program,
                CreateAccount {
                    from: payer,
                    to: pda,
                },
            )
            .with_signer(&[seeds]),
            rent_exempt_lamports,
            space as u64,
            &crate::ID,
        )?;
        return Ok(());
    }

    let top_up_lamports = rent_exempt_lamports.saturating_sub(current_lamports);
    if top_up_lamports > 0 {
        transfer(
            CpiContext::new(
                system_program,
                Transfer {
                    from: payer,
                    to: pda.clone(),
                },
            ),
            top_up_lamports,
        )?;
    }
    allocate(
        CpiContext::new(
            system_program,
            Allocate {
                account_to_allocate: pda.clone(),
            },
        )
        .with_signer(&[seeds]),
        space as u64,
    )?;
    assign(
        CpiContext::new(
            system_program,
            Assign {
                account_to_assign: pda,
            },
        )
        .with_signer(&[seeds]),
        &crate::ID,
    )?;
    Ok(())
}
