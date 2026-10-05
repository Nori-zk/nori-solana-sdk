import { type Account, type Address, type ReadonlyUint8Array } from '@solana/kit';
import { SolanaAccountMismatchError } from './errors.js';

/**
 * Checks a fetched account is owned by the program and carries the
 * expected Anchor discriminator before its decoded data is trusted.
 */
export function assertProgramAccount<TData extends { discriminator: ReadonlyUint8Array }>(
    account: Account<TData>,
    programAddress: Address,
    discriminator: ReadonlyUint8Array
): void {
    const owned = account.programAddress === programAddress;
    const tagged =
        account.data.discriminator.length === discriminator.length &&
        discriminator.every((byte, index) => account.data.discriminator[index] === byte);
    if (!owned || !tagged) {
        throw new SolanaAccountMismatchError(
            account.address,
            `Account ${account.address} is not a ${programAddress} program account of the expected type.`
        );
    }
}
