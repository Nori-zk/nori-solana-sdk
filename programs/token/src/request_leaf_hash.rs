use alloy_primitives::{Address, B256, U256};
use solana_sha256_hasher::hashv;

// FIXME migrate this to its own crate the client needs to do this offline
// FIXME migrate to the sha scheme the high byte shenanigans are not needed.
// FIXME a branch of bridge head could be done so this could simply be imported.

/// Field layout:
/// - field 1: `target` (20) ++ `collection_keys_count` ++ `key_0[0]` ++ `key_1[0]` ++ `value[0]`
/// - field 2: `key_0[1..32]`
/// - field 3: `key_1[1..32]`
/// - field 4: `value[1..32]`
///
/// `collection_keys_count` is hashed so that an unused trailing key, which is
/// zero, cannot collide with a request that supplied a zero key.
///
pub fn pack_request_leaf_fields(
    target: &Address,
    collection_keys_count: u8,
    collection_key_0: &B256,
    collection_key_1: &B256,
    value: &U256,
) -> [[u8; 32]; 4] {
    let target_bytes = target.as_slice();
    let key_0_bytes = collection_key_0.as_slice();
    let key_1_bytes = collection_key_1.as_slice();
    let value_bytes = value.to_be_bytes::<32>();

    let mut first_field_bytes = [0u8; 32];
    first_field_bytes[0..20].copy_from_slice(target_bytes);
    first_field_bytes[20] = collection_keys_count;
    first_field_bytes[21] = key_0_bytes[0];
    first_field_bytes[22] = key_1_bytes[0];
    first_field_bytes[23] = value_bytes[0];

    let mut second_field_bytes = [0u8; 32];
    second_field_bytes[0..31].copy_from_slice(&key_0_bytes[1..32]);

    let mut third_field_bytes = [0u8; 32];
    third_field_bytes[0..31].copy_from_slice(&key_1_bytes[1..32]);

    let mut fourth_field_bytes = [0u8; 32];
    fourth_field_bytes[0..31].copy_from_slice(&value_bytes[1..32]);

    [
        first_field_bytes,
        second_field_bytes,
        third_field_bytes,
        fourth_field_bytes,
    ]
}

/// Hashes one verified queue request into a Merkle leaf.
///
/// Packs 117 bytes of leaf data into four 32-byte fields via
/// `pack_request_leaf_fields`, then applies SHA-256.
pub fn request_leaf_hash(
    target: &Address,
    collection_keys_count: u8,
    collection_key_0: &B256,
    collection_key_1: &B256,
    value: &U256,
) -> B256 {
    let fields = pack_request_leaf_fields(
        target,
        collection_keys_count,
        collection_key_0,
        collection_key_1,
        value,
    );
    B256::from(
        hashv(&[
            fields[0].as_slice(),
            fields[1].as_slice(),
            fields[2].as_slice(),
            fields[3].as_slice(),
        ])
        .to_bytes(),
    )
}
