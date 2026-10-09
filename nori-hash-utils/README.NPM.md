# @nori-zk/ethereum-solana-proof-queue-utils-glam

WebAssembly utilities for the Nori Ethereum to Solana bridge's proof queue: request leaf hashing, batch roots and Merkle witnesses.

## Installation

```bash
npm install @nori-zk/ethereum-solana-proof-queue-utils-glam
```

## Overview

Every hash in this package is computed by `nori_hash::merkle_sha256_fixed` from nori-bridge-head, the same code the SP1 guest runs to build each proof queue batch's `verified_requests_root`. A witness built here verifies against the batch root the Solana program stores.

All hashes, addresses, keys and values are 0x-prefixed hex strings.

## TypeScript API

--------------------------------------------------------------------------------------------

### Types

#### `RequestLeaf`
One proof request queue entry, as the bridge hashes it into a Merkle leaf.

Every value is 0x-prefixed hex:
- `target`: the 20-byte address whose storage the request proves.
- `collectionKeysCount`: how many collection keys the request supplied.
- `collectionKeys`: up to two 32-byte keys; absent keys hash as zero.
- `value`: the 32-byte big-endian storage word read at the batch's output block.

```typescript
interface RequestLeaf {
  target: string;
  collectionKeysCount: number;
  collectionKeys: string[];
  value: string;
}
```

#### `RequestBatch`
Every request in one proof queue batch, in queue order.

```typescript
interface RequestBatch {
  leaves: RequestLeaf[];
}
```

#### `RequestWitnessInput`
Every request in one proof queue batch, in queue order, and the index of the request to build a witness for (its request id minus the batch's `inputQueueCursor`).

```typescript
interface RequestWitnessInput {
  leaves: RequestLeaf[];
  index: number;
}
```

#### `RequestWitness`
The Merkle witness for one request in a proof queue batch, as 0x-prefixed hex.

- `root`: the batch root; it must equal the root committed on Solana.
- `index`: the request's leaf index in the batch.
- `leaf`: the request's leaf hash.
- `path`: the sibling hashes from the leaf up to the root, bottom-up.

```typescript
interface RequestWitness {
  root: string;
  index: number;
  leaf: string;
  path: string[];
}
```

#### `MerkleRootFromPathInput`
A leaf hash, its leaf index and its bottom-up sibling path, as 0x-prefixed hex.

```typescript
interface MerkleRootFromPathInput {
  leaf: string;
  index: number;
  path: string[];
}
```

--------------------------------------------------------------------------------------------

### Functions

#### `request_leaf_hash`

```typescript
export function request_leaf_hash(leaf: RequestLeaf): string;
```

Hashes one proof request queue entry into its Merkle leaf, exactly as the SP1 guest does (`hash_request_leaf`).

Returns the leaf hash as 0x-prefixed hex.

**Errors**

Throws if:
- `target`, a collection key or `value` is not valid hex of the expected length
- More than two collection keys are supplied

#### `request_batch_root`

```typescript
export function request_batch_root(batch: RequestBatch): string;
```

Computes a proof queue batch's root from every request in the batch, in queue order, exactly as the SP1 guest computes `verified_requests_root`.

Returns the root as 0x-prefixed hex.

**Errors**

Throws if:
- The batch is empty
- The batch has more requests than `MAX_BATCH`
- Any request fails `request_leaf_hash`

#### `request_witness`

```typescript
export function request_witness(input: RequestWitnessInput): RequestWitness;
```

Builds the Merkle witness for one request in a proof queue batch from every request in the batch, in queue order. The witness's `root` must equal the batch root committed on Solana.

**Errors**

Throws if:
- The batch fails `request_batch_root`
- `index` is outside the batch

#### `merkle_root_from_path`

```typescript
export function merkle_root_from_path(input: MerkleRootFromPathInput): string;
```

Recomputes a Merkle root from a leaf hash, its leaf index and its bottom-up sibling path (`compute_merkle_root_from_path`).

Returns the root as 0x-prefixed hex.

**Errors**

Throws if the leaf or a path entry is not a 32-byte hex value.

## Usage Examples

### Building a request's witness

```typescript
import { request_witness } from '@nori-zk/ethereum-solana-proof-queue-utils-glam';

// Every request in the batch, in queue order, read from Ethereum
const leaves = [
  {
    target: '0x5FbDB2315678afecb367f032d93F642f64180aa3',
    collectionKeysCount: 1,
    collectionKeys: ['0x000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266'],
    value: '0x00000000000000000000000000000000000000000000000000000000000f4240',
  },
];

const witness = request_witness({ leaves, index: 0 });

// witness.root must equal the batch root committed on Solana
```

### Checking a witness

```typescript
import { merkle_root_from_path } from '@nori-zk/ethereum-solana-proof-queue-utils-glam';

const root = merkle_root_from_path({
  leaf: witness.leaf,
  index: witness.index,
  path: witness.path,
});

// root === witness.root
```

## License

Apache-2.0
