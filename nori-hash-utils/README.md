# @nori-zk/nori-hash-utils

Proof queue request leaf and Merkle witness hashing for the Nori bridge,
compiled to WebAssembly from `nori-hash`, the code the SP1 guest runs to
build each proof queue batch's `verified_requests_root`. A witness built
here verifies against the batch root the Solana program stores.

## Functions

| Function | Returns |
|---|---|
| `request_leaf_hash(leaf)` | The request's leaf hash |
| `request_batch_root({ leaves })` | The batch root over every request in the batch, in queue order |
| `request_witness({ leaves, index })` | `{ root, index, leaf, path }` for the request at `index` |
| `merkle_root_from_path({ leaf, index, path })` | The root recomputed from a leaf, its index and its bottom-up path |

All values are 0x-prefixed hex. Types and their docs are in
`pkg/nori_hash_utils.d.ts`, generated from the Rust with `tsify`.

## Build

From the nori-solana-sdk root (see its README for prerequisites):

```bash
npm run build -w @nori-zk/nori-hash-utils
```

This runs `wasm-pack build --no-pack --features wasm` into `pkg/`. The crate
is a member of the SDK's Cargo workspace and shares its lock, and depends on
`nori-hash` from nori-bridge-head's `FEAT/solana-bridge-sepolia-glamsterdam`
branch without its default `helios` feature.
