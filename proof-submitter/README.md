# proof-submitter

Client crate for the Nori Ethereum→Solana bridge. It loads SP1 Groth16
proof JSONs — as produced by
[nori-bridge-head](https://github.com/Nori-zk/nori-bridge-head) — into the
on-chain wire format and submits `update` transactions to the deployed
token program over JSON-RPC.

Library only: there is no CLI/binary yet; submission is driven from
nori-bridge-head's submitter side.

## Loading proofs

`load_update_proof(path)` loads one JSON; `load_update_proofs_dir(dir)`
loads every `*.json` in a directory, ordered by file name (the
`<slot>-v<x.y.z>.json` naming sorts into chain-continuation order). Both
return `LoadedProof { wire: SP1Groth16Proof, program_vkey: [u8; 32] }`.

Wire format, from the proof JSON fields:

- `wire.proof` — 356 bytes: the first 4 bytes of `groth16_vkey_hash` ++
  the 352-byte `encoded_proof`
  (`[exit_code 32][vk_root 32][nonce 32][groth16 256]`).
- `wire.sp1_public_inputs` — `public_values.buffer.data`, the 220-byte
  Borsh `ProofOutputs`.
- `program_vkey` — `public_inputs[0]` (decimal), the SP1 program vkey
  hash to pin as `nori_bridge_vk` at `initialize`.

## Submitting

`SolanaProofSubmitter`:

- `from_env()` — config from environment (see below); `.env` files are
  picked up via dotenvy.
- `new(rpc_url, payer, program_id)` — explicit construction.
- `build_update_instructions(&proof.wire)` — the `update` instruction
  plus a 1.4M compute-unit-limit instruction (Groth16 verification is
  the dominant cost; unused units are not charged).
- `build_initialize_instruction(init_values)` — one-off bridge setup.
- `submit_update(&proof.wire).await` — sends and confirms the
  transaction, returns `SolanaTransactionResult { tx_hash }`.

### Environment variables

| Variable | Required | Meaning |
|---|---|---|
| `SOLANA_RPC_NETWORK_URL` | yes | Solana JSON-RPC endpoint |
| `SOLANA_PAYER_KEYPAIR_PATH` | yes | Payer keypair file (Solana CLI `id.json` format: JSON array of 64 bytes) |
| `NORI_SOL_TOKEN_PROGRAM_ID` | no | Program id override; defaults to the program's declared id |

## Build & deploy the program

The program must be deployed before any `update` will land. From the
repo root:

```bash
export PATH="$HOME/.local/share/solana/install/active_release/bin:$PATH"

# CFLAGS points ring's C build at the platform-tools freestanding headers
CFLAGS="-isystem $HOME/.cache/solana/v1.54/platform-tools/llvm/sbpf/include" \
    cargo build-sbf --manifest-path programs/token/Cargo.toml

solana program deploy target/deploy/token.so --url <cluster>
```

## Tests

From the repo root:

```bash
export PATH="$HOME/.local/share/solana/install/active_release/bin:$PATH"
cargo test -p proof-submitter
```

Two suites:

- `tests/proof_file.rs` — host tests for the JSON loader.
- `tests/update_surfpool.rs` — end-to-end against a local Surfnet
  validator. Requires `surfpool` and the `solana` CLI on PATH and
  `target/deploy/token.so` built (above). Each test boots its own
  `surfpool start --offline --no-deploy --ci` on free ports, deploys the
  program, initializes the bridge from the first example proof, and
  submits updates through `SolanaProofSubmitter` over RPC.

Run the host tests only with
`cargo test -p proof-submitter --test proof_file`.

The example proofs live in `proof-submitter/example-proofs/`: four
chained `update` proofs with no deposits, so `mint` is not exercised by
them.

## Manual smoke test on a local surfpool

```bash
# 1. Start a local validator (RPC on 8899, ws on 8900). It airdrops to
#    ~/.config/solana/id.json on start.
surfpool start --offline --no-deploy

# 2. Deploy the program (separate shell).
export PATH="$HOME/.local/share/solana/install/active_release/bin:$PATH"
solana program deploy target/deploy/token.so --url http://127.0.0.1:8899

# 3. Point the submitter at it.
export SOLANA_RPC_NETWORK_URL=http://127.0.0.1:8899
export SOLANA_PAYER_KEYPAIR_PATH=$HOME/.config/solana/id.json
```

Then initialize the bridge (`build_initialize_instruction`, with
`nori_bridge_vk` set to the first proof's `program_vkey` and the
continuity fields from its public values) and call `submit_update` per
proof — see `tests/update_surfpool.rs` for the full sequence.
