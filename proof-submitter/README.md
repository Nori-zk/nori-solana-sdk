# proof-submitter

Client crate for the Nori Ethereum→Solana bridge. It loads SP1 Groth16
proof JSONs — as produced by
[nori-bridge-head](https://github.com/Nori-zk/nori-bridge-head) — into the
on-chain wire format and submits `update` transactions to the deployed
token program over JSON-RPC.

Library only: `update` submission is driven from nori-bridge-head's
submitter side. The one-off `initialize` of a deployed program is sent
with `nori-cli` (`cli/`, DEPLOYMENT.md §6), built on this crate.

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
  hash; must equal the `nori_bridge_vk` that `initialize` pins from
  `nori-bridge-head/nori-elf`.

## Submitting

`SolanaProofSubmitter`:

- `from_env()` — config from environment (see below); `.env` files are
  picked up via dotenvy.
- `new(rpc_url, payer, program_id)` — explicit construction.
- `build_update_instructions(&proof.wire, proof_queue_batch_count)` — the
  `update` instruction (payer, state, the proof queue batch PDA for
  `proof_queue_batch_count`, system program) plus a 200k
  compute-unit-limit instruction (measured ~103.4k CU per update with an
  empty batch; unused units are not charged).
- `deploy_program(&program_keypair, &so_bytes).await` — deploys a compiled
  program via the upgradeable loader (create buffer → chunked writes →
  deploy). Dev/test convenience: sequential, no resume. Production deploys
  should use the CLI below, which batches in parallel and resumes.
- `build_initialize_instruction(init_values)` — one-off bridge setup.
- `state_address()` / `mint_address()` — the state (`[b"STATE"]`) and
  mint (`[b"NETH"]`) PDAs for the configured program id.
- `submit_initialize(init_values).await` — sends and confirms the one-off
  `initialize` transaction (creates the state PDA and the token mint). The
  init values come from the first proof:
  `proofs[0].bridge_init(verified_state_root, eth_token_bridge_address)`,
  which resumes the bridge from the proof's input side (slot, store hash,
  queue cursor) and pins its queue address.
- `submit_update(&proof.wire).await` — reads `proof_queue_batch_count`
  from the bridge state, then sends and confirms the transaction; returns
  `SolanaTransactionResult { tx_hash, cu_consumed }`. If another update
  lands between the read and the send, the program's continuity checks
  reject this one.

### Environment variables

| Variable | Required | Meaning |
|---|---|---|
| `SOLANA_RPC_NETWORK_URL` | yes | Solana JSON-RPC endpoint |
| `SOLANA_PAYER_KEYPAIR_PATH` | yes | Payer keypair file (Solana CLI `id.json` format: JSON array of 64 bytes) |
| `NORI_SOL_TOKEN_PROGRAM_ID` | no | Program id override; defaults to the program's declared id |

## Build & deploy the program

The program must be deployed before any `update` will land. For tests and
local validators, `SolanaProofSubmitter::deploy_program` does it from
Rust — that is what the e2e suite uses, so it needs only `surfpool`.
Production deploys use the CLI (parallel batches, resumable). From the
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
  validator through `test-utils` (`surfpool start --offline --no-deploy
  --ci` on free ports, killed on drop), deploys the program, initializes
  the bridge from the first example proof, and submits updates through
  `SolanaProofSubmitter` over RPC.

Run the host tests only with
`cargo test -p proof-submitter --test proof_file`.

The example proofs live in `proof-submitter/example-proofs/`: four
chained `update` proofs with empty batches (no deposits), so they create
no proof queue batch accounts and do not exercise `mint`. `mint` is
covered by `programs/token/tests/test_mint.rs`, which writes the batch
account directly with surfpool's `surfnet_setAccount` cheatcode.

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

Then initialize the bridge with the first proof's init values — from the
repo root, `cargo run -p nori-cli -- initialize --proof <first proof>
--verified-state-root <root> --eth-token-bridge-address <addr>` (it reads
the two variables above), or in Rust
`proofs[0].bridge_init(verified_state_root, eth_token_bridge_address)` via
`submit_initialize` — and call `submit_update` per proof; see
`tests/update_surfpool.rs` for the full sequence.
