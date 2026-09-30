# AGENTS.md — working notes for AI agents

## What this repo is

SDK + on-chain code for the Nori Ethereum→Solana token bridge:

- Users lock ETH in `NoriTokenBridge.sol`; each lock enqueues a storage-proof
  request on `NoriProofRequestQueue.sol`.
- [nori-bridge-head](https://github.com/Nori-zk/nori-bridge-head) (Helios
  light client + SP1) proves Ethereum consensus/execution state and folds
  pending deposits into a Merkle root.
- The Solana program (`programs/token`) verifies SP1 Groth16 proofs via
  [sp1-solana](https://github.com/Nori-zk/sp1-solana) and mints SPL tokens
  against proven deposits.

The `ethereum/` contracts take the Nori bridge's Ethereum-side design as a
baseline, scoped down for this route: one-way, lock only, no unlock path.

## Layout

| Path | Contents |
| --- | --- |
| `ethereum/contracts/` | `NoriTokenBridge.sol`, `NoriProofRequestQueue.sol`, `TimeLockController.sol` (vanilla OZ, for deploys) |
| `ethereum/tasks/` | Hardhat tasks: deploy, deployTimelock, lockTokens, fee admin, previews |
| `ethereum/test/` | Mocha tests (run via Hardhat); `test-vectors/` holds storage-layout vectors shared with the SP1 guest |
| `ethereum/types/ethers-contracts/` | **Generated** by `hardhat compile`; committed after `stabilize-types.mjs` sorts unstable lines. Never hand-edit |
| `programs/token/` | Anchor program: `initialize`, `mint`, `update` handlers + state |
| `DEPLOYMENT.md` | Production runbook (Safe → Timelock → ETH contracts → Solana program) |
| `DEVELOPMENT_GUIDE.md` | Toolchain setup (Solana CLI, Anchor, Surfpool) |

## Commands that work today

```bash
cd ethereum
npm ci
npm test                    # ETH_NETWORK=hardhat hardhat test → 126 passing
npm run typecheck           # tsc --noEmit → clean
npm run build               # compile + stabilize-types + tsc -p tsconfig.package.json
cargo check -p token        # from repo root; native check of the Solana program → clean
```

## Commands that do NOT work today (known, not your fault)

- `anchor build` — alloy 1.8 crates (via helios 0.11.0) require rustc ≥ 1.90;
  the SBF toolchain ships 1.89. Intended fix: downgrade alloy versions in
  `Cargo.lock` (`cargo update <name> --precise <older>`), not done yet.
- `cargo test -p token` — the LiteSVM test `include_bytes!`s
  `target/deploy/token.so`, which only `anchor build` produces. Blocked by
  the above. Host-side `cargo check -p token --tests` passes except for that
  missing artifact.

## Invariants that bite if broken

- `lockedTokens` must stay at storage slot 2 of `NoriTokenBridge.sol`
  (`LOCKED_TOKENS_SLOT_INDEX`, and the "Should keep lockedTokens at slot 2"
  test). Reordering state vars above it invalidates every enqueued proof
  request and the SP1 circuit.
- The storage-slot indices in `test/NoriProofRequestQueue.ts` are mirrored in
  the SP1 guest (`nori-primitives` in nori-bridge-head). Changing them
  invalidates previously generated proofs.
- 1 bridge unit = 10¹² wei on the Ethereum side (`DECIMALS = 6`).
  `MAX_MAGNITUDE = 2⁶⁴−1` BU keeps the Solana-side `u64` mint conversion
  sound — do not raise `DECIMALS` or drop the cap.
- The bridge pins `proofQueue` as an immutable with no setter; the Solana
  side pins the same two addresses at `initialize`. Both sides move together
  or not at all.

## Known open items (also in README.md)

- `mint`/`update` handlers are not registered in the `#[program]` block in
  `programs/token/src/lib.rs` — only `initialize` is reachable on-chain.
- `mint` computes the witness root but does not check it against the ring
  buffer yet (deposit membership unverified).

## Conventions

- Docs and comments must not reference the chain this SDK was forked from;
  describe the ETH→Solana design as it stands.
- Run `npm test` in `ethereum/` after any contract change; regenerate types
  via `npm run build` and commit the stabilized output.
- `.env.nori-eth-token-bridge` / `.env.nori-eth-timelock` are gitignored
  deploy outputs; the `.example` files are the committed templates.
- Keep README.md, DEPLOYMENT.md, and this file in sync with code changes.
