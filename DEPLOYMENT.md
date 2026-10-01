# Nori Ethereum→Solana Bridge — Production Deployment Runbook

End-to-end procedure to deploy the ETH→SOL bridge: the Ethereum contracts
(`ethereum/`) and the Solana program (`programs/token`). Numbered steps record
values into the deployment ledger (§8).

---

## 0. Inputs and conventions

| Symbol     | Meaning                                  |
| ---------- | ---------------------------------------- |
| `Operator` | The Ethereum SAFE multisig               |
| `Timelock` | OZ `TimelockController` instance         |
| `EthBridge` | The deployed `NoriTokenBridge.sol` address |
| `EthQueue` | The deployed `NoriProofRequestQueue.sol` address |
| `Program`  | The deployed Solana `token` program id   |

- All `bytes32` values are 0x-prefixed 64 hex characters (big-endian).
- All Ethereum addresses are 0x-prefixed 40 hex characters.
- The Solana bridge state PDA and mint PDA are derived from the program id;
  they are not inputs.

---

## 1. Set up the Ethereum SAFE

Set up a multisig SAFE on the target Ethereum network (mainnet, Sepolia, …).

### Record

- [ ] `OperatorSafeAddress`: `0x...`

> The SAFE is **not** the bridge operator directly — it becomes the proposer
> and executor of the Timelock in §2. The Timelock is the operator.

---

## 2. Deploy `TimelockController` (Ethereum)

```bash
cd ethereum
cp .env.nori-eth-timelock.example .env   # fill in
npm run deploy-timelock
```

Constructor args (from env):

| Param       | Value                                                       |
| ----------- | ----------------------------------------------------------- |
| `minDelay`  | `NORI_ETH_TIMELOCK_MIN_DELAY_SEC` — recommended `172800` (48 h) |
| `proposers` | `NORI_ETH_TIMELOCK_PROPOSERS` — the SAFE                    |
| `executors` | `NORI_ETH_TIMELOCK_EXECUTORS` — the SAFE (or `0x0…0` for permissionless execution) |
| `admin`     | `NORI_ETH_TIMELOCK_ADMIN` — omit for `address(0)` (self-administered) |

The task writes `.env.nori-eth-timelock` with the deployed address.

### Record

- [ ] `TimelockAddress`: `0x...`
- [ ] `TimelockMinDelay`: e.g. `172800`

---

## 3. Deploy the Ethereum contracts

`npm run deploy` (tasks/deploy.ts) deploys **two** contracts in one run and
wires them together:

1. `NoriProofRequestQueue` — args: `bridgeOperator`, `feeRecipient`, `proofRequestQueueFeeWei`
2. `NoriTokenBridge` — args: `(_bridgeOperator, _proofQueueAddr, _feeRecipient)`

### Required env

```bash
ETH_NETWORK=<network>
ETH_PRIVATE_KEY=<deployer key>
ETH_RPC_URL=<rpc url>

# Operator → the Timelock from §2, not the SAFE
NORI_ETH_BRIDGE_OPERATOR_ADDRESS=<TimelockAddress>

# Fee config (optional)
NORI_ETH_BRIDGE_FEE_RECIPIENT_ADDRESS=<treasury or unset>
NORI_ETH_BRIDGE_LOCK_FEE_RATE=<e.g. 500 = 0.5%>
NORI_ETH_BRIDGE_PROOF_REQUEST_QUEUE_FEE_WEI=<wei, multiple of 1e12>
```

### Run

```bash
cd ethereum
npm run deploy
```

Addresses are written to `.env.nori-eth-token-bridge`.

### Record

- [ ] `EthQueue`: `0x...` (`NORI_ETH_PROOF_QUEUE_ADDRESS`)
- [ ] `EthBridge`: `0x...` (`NORI_ETH_TOKEN_BRIDGE_ADDRESS`)

### Verify

```bash
cast call <EthBridge> "bridgeOperator()(address)"     # == TimelockAddress
cast call <EthBridge> "proofQueue()(address)"         # == EthQueue
cast call <EthBridge> "feeRecipient()(address)"
```

`EthBridge` and `EthQueue` are the `eth_token_bridge_address` and
`eth_proof_queue_address` for the Solana `initialize` in §6.

---

## 4. Record bridge integrity constants

Fixed by the [nori-bridge-head](https://github.com/Nori-zk/nori-bridge-head)
circuit build, not generated at deploy. Pull them from the build that produced
the SP1 vkey and freeze them.

### Record

- [ ] `noriBridgeVk` (bytes32) — SP1 vkey hash of the bridge-head program
- [ ] `initialVerifiedStateRoot` (bytes32) — execution state root at the chosen start block
- [ ] `initialStoreHash` (bytes32) — Helios store input hash at the start point
- [ ] `initialQueueCursor` (u64) — `0` if the queue is fresh; otherwise the
      queue's settled cursor at the start point

---

## 5. Build and deploy the Solana program

```bash
CFLAGS="-isystem $HOME/.cache/solana/v1.54/platform-tools/llvm/sbpf/include" \
    cargo build-sbf --manifest-path programs/token/Cargo.toml
solana program deploy target/deploy/token.so   # --url per target cluster
```

> The alloy tree is pinned to 1.6.3 in Cargo.lock because the SBF toolchain
> ships rustc 1.89 (alloy 1.7+ requires 1.91). The `CFLAGS` line points
> ring's C build at the platform-tools freestanding headers. Both are also
> noted in README.md.

### Record

- [ ] `Program`: the deployed program id
- [ ] `ProgramSoSha256`: sha256 of the deployed `.so`

The program id is also the derive base for the state PDA (`[b"STATE"]`) and
mint PDA (`[b"NETH"]`) — record the derived addresses after §6.

---

## 6. Initialize the Solana bridge state

One `initialize` call creates the state PDA and the SPL mint (12 decimals,
mint & freeze authority = state PDA) and pins the integrity constants.

Init values (`NoriSolTokenBridgeInit`):

| Field                          | Source                    |
| ------------------------------ | ------------------------- |
| `verified_state_root`          | §4 `initialVerifiedStateRoot` |
| `latest_helios_store_input_hash` | §4 `initialStoreHash`   |
| `eth_proof_queue_address`      | §3 `EthQueue`             |
| `eth_token_bridge_address`     | §3 `EthBridge`            |
| `latest_head`                  | beacon slot at the start point — the first `update` must resume from it |
| `queue_cursor`                 | §4 `initialQueueCursor`   |

The transaction payer becomes `authority` in the stored state. `update` is
permissionless (only a valid proof matters), so `authority` currently has no
privileged instruction — keep it a controlled key regardless.

> Tooling gap: no initialize CLI ships in this repo yet — invoke through a
> script built on the generated IDL/client. Track in Appendix B.

### Record

- [ ] Initialize tx signature
- [ ] State PDA, mint PDA addresses

---

## 7. Post-deploy hardening

1. **Timelock admin**: confirm
   `TimelockController.hasRole(DEFAULT_ADMIN_ROLE, <SAFE>)` is `false` and the
   contract is self-administered (automatic if `admin = address(0)` in §2).
2. **Dry-run the SAFE → Timelock → bridge path**: schedule a no-op admin call
   (e.g. set the lock fee rate to its current value) through the Timelock
   before any value flows.
3. **First proof end-to-end**: submit one `update` carrying a real
   nori-bridge-head proof, then one `mint` against a real deposit, on a
   testnet before mainnet.
4. **Archive env outputs**: `.env.nori-eth-token-bridge`,
   `.env.nori-eth-timelock`, program id, and the §4 constants go in the
   deployment ledger (no secrets).

---

## 8. Final ledger (fill in)

| Field                          | Value |
| ------------------------------ | ----- |
| Network (Ethereum)             |       |
| Cluster (Solana)               |       |
| Deploy date (UTC)              |       |
| `OperatorSafeAddress`          |       |
| `TimelockAddress`              |       |
| Timelock `minDelay`            |       |
| `EthQueue`                     |       |
| `EthBridge`                    |       |
| Initial `feeRecipient`         |       |
| Initial `lockFeeRate`          |       |
| `noriBridgeVk`                 |       |
| `initialVerifiedStateRoot`     |       |
| `initialStoreHash`             |       |
| `initialQueueCursor`           |       |
| `Program` (program id)         |       |
| State PDA / mint PDA           |       |
| Ethereum deploy tx hashes      |       |
| Solana initialize tx signature |       |

---

## Appendix A — Constructor and initialize signatures

```solidity
// ethereum/contracts/NoriTokenBridge.sol
constructor(
    address _bridgeOperator,   // = TimelockAddress (NOT the SAFE directly)
    address _proofQueueAddr,   // = EthQueue (§3)
    address _feeRecipient      // = treasury or address(0) to defer
)

// ethereum/contracts/NoriProofRequestQueue.sol
constructor(
    address _bridgeOperator,   // = TimelockAddress
    address _feeRecipient,     // = treasury or address(0)
    uint256 _proofRequestQueueFeeWei
)
```

```rust
// programs/token/src/state.rs
pub struct NoriSolTokenBridgeInit {
    pub verified_state_root: B256,
    pub latest_helios_store_input_hash: B256,
    pub eth_proof_queue_address: Address,
    pub eth_token_bridge_address: Address,
    pub latest_head: u64,
    pub queue_cursor: u64,
}
```

## Appendix B — Open tooling gaps

- [ ] Initialize CLI / script for the Solana program (§6); until then,
      `SolanaProofSubmitter::build_initialize_instruction` covers it.
- [ ] Dry-run script proposing a no-op admin call through the Timelock (§7.2).
