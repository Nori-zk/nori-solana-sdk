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
| `programs/token/` | Anchor program: `initialize`, `update`, `mint` + zero-copy state and append-only proof queue batch PDAs |
| `proof-submitter/` | Client crate: proof-JSON loader + `SolanaProofSubmitter` (RPC `update` sender) |
| `cli/` | `nori-cli` operator binary on top of `proof-submitter`: `initialize` for an already deployed program (DEPLOYMENT.md §6) |
| `idl/`, `sdk/src/program/` | **Generated** from `programs/token` by `anchor idl build` and Codama (sdk/README.md "Development"). Never hand-edit; regenerate with the program change |
| `sdk/` | npm `@nori-zk/nori-bridge-solana-sdk`: ystate machines over the connections (reads kept current, sends, the Solana wallet, minting); unit specs in `src/tests/unit/`, specs against a running surfpool in `src/tests/integration/` |
| `nori-hash-utils/` | Workspace crate compiling nori-bridge-head's `nori-hash` (without its default `helios` feature) to WebAssembly with `wasm-bindgen` + `tsify`; `pkg/` is build output |
| `test-utils/` | Surfpool test harness (validator kill-on-drop, funded keypairs, CLI deploy incl. at a given program id, `set_account`/`time_travel` cheatcodes, `send` with compute units and logs, custom error codes); no dependency on `token` or Anchor, so the program's own tests and apps built on it can use it |
| `proof-submitter/example-proofs/` | Four chained SP1 Groth16 proofs (no deposits) used by the test suites |
| `DEPLOYMENT.md` | Production runbook (Safe → Timelock → ETH contracts → Solana program) |
| `DEVELOPMENT_GUIDE.md` | Toolchain setup (Solana CLI, Anchor, Surfpool) |

## Commands that work today

```bash
cd ethereum
npm ci
npm test                    # ETH_NETWORK=hardhat hardhat test → 126 passing
npm run typecheck           # tsc --noEmit → clean
npm run build               # compile + stabilize-types + tsc -p tsconfig.package.json

cd ..   # repo root
CFLAGS="-isystem $HOME/.cache/solana/v1.54/platform-tools/llvm/sbpf/include" \
    cargo build-sbf --manifest-path programs/token/Cargo.toml   # → target/deploy/token.so
cargo test                # surfpool suites (needs surfpool + solana CLI on PATH)
cargo run -p nori-cli -- initialize --help   # one-off initialize of a deployed program
cargo clippy --workspace --all-targets   # clean
cargo fmt --all --check                  # clean

npm run build -w sdk              # its prebuild regenerates sdk/src/program/ from idl/
npm run lint -w sdk               # clean
npm run test:unit -w sdk          # 118 passing
surfpool start --offline --no-deploy --ci &   # then, with target/deploy/token.so built:
npm run test:integration -w sdk   # 12 passing against the running surfpool; no reset between runs
```

## Build quirks that cost hours

- **Alloy MSRV**: the SBF toolchain ships rustc 1.89; alloy 1.7+ requires
  1.91. Cargo.lock pins the alloy tree to 1.6.3 (MSRV 1.88). Do not
  `cargo update` the alloy crates past that without an SBF toolchain bump.
- **Helios feature**: nori-bridge-head's helios 0.12.0 depends on alloy 2.x,
  which requires rustc 1.94.1. The root `Cargo.toml` takes
  `nori-sp1-helios-primitives` and `nori-hash` with `default-features = false`,
  leaving out their `helios` feature, so neither helios nor alloy 2.x enters
  the lock. Keep it that way without an SBF toolchain bump.
- **getrandom 0.2** (via rand_core ← k256/bls12_381) has no backend cfg for
  the sbpf target; `programs/token/Cargo.toml` forces its `custom` feature,
  which unifies across the graph. Nothing on-chain calls getrandom.
- **ring** (via ethereum_hashing ← tree_hash) compiles C with the platform
  clang and needs the freestanding headers passed via `CFLAGS` (above).
- The stack analyzer in `cargo build-sbf` prints "Error: Function …
  overflows the maximum allowed frame space" for dead third-party code
  (ring::rsa, crossbeam). Only frames in `token::*` matter.

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
- `NoriSolTokenBridge` is a **zero-copy** account (`AccountLoader`, 192
  bytes + 8-byte discriminator): the struct must stay `Pod` — fixed-size
  fields only, ordered so there is no padding, no Borsh/Vec.
- `update`'s state account must stay `mut` — `load_mut()` rejects read-only
  accounts with `AccountNotMutable`. `mint`'s stays read-only so mints do
  not write-lock state against `update`.
- Proof queue batches are append-only PDAs at
  `[b"PROOF_QUEUE_BATCH", index.to_le_bytes()]`, created only by `update`
  and only for non-empty batches, with contiguous indices from 0
  (`proof_queue_batch_count`). Never overwrite or close them: `mint` trusts
  any program-owned `ProofRequestRootEntry` as a committed batch.
- Create program-owned PDAs only through `pda::create_program_owned_pda`:
  their addresses are predictable, and a bare `create_account` fails
  forever once someone pre-funds the address.
- Serialize into account data with `try_serialize(&mut &mut data[..])`,
  never `&mut *data` — the latter advances the account's stored slice, so
  later reads in the same instruction see empty data.

## Conventions

- Docs and comments must not reference the chain this SDK was forked from;
  describe the ETH→Solana design as it stands.
- Run `npm test` in `ethereum/` after any contract change; regenerate types
  via `npm run build` and commit the stabilized output.
- `.env.nori-eth-token-bridge` / `.env.nori-eth-timelock` are gitignored
  deploy outputs; the `.example` files are the committed templates.
- Keep README.md, DEPLOYMENT.md, and this file in sync with code changes.
- Changing and releasing the sdk (`@nori-zk/nori-bridge-solana-sdk`), in this order:
    1. Collect everything the apps need from it first, so one release covers it.
    2. Explain each change and why, and ask. Change nothing until the user says yes.
    3. Make the changes, bump the version (`package.json`, `sdk/package.json`, `package-lock.json`), and run its build, lint and unit tests.
    4. `git add` the changed paths and give the user the commit message. The user commits and pushes.
    5. Run `npm run publish -- --dry-run` (`nw-publish`) and report it. The user publishes.

## Lessons

jk89 was sad the agents ignored his abstraction and worked around it

- ystate and yaw are not in the training data. Without them, agents reach
  for what they know: promises, `async`/`await`, `firstValueFrom`, helper
  functions, one-shot reads, loading flags and `catchError` fallbacks. Before
  writing anything, read the ystate README, `rpc/connection/readThroughConnections.ts`
  with its `.impl.ts`, and one machine built on it
  (`proofQueue/bridgeState.ts` with its `.impl.ts`); then build the value as
  a machine the same way.
- The public API is the funnel: it exports machines, live streams, and the
  shared shapes an app builds its own machines from: a read
  (`readThroughConnectionsOf`, `startReadThroughConnectionsMachine`, whose
  read is given the clients), gated on another read machine
  (`readMachineValue$`, `readMachineChanged$`), with the lowest steps that
  read is built from (`ethRpcRead$`, `solanaRpcRead$`, `blockRanges`); a
  transaction (`sentTransactionOf`, `sentSolanaTransactionOf`); and the
  program's addresses as observables (`stateAddress$`, `mintCall$`). An
  app's own contracts and programs are its own machines, never the sdk's.
  Never export a runner (`forCalls$`,
  `forLogs$`, `forSolana$`), a one-shot read, a readiness gate or a
  transport's provider or RPC: every public piece a machine is built from
  becomes a way around the machine.
- Every value read through the connections is a machine: a two-line graph
  `define(readThroughConnectionsOf({ value }))` and a `createXMachine` that
  passes `startReadThroughConnectionsMachine` only what is its own: the
  read, the chains it needs and its refresh trigger (`bridgeState`,
  `solana/account`, `proofRequestWitness`). The factory holds everything
  every machine repeats. A value read once (a witness, a block's time) is
  still a machine whose refresh never comes. Pure functions stay functions.
- A read returns the graph's `current` data: `startReadThroughConnectionsMachine`
  checks it against the graph. A graph whose read can arrive at another
  node names its wider data type explicitly (`proofRequest.impl.ts`). A type
  that does not infer is fixed at its source, never annotated around.
- A `createXMachine` returns the running machine itself, with its controls
  on it (`retry()`, `close()`, `loadMore()`): never a wrapper whose
  `.machine` the caller has to reach into.
- A read tries once. Retrying belongs to the machine (`failedWhile…`,
  `retryDue`, `retry()`); a retry loop inside a read hides the failure, keeps
  the machine in `loading` and tells the transport late.
- A transaction is a machine too, made for what it sends (an Ethereum call
  `{ to, data, value }`, or Solana instructions, its starting data) and sent
  only on a send request from outside (`send()`), gated on what sends it
  (`notReadyToSend` when it cannot). Ethereum's spread `sentTransactionOf`,
  Solana's `sentSolanaTransactionOf`, and each adds its sending node
  (`askingToSign` for a wallet, `sending` for a signer); the sent
  transaction is followed on the chain until it ends (`confirmed`,
  `reverted`, `replaced` or `dropped` on Ethereum; `confirmed`, `reverted`
  or `expired` past its `lastValidBlockHeight` on Solana). A transaction
  sent elsewhere is followed by its signature
  (`createSignatureStatusMachine`). Never wait for a receipt or a
  confirmation inside a promise; the machine owns it. A program call is one
  builder every path uses (`mintCall$`), never a send per signer.
- A machine's state holds what it shows in plain data: ystate widens every
  string type (`Widen`), brands included, so a value kit brands (an
  `Address`, a `Signature`, an `Instruction`) is held as its string or
  base64 form (`InstructionToSend`) and turned back with kit's checked
  constructors (`address()`, `signature()`) where it is sent, never cast.
- Tests read a machine as a stream that ends: `waitForNode`, `waitForState`,
  `nodesUntil`, `statesDuring`. Never subscribe into an array a test
  inspects later, never `sleep` and check, never a promise helper with
  `try/finally` around a machine. "Nothing happens" is asserted as the
  states it stays in (`statesDuring`).
- A subscription the sdk sends is confirmed against what a real local node
  answers, in an integration test on a running `surfpool start`
  (`sdk/src/tests/integration/`), never in a probe script. Surfpool refuses
  `rootSubscribe` and answers `slotSubscribe`, whose pushes carry the root,
  so new roots come from `slotSubscribe`. Two airdrops of the same amount
  within one blockhash are the same transaction with the same signature. An
  Ethereum event's topic comes from the contract that emits it
  (`PROOF_REQUESTED_TOPIC`, from the queue's own interface), never from an
  event added to a contract interface for the sdk's sake.
- Removing a public read removes a capability. Build its machine first,
  prove it with the tests, then take the read out of the public API, never
  the other way round.
- Machines are isolated by domain: each holds only its own domain's
  states. One machine uses another by gating its own transitions on the
  other's `state$`, as the ystate README shows: the heater's transitions
  gate on `temperature$` (the physics example, "Coupled systems"), and
  Payment's `pay` gates on `deps.auth.state$` (the auth example, README
  "Quick look" and the studio's checkout workspace). A graph can also
  spread another graph's nodes and edges into its own
  (`...readThroughConnectionsOf(...)`, as `transaction/signatureStatus.ts`
  adds `expired`). Never copy another machine's states into your own data.
- A machine describes one domain; everything else is the outside world,
  which it reaches only through `$`. Fixed configuration is not input: an
  address, a program id or an RPC URL known when the machine is made
  defines the machine. What arrives or changes during its life (data,
  another machine's state, the user, the chain) comes in through `$`, and
  the graph shows three things for it: the input arriving through `$`, an
  edge that fires only when the outside allows it (the gate), and the node
  the machine waits in until it does. That node is usually the one the
  gated edge leaves from: the heater waits in `idle` until
  `belowLowerLimit` lets it through, and Payment waits in `checkout` until
  `payRequest` brings the card. A node of its own is needed only when why
  it waits is something the domain shows or handles: Payment's
  `notLoggedIn`, which the user sees and `dismiss` leaves, or
  `waitingForConnectionWhile…`, which names the chain it waits on. A gate
  either waits or refuses: a filter in `$` waits silently (the heater's
  thresholds), an error edge refuses explicitly (`pay` errors into
  `notLoggedIn`); it refuses when the outside saying no is a state of the
  domain. Without these, the dependency is handled in code around the
  graph, and the graph no longer describes what the machine does.
- A machine has two environmental inputs (`start(entry, runningMachines,
  initialNodeData)` and each transition's `$`):
  - its initial state: what is static, known when the machine is made,
    flows in as the starting node's data (`initialNodeData`), so the
    machine's state shows it;
  - `$`: what may or may not have happened yet (an outcome still to come,
    another machine's state, the user, the chain).
  A signature status machine made for one signature reads until it reaches
  its commitment or expires; a receipt machine made for one transaction
  hash reads until it is mined. That input is its initial state, in the
  graph's data; never a value captured in a closure, where the machine's
  state does not show it.
- A transition's `$` is the external environment: "an Observable factory
  over the external environment", whose "emissions are outside the
  machine's control" (`clean/yaw/ystate/packages/ystate/README.md`, "Core
  concepts", the Σ row of "The formalism" and "Transitions";
  `TransitionShape.$` in `clean/yaw/ystate/packages/ystate/src/transitions.ts`).
  Everything outside the graph enters a machine only there: another
  system (Ethereum, Solana, Nori's server, the browser), another machine
  (`deps.auth.state$`), and the inputs of a call. So:
  - External state belongs in the streams that feed `$`; the graph holds
    only what the machine itself decides.
  - Input that arrives during a machine's life comes through an edge's
    `$`. Payment sits in `checkout` from the start; `pay` fires when
    `payRequest` emits, and `payRequest` carries the card
    (`clean/yaw/ystate/packages/example/src/payment.ts`, or the studio's
    checkout workspace in
    `clean/yaw/ystate/packages/studio/src/app-root/default-workspaces.ts`).
    Input the machine is made with is its initial state instead (see the
    two environmental inputs above).
  - Gating on another machine is also `$`: `pay` reads `deps.auth.state$`
    with `withLatestFrom`; when Auth is not `authenticated` its error edge
    goes to `notLoggedIn`, and `dismiss` brings it back to `checkout`. No
    helper and no spawned machine.
- The graph holds the machine's own domain state; the outside world is
  observed in the shape it has, like `temperature$` in the heater example.
  Holding the outside world outside the graph is the idiom, not a break of
  it, when the state classifies an external system's behaviour. ystate's
  README shows both kinds of external system: the heater classifies the
  room by observing `temperature$` (the physics example), and Basket
  classifies auth by gating `checkout` on `deps.auth.state$` being
  `authenticated` (the auth example); neither holds the other's states in
  its graph. Read them before judging any state:
  - the heater and the room: `clean/yaw/ystate/packages/ystate/README.md`,
    section "Coupled systems" ("A heater FSM", then "A Physics
    simulation"); runnable in `clean/yaw/ystate/packages/example/src/heater.ts`.
  - Basket and auth: the same README, section "Quick look" (the Basket
    graph and its `checkout` transition), and "Machine sets with multiple
    machines" (Auth started by its owner and passed into
    `Basket.close().start('empty', { auth })`); runnable in
    `clean/yaw/ystate/packages/example/src/basket.ts`, `auth.ts` and
    `payment.ts`; the studio's checkout workspace in
    `clean/yaw/ystate/packages/studio/src/app-root/default-workspaces.ts`
    (search `checkout`).
  So in this sdk:
  - `sinceRead$` classifies the chain, as the heater classifies the room:
    whether it moved on since a read began.
  - the unprocessed machine's `scope.applied` classifies Nori's server, as
    Basket classifies auth: which update its pipeline has applied.
  - the wallet machine's `wallets` map classifies the browser: which
    wallets have announced themselves (EIP-6963).
  Keeping such an observation subscribed while the machine moves between
  nodes (`heldAcrossMoves`) is part of observing it, not state. A `$`
  reading the entered node's data (`dataOnEntry$`) reads the graph's own
  state, which is in the graph. Before calling something "state outside the
  graph", ask what it classifies: our domain belongs in the graph, an
  external system's behaviour does not.
- ystate's source is in `clean/yaw/ystate/packages/ystate/src`. Before
  writing any type or helper about a graph, look there first: a transition's
  name is `TransitionNames`, a transition `TransitionDef`, a node's data
  `ResolveNodeData`, a state `StateUnion`, a running machine
  `RunningMachine` / `RunningMachineSet`, a defined graph
  `IncidenceGraphSetMixin`. Writing your own copy of one is the same mistake
  as a hand-written union beside the graph.
- A shape several graphs share is written once and spread:
  `readThroughConnectionsOf(data)` (a value read and kept current),
  `healthCheckedOf(carried)` (a connection checked by its health; http and
  the wallet), `ChainChangesGraph` (Ethereum's and Solana's changes, both
  started by `startChainChangesMachine`). Their transitions are written once
  too (`readThroughConnectionsTransitions`, `healthCheckedTransitions`), and
  so is a pattern the machines repeat: one request per entry into a node,
  shared by its outcome edges, is `requestOnEntry$`; outcomes split by a
  field are `withOutcome`.
- `refreshOn` is followed from before each read: its first emission says it
  is following, each later one that a refresh is due. A chain trigger is
  `changesOf$` of an owned chain changes machine, following once it knows
  where it starts (its first poll, or the node acknowledging the
  subscription); anything without a starting point goes through `dueOn`.
  On Solana the trigger is the narrowest watch that names the change
  (`{ account }` for an account's value, `{ mentions }` for transactions,
  `{ signature, commitment }` for one transaction), never every root or
  every program call. Starting a trigger only once the value is held
  misses what changes during the read.
- A read from a chain is an observable named for what it gives
  (`bridgeState$`, `proofQueueBatches$`), built on one lowest step:
  `ethRpcRead$` (the one ethers call, its failure an
  `EthRpcTransportError`) or `solanaRpcRead$` (the one `@solana/kit` call,
  abortable, its transport failure a `SolanaRpcTransportError`); loops over
  block ranges are `from(blockRanges(…)).pipe(concatMap(…))`. A read that
  returns what a later read needs (a batch's account address) returns it,
  never derives it again. A read that awaits another read is logic in
  promises.
- Stop using promises: this is an rxjs library. Every function, lambda,
  runner and API the sdk exposes takes and returns observables or machines.
  A promise exists only inside the lowest step, where a dependency (ethers,
  `@solana/kit`) returns one, wrapped once with `defer`; nothing above it
  sees a promise.

- The connection layer (`createConnections`, each transport's machine,
  `readThroughConnections$`, the readiness streams) and the machines exist
  so that nothing else handles connectivity. Use them; never work around
  them. Before writing code, find how the abstraction already does it.
- Machines are for abstraction. Every read through the connections is one
  machine driven by a lambda (the read, the chains it needs, when to read
  again); domain state lives in the value the lambda returns. Code is never
  copied: the second time the same code appears, it is abstracted into one
  place every user calls (`utils/arrays.ts`'s `chunk` and `range`). That
  covers a machine's waiting, failed and retry edges, and the setup every
  `createXMachine` repeats (its subjects, `stateOf$`, `implement`, `start`,
  `retry()` and `close()`).
- Nothing is read once. A value that can change is kept current on its
  trigger (new heads, logs, an account changing, a recheck signal). A
  one-shot read (`fetch…`, `…From`, a promise) is only ever the step a
  machine runs; no stream swallows a failure with `catchError(() => EMPTY)`:
  a failure is a state.
- Finding a shortcut means searching the whole codebase for the same
  pattern and fixing every instance.
