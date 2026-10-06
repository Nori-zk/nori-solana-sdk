# Solana: nori-bridge-solana-sdk

TypeScript SDK for the Nori bridge to Solana: follow an Ethereum proof request through the bridge until its proof is available on Solana, and build its Merkle witness against the proof queue batch root committed on Solana.

## Exports

### Proof request state

- `getProofRequestStateSnapshot(request)`: where a proof request is, from Ethereum and Solana alone: `unprocessed` or `proofAvailable`.
- `recheckProofRequestStateSnapshot(current, request)`: refresh a snapshot; `proofAvailable` is terminal.
- `createProofRequestStateMachine(request, initial?, pollIntervalMs?, recheckTrigger$?)`: the YState machine `undetermined -> unprocessed -> proofAvailable`, polling on an interval and on an optional recheck signal such as bridge websocket stage changes.
- `createUnprocessedProofRequestStateMachine(blockNumber, topics)`: the waiting sub-states (`WaitingForEthFinality`, `WaitingForPreviousJobCompletion`, `WaitingForCurrentJobCompletion`, `FinishedWaiting`) driven by the bridge websocket topics.

### Witness

- `fetchProofRequestWitness(proofAvailable, request)`: fetch every request in the proof request's batch from Ethereum, build the witness with `@nori-zk/nori-hash-utils`, and check its root against the batch root committed on Solana.

### Solana reads

- `fetchBridgeState(connection, programId?)`: queue cursor and proof queue batch count.
- `findProofQueueBatch(connection, requestId, proofQueueBatchCount, programId?)`: the committed batch covering a request id, by k-ary search over batch indices (100 accounts per RPC call).

### WebSocket / reactive API

- `getBridgeSocketWithConnectivity$(url?)`: the reconnecting bridge websocket stream and its connectivity machine (`connecting`, `open`, `closed`, `reconnecting`, `permanently-closed`).
- `getBridgeStateTopic$`, `getEthStateTopic$`, `getBridgeTimingsTopic$`: the bridge websocket topics.

## How to build

From the repo root:

```sh
npm install
npm run build
```

## How to regenerate the Solana client

`src/program/` is generated from the program's IDL with Codama and is never edited by hand. Regenerate it, and commit it with the program change, whenever `programs/token` changes an account, instruction, argument type or PDA seed.

Requires Anchor CLI 1.1.2 (see the repo's DEVELOPMENT_GUIDE.md).

```sh
# 1. From the repo root: the program IDL into idl/ (--locked keeps Cargo.lock untouched)
anchor idl build -p token -o idl/token.json -t idl/token.ts -- --locked

# 2. From this folder: the typed client into src/program/ (scripts/generate-client.mjs)
npm run generate:client
```
