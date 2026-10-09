# Solana: nori-bridge-solana-sdk

TypeScript SDK for the Nori Ethereum→Solana proof queue. An Ethereum contract enqueues a storage-proof request on [NoriProofRequestQueue.sol](../ethereum/contracts/NoriProofRequestQueue.sol); Nori's bridge infrastructure proves it and commits the batch that settled it on Solana. This SDK follows a request from the transaction that enqueued it until its batch is committed, builds the request's Merkle witness against that batch root, shows a submitting address's requests as a paged history or a live view, and follows Nori's prover pipeline as it works.


## Install

```sh
npm install @nori-zk/nori-bridge-solana-sdk @solana/kit
```

`@solana/kit` is a peer dependency. The witness hashing comes from `@nori-zk/ethereum-solana-proof-queue-utils-glam`, the SP1 guest's own hashing compiled to WebAssembly, installed as a dependency.

The package has three entry points:

- `@nori-zk/nori-bridge-solana-sdk`: everything below;
- `@nori-zk/nori-bridge-solana-sdk/utils`: `stateOf$`, `dataOnEntry$`, `atNode`, `AsNodeData` and `StartedMachine`, for an app's own YState machines;
- `@nori-zk/nori-bridge-solana-sdk/program`: the Solana program's generated client.

## How a proof request gets proven

A request enqueued on Ethereum waits for Ethereum to finalize its block. Nori's bridge head then takes the requests enqueued since the last batch as one job, proves Ethereum's state with SP1, and its Solana processor submits the proof to the Nori program on Solana in an `update`, which commits the batch: the queue cursor moves past the requests, and, when the job had any, a proof queue batch account records their Merkle root. Nori publishes each step as it happens, and the SDK follows them with this machine:

![Nori bridge infra transition graph](src/rpc/nori/NoriBridgeInfraTransitionGraph.svg)

- `BridgeHeadJobCreated`: the bridge head has taken the next requests as a job.
- `BridgeHeadJobSucceeded`: their proof is done; its data lists the requests it covers (`verified_requests`). A failed job (`BridgeHeadJobFailed`) is staged again.
- `EthProcessorTransactionSubmitting`, then `…SubmitSucceeded` (with the Solana transaction) or `…SubmitFailed`.
- `…FinalizationSucceeded` or `…FinalizationFailed`: whether the `update` was finalized on Solana. After a failure the bridge head checks the program: still aligned, it stages the job again; moved on, it advances.
- `BridgeHeadAdvanced`: the batch is committed; the next job follows.
- `BridgeHeadStarted` and `EthProcessorStarted`: a restart, which resumes from its checkpoint.
- `joining`: connected, before the first step arrives. It carries Nori's latest summary of the stage, and every reconnect comes back here, since steps sent while disconnected are lost.

Each state carries that step's data exactly as Nori sent it. Nori reports these states itself, so this machine is not driven by the SDK; `startNoriBridgeInfraTransitions(nori)` returns its states as they arrive:

```ts
import { startNoriBridgeInfraTransitions } from '@nori-zk/nori-bridge-solana-sdk';

const { noriBridgeInfraTransitions, finalityTransitions$, warnings$ } =
    startNoriBridgeInfraTransitions(nori);

noriBridgeInfraTransitions.state$.subscribe(({ node, data }) => {
    if (node === 'BridgeHeadJobSucceeded')
        console.log('Proven:', data.verified_requests);
});
```

`finalityTransitions$` emits each time the bridge head sees Ethereum finality move on, and `warnings$` its warnings: events during any step, not steps themselves.

## State machines

Everything that changes over time is a state machine built with [YState](https://github.com/yaw-rx/ystate), a small library of finite state machines over RxJS. A machine is a graph: named states (nodes), each carrying its own data, and the moves between them (edges), each made when something happens, such as a read finishing or a timer firing. The graphs in this README are the machines' exact definitions.

The transports' machines and the proof request, history and live view machines are returned running, with a `close()` that moves the machine to its `closed` state and ends its streams. The unprocessed request's machine is returned ready to run: `.close().start(firstState)` runs it, and `.stop()` ends it.

A running machine has:

- `state$`: emits `{ node, data }` each time it moves, starting with where it is now;
- `event$`: emits `{ edge, from, to }` for each move;
- `status$`: `running`, `complete`, `error` or `stopped`.

## Connections

The app names the transports it has, once, and `createConnections` opens them, each with its own machine:

```ts
import { createConnections } from '@nori-zk/nori-bridge-solana-sdk';

const { network, ethereum, solana, nori, close } = createConnections({
    ethereum: {
        expectedChainId: 11155111n,
        http: { rpcUrl: 'https://…' }, // optional
        websocket: { url: 'wss://…' }, // optional
        wallet: true, // optional: the user's browser wallet
        order: {
            calls: ['http', 'wallet'],
            logs: ['http', 'wallet'],
            subscriptions: ['websocket', 'wallet'],
        },
    },
    solana: {
        cluster: 'devnet', // or { expectedGenesisHash, http: { rpcUrls } } for any other cluster
        http: { rpcUrls: ['https://…'] }, // optional: default, the cluster's public URL
        websocket: { url: 'wss://…' }, // optional: default, the http URL over wss
    },
    nori: { websocket: { url: 'wss://…' } }, // optional: default, wss://wss.solana.nori.it.com
    healthChecks: { intervalMs: 15_000, timeoutMs: 10_000 }, // optional, for every transport
    retries: { initialDelayMs: 1_000, maxDelayMs: 30_000 }, // optional, for every transport
});
```

What comes back has one object per transport:

| Transport            | Has                                                                                          |
| -------------------- | -------------------------------------------------------------------------------------------- |
| `ethereum.http`      | `connection`, `ready(): Promise<EthereumProvider>`, `close()`                                 |
| `ethereum.websocket` | `connection`, `socket`, `ready(): Promise<EthereumProvider>`, `close()`                       |
| `ethereum.wallet`    | `connection`, `ready(): Promise<BrowserProvider>`, `chooseWallet(uuid)`, `switchToExpectedChain()`, `close()` |
| `solana.http`        | `connection`, `ready(): Promise<SolanaRpc>`, `close()`                                        |
| `solana.websocket`   | `connection`, `socket`, `close()`                                                            |
| `nori.websocket`     | `connection`, `socket`, `close()`                                                            |

`connection` is the transport's running machine (`undefined` for an Ethereum transport the app left out). `ready()` resolves with the client to use when the transport is ready, and otherwise rejects at once with `ConnectionNotReadyError`, whose `notReady` lists each transport and the state it is in. On an Ethereum transport the app left out, `ready()` rejects with `NoWalletConfiguredError`, `NoEthereumHttpConfiguredError` or `NoEthereumWebsocketConfiguredError` instead: a setup mistake, not a state to wait out. `close()` on the top level closes everything.

### Which transport serves what

Calls, log queries and subscriptions can each go over more than one Ethereum transport, in the order the app gives in `order`; the SDK sets none. When only one configured transport can serve a kind of request, that one is used; when several can and no order is given, `createConnections` throws. Signing only ever goes through the wallet: `(await ethereum.wallet.ready()).getSigner()`. On Solana each kind of request has one transport: calls over http, subscriptions over the websocket.

Every function below takes the chain object (`ethereum`, `solana`, `nori`) and picks the transport itself. An app's own Ethereum requests go the same way, through `forCalls`, `forLogs` and `forSubscriptions`. `forCalls` and `forLogs` are given a function whose first argument is the provider, and that function's other arguments, and run it on the right transport.

```ts
import { forCalls, type EthereumProvider } from '@nori-zk/nori-bridge-solana-sdk';

async function fetchTokenSymbol(provider: EthereumProvider, token: string): Promise<string> {
    return new Contract(token, ['function symbol() view returns (string)'], provider).symbol();
}

const symbol = await forCalls(ethereum, fetchTokenSymbol, tokenAddress);
```

`forCalls` runs the function on the first ready transport in the calls order, `forLogs` in the logs order. When a request in it fails to reach its node, the function runs again from the start on the next ready transport, so one run never mixes nodes. Any other failure (a revert, a bad request, the user declining) is thrown as it is. With nothing ready, it fails at once with `ConnectionNotReadyError`, listing every transport's state.

`forSubscriptions(ethereum, subscribe, poll, ...args)` is given two functions and their other arguments: `subscribe(socket, ...args)` returns the subscription's observable through `socket.ethSubscribe(params)`, and `poll(provider, ...args)` returns what arrived since its last call, as an array. It subscribes on the first ready transport in the subscriptions order, moves to the next when that one stops being ready and back when a higher one recovers, and while none is ready calls `poll` through the calls order, emitting each item in the same shape.

### Network

![Network graph](src/rpc/connection/NetworkGraph.svg)

Whether the device can reach the internet, in a browser or in Node. It checks for itself by requesting a few well-known endpoints (any one answering means online; set your own with `network.probeUrls`), because a browser reports being online on a network with no internet. The browser's own online and offline events only make it check sooner. While `offline`, every transport pauses, and when it comes back `online` they check again at once.

### HTTP

![HTTP connection graph](src/rpc/connection/HttpConnectionGraph.svg)

Ethereum's and Solana's http transports. `ready` means a health check passed on the expected network: for Ethereum `eth_chainId` then `eth_blockNumber` (`data.health.blockNumber`), for Solana `getGenesisHash` (the cluster) then `getSlot` (`data.health.slot`). While ready it checks again in the background. `unreachable` and `wrongNetwork` (the endpoint serves another chain or cluster; `data.found` and `data.expected` say which) check again after a wait that doubles each time, so an endpoint that comes back, or is fixed, recovers by itself. With several Solana endpoints each failed check moves on to the next, so one that is down or rate-limited is skipped; `data.url` says which endpoint each state is about. A request that fails to reach the node makes it check at once.

### WebSocket

![WebSocket connection graph](src/rpc/connection/WebSocketConnectionGraph.svg)

Ethereum's, Solana's and Nori's websockets. `open` means the socket is connected; a socket that closes, errors or stops answering its heartbeat (Nori's ping and pong) drops to `reconnecting`, which connects again after a wait that doubles each time. A subscription is sent again every time the socket opens. `gaveUp` is reached only when `retries.maxAttempts` is set and runs out; the socket's `retry()` starts again. Messages sent while connecting are queued until the socket opens.

### Ethereum wallet

![Ethereum wallet graph](src/rpc/eth/EthereumWalletGraph.svg)

The user's wallet, found through EIP-6963 (with the older `window.ethereum` as a fallback). Its `checking`, `ready`, `wrongNetwork`, `unreachable`, `offline` and `closed` states are the HTTP connection's, with the wallet added to their data, so the app gates on it exactly as on http; `data.url` is the wallet's reverse-DNS id (e.g. `io.metamask`).

| State                 | What it means                                                                        | What the app can do                                                                                   |
| --------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `lookingForWallets`   | Asking installed wallets to announce themselves.                                     | Wait.                                                                                                 |
| `noWalletFound`       | No wallet is installed. A wallet installed or enabled later is picked up.            | Ask the user to install one.                                                                          |
| `choosingWallet`      | Several wallets are installed; `data.wallets` lists them (name, icon).               | Let the user pick, then `ethereum.wallet.chooseWallet(uuid)`.                                         |
| `checking`            | Asking the wallet for its chain, then its latest block.                              | Wait.                                                                                                 |
| `ready`               | Ready to use; `data.health.blockNumber` is the latest block.                         | Nothing.                                                                                              |
| `wrongNetwork`        | The wallet is on chain `data.found`, not `data.expected`.                            | Ask the user to switch, or call `ethereum.wallet.switchToExpectedChain()` to have the wallet ask them. |
| `askingToSwitchChain` | The wallet is showing the switch request.                                            | Ask the user to confirm it.                                                                           |
| `switchDeclined`      | The user said no. The wallet is not asked again until they change chain themselves.  | Tell them they can switch in their wallet whenever they're ready.                                     |
| `unreachable`         | The wallet did not answer, or disconnected; it is asked again after a wait.          | Show `data.error`.                                                                                    |
| `offline`             | The device is offline.                                                               | Wait.                                                                                                 |

A switch request the wallet fails for another reason (e.g. it doesn't know the chain) returns to `wrongNetwork` with the wallet's error in `data.lastSwitchError`. Any chain change, in any state with a wallet, checks again.

```ts
ethereum.wallet.connection?.state$.subscribe(({ node }) => {
    if (node === 'noWalletFound') console.log('Install a wallet such as MetaMask to continue.');
    if (node === 'wrongNetwork') console.log('Switch your wallet to Sepolia.');
    if (node === 'askingToSwitchChain') console.log('Confirm the switch in your wallet.');
});
```

## Subscriptions

Every subscription has a `$` name and stays live: pushed while a transport that can subscribe is ready, and polled while none is, in the same shape. A one-off read has the same name without the `$`.

| Ethereum                            | Solana                                              | Nori                                     |
| ----------------------------------- | --------------------------------------------------- | ---------------------------------------- |
| `getNewHeads$(ethereum)`            | `getAccount$(solana, address, commitment?)`         | `getNoriBridgeInfraState$(nori)`         |
| `getEthereumLogs$(ethereum, filter)` | `getSolanaLogs$(solana, address, commitment?)`      | `getNoriBridgeInfraTimings$(nori)`       |
|                                     | `getSignatureStatus$(solana, signature, commitment?)` | `getNoriBridgeInfraEthState$(nori)`      |
|                                     |                                                     | `getNoriBridgeInfraTransitionNotices$(nori)` |
|                                     |                                                     | `getNoriBridgeInfraSystemNotices$(nori)` |
|                                     |                                                     | `getNoriBridgeInfraStateWithTimings$(nori)` |

Ethereum's subscriptions go in the `subscriptions` order: the websocket, or the wallet's own `eth_subscribe` where the wallet supports it. Solana's go over its websocket. Nori's only exist on its websocket, so they have no polling and no one-off reads; its state, timings and Ethereum state replay the latest to each new subscriber. `getAccount$` replays the latest too.

## Following one proof request

![Proof request state graph](src/proofRequest/ProofRequestStateGraph.svg)

`createProofRequestStateMachine` follows the request from the transaction that enqueued it:

- `undetermined` looks the request up on Ethereum. A transaction that is not mined yet is waited for.
- `unprocessed` means the bridge has not proven it yet; the machine checks the bridge every poll interval (15 s by default) and on every recheck signal, such as the bridge infra state moving on.
- `proofAvailable` means a committed proof queue batch covers it. Batches are append-only, so it stays there.
- `waitingForConnection…` waits for Ethereum and Solana, naming them in `data.waitingOn`, and resumes where it was.
- `failed…` holds the error in `data.error` and reads again by itself after a wait that doubles with each failure in a row (`data.failedReads`); `retry()` reads again at once.

Once the request is `proofAvailable`, `getProofRequestWitness` fetches every request in its batch from Ethereum, rebuilds the batch's Merkle tree, and returns the request's leaf, its bottom-up path and the root, checked against the root committed on Solana (a mismatch throws `ProofRequestWitnessRootMismatchError`):

```ts
import { filter } from 'rxjs';
import {
    createConnections,
    createProofRequestStateMachine,
    getNoriBridgeInfraState$,
    getProofRequestWitness,
    type ProofRequestStateNodeUnion,
} from '@nori-zk/nori-bridge-solana-sdk';

const { ethereum, solana, nori } = createConnections({
    ethereum: { expectedChainId: 11155111n, wallet: true },
    solana: { cluster: 'devnet' },
});
const proofQueueAddress = '0x…'; // the NoriProofRequestQueue address
const proofRequestTxHash = '0x…'; // the transaction that enqueued the request

const { proofRequestState } = createProofRequestStateMachine(
    { ethereum, solana },
    { proofQueueAddress, proofRequestTxHash },
    undefined,
    15_000,
    getNoriBridgeInfraState$(nori) // check again whenever the bridge moves on
);

proofRequestState.state$
    .pipe(
        filter(
            (state): state is Extract<ProofRequestStateNodeUnion, { node: 'proofAvailable' }> =>
                state.node === 'proofAvailable'
        )
    )
    .subscribe(async ({ data }) => {
        const { leaf, path, root } = await getProofRequestWitness(ethereum, data, proofQueueAddress);
    });
```

To resume a request whose state the app already knows, pass that snapshot as the third argument; the machine starts there instead of looking the request up.

### While unprocessed: what it is waiting on

![Unprocessed proof request state graph](src/proofRequest/UnprocessedProofRequestStateGraph.svg)

Nori's bridge infra state says what an unprocessed request is waiting on: Ethereum finality for its block, the job ahead of it, then the job that includes it. `createUnprocessedProofRequestStateMachine(blockNumber, nori)` follows these from the request's block number and Nori's state, timings and Ethereum state. Every new `state.eth`, `state.bridge` or timings message recomputes where the request waits and how long it has left, and the estimate counts down each second in between. Each state's data carries `time_remaining_sec` for the step it is in, `commit_time_remaining_sec` until the batch covering the request is committed on Solana (negative once that is taking longer than expected), and `waiting_elapsed_sec` for how long it has been in that state, which together draw a progress bar.

Nori creates a job on each Ethereum finality transition, one epoch (`ETHEREUM_EPOCH_SEC`, 384 s) after the last, proving the epoch's blocks, and commits it on Solana when it finishes; a job takes at most `MAX_BATCH_SIZE` (2¹⁶) requests. A job goes through the stages in `NORI_JOB_STAGES`: proving it (`BridgeHeadJobCreated`, `BridgeHeadJobSucceeded`), then submitting it to Solana (`EthProcessorTransactionSubmitting`, `EthProcessorTransactionSubmitSucceeded`); it is committed when it reaches `EthProcessorTransactionFinalizationSucceeded`. `getCommitTimes(stage, timings)` gives, from where Nori is in its loop, the seconds until the job it is running is committed and until the next job is committed. `jobTimingsOf(timings)` gives the seconds each job stage takes: Nori's timings where it reports them, and `FALLBACK_NORI_JOB_TIMINGS` (a proof about two minutes, submitting and finalization seconds) otherwise. `getFinalityTimeRemainingSec(blockNumber, finality)` gives the seconds until a block is finalized.

## Where the waiting requests are

`sortWaitingProofRequests(waiting, finalizedBlock, job?)` sorts the requests no batch covers yet into three sets, each oldest first: those whose block is not finalized yet, those finalized and scheduled for a later job, and those in the job Nori is running now. The first two come from Ethereum and Solana alone. Telling the job Nori is running apart from the later ones needs Nori's `state.bridge`; without it, every finalized request is scheduled.

```ts
import {
    getBridgeState,
    getEnqueuedProofRequests,
    getFinalizedBlockNumber,
    getLatestBlockHeight,
    sortWaitingProofRequests,
} from '@nori-zk/nori-bridge-solana-sdk';

const { queueCursor } = await getBridgeState(solana);
const waiting = await getEnqueuedProofRequests(ethereum, proofQueueAddress, {
    fromBlock: queueDeploymentBlock,
    toBlock: await getLatestBlockHeight(ethereum),
    fromRequestId: queueCursor,
});
const { waitingForFinality, scheduled, processing } = sortWaitingProofRequests(
    waiting,
    await getFinalizedBlockNumber(ethereum),
    bridgeState // the latest from getNoriBridgeInfraState$(nori), or undefined without Nori
);
```

## A submitting address's requests

The submitting address is the contract that enqueued the requests: the queue records `msg.sender` as each request's `target`. Its requests are found from the queue's `ProofRequested` logs, filtered on `target`, in block ranges small enough for providers' limits, from `fromBlock` (e.g. the queue's deployment block). Each request comes back with where it is now: `unprocessed`, or `proofAvailable` with the batch covering it, ready for `getProofRequestWitness`.

### Paged history

![Proof request history graph](src/proofRequest/ProofRequestHistoryGraph.svg)

```ts
import { createConnections, createProofRequestHistoryMachine } from '@nori-zk/nori-bridge-solana-sdk';

const { ethereum, solana } = createConnections({
    ethereum: { expectedChainId: 11155111n, http: { rpcUrl: 'https://…' } },
    solana: { cluster: 'devnet' },
});
const proofQueueAddress = '0x…'; // the NoriProofRequestQueue address
const submittingAddress = '0x…'; // the contract that enqueued the requests
const queueDeploymentBlock = 0; // the block the queue was deployed in

const { proofRequestHistory, loadMore } = createProofRequestHistoryMachine(
    { ethereum, solana },
    { proofQueueAddress },
    { target: submittingAddress, fromBlock: queueDeploymentBlock, order: 'desc', pageSize: 20 }
);

loadMore(); // the next page, while waiting for more
```

The first page loads at once and every state carries the requests loaded so far in `data.loaded`, newest first with `desc` or oldest first with `asc`. `allLoaded` means the block range is exhausted. Lost connections and failed reads are handled as for a single request.

### Live view of the newest requests

![Latest proof requests graph](src/proofRequest/LatestProofRequestsGraph.svg)

```ts
import {
    createConnections,
    createLatestProofRequestsMachine,
    getNoriBridgeInfraState$,
} from '@nori-zk/nori-bridge-solana-sdk';

const { ethereum, solana, nori } = createConnections({
    ethereum: { expectedChainId: 11155111n, http: { rpcUrl: 'https://…' } },
    solana: { cluster: 'devnet' },
});

const { latestProofRequests } = createLatestProofRequestsMachine(
    { ethereum, solana },
    { proofQueueAddress },
    { target: submittingAddress, fromBlock: queueDeploymentBlock, count: 10 },
    15_000, // refresh interval
    getNoriBridgeInfraState$(nori) // and refresh whenever the bridge moves on
);
```

It shows the newest `count` requests in `data.view` and keeps them current: new requests enter at the top, and requests move from `unprocessed` to `proofAvailable` as the bridge commits their batches. A request that an Ethereum reorg removes leaves the view, which refills from older blocks. It never ends on its own.

### Gating

`bothReady$({ ethereum, solana })` emits once Ethereum (any transport in its calls order) and Solana's http can both serve a read, and `waitingOnChanged$` each time the set of chains that cannot changes; the reading machines use them, and an app can too.

## One-off reads

Without a machine, each read runs once through the chain objects and throws on failure, including `ConnectionNotReadyError` when nothing suitable is ready, rather than waiting:

| Proof requests                                                          | Proof queue                                                        | Ethereum                             |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------ |
| `getProofRequestStateSnapshot({ ethereum, solana }, request)`           | `getProofQueueHead(ethereum, proofQueueAddress)`                   | `getLatestBlockHeight(ethereum)`     |
| `getProofRequestWitness(ethereum, proofAvailable, proofQueueAddress)`    | `getEnqueuedProofRequests(ethereum, proofQueueAddress, query)`     | `getFinalizedBlockNumber(ethereum)`  |
| `getProofRequestHistoryPage({ ethereum, solana }, addresses, query)`     | `getProofRequestBatch(ethereum, proofQueueAddress, …)`             |                                      |
| `getProofRequestCountsByTarget({ ethereum, solana }, addresses, query)`  | `getBridgeState(solana)`                                           |                                      |
| `getProofRequestsByTarget(ethereum, proofQueueAddress, query)`           | `getProofQueueBatches(solana, indices)`                            |                                      |
| `getRequestIdByTxHash(ethereum, proofQueueAddress, txHash)`              | `getProofQueueBatchSummaries(solana, indices)`                     |                                      |
| `getProofRequestAge(ethereum, blockNumber)`                              | `getProofQueueBatch(solana, requestId, batchCount)`                |                                      |
|                                                                         | `getProofQueueBatchesForRequests(solana, requestIds, batchCount)`  |                                      |
|                                                                         | `findProofQueueBatchPda(index)`: an address, no read               |                                      |

```ts
import { firstValueFrom } from 'rxjs';
import { bothReady$, getProofRequestCountsByTarget } from '@nori-zk/nori-bridge-solana-sdk';

await firstValueFrom(bothReady$({ ethereum, solana }));
const { total, proofAvailable, unprocessed } = await getProofRequestCountsByTarget(
    { ethereum, solana },
    { proofQueueAddress },
    { target: submittingAddress, fromBlock: queueDeploymentBlock }
);
```

## Development

From the repo root:

```sh
npm run reinstall
npm run build
```

`npm run build` builds the Solana program, regenerates `idl/` with `anchor idl build`, and builds the `ethereum/` and `sdk/` workspaces. This package's `prebuild` regenerates `src/program/` from `idl/token.json` with Codama ([scripts/generate-client.mjs](scripts/generate-client.mjs)), so the typed client follows the program with every build.

Tests, from this folder:

```sh
npm run test:unit
```

The state machine graphs' images are generated from their definitions with [ystate-visualizer](https://github.com/yaw-rx/ystate), next to each definition:

```sh
ystate-visualizer --strict -o src/proofRequest src/proofRequest/proofRequest.ts
```
