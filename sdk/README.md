# Solana: nori-bridge-solana-sdk

TypeScript SDK for the Nori Ethereum→Solana proof queue. An Ethereum contract enqueues a storage-proof request on [NoriProofRequestQueue.sol](../ethereum/contracts/NoriProofRequestQueue.sol); Nori's bridge infrastructure proves it and commits the batch that settled it on Solana. This SDK follows a request from the transaction that enqueued it until its batch is committed, builds the request's Merkle witness against that batch root, shows a submitting address's requests as a paged history or a live view, shows the bridge's batches and the requests waiting for one, and follows Nori's prover pipeline as it works.

The SDK is built on RxJS: every function returns an observable, and everything that changes over time is a state machine.

## Install

```sh
npm install @nori-zk/nori-bridge-solana-sdk @solana/kit
```

`@solana/kit` is a peer dependency. The witness hashing comes from `@nori-zk/ethereum-solana-proof-queue-utils-glam`, the SP1 guest's own hashing compiled to WebAssembly, installed as a dependency.

The package has three entry points:

- `@nori-zk/nori-bridge-solana-sdk`: everything below;
- `@nori-zk/nori-bridge-solana-sdk/utils`: `stateOf$`, `dataOnEntry$`, `atNode`, `AsNodeData`, `StartedMachine` and `GraphState`, for an app's own YState machines;
- `@nori-zk/nori-bridge-solana-sdk/program`: the Solana program's generated client.

## Nori prover pipeline (`getNoriBridgeInfraTransitions`)

A request enqueued on Ethereum waits for Ethereum to finalize its block. Nori's bridge head then takes the requests enqueued since the last batch as one job, proves Ethereum's state with SP1, and its Solana processor submits the proof to the Nori program on Solana in an `update`, which commits the batch: the queue cursor moves past the requests, and, when the job had any, a proof queue batch account records their Merkle root. Nori publishes each step as it happens, and the SDK follows them with this machine:

![Nori bridge infra transition graph](src/rpc/nori/NoriBridgeInfraTransitionGraph.svg)

- `BridgeHeadJobCreated`: the bridge head has taken the next requests as a job.
- `BridgeHeadJobSucceeded`: their proof is done; its data lists the requests it covers (`verified_requests`). A failed job (`BridgeHeadJobFailed`) is staged again.
- `EthProcessorTransactionSubmitting`, then `…SubmitSucceeded` (with the Solana transaction) or `…SubmitFailed`.
- `…FinalizationSucceeded` or `…FinalizationFailed`: whether the `update` was finalized on Solana. After a failure the bridge head checks the program: still aligned, it stages the job again; moved on, it advances.
- `BridgeHeadAdvanced`: the batch is committed; the next job follows.

Each state carries that step's data exactly as Nori sent it. Nori sends its steps live only, so `state$` emits nothing until the next step after connecting, and a step sent while disconnected is lost; `stage$` below says where the pipeline is meanwhile. Nori reports these states itself, so this machine is not driven by the SDK. `createConnections` starts following it once for the app, on its one Nori websocket, and `getNoriBridgeInfraTransitions(nori)` gives that instance:

```ts
import { getNoriBridgeInfraTransitions } from '@nori-zk/nori-bridge-solana-sdk';

const { noriBridgeInfraTransitions, stage$, finalityTransitions$, warnings$ } =
    getNoriBridgeInfraTransitions(nori);

noriBridgeInfraTransitions.state$.subscribe(({ node, data }) => {
    if (node === 'BridgeHeadJobSucceeded') console.log('Proven:', data.verified_requests);
});
stage$.subscribe(({ stage, sinceMs }) => console.log(`${stage} since ${new Date(sinceMs).toISOString()}`));
```

`stage$` is the stage the pipeline is at and since when. Nori sends its steps live only, so `stage$` takes its position from Nori's `state.bridge`, which the server sends on every (re)connect with the time already spent in the stage, and moves on with each step; a subscriber joining at any time gets the stage the pipeline is at. `finalityTransitions$` emits each time the bridge head sees Ethereum finality move on, and `warnings$` its warnings: events during any step, not steps themselves.

## State machines

Everything that changes over time is a state machine built with [YState](https://github.com/yaw-rx/ystate), a small library of finite state machines over RxJS. A machine is a graph: named states (nodes), each carrying its own data, and the moves between them (edges), each made when something happens, such as a read finishing or a timer firing. The graphs in this README are the machines' exact definitions, and each machine's state type is derived from its graph (`StateUnion<typeof Graph.nodes>`).

The transports' machines, the connections' status machines and every reading machine are returned running, with a `close()` that moves the machine to its `closed` state and ends its streams. A reading machine is the running machine itself, with its controls on it: `close()`, `retry()` to read again at once while failed, and its own, such as the history's `loadMore()`. The unprocessed request's machine is returned ready to run: `.close().start(firstState)` runs it, and `.stop()` ends it.

A running machine has:

- `state$`: emits `{ node, data }` each time it moves, starting with where it is now;
- `event$`: emits `{ edge, from, to }` for each move;
- `status$`: `running`, `complete`, `error` or `stopped`.

### Read machines (`readThroughConnectionsOf`)

![Read through connections graph](src/rpc/connection/ReadThroughConnectionsGraph.svg)

Every value the SDK reads from the chains goes through the same states: the proof request and its witness, the paged history, the live view, the counts, the bridge's batches, a batch's requests, the bridge's state, the committed batches, Ethereum's blocks, the queue's head, a request's block time, the request a transaction enqueued, an Ethereum transaction's receipt, a Solana transaction's status, a Solana account, an address's transaction logs, a recipient's minted-so-far and token balance, the wallet's account, and every transaction the app sends, once sent (see [Transaction machines](#transaction-machines)). Each of their graphs is `readThroughConnectionsOf(data)`, given the data the value holds, with any states of its own added.

- `loading` reads the value. It carries the data already on screen: empty before the first read.
- `current` holds the value until a refresh is due: a new block or root, a log, an account changing, a signal from Nori, a timer, or a call such as `loadMore()`, depending on the machine. What makes it due is followed from before each read starts, and the read waits until it is following (a chain trigger once it knows where it starts from), so a change while the value is being read is not missed: it makes a refresh due as soon as the value arrives.
- `refreshing` reads the value again while still holding it.
- A read that fails to reach a connection reports it to that connection, which checks itself again, and the machine stays where it is until the check settles: a connection that turns out to be down is a lost connection; one that passes straight back means the read itself failed.
- `waitingForConnectionWhileLoading` and `waitingForConnectionWhileRefreshing` name the chains (or the one transport, such as `ethereum.wallet`) they wait on in `data.waitingOn`, updated as they change, and read again once those can serve a read. A read waiting to follow its refresh trigger moves there as soon as a chain it needs cannot serve it.
- `failedWhileLoading` and `failedWhileRefreshing` hold the error in `data.error` and read again by themselves after a wait that doubles with each failure in a row (`data.failedReads`); `retry()` reads again at once. A read that arrives resets `data.failedReads`.
- `closed` is terminal.

Nothing is a dead end: a lost connection is waited for, and a failure retries itself.

A machine made for one thing holds that thing in its data from its first state on, so its state shows what it is about: a proof request's `proofRequestTxHash`, a witness's `proofAvailable`, a batch's `batch`, a receipt's `transactionHash`, a Solana transaction's `signature`, an account's `address`, a view's `target`. Contract and program addresses are the machine's configuration and stay outside its data.

## Connections (`createConnections`)

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
        websocket: { url: 'wss://…' }, // optional: default, the ready http URL over wss
        wallet: true, // optional: the user's Solana wallet, to sign and send the app's Solana transactions
    },
    nori: { websocket: { url: 'wss://…' } }, // optional: default, wss://wss.solana.nori.it.com
    healthChecks: { intervalMs: 5_000, timeoutMs: 3_000 }, // optional, for every transport
    requests: { timeoutMs: 10_000 }, // optional: an http request taking longer fails and re-checks its transport
    retries: { initialDelayMs: 1_000, maxDelayMs: 30_000 }, // optional, for every transport
});
```

Every call with the same options gets the same connections: the same transports, machines and Nori websocket. Nori runs several websocket servers, each with its own cached state, so the whole app reads Nori through one socket. Each caller's `close()` releases its hold, and the connections close once every caller has closed.

What comes back has one object per transport, and a status per chain:

| Object               | Has                                                                                          |
| -------------------- | -------------------------------------------------------------------------------------------- |
| `network`            | `connection`, `status`                                                                       |
| `ethereum`           | `status`, `http`, `websocket`, `wallet`                                                      |
| `ethereum.http`      | `connection`, `close()`                                                                      |
| `ethereum.websocket` | `connection`, `socket`, `close()`                                                            |
| `ethereum.wallet`    | `connection`, `account`, `ready$(): Observable<BrowserProvider>`, `chooseWallet(uuid)`, `switchToExpectedChain()`, `close()` |
| `solana`             | `status`, `http`, `websocket`, `wallet`                                                      |
| `solana.http`        | `connection`, `close()`                                                                      |
| `solana.websocket`   | `connection`, `socket`, `close()`                                                            |
| `solana.wallet`      | `connection`, `ready$(): Observable<TransactionSendingSigner>`, `chooseWallet(name)`, `connect()`, `disconnect()`, `close()` |
| `nori`               | `status`, `websocket`, `transitions`, `bridgeState$`, `finality$`                            |

`connection` is the transport's running machine (`undefined` for an Ethereum transport the app left out). Reads go through the machines below, never through a transport directly. The wallet's `ready$()` emits its `BrowserProvider` once, when the wallet is ready, to sign with; otherwise it errors at once with `ConnectionNotReadyError`, whose `notReady` lists the wallet's state, or with `NoWalletConfiguredError` when the app gave no wallet: a setup mistake, not a state to wait out. The Solana wallet's `ready$()` emits a `@solana/kit` sending signer over the account the user shares, the same way (`NoSolanaWalletConfiguredError` without one). Wallets announce themselves on the window; outside a browser, `walletEvents` names another `EventTarget`. `nori.bridgeState$` and `nori.finality$` are Nori's `state.bridge` and `state.eth`, each stamped with when it arrived (`{ value, atMs }`), so a time inside it ages from its arrival. `close()` on the top level releases the connections.

### Connection status (`createConnectionStatusMachine`)

![Connection status graph](src/rpc/connection/ConnectionStatusGraph.svg)

`network.status`, `ethereum.status`, `solana.status` and `nori.status` each hold a running machine (`connection`) that says whether that connection can serve requests, over its transports in its calls order:

- `connected`: a transport is up; `data.transport` names the first one up in the order (`ethereum.http`, `solana.http`, …).
- `unsure`: no transport is up, and one has no outcome yet or has failed only once in a row and is checking again.
- `down`: no transport is up or unsure; `data.reason` is the state of the first transport's machine (`unreachable`, `offline`, `wrongNetwork`, …).

Any of the three moves to any other as the transports change. An app shows these as they are, e.g. green, blue and red. A wallet's own status comes from `walletStatus('ethereum.wallet')` over `ethereum.wallet.connection.state$`, and `createConnectionStatusMachine` builds a status machine over any transports, with `httpStatus`, `websocketStatus`, `walletStatus` and `networkStatus` turning each transport's states into statuses.

### Transport order per request kind

Calls, log queries and subscriptions can each go over more than one Ethereum transport, in the order the app gives in `order`; the SDK sets none. When only one configured transport can serve a kind of request, that one is used; when several can and no order is given, `createConnections` throws. Signing only ever goes through the wallet: `ethereum.wallet.ready$()` gives its `BrowserProvider`, whose `getSigner()` signs. On Solana each kind of request has one transport: calls over http, subscriptions over the websocket.

Every machine and stream below takes the connections or a chain object (`ethereum`, `solana`, `nori`) and picks the transport itself. A read runs on the first ready transport in its chain's calls order, or logs order for Ethereum's log queries. When a request in it fails to reach its node, it tells that transport and runs the read again from the start on the next ready one, so one run never mixes nodes. An app's own values are read the same way, as machines (see [Custom read machines](#custom-read-machines-startreadthroughconnectionsmachine)).

The machines read again when a chain changes (see [Chain change machines](#chain-change-machines-createchainchangesmachine-createsolanachangesmachine)). On Ethereum they subscribe on the first ready transport in the subscriptions order (the websocket, or the wallet's own `eth_subscribe` where the wallet supports it), move to the next when that one stops being ready and back when a higher one recovers, and while none is ready poll through the calls order. On Solana they subscribe on the websocket while it is open, and poll through http while it is not.

### Network connectivity machine

![Network graph](src/rpc/connection/NetworkGraph.svg)

Whether the device can reach the internet, in a browser or in Node. It checks for itself by requesting a few well-known endpoints (any one answering means online; set your own with `network.probeUrls`), because a browser reports being online on a network with no internet. The browser's own online and offline events only make it check sooner. While `offline`, every transport pauses, and when it comes back `online` they check again at once.

### HTTP transport machine

![HTTP connection graph](src/rpc/connection/HttpConnectionGraph.svg)

Ethereum's and Solana's http transports. `ready` means a health check passed on the expected network: for Ethereum `eth_chainId` then `eth_blockNumber` (`data.health.blockNumber`), for Solana `getGenesisHash` (the cluster) then `getSlot` (`data.health.slot`). While ready it checks again in the background. `unreachable` and `wrongNetwork` (the endpoint serves another chain or cluster; `data.found` and `data.expected` say which) check again after a wait that doubles each time, so an endpoint that comes back, or is fixed, recovers by itself. With several Solana endpoints each failed check moves on to the next, so one that is down or rate-limited is skipped; `data.url` says which endpoint each state is about. A request that fails to reach the node, or takes longer than `requests.timeoutMs`, makes it check at once.

### WebSocket transport machine

![WebSocket connection graph](src/rpc/connection/WebSocketConnectionGraph.svg)

Ethereum's, Solana's and Nori's websockets. `open` means the socket is connected; a socket that closes, errors or stops answering its heartbeat (Nori's ping and pong) drops to `reconnecting`, which connects again after a wait that doubles each time. A subscription is sent again every time the socket opens. `gaveUp` is reached only when `retries.maxAttempts` is set and runs out; the socket's `retry()` starts again. Messages sent while connecting are queued until the socket opens. Without a websocket URL of its own, Solana's websocket follows http: it connects to the URL whose health check passed, over wss, and reconnects when http moves on to another endpoint.

### Ethereum wallet machine (`ethereum.wallet`)

![Ethereum wallet graph](src/rpc/eth/EthereumWalletGraph.svg)

The user's wallet, found through EIP-6963 (with the older `window.ethereum` as a fallback). Its `checking`, `ready`, `wrongNetwork`, `unreachable`, `offline` and `closed` states, with their moves, are the same checked connection as http's (`healthCheckedOf`), each carrying the wallet, so the app gates on it exactly as on http; `data.url` is the wallet's reverse-DNS id (e.g. `io.metamask`). Unlike an RPC URL, a wallet on another chain is not checked again on a timer: it waits for the user to change chain.

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
| `unreachable`         | The wallet did not answer, or disconnected; it is asked again after a wait.          | Wait; the wallet's status is `down`.                                                                  |
| `offline`             | The device is offline.                                                               | Wait.                                                                                                 |

A switch request the wallet fails for another reason (e.g. it doesn't know the chain), or leaves unanswered for `switchTimeoutMs` (60 s by default), returns to `wrongNetwork` with the reason in `data.lastSwitchError`, and the app may ask again. Any chain change, in any state with a wallet, checks again. The wallet's `disconnect` moves every state with a wallet to `unreachable`, and going offline moves each of them to `offline`.

```ts
ethereum.wallet.connection?.state$.subscribe(({ node }) => {
    if (node === 'noWalletFound') console.log('Install a wallet such as MetaMask to continue.');
    if (node === 'wrongNetwork') console.log('Switch your wallet to Sepolia.');
    if (node === 'askingToSwitchChain') console.log('Confirm the switch in your wallet.');
});
```

#### Ethereum wallet account (`ethereum.wallet.account`)

![Wallet account graph](src/rpc/eth/WalletAccountGraph.svg)

`ethereum.wallet.account` is the account the wallet shares with the app, read through the wallet alone: `eth_accounts`, which never prompts the user, read again on the wallet's `accountsChanged`. `current` holds `data.account`, `undefined` while none is shared. While the wallet is not `ready`, it waits for it (`waitingOn: ['ethereum.wallet']`).

| State                  | What it means                                                                                  | What the app can do                                              |
| ---------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `current`              | `data.account` is the shared account, or `undefined`.                                          | With no account, `ethereum.wallet.account.shareAccount()` asks the user. |
| `askingToShareAccount` | The wallet is showing the request (`eth_requestAccounts`).                                     | Ask the user to confirm it.                                      |
| `shareDeclined`        | The user said no. Connecting from inside the wallet still moves it on.                         | `shareAccount()` asks again when the user clicks.                |

Another failure of the request (e.g. one already pending) goes back to `current`, with the wallet's error in `data.lastShareError` for logs; the app shows the state, not the text.

### Solana wallet machine (`solana.wallet`)

![Solana wallet graph](src/rpc/solana/SolanaWalletGraph.svg)

The user's Solana wallet, found through the Wallet Standard, which Phantom, Solflare, Backpack and the other Solana wallets implement. It signs and sends the app's Solana transactions; Solana reads always go through http. It sends on the cluster's chain (`solana:devnet`, `solana:mainnet`, …; `solana:localnet` for any other cluster).

| State               | What it means                                                                            | What the app can do                                         |
| ------------------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| `lookingForWallets` | Asking installed wallets to register themselves.                                          | Wait.                                                       |
| `noWalletFound`     | No Solana wallet is installed. A wallet installed or enabled later is picked up.         | Ask the user to install one.                                |
| `choosingWallet`    | Several wallets are installed; `data.wallets` lists them (name, icon).                    | Let the user pick, then `solana.wallet.chooseWallet(name)`. |
| `connecting`        | Asking the wallet, without a prompt, for an account it already shares with the app.     | Wait.                                                       |
| `notConnected`      | No account is shared.                                                                     | `solana.wallet.connect()` asks the user.                    |
| `askingToConnect`   | The wallet is showing the request to share an account.                                    | Ask the user to confirm it.                                 |
| `connectDeclined`   | The user said no.                                                                         | `connect()` asks again when the user clicks.                |
| `unsupportedWallet` | The wallet does not serve the cluster's chain, or cannot sign and send; `data.reason` says which. | Ask the user to pick another wallet.                 |
| `ready`             | `data.account` is the shared account, which signs.                                        | Nothing.                                                    |

A request to connect that fails for another reason goes back to `notConnected`, with the wallet's error in `data.lastConnectError`. The user connecting or switching account from inside the wallet moves to `ready` with that account; the wallet no longer sharing one, or `disconnect()`, moves to `notConnected`.

## Chain change machines (`createChainChangesMachine`, `createSolanaChangesMachine`)

![Chain changes graph](src/rpc/connection/ChainChangesGraph.svg)

Whether a chain has changed, for the machines to read again. It is what the machines read again on, and an app's own machine can too. Whoever starts one owns it and closes it. A reading machine gates on it with `changesOf$(changes)`, which emits once the machine follows the chain, then once per change, and starts nothing.

On Ethereum, `createChainChangesMachine(connections.ethereum)` counts every new block, and `createChainChangesMachine(connections.ethereum, { address, topics })` each new block holding a matching log. On Solana, `createSolanaChangesMachine(connections.solana)` counts every new root (`slotSubscribe`, a change each time the root moves), and a watch names what else counts:

| Watch | A change | Subscription |
| --- | --- | --- |
| `{ mentions: address }` | a finalized transaction mentioning the address, such as one calling a program | `logsSubscribe` |
| `{ account: address }` | the account's finalized state changing; the address may be an observable, as a program's state address is (`stateAddress$(programAddress)`) | `accountSubscribe` |
| `{ signature, commitment }` | the transaction reaching the commitment | `signatureSubscribe` |

- `subscribed`: subscribed on `data.transport`, the first transport in the chain's subscriptions order that is usable and serves the subscription (Solana's only one is its websocket); the node's acknowledgement (`acknowledged`) is where it starts following, and each push after it is a change. A transport that refuses the subscription (a wallet without `eth_subscribe`, a node refusing the filter) is recorded in `unsupported` and passed over (`subscriptionRefused`). A subscription that ends any other way is lost (`subscriptionLost`): it polls, holding the transport in `lostOn`, and subscribes on that transport again once it has been unusable and is usable again; another usable transport is subscribed on at once.
- `polling`: no transport can subscribe; it polls the calls order every interval (5 s by default), and waits in `polling` while no transport in that order can take a call. `lastPolled` is the last block (on Solana, the last finalized slot) polled; the first poll, sent at once, is where it starts following. A new block is a change, or with a filter, a new block holding a matching log; on Solana, a new finalized slot, or with an address, a transaction mentioning it in a slot after it. On Ethereum, after a gap longer than one log query, it moves on to the latest block and counts one change instead of reading the gap.
- `pollFailed`: a poll failed; it polls again after the interval, or subscribes once a transport can.
- Moving between subscribing and polling counts a change, as one may fall between the two.

## Nori websocket topics

Nori publishes its state on its websocket:

| Topic |
| --- |
| `noriBridgeInfraState$(nori)` |
| `noriBridgeInfraTimings$(nori)` |
| `noriBridgeInfraEthState$(nori)` |
| `noriBridgeInfraTransitionNotices$(nori)` |
| `noriBridgeInfraSystemNotices$(nori)` |
| `noriBridgeInfraStateWithTimings$(nori)` |

They only exist on Nori's websocket, so they have no polling; its state, timings and Ethereum state replay the latest to each new subscriber.

## Chain read machines

### Solana bridge state account (`createBridgeStateMachine`)

![Bridge state graph](src/proofQueue/BridgeStateGraph.svg)

`createBridgeStateMachine({ ethereum, solana }, programAddress?)` reads the bridge program's state account (queue cursor, batch count, latest proven head and root) through Solana, holds it in `current` as `data.bridgeState`, and reads it again each time the account changes. `programAddress` defaults to the Nori program's address.

### Ethereum latest and finalized blocks (`createEthereumBlocksMachine`)

![Ethereum blocks graph](src/proofQueue/EthereumBlocksGraph.svg)

`createEthereumBlocksMachine({ ethereum, solana })` reads Ethereum's latest and finalized blocks through Ethereum, holds them in `current` as `data.latestBlock` and `data.finalizedBlock`, and reads them again on each new block. A proof request is proven only once its block is finalized.

### Committed proof queue batches (`createCommittedProofQueueBatchesMachine`)

![Committed proof queue batches graph](src/proofQueue/CommittedProofQueueBatchesGraph.svg)

`createCommittedProofQueueBatchesMachine({ ethereum, solana }, fromIndex?, programAddress?)` reads each proof queue batch the bridge commits from its batch accounts, and reads again each time the bridge's state account changes. `current` holds `data.committed`, the batches committed since the read before, oldest first (each with its index, account address, root, cursors and output block). `data.batchCount` is the batch the next read starts from: `fromIndex` at first (default: the bridge's batch count), then how many batches the bridge held at the last read, so every batch arrives once.

### Proof queue head (`createProofQueueHeadMachine`)

![Proof queue head graph](src/proofQueue/ProofQueueHeadGraph.svg)

`createProofQueueHeadMachine({ ethereum, solana }, proofQueueAddress)` reads the queue's `head` (how many proof requests have ever been enqueued, which is also the next request's id) through Ethereum, holds it in `current` as `data.head`, and reads it again on each `ProofRequested` the queue emits.

### Ethereum transaction receipt (`createTransactionReceiptMachine`)

![Transaction receipt graph](src/rpc/connection/TransactionReceiptGraph.svg)

`createTransactionReceiptMachine({ ethereum, solana }, transactionHash)` reads an Ethereum transaction's receipt and reads it again on each new block until the transaction is mined. Every state holds the transaction in `data.transactionHash`; `current` holds `data.receipt`: `undefined` until mined, then `{ transactionHash, blockNumber, status }`, which is not read again.

### Solana signature status (`createSignatureStatusMachine`)

![Signature status graph](src/transaction/SignatureStatusGraph.svg)

`createSignatureStatusMachine({ ethereum, solana }, signature, commitment?, lastValidBlockHeight?)` reads a Solana transaction's status (`getSignatureStatuses`, searching the transaction history), and reads it again when the transaction reaches `commitment` (`finalized` by default). Every state holds `data.signature`; `current` holds `data.status` (its slot, error and confirmation status), `undefined` while the cluster does not know the transaction. A status that reached the commitment is not read again.

With `lastValidBlockHeight`, the block height after which the transaction's blockhash no longer lands, it also reads the finalized block height (`data.blockHeight`) every poll interval while the cluster does not know the transaction, and ends `expired` once that height passes `lastValidBlockHeight`: it never will land, and sending it again is a new transaction.

### Solana account (`createSolanaAccountMachine`)

![Solana account graph](src/solana/SolanaAccountGraph.svg)

`createSolanaAccountMachine({ ethereum, solana }, address)` reads an account through Solana and reads it again each time its finalized state changes. Every state holds `data.address`; `current` holds `data.account` (`lamports`, `owner`, `data` in base64, `executable`, `space`), `undefined` while no account exists at the address.

### Solana transaction logs by address (`createTransactionLogsMachine`)

![Transaction logs graph](src/solana/TransactionLogsGraph.svg)

`createTransactionLogsMachine({ ethereum, solana }, address)` gives the logs of each finalized transaction mentioning an address, such as a program's, read again on each new one. The first read finds the newest transaction there is (`data.lastSignature`); each later read gives the transactions after the last one read in `data.transactions`, oldest first, each with its `signature`, `slot`, `err` (`null` when it succeeded) and `logs`, so every transaction from the first read on arrives once.

## Proof request state (`createProofRequestStateMachine`)

![Proof request state graph](src/proofRequest/ProofRequestStateGraph.svg)

`createProofRequestStateMachine` follows the request from the transaction that enqueued it until its proof is available. While it follows the request, its value is the request's snapshot, `data.snapshot`, whose `state` says where the request is:

- `undetermined`: not found yet. `loading` looks the request up on Ethereum; a transaction that is not mined yet leaves it `undetermined`, and it is looked up again every poll interval (15 s by default) and on every recheck signal.
- `unprocessed`: the bridge has not proven it yet. It is read again every poll interval and on every recheck signal, such as Nori's stage moving on.

A read that finds a committed proof queue batch covering the request moves to the terminal node `proofAvailable`. Its `data.snapshot` carries that batch: `proofQueueBatchIndex`, `proofQueueBatchAddress` (the batch's account), its root, cursors and output blocks, and the request's `indexInBatch`. Batches are append-only, so it is never read again.

Every state holds the enqueuing transaction's hash in `data.proofRequestTxHash`. While the request is `undetermined`, each read also follows that transaction (`data.transaction`): its sender and nonce once the node knows it, the sender's mined nonce, then its receipt. A transaction that will never be mined ends the machine in a terminal node, as the request was never enqueued:

- `transactionReplaced`: the sender's mined nonce moved past the transaction's own with no receipt for it, so another transaction took its nonce.
- `transactionDropped`: the node has not known the transaction for `droppedAfterBlocks` blocks (12 by default, the machine's last argument).

Lost connections and failed reads are the waiting and failed states above. To resume a request whose snapshot the app already knows, pass it as the third argument: an `undetermined` or `unprocessed` one starts in `current`, a proven one in `proofAvailable`, instead of looking the request up.

### Proof request witness (`createProofRequestWitnessMachine`)

![Proof request witness graph](src/proofRequest/ProofRequestWitnessGraph.svg)

Once the snapshot is `proofAvailable`, `createProofRequestWitnessMachine(connections, proofQueueAddress, proofAvailable)` reads every request in its batch from Ethereum's logs, rebuilds the batch's Merkle tree, and checks it against the root committed on Solana (a mismatch is a failed read, `ProofRequestWitnessRootMismatchError`). `current` holds `data.witness`, the request's leaf, its bottom-up path and the root, and `data.verifiedWitness`, the same in the shape the program's `mint` takes (`VerifiedRequestWitnessInput`), for `mintCall$`. A committed batch never changes, so the witness is never read again.

```ts
import { filter, map, switchMap, take } from 'rxjs';
import {
    createConnections,
    createProofRequestStateMachine,
    createProofRequestWitnessMachine,
    noriBridgeInfraState$,
    type ProofRequestStateNodeUnion,
    type ProofRequestWitnessState,
} from '@nori-zk/nori-bridge-solana-sdk';

const { ethereum, solana, nori } = createConnections({
    ethereum: { expectedChainId: 11155111n, http: { rpcUrl: 'https://…' } },
    solana: { cluster: 'devnet' },
});
const proofQueueAddress = '0x…'; // the NoriProofRequestQueue address
const proofRequestTxHash = '0x…'; // the transaction that enqueued the request

const proofRequestState = createProofRequestStateMachine(
    { ethereum, solana },
    { proofQueueAddress, proofRequestTxHash },
    undefined,
    15_000,
    noriBridgeInfraState$(nori) // read again whenever the bridge moves on
);

// The witness machine's states, once the request's proof is available.
const witnessState$ = proofRequestState.state$.pipe(
    filter(
        (state): state is Extract<ProofRequestStateNodeUnion, { node: 'proofAvailable' }> =>
            state.node === 'proofAvailable'
    ),
    map(({ data }) => data.snapshot),
    take(1),
    switchMap(
        (proofAvailable) =>
            createProofRequestWitnessMachine({ ethereum, solana }, proofQueueAddress, proofAvailable).state$
    )
);
const witness$ = witnessState$.pipe(
    filter((state): state is Extract<ProofRequestWitnessState, { node: 'current' }> => state.node === 'current'),
    map(({ data }) => data.witness)
);
```

### Proof request of a transaction (`createProofRequestOfTransactionMachine`)

![Proof request of transaction graph](src/proofRequest/ProofRequestOfTransactionGraph.svg)

`createProofRequestOfTransactionMachine({ ethereum, solana }, proofQueueAddress, transactionHash)` reads the proof request a transaction enqueued (its id, target, slot key and block) and reads it again on each new block until the transaction is mined. Every state holds `data.transactionHash`; `current` holds `data.proofRequest`, `undefined` until mined, and never read again after.

### Proof request block time (`createProofRequestBlockTimeMachine`)

![Proof request block time graph](src/proofRequest/ProofRequestBlockTimeGraph.svg)

`createProofRequestBlockTimeMachine({ ethereum, solana }, blockNumber)` reads when the Ethereum block that enqueued a request was produced, once: a produced block's time never changes. Every state holds `data.blockNumber`; `current` holds `data.timestampMs`. The request's age is the time since `timestampMs`.

### Unprocessed proof request progress (`createUnprocessedProofRequestStateMachine`)

![Unprocessed proof request state graph](src/proofRequest/UnprocessedProofRequestStateGraph.svg)

Nori's bridge infra state says what an unprocessed request is waiting on: Ethereum finality for its block, the job ahead of it, then the job that includes it. `createUnprocessedProofRequestStateMachine(blockNumber, nori)` follows these from the request's block number and Nori's state, timings and Ethereum state. Every new `state.eth`, `state.bridge` or timings message recomputes where the request waits and how long it has left, and the estimate counts down each second in between. Each state's data carries `time_remaining_sec` for the step it is in, `commit_time_remaining_sec` until the batch covering the request is committed on Solana (negative once that is taking longer than expected), and `waiting_elapsed_sec` for how long it has been in that state, which together draw a progress bar.

Nori creates a job on each Ethereum finality transition, one epoch (`ETHEREUM_EPOCH_SEC`, 384 s) after the last, proving the epoch's blocks, and commits it on Solana when it finishes; a job takes at most `MAX_BATCH_SIZE` (2¹⁶) requests. A job goes through the stages in `NORI_JOB_STAGES`: proving it (`BridgeHeadJobCreated`, `BridgeHeadJobSucceeded`), then submitting it to Solana (`EthProcessorTransactionSubmitting`, `EthProcessorTransactionSubmitSucceeded`); it is committed when it reaches `EthProcessorTransactionFinalizationSucceeded`. `getCommitTimes(stage, timings)` gives, from where Nori is in its loop, the seconds until the job it is running is committed and until the next job is committed. `jobTimingsOf(timings)` gives the seconds each job stage takes: Nori's timings where it reports them, and `FALLBACK_NORI_JOB_TIMINGS` (a proof about two minutes, submitting and finalization seconds) otherwise. `getFinalityTimeRemainingSec(blockNumber, finality)` gives the seconds until a block is finalized.

## Proof queue batches and waiting requests (`createProofQueueBatchesMachine`)

![Proof queue batches graph](src/proofQueue/ProofQueueBatchesGraph.svg)

`createProofQueueBatchesMachine` keeps a live view of the bridge's newest proof queue batches and the requests no batch covers yet, for every submitting address or one (`target`). The view, `data.view`, holds `batches` (newest first, each with the target's request ids in it as `matching`), `waiting` (oldest first), `batchCount`, `queueCursor`, and Ethereum's `finalizedBlock` and `latestBlock`.

The bridge's state and Ethereum's blocks come from their machines, which the app passes in (`sources`) and can share with other views. The view stays in `loading` until both hold a value, then reads; a source that has failed fails the read. Like every reading machine, it waits for its own chains in `waitingForConnectionWhile…`. It refreshes when the bridge's batch count or queue cursor changes, on each Ethereum block past the view, when a source waits or fails, and on each recheck signal. `data.target` is the submitting address it is filtered on, its starting data.

`sortWaitingProofRequests(waiting, finalizedBlock, job?)` sorts the waiting requests into three sets, each oldest first: those whose block is not finalized yet, those finalized and scheduled for a later job, and those in the job Nori is running now. The first two come from Ethereum and Solana alone. Telling the job Nori is running apart from the later ones needs Nori's `state.bridge`; without it, every finalized request is scheduled.

```ts
import { combineLatest, filter, map, startWith } from 'rxjs';
import {
    createBridgeStateMachine,
    createEthereumBlocksMachine,
    createProofQueueBatchesMachine,
    sortWaitingProofRequests,
    type ProofQueueBatchesState,
} from '@nori-zk/nori-bridge-solana-sdk';

const bridgeState = createBridgeStateMachine({ ethereum, solana });
const ethereumBlocks = createEthereumBlocksMachine({ ethereum, solana });
const proofQueueBatches = createProofQueueBatchesMachine(
    { ethereum, solana },
    { proofQueueAddress },
    { fromBlock: queueDeploymentBlock, count: 10 },
    { bridgeState, ethereumBlocks },
    nori.transitions.stage$ // and refresh whenever Nori's stage moves on
);

const waiting$ = combineLatest([
    proofQueueBatches.state$.pipe(
        filter((state): state is Extract<ProofQueueBatchesState, { node: 'current' }> => state.node === 'current')
    ),
    nori.bridgeState$.pipe(map(({ value }) => value), startWith(undefined)),
]).pipe(map(([{ data }, job]) => sortWaitingProofRequests(data.view.waiting, data.view.finalizedBlock, job)));
```

### Proof queue batch requests (`createProofQueueBatchRequestsMachine`)

![Proof queue batch requests graph](src/proofQueue/ProofQueueBatchRequestsGraph.svg)

`createProofQueueBatchRequestsMachine(connections, proofQueueAddress, batch, query)` reads one committed batch's requests (the target's only, when given) over the batch's own block and request id range. A committed batch never changes, so they are never read again.

## Proof requests by submitting address

The submitting address is the contract that enqueued the requests: the queue records `msg.sender` as each request's `target`. Its requests are found from the queue's `ProofRequested` logs, filtered on `target`, in block ranges small enough for providers' limits, from `fromBlock` (e.g. the queue's deployment block). Each request comes back with its snapshot: `unprocessed`, or `proofAvailable` with the batch covering it, ready for `createProofRequestWitnessMachine`.

### Proof request history (`createProofRequestHistoryMachine`)

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

const proofRequestHistory = createProofRequestHistoryMachine(
    { ethereum, solana },
    { proofQueueAddress },
    { target: submittingAddress, fromBlock: queueDeploymentBlock, order: 'desc', pageSize: 20 }
);

proofRequestHistory.loadMore(); // the next page, while current
```

The first page loads at once. `current` holds the requests loaded so far in `data.loaded`, newest first with `desc` or oldest first with `asc`, and `loadMore()` reads the next page (`refreshing`), appended to them. The page that exhausts the block range moves to `allLoaded`.

### Latest proof requests (`createLatestProofRequestsMachine`)

![Latest proof requests graph](src/proofRequest/LatestProofRequestsGraph.svg)

```ts
import { createLatestProofRequestsMachine } from '@nori-zk/nori-bridge-solana-sdk';

const latestProofRequests = createLatestProofRequestsMachine(
    { ethereum, solana },
    { proofQueueAddress },
    { target: submittingAddress, fromBlock: queueDeploymentBlock, count: 10 },
    15_000, // refresh interval
    nori.transitions.stage$ // and refresh whenever Nori's stage moves on
);
```

It shows the newest `count` requests in `data.view` and keeps them current: new requests enter at the top, and requests move from `unprocessed` to `proofAvailable` as the bridge commits their batches. A request the view showed that a read no longer finds, by its id, was removed by an Ethereum reorg (`requestDroppedByReorg`): the view loads again from `fromBlock` and refills from older blocks, keeping the view on screen meanwhile. A request pushed out of the view by newer ones is not one of them. It never ends on its own.

### Proof request counts (`createProofRequestCountsMachine`)

![Proof request counts graph](src/proofRequest/ProofRequestCountsGraph.svg)

`createProofRequestCountsMachine({ ethereum, solana }, { proofQueueAddress }, { target, fromBlock })` counts a submitting address's requests and how many of them have a proof available, and reads again each time the queue enqueues a request or the bridge's state account changes. Every state holds `data.target`; `current` holds `data.counts`, `{ total, proofAvailable, unprocessed }`.

## Transaction machines

A transaction is a machine made for what it sends, which it holds as its starting data, and sent only when the app asks: on Ethereum one call, `{ to, data, value }`; on Solana one or more instructions, such as `mintCall$`'s or any program's generated builders. Each is sent through the user's wallet or with a signer of the app's own:

| Machine | Sends with | Waits on |
| --- | --- | --- |
| `createWalletTransactionMachine(connections, call, options?)` | the Ethereum wallet | `ethereum.wallet` |
| `createSignerTransactionMachine(connections, signer, call, options?)` | an ethers `Signer`, such as a `Wallet` | `ethereum` |
| `createSolanaWalletTransactionMachine(connections, instructions, options?)` | the Solana wallet's shared account | `solana.wallet` |
| `createSolanaSignerTransactionMachine(connections, signer, instructions, options?)` | a `@solana/kit` signer, such as a key pair signer | `solana` |

Every one of them:

- holds what it sends in `ready` (`data.transaction`, or `data.instructions`) until the app calls `send()`;
- moves to `notReadyToSend` on a send request while what sends it cannot, naming it in `data.waitingOn`, and back to `ready` once it can, or on `dismiss()`; it never sends by itself;
- asks the wallet (`askingToSign`) or sends (`sending`), one request per entry. The user saying no in their wallet is `declined`. The contract or program refusing it before it is sent is `refused`: on Ethereum at gas estimation, with the revert's name in `data.errorName` when the machine is given the contract's ABI (`options.errors`); on Solana in the cluster's preflight simulation, with the program in `data.programAddress`, its custom error in `data.errorCode` and `data.errorName` (named by `options.errorNames`, the Nori program's by default, `TOKEN_PROGRAM_ERROR_NAMES`; `programErrorNamesOf` builds them from any program's generated errors), and the simulation's `data.logs`. Anything else is `sendFailed` with `data.error`. Each of these shows the outcome until the next `send()`, which goes through the same gate;
- follows the sent transaction from `loading`, through the chain's connection. `current` holds it sent and not landed yet.

On Ethereum it is read again on each new block, and ends `confirmed` or `reverted` (mined, with `data.receipt`), `replaced` (the sender's mined nonce moved past its own with no receipt), or `dropped` (the node has not known it for `options.droppedAfterBlocks` blocks, 12 by default). `dropped` accepts `send()` again; a signer sends with the dropped transaction's nonce, so only one of the two can land.

On Solana it is read again when it reaches `options.commitment` (`finalized` by default; `signatureSubscribe`) and every poll interval meanwhile, and ends `confirmed` (succeeded, with `data.status`), `reverted` (failed, with the program error's name in `data.errorName`), or `expired`: the finalized block height passed the transaction's `lastValidBlockHeight` while the cluster never knew it, so it never will land. `expired` accepts `send()` again, with a new blockhash. A transaction sent with a signer whose send got no answer is followed all the same, since it may have landed. The signer pays the fee and signs every account at its address.

### Ethereum wallet transaction (`createWalletTransactionMachine`)

![Ethereum wallet transaction graph](src/rpc/connection/WalletTransactionGraph.svg)

Sent through the user's Ethereum wallet, gated on `ethereum.wallet` being ready. `askingToSign` is the wallet showing the transaction; the user saying no is `declined`, and the wallet dropping while asking is `sendFailed`. The wallet picks the nonce, so a send again after `dropped` cannot pin it.

### Ethereum signer transaction (`createSignerTransactionMachine`)

![Ethereum signer transaction graph](src/transaction/SignerTransactionGraph.svg)

Sent with an ethers `Signer` of the app's own, gated on Ethereum's connection. `sending` signs and sends it; a send again after `dropped` pins the dropped transaction's nonce.

### Solana wallet transaction (`createSolanaWalletTransactionMachine`)

![Solana wallet transaction graph](src/transaction/SolanaWalletTransactionGraph.svg)

Signed, fee-paid and sent by the account the user shares through the Solana wallet, gated on `solana.wallet` being ready. `askingToSign` is the wallet showing the transaction; the user saying no is `declined`, and the wallet dropping while asking is `sendFailed`.

### Solana signer transaction (`createSolanaSignerTransactionMachine`)

![Solana signer transaction graph](src/transaction/SolanaSignerTransactionGraph.svg)

Signed and fee-paid by a `@solana/kit` signer of the app's own and sent through Solana's connection, gated on it. `sending` reads a recent blockhash, signs and sends; a send that got no answer is followed as sent.

### Sending a Solana transaction with a signer

```ts
import { createSolanaSignerTransactionMachine, TOKEN_PROGRAM_ERROR_NAMES } from '@nori-zk/nori-bridge-solana-sdk';

const sending = createSolanaSignerTransactionMachine({ ethereum, solana }, signer, instructions, {
    errorNames: TOKEN_PROGRAM_ERROR_NAMES,
});
sending.send();
sending.state$.subscribe(({ node, data }) => {
    if (node === 'refused') console.log(`Refused: ${data.errorName}`);
    if (node === 'expired') console.log('It did not land in time; send it again.');
    if (node === 'confirmed') console.log('Finalized.');
});
```

## Minting on Solana (`mintCall$`)

The Nori program mints the bridged token against a deposit proven on Ethereum: a proof request whose leaf commits to `sha256` of the Solana recipient's address. Once the request's snapshot is `proofAvailable`, its witness machine's `verifiedWitness` is the deposit's witness in the shape `mint` takes, and `mintCall$(recipient, verifiedWitness, proofQueueBatchAddress)` gives the instruction, with every account derived: the bridge's state and token, the recipient's associated token account and minted-so-far account, and the committed batch. The recipient signs it and pays for the accounts it creates, so it is sent by `createSolanaWalletTransactionMachine` with the recipient's wallet, or `createSolanaSignerTransactionMachine` with the recipient's signer. Minting is delta-based: the program mints what was locked and not yet minted, and minting again with nothing new locked is refused with `ZERO_MINT_AMOUNT`.

```ts
import { filter, map, switchMap, take } from 'rxjs';
import { createSolanaWalletTransactionMachine, mintCall$ } from '@nori-zk/nori-bridge-solana-sdk';

// witnessState$ and proofAvailable: the request's witness machine and snapshot, as above;
// recipient: the wallet's signer, from solana.wallet.ready$().
const minting$ = witnessState$.pipe(
    filter((state): state is Extract<ProofRequestWitnessState, { node: 'current' }> => state.node === 'current'),
    take(1),
    switchMap(({ data }) => mintCall$(recipient, data.verifiedWitness, proofAvailable.proofQueueBatchAddress)),
    map((instruction) => createSolanaWalletTransactionMachine({ ethereum, solana }, [instruction]))
);
```

### Minted so far (`createMintedSoFarMachine`)

![Minted so far graph](src/tokenBridge/MintedSoFarGraph.svg)

`createMintedSoFarMachine(connections, recipient, programAddress?)` reads what a recipient has minted so far, from its minted-so-far account, and reads again each time that account changes. Every state holds `data.recipient`; `current` holds `data.minted`, `0` before the first mint.

### Token balance (`createTokenBalanceMachine`)

![Token balance graph](src/tokenBridge/TokenBalanceGraph.svg)

`createTokenBalanceMachine(connections, owner, mint?)` reads an owner's balance of the bridged token (or `mint`) from its associated token account, and reads again each time that account changes. Every state holds `data.owner`; `current` holds `data.balance`, `0` while the owner has no token account.

## Custom read machines (`startReadThroughConnectionsMachine`)

A value of the app's own, such as a read of its own contract or program account, is a machine too: its graph spreads `readThroughConnectionsOf`, given the data the value holds, and `startReadThroughConnectionsMachine` starts it, the way every machine in the SDK is started. The read is given `clients`, one runner per chain, and runs only inside the machine; the machine waits out lost connections and retries failed reads as described in [Read machines](#read-machines-readthroughconnectionsof).

```ts
import { define } from '@yaw-rx/ystate';
import { Contract } from 'ethers';
import { map, NEVER } from 'rxjs';
import {
    dueOn,
    ethRpcRead$,
    readThroughConnectionsOf,
    startReadThroughConnectionsMachine,
} from '@nori-zk/nori-bridge-solana-sdk';

const TokenSymbolGraph = define(readThroughConnectionsOf({ symbol: undefined as string | undefined }));

const tokenSymbol = startReadThroughConnectionsMachine(TokenSymbolGraph, {
    connections: { ethereum, solana },
    read: (clients) =>
        clients
            .ethereum((provider) =>
                ethRpcRead$(
                    () => new Contract(token, ['function symbol() view returns (string)'], provider).symbol(),
                    'Failed to read the token symbol.'
                )
            )
            .pipe(map((symbol: string) => ({ symbol }))),
    refreshOn: () => dueOn(NEVER), // a token's symbol never changes
    needs: ['ethereum'],
});
```

`clients.solana(rpc => …)` runs a read on Solana's http the same way, given its `@solana/kit` RPC. Each chain call is wrapped once in the lowest step the SDK's own reads are built from: `ethRpcRead$(() => call, failure)` for ethers, and `solanaRpcRead$((abortSignal) => rpc.….send({ abortSignal }), failure)` for kit, which cancels the call when the machine closes and tells Solana's http when the node gave no answer, an HTTP error or reported itself unhealthy, so the machine waits for the connection instead of failing. A log query over a long block range goes through `from(blockRanges(fromBlock, toBlock, MAX_BLOCK_RANGE_PER_QUERY, 'asc')).pipe(concatMap(…))`, in ranges providers accept. A read that needs another read machine's value, such as the bridge's batches, takes it with `readMachineValue$(machine)` (its `current` data once, or its error once it fails) and is due again on `readMachineChanged$(machine, changed)` (each `current` value `changed` says differs from what was read, and each wait or failure), as the SDK's batches view does with the bridge's state. `refreshOn` says when the value is due to be read again. It is followed from before each read: its first emission says it is following, and each later one that a refresh is due. A change on a chain is `changesOf$(changes)` of a chain changes machine the reading machine's creator starts and passes in `owns`, so the reading machine's `close()` closes it too (see [Chain changes](#chain-change-machines-createchainchangesmachine-createsolanachangesmachine)); it says it is following once that machine knows where it starts from; a trigger with no starting point (a timer, a signal, a Nori topic) goes through `dueOn(trigger$)`, and a value that cannot change uses `dueOn(NEVER)`. `needs` names the chains the read goes through (both by default), or one transport such as `'ethereum.wallet'`, which the read reaches through `clients.transport`; `kind: 'logs'` sends Ethereum's part through its logs order. A read returns the graph's `current` data. A graph with states of its own passes their transitions as `ownTransitions`, given the shared read's outcomes, and `start` starts it in another node with its data.

## API

Everything below comes from `@nori-zk/nori-bridge-solana-sdk`. Every machine also exports its graph (`…Graph`, the definition its image is drawn from) and its state type (`…State`, or `…NodeUnion`, from `StateUnion<typeof …Graph.nodes>`), and its options or query type where it takes one.

### Connection exports

| Export | What it is |
| --- | --- |
| `createConnections(options)` | The app's connections: `network`, `ethereum`, `solana`, `nori`, `close()` ([Connections](#connections-createconnections)) |
| `createConnectionStatusMachine`, `httpStatus`, `websocketStatus`, `walletStatus`, `networkStatus` | A status machine over any transports, and each transport's states as statuses ([Connection status](#connection-status-createconnectionstatusmachine)) |
| `createChainChangesMachine(ethereum, filter?)`, `createSolanaChangesMachine(solana, watch?)`, `changesOf$` | Whether a chain has changed, and the trigger a reading machine gates on ([Chain change machines](#chain-change-machines-createchainchangesmachine-createsolanachangesmachine)) |
| `PUBLIC_SOLANA_CLUSTERS`, `DEFAULT_NORI_WEBSOCKET_URL`, `DEFAULT_CONNECTIVITY_PROBE_URLS` | The public clusters' URLs and genesis hashes, Nori's websocket, the network check's endpoints |
| `ConnectionNotReadyError`, `NoWalletConfiguredError`, `NoSolanaWalletConfiguredError` | A transport not ready, and a wallet the app gave none of |
| `requestErrorCode`, `USER_REJECTED_REQUEST` | An EIP-1193 error's code, and the user's refusal (4001) |

### Read machine exports

| Machine | Section |
| --- | --- |
| `createBridgeStateMachine` | [Solana bridge state account](#solana-bridge-state-account-createbridgestatemachine) |
| `createEthereumBlocksMachine` | [Ethereum latest and finalized blocks](#ethereum-latest-and-finalized-blocks-createethereumblocksmachine) |
| `createCommittedProofQueueBatchesMachine` | [Committed proof queue batches](#committed-proof-queue-batches-createcommittedproofqueuebatchesmachine) |
| `createProofQueueHeadMachine` | [Proof queue head](#proof-queue-head-createproofqueueheadmachine) |
| `createTransactionReceiptMachine` | [Ethereum transaction receipt](#ethereum-transaction-receipt-createtransactionreceiptmachine) |
| `createSignatureStatusMachine` | [Solana signature status](#solana-signature-status-createsignaturestatusmachine) |
| `createSolanaAccountMachine` | [Solana account](#solana-account-createsolanaaccountmachine) |
| `createTransactionLogsMachine` | [Solana transaction logs by address](#solana-transaction-logs-by-address-createtransactionlogsmachine) |
| `createProofRequestStateMachine` | [Proof request state](#proof-request-state-createproofrequeststatemachine) |
| `createProofRequestWitnessMachine` | [Proof request witness](#proof-request-witness-createproofrequestwitnessmachine) |
| `createProofRequestOfTransactionMachine` | [Proof request of a transaction](#proof-request-of-a-transaction-createproofrequestoftransactionmachine) |
| `createProofRequestBlockTimeMachine` | [Proof request block time](#proof-request-block-time-createproofrequestblocktimemachine) |
| `createUnprocessedProofRequestStateMachine` | [Unprocessed proof request progress](#unprocessed-proof-request-progress-createunprocessedproofrequeststatemachine) |
| `createProofQueueBatchesMachine` | [Proof queue batches and waiting requests](#proof-queue-batches-and-waiting-requests-createproofqueuebatchesmachine) |
| `createProofQueueBatchRequestsMachine` | [Proof queue batch requests](#proof-queue-batch-requests-createproofqueuebatchrequestsmachine) |
| `createProofRequestHistoryMachine` | [Proof request history](#proof-request-history-createproofrequesthistorymachine) |
| `createLatestProofRequestsMachine` | [Latest proof requests](#latest-proof-requests-createlatestproofrequestsmachine) |
| `createProofRequestCountsMachine` | [Proof request counts](#proof-request-counts-createproofrequestcountsmachine) |
| `createMintedSoFarMachine` | [Minted so far](#minted-so-far-createmintedsofarmachine) |
| `createTokenBalanceMachine` | [Token balance](#token-balance-createtokenbalancemachine) |

### Transaction exports

| Export | What it is |
| --- | --- |
| `createWalletTransactionMachine` | An Ethereum transaction through the wallet ([Ethereum wallet transaction](#ethereum-wallet-transaction-createwallettransactionmachine)) |
| `createSignerTransactionMachine` | An Ethereum transaction with the app's signer ([Ethereum signer transaction](#ethereum-signer-transaction-createsignertransactionmachine)) |
| `createSolanaWalletTransactionMachine` | A Solana transaction through the wallet ([Solana wallet transaction](#solana-wallet-transaction-createsolanawallettransactionmachine)) |
| `createSolanaSignerTransactionMachine` | A Solana transaction with the app's signer ([Solana signer transaction](#solana-signer-transaction-createsolanasignertransactionmachine)) |
| `mintCall$(recipient, verifiedWitness, proofQueueBatchAddress, programAddress?)` | The program's `mint` instruction ([Minting on Solana](#minting-on-solana-mintcall)) |
| `TOKEN_PROGRAM_ERROR_NAMES`, `programErrorNamesOf(programAddress, errors, prefix)` | The Nori program's custom error names, and any program's from its generated errors |
| `instructionsToSendOf(instructions)` | Instructions as a Solana transaction machine's state holds them (`InstructionToSend`) |
| `NotReadyToSendError` | A send request while what sends it is not usable |

### Nori exports

| Export | What it is |
| --- | --- |
| `getNoriBridgeInfraTransitions(nori)` | The pipeline's transitions and `stage$` ([Nori prover pipeline](#nori-prover-pipeline-getnoribridgeinfratransitions)) |
| `noriBridgeInfraState$`, `noriBridgeInfraTimings$`, `noriBridgeInfraEthState$`, `noriBridgeInfraTransitionNotices$`, `noriBridgeInfraSystemNotices$`, `noriBridgeInfraStateWithTimings$` | Nori's topics ([Nori websocket topics](#nori-websocket-topics)) |
| `getCommitTimes`, `jobTimingsOf`, `getFinalityTimeRemainingSec`, `NORI_JOB_STAGES`, `FALLBACK_NORI_JOB_TIMINGS`, `ETHEREUM_EPOCH_SEC`, `MAX_BATCH_SIZE` | Nori's job loop and its timings ([Unprocessed proof request progress](#unprocessed-proof-request-progress-createunprocessedproofrequeststatemachine)) |
| `sortWaitingProofRequests(waiting, finalizedBlock, job?)` | The waiting requests by what they wait on |
| `arrived` | A Nori topic's values stamped with when they arrived (`{ value, atMs }`) |
| `BridgeProofRequestProcessingStatus` | Where Nori's pipeline holds an unprocessed request |

### Custom machine exports

| Export | What it is |
| --- | --- |
| `readThroughConnectionsOf(data)`, `startReadThroughConnectionsMachine(graph, options)`, `dueOn(trigger$)` | A value of the app's own, read and kept current ([Custom read machines](#custom-read-machines-startreadthroughconnectionsmachine)) |
| `readMachineValue$`, `readMachineChanged$` | One read machine gated on another |
| `ethRpcRead$`, `solanaRpcRead$` | The lowest step: one ethers or kit call |
| `blockRanges`, `MAX_BLOCK_RANGE_PER_QUERY` | A long log query in ranges providers accept |
| `stateAddress$`, `proofQueueBatchAddress$`, `bridgedTokenAddress$`, `tokenAccountStorageAddress$`, `associatedTokenAddress$`, `SPL_TOKEN_PROGRAM_ADDRESS`, `ASSOCIATED_TOKEN_PROGRAM_ADDRESS` | The Nori program's accounts and a token account's address |
| `sentTransactionOf(sending)`, `sentSolanaTransactionOf(sending)` | The send-and-follow shape a transaction graph spreads, with its own sending node |
| `readThroughConnectionsTransitions`, `withOutcome` | The read shape's transitions, and outcomes split by a field |
| `ProofRequestState` | A snapshot's `state`: `undetermined`, `unprocessed`, `proofAvailable` |

`@nori-zk/nori-bridge-solana-sdk/utils` holds `stateOf$`, `dataOnEntry$`, `atNode`, `AsNodeData`, `StartedMachine` and `GraphState`, and `@nori-zk/nori-bridge-solana-sdk/program` the program's generated client: its accounts, instructions, PDAs, constants and error codes.

### Error classes

| Error | When |
| --- | --- |
| `EthRpcTransportError`, `SolanaRpcTransportError` | A read that may not have reached its node; it tells the transport |
| `ProofRequestTransactionNotMinedError` | The transaction that enqueues a proof request is not mined yet |
| `ProofRequestWitnessRootMismatchError` | A rebuilt batch root differs from the committed one |
| `ProofQueueBatchSearchError` | No committed batch covers a request |
| `SolanaAccountMismatchError` | An account is not the program's, or not of the type expected |
| `MalformedProofRequestError` | A proof request whose fields do not decode (`requestId`); for an app's own reads, as no read of the SDK raises it |

### Exported types

Besides each machine's state type, these are exported as types:

| Area | Types |
| --- | --- |
| Connections | `ConnectionsOptions`, `NetworkOptions`, `EthereumOrder`, `Connections`, `Ethereum`, `Solana`, `Nori`, `ProofRequestConnections`, `ConnectionName`, `ChainTransportName`, `ReadNeed`, `TransportName`, `TransportState`, `LiveConnectionStatus`, `StatusTransport`, `EthereumSubscriptionSocket`, `EthereumProvider`, `Eip1193EventProvider`, `SolanaRpc`, `SolanaCluster`, `EthereumHealth`, `SolanaHealth`, `WalletInfo`, `WalletAccount`, `SolanaWalletInfo` |
| An app's own machines | `ChainRead`, `SolanaRead`, `ConnectedReadClients`, `ReadThroughConnectionsOptions`, `ReadThroughConnectionsMachineOptions`, `ReadThroughConnectionsRead`, `ReadRetryBackoff`, `ChainChanges`, `SolanaWatch`, `EthereumLogsFilter` |
| Sending | `TransactionCall`, `TransactionToSend`, `SentTransaction`, `TransactionMachineOptions`, `InstructionToSend`, `SentSolanaTransaction`, `SolanaTransactionMachineOptions`, `ProgramErrorNames` |
| Chain values | `EthereumTransactionReceipt`, `SolanaSignatureStatus`, `SolanaAccount`, `SolanaTransactionLogs`, `SolanaBridgeState` (the program's state account) |
| Proof requests | `FollowedProofRequest`, `ProofRequest`, `ProofRequestStateSnapshot`, `UndeterminedProofRequestSnapshot`, `UnprocessedProofRequestSnapshot`, `ProofAvailableProofRequestSnapshot`, `ProofRequestStateSnapshotRequest`, `RequestLeaf`, `RequestWitness`, `VerifiedRequestWitness`, `ProofRequestWitnesses`, `ProofRequestHistoryQuery`, `ProofRequestHistoryAddresses`, `ProofRequestHistoryPage`, `ProofRequestHistoryEntry`, `ProofRequestHistoryCursor`, `ProofRequestHistoryOrder`, `ProofRequestsByTargetQuery`, `ProofRequestsByTargetPage`, `LatestProofRequestsQuery`, `ProofRequestCountsQuery`, `ProofRequestCounts` |
| Proof queue | `ProofQueueBatchesQuery`, `ProofQueueBatchesSources`, `ProofQueueBatchesView` (`EMPTY_PROOF_QUEUE_BATCHES_VIEW` before the first read), `ShownProofQueueBatch`, `ProofQueueBatchSummary`, `FoundProofQueueBatch`, `ProofQueueBatchRequestsQuery`, `ProofRequestBatchEntry`, `EnqueuedProofRequest`, `EnqueuedProofRequestsQuery` |
| Nori | `NoriBridgeInfraTransitions`, `NoriBridgeInfraStage`, `NoriBridgeInfraStageSince`, `NoriStage`, `NoriJobStage`, `NoriJobTimings`, `NoriJob`, `CommitTimes`, `EthereumFinality`, `WaitingProofRequests`, `Arrived` |

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
surfpool start --offline --no-deploy --ci   # in another terminal, for the integration tests
npm run test:integration
```

`test:integration` runs the Solana changes, signature status, account and transaction logs machines, the Solana signer and wallet transaction machines (the wallet a Wallet Standard wallet over a key pair), and a mint, against a running local validator: `SOLANA_RPC_URL` (default `http://127.0.0.1:8899`) and `SOLANA_WS_URL` (default `ws://127.0.0.1:8900`). The mint plants the Nori program from `../target/deploy/token.so` and initializes the bridge when they are not there yet, plants a committed batch with surfpool's `surfnet_setAccount` cheatcode, sends `mint` with `mintCall$`, and follows the minted-so-far and token balance machines. It airdrops to fresh addresses and plants each batch at a fresh index, so it needs no reset between runs.

The state machine graphs' images are generated from their definitions with [ystate-visualizer](https://github.com/yaw-rx/ystate), next to each definition. It reads a graph written as `define({ nodes, edges })` or as `define(readThroughConnectionsOf({ … }))`, and `--strict` fails on any graph it cannot draw exactly. From `src/`, for every graph:

```sh
for f in $(grep -rl "= define(" --include=*.ts . | grep -v spec); do
    ystate-visualizer --strict -o "$(dirname "$f")" "$f"
done
```
