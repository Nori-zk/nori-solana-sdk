# Solana: nori-bridge-solana-sdk

TypeScript SDK for the Nori Ethereum→Solana proof queue. An Ethereum contract enqueues a storage-proof request on [NoriProofRequestQueue.sol](../ethereum/contracts/NoriProofRequestQueue.sol); the bridge proves it and commits the batch that settled it on Solana. This SDK follows a request from the transaction that enqueued it until its batch is committed, builds the request's Merkle witness against that batch root, and shows a submitting address's requests as a paged history or a live view.

It is built to keep working through whatever the user's setup does: no wallet, several wallets, the wallet on the wrong chain, a public RPC rate-limiting, the device going offline. Nothing it does gets stuck. Lost connections are waited for, failed reads retry themselves, and every state that holds reading up says why, so the app can tell the user.

## Install

```sh
npm install @nori-zk/nori-bridge-solana-sdk @solana/kit
```

`@solana/kit` is a peer dependency. The witness hashing comes from `@nori-zk/ethereum-solana-proof-queue-utils-glam`, the SP1 guest's own hashing compiled to WebAssembly, installed as a dependency.

## State machines

Everything that changes over time is a state machine built with [YState](https://github.com/yaw-rx/ystate), a small library of finite state machines over RxJS. A machine is a graph: named states (nodes), each carrying its own data, and the moves between them (edges), each made when something happens, such as a read finishing or a timer firing. The graphs below are the machines' exact definitions.

The connection, proof request, history and live view machines are returned running, with a `close()` that moves the machine to its `closed` state and ends its streams. The two websocket machines (the unprocessed request's progress and the socket's connectivity) are returned ready to run: `.close().start(firstState)` runs one, and `.stop()` ends it.

A running machine has:

- `state$`: emits `{ node, data }` each time it moves, starting with where it is now;
- `event$`: emits `{ edge, from, to }` for each move;
- `status$`: `running`, `complete`, `error` or `stopped`.

## Connections

Reads go through two connections, each tracked by its own machine:

- **Ethereum**: the user's wallet, found through EIP-6963 (with the older `window.ethereum` as a fallback), unless the app gives an RPC URL.
- **Solana**: the cluster's public RPC endpoint, unless the app gives its own endpoints. Solana wallets do not serve reads, so this is always an RPC node.

Both follow a third machine that checks whether the device is online. `createProofRequestConnections` starts all three:

```ts
import { createProofRequestConnections } from '@nori-zk/nori-bridge-solana-sdk';

const { network, connections, close } = createProofRequestConnections({
    // Reads through the user's wallet; add `rpcUrl` to read through a node instead.
    ethereum: { expectedChainId: 11155111n },
    // Reads through https://api.devnet.solana.com; add `rpcUrls` to use your own.
    solana: { cluster: 'devnet' },
});
```

`connections` is what the reading machines below take. The states of these three machines are what an app shows when reading is held up:

```ts
import { createProofRequestConnections } from '@nori-zk/nori-bridge-solana-sdk';

const { network, connections } = createProofRequestConnections({
    ethereum: { expectedChainId: 11155111n },
    solana: { cluster: 'devnet' },
});

network.state$.subscribe(({ node }) => {
    if (node === 'offline')
        console.log('You are offline. Everything resumes when you are back.');
});

const wallet = connections.ethereum.wallet; // undefined when reading through an RPC URL
wallet?.ethereumWallet.state$.subscribe(({ node }) => {
    if (node === 'noWalletFound')
        console.log('Install a wallet such as MetaMask to continue.');
    if (node === 'onOtherChain') console.log('Switch your wallet to Sepolia.');
    if (node === 'askingToSwitchChain')
        console.log('Confirm the switch in your wallet.');
    if (node === 'switchDeclined')
        console.log('Switch to Sepolia in your wallet when you are ready.');
});
```

### Network

![Network graph](src/proofRequest/rpc/ystate/NetworkGraph.svg)

Whether the device can reach the internet, in a browser or in Node. It checks for itself by requesting a few well-known endpoints (any one answering means online; set your own with `network.probeUrls`), because a browser reports being online on a network with no internet. The browser's own online and offline events only make it check sooner. While `offline`, both connections pause, and when it comes back `online` they check again at once.

### Ethereum wallet

![Ethereum wallet graph](src/proofRequest/rpc/eth/ystate/EthereumWalletGraph.svg)

Runs only when reads go through the wallet.

| State                 | What it means                                                                       | What the app can do                                                                           |
| --------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `lookingForWallets`   | Asking installed wallets to announce themselves.                                    | Wait.                                                                                         |
| `noWalletFound`       | No wallet is installed. A wallet installed or enabled later is picked up.           | Ask the user to install one.                                                                  |
| `choosingWallet`      | Several wallets are installed; `data.wallets` lists them (name, icon).              | Let the user pick, then `wallet.chooseWallet(uuid)`.                                          |
| `checkingChain`       | Asking the wallet which chain it is on.                                             | Wait.                                                                                         |
| `walletNotResponding` | The wallet did not answer; it is asked again after a wait that doubles each time.   | Show `data.error`.                                                                            |
| `onExpectedChain`     | Ready to read.                                                                      | Nothing.                                                                                      |
| `onOtherChain`        | The wallet is on `data.chainId`, not `data.expectedChainId`.                        | Ask the user to switch, or call `wallet.switchToExpectedChain()` to have the wallet ask them. |
| `askingToSwitchChain` | The wallet is showing the switch request.                                           | Ask the user to confirm it.                                                                   |
| `switchDeclined`      | The user said no. The wallet is not asked again until they change chain themselves. | Tell them they can switch in their wallet whenever they're ready.                             |

A switch request the wallet fails for another reason (e.g. it doesn't know the chain) returns to `onOtherChain` with the wallet's error in `data.lastSwitchError`. Any chain change, in any state, checks the chain again.

### Ethereum provider

![Ethereum provider connectivity graph](src/proofRequest/rpc/eth/ystate/EthereumProviderConnectivityGraph.svg)

Whether reads can run through the wallet or the RPC URL. It is `ready` when a health check (`eth_chainId`, then `eth_blockNumber`) passes on the expected chain, and checks again in the background while ready. `waitingForWallet` means the wallet machine is not on the expected chain yet; its state says why. `unreachable` and `wrongNetwork` (an RPC URL on another chain) check again after a wait that doubles each time, so an endpoint that comes back, or is fixed, recovers by itself. A read that fails to reach the provider makes it check at once.

### Solana RPC

![Solana RPC connectivity graph](src/proofRequest/rpc/solana/ystate/SolanaRpcConnectivityGraph.svg)

The same shape for the Solana endpoints, checked with `getGenesisHash` (to confirm the cluster) and `getSlot`. With several endpoints, each failed check moves on to the next, so one that is down or rate-limited is skipped; `data.rpcUrl` says which endpoint each state is about. For a cluster other than the public ones (e.g. a local validator), give `rpcUrls` and its `expectedGenesisHash`.

## Following one proof request

![Proof request state graph](src/proofRequest/ystate/ProofRequestStateGraph.svg)

`createProofRequestStateMachine` follows the request from the transaction that enqueued it:

- `undetermined` looks the request up on Ethereum. A transaction that is not mined yet is waited for.
- `unprocessed` means the bridge has not proven it yet; the machine checks the bridge every poll interval (15 s by default) and on every recheck signal, such as bridge state changes from the websocket.
- `proofAvailable` means a committed proof queue batch covers it. Batches are append-only, so it stays there.
- `waitingForConnection…` waits for both connections, naming them in `data.waitingOn`, and resumes where it was.
- `failed…` holds the error in `data.error` and reads again by itself after a wait that doubles with each failure in a row (`data.failedReads`); `retry()` reads again at once.

Once the request is `proofAvailable`, `fetchProofRequestWitness` fetches every request in its batch from Ethereum, rebuilds the batch's Merkle tree, and returns the request's leaf, its bottom-up path and the root, checked against the root committed on Solana (a mismatch throws `ProofRequestWitnessRootMismatchError`):

```ts
import { filter } from 'rxjs';
import {
    createProofRequestConnections,
    createProofRequestStateMachine,
    fetchProofRequestWitness,
    type ProofRequestStateNodeUnion,
} from '@nori-zk/nori-bridge-solana-sdk';

const { connections } = createProofRequestConnections({
    ethereum: { expectedChainId: 11155111n },
    solana: { cluster: 'devnet' },
});
const proofQueueAddress = '0x…'; // the NoriProofRequestQueue address
const proofRequestTxHash = '0x…'; // the transaction that enqueued the request

const { proofRequestState } = createProofRequestStateMachine(connections, {
    proofQueueAddress,
    proofRequestTxHash,
});

proofRequestState.state$
    .pipe(
        filter(
            (
                state
            ): state is Extract<
                ProofRequestStateNodeUnion,
                { node: 'proofAvailable' }
            > => state.node === 'proofAvailable'
        )
    )
    .subscribe(async ({ data }) => {
        const { leaf, path, root } = await fetchProofRequestWitness(data, {
            provider: connections.ethereum.currentProvider(),
            proofQueueAddress,
        });
    });
```

To resume a request whose state the app already knows, pass that snapshot as the third argument; the machine starts there instead of looking the request up.

### While unprocessed: the bridge's progress

![Unprocessed proof request state graph](src/proofRequest/ystate/UnprocessedProofRequestStateGraph.svg)

The bridge websocket says what an unprocessed request is waiting on: Ethereum finality for its block, the bridge job ahead of it, then the job that includes it. `createUnprocessedProofRequestStateMachine(blockNumber, topics)` follows these from the request's block number and the `ethStateTopic$`, `bridgeStateTopic$` and `bridgeTimingsTopic$` websocket topics, with an estimate of the time remaining in each state's data.

The topics come from one reconnecting websocket. `getBridgeSocketWithConnectivity$(url?)` opens it (default [`wss://wss.solana.nori.it.com`](https://wss.solana.nori.it.com)) and returns the message stream with a machine tracking its connection:

![Bridge socket connectivity graph](src/rx/ystate/BridgeSocketConnectivityGraph.svg)

## A submitting address's requests

The submitting address is the contract that enqueued the requests: the queue records `msg.sender` as each request's `target`. Its requests are found from the queue's `ProofRequested` logs, filtered on `target`, in block ranges small enough for providers' limits, from `fromBlock` (e.g. the queue's deployment block). Each request comes back with where it is now: `unprocessed`, or `proofAvailable` with the batch covering it, ready for `fetchProofRequestWitness`.

### Paged history

![Proof request history graph](src/proofRequest/ystate/ProofRequestHistoryGraph.svg)

```ts
import {
    createProofRequestConnections,
    createProofRequestHistoryMachine,
} from '@nori-zk/nori-bridge-solana-sdk';

const { connections } = createProofRequestConnections({
    ethereum: { expectedChainId: 11155111n },
    solana: { cluster: 'devnet' },
});
const proofQueueAddress = '0x…'; // the NoriProofRequestQueue address
const submittingAddress = '0x…'; // the contract that enqueued the requests
const queueDeploymentBlock = 0; // the block the queue was deployed in

const history = createProofRequestHistoryMachine(
    connections,
    { proofQueueAddress },
    {
        target: submittingAddress,
        fromBlock: queueDeploymentBlock,
        order: 'desc',
        pageSize: 20,
    }
);

history.loadMore(); // the next page, while waiting for more
```

The first page loads at once and every state carries the requests loaded so far in `data.loaded`, newest first with `desc` or oldest first with `asc`. `allLoaded` means the block range is exhausted. Lost connections and failed reads are handled as for a single request.

### Live view of the newest requests

![Latest proof requests graph](src/proofRequest/ystate/LatestProofRequestsGraph.svg)

```ts
import {
    createLatestProofRequestsMachine,
    createProofRequestConnections,
    getBridgeSocketWithConnectivity$,
    getBridgeStateTopic$,
} from '@nori-zk/nori-bridge-solana-sdk';

const { connections } = createProofRequestConnections({
    ethereum: { expectedChainId: 11155111n },
    solana: { cluster: 'devnet' },
});
const proofQueueAddress = '0x…'; // the NoriProofRequestQueue address
const submittingAddress = '0x…'; // the contract that enqueued the requests
const queueDeploymentBlock = 0; // the block the queue was deployed in

const { bridgeSocket$ } = getBridgeSocketWithConnectivity$();
const latest = createLatestProofRequestsMachine(
    connections,
    { proofQueueAddress },
    { target: submittingAddress, fromBlock: queueDeploymentBlock, count: 10 },
    15_000, // refresh interval
    getBridgeStateTopic$(bridgeSocket$) // and refresh whenever the bridge moves on
);
```

It shows the newest `count` requests in `data.view` and keeps them current: new requests enter at the top, and requests move from `unprocessed` to `proofAvailable` as the bridge commits their batches. A request that an Ethereum reorg removes leaves the view, which refills from older blocks. It never ends on its own.

### One-off reads

Without a machine, `fetchProofRequestHistoryPage(request, query)` reads one page and `fetchProofRequestCountsByTarget(request, query)` counts a submitting address's requests, and how many have a proof available:

```ts
import { firstValueFrom } from 'rxjs';
import {
    bothReady$,
    createProofRequestConnections,
    fetchProofRequestCountsByTarget,
} from '@nori-zk/nori-bridge-solana-sdk';

const { connections } = createProofRequestConnections({
    ethereum: { expectedChainId: 11155111n },
    solana: { cluster: 'devnet' },
});
const proofQueueAddress = '0x…'; // the NoriProofRequestQueue address
const submittingAddress = '0x…'; // the contract that enqueued the requests
const queueDeploymentBlock = 0; // the block the queue was deployed in

await firstValueFrom(bothReady$(connections));
const { total, proofAvailable, unprocessed } =
    await fetchProofRequestCountsByTarget(
        {
            proofQueueAddress,
            provider: connections.ethereum.currentProvider(),
            rpc: connections.solana.currentRpc(),
        },
        { target: submittingAddress, fromBlock: queueDeploymentBlock }
    );
```

They take the clients directly, so they run once and throw on failure rather than waiting for connections. Read them while both connections are `ready`, or with clients of your own. `getProofRequestStateSnapshot` does the same for one request.

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

The state machine graphs' images are generated from their definitions with [ystate-visualizer](https://github.com/yaw-rx/ystate):

```sh
ystate-visualizer --strict -o src/proofRequest/ystate src/proofRequest/ystate/proofRequest.ts
```
