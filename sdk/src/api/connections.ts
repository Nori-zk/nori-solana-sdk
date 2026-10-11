export {
    createConnections,
    type Connections,
    type ConnectionsOptions,
    type Ethereum,
    type EthereumOrder,
    type EthereumSubscriptionSocket,
    type Nori,
    type Solana,
} from '../rpc/connection/connections.js';
export {
    ConnectionNotReadyError,
    type TransportName,
    type TransportState,
} from '../rpc/connection/connectionNotReady.js';
export { NoWalletConfiguredError } from '../rpc/eth/errors.js';
export {
    type ChainRead,
    type ChainTransportName,
    type ConnectedReadClients,
    type ConnectionName,
    type ProofRequestConnections,
    type ReadNeed,
    type SolanaRead,
} from '../proofRequest/connectedRead.js';

// A value read through the connections and kept current: an app's own value
// is a graph spreading `readThroughConnectionsOf`, started with
// `startReadThroughConnectionsMachine`, whose read is given the clients.
export {
    readThroughConnectionsOf,
    ReadThroughConnectionsGraph,
    type ReadThroughConnectionsState,
} from '../rpc/connection/readThroughConnections.js';
// One read machine gated on another: `readMachineValue$` gives the other's
// value for the read, `readMachineChanged$` what makes it due again.
export {
    dueOn,
    readMachineChanged$,
    readMachineValue$,
    readThroughConnectionsTransitions,
    startReadThroughConnectionsMachine,
    type ReadRetryBackoff,
    type ReadThroughConnectionsMachineOptions,
    type ReadThroughConnectionsOptions,
    type ReadThroughConnectionsRead,
} from '../rpc/connection/readThroughConnections.impl.js';
export { withOutcome } from '../utils/machines.js';

// Whether a chain has changed (on Ethereum a new block or a log matching a
// filter, on Solana a new root or a transaction mentioning an address), for
// an app's own machine to read again on: it starts and owns a chain changes
// machine, and its `refreshOn` gates on `changesOf$` of it.
export { ChainChangesGraph, type ChainChangesState } from '../rpc/connection/chainChanges.js';
export { changesOf$, createChainChangesMachine, type ChainChanges } from '../rpc/connection/chainChanges.impl.js';
export { createSolanaChangesMachine } from '../rpc/solana/solanaChanges.impl.js';

// Each connection's status: one machine per connection over its transports.
export { ConnectionStatusGraph, type ConnectionStatusState } from '../rpc/connection/connectionStatus.js';
export {
    createConnectionStatusMachine,
    httpStatus,
    networkStatus,
    walletStatus,
    websocketStatus,
    type LiveConnectionStatus,
    type StatusTransport,
} from '../rpc/connection/connectionStatus.impl.js';

// The machines' graphs and states, for showing and gating on each transport.
export { NetworkGraph, type NetworkState } from '../rpc/connection/network.js';
export { DEFAULT_CONNECTIVITY_PROBE_URLS, type NetworkOptions } from '../rpc/connection/network.impl.js';
export { HttpConnectionGraph, type HttpConnectionState } from '../rpc/connection/httpConnection.js';
export {
    WebSocketConnectionGraph,
    type WebSocketConnectionState,
} from '../rpc/connection/websocketConnection.js';
export {
    EthereumWalletGraph,
    type EthereumWalletState,
    type WalletInfo,
} from '../rpc/eth/ethereumWallet.js';
// The account the Ethereum wallet shares: `ethereum.wallet.account`.
export { WalletAccountGraph, type WalletAccountState } from '../rpc/eth/walletAccount.js';
export { type WalletAccount } from '../rpc/eth/walletAccount.impl.js';
export { type EthereumHealth } from '../rpc/eth/ethereumHttp.js';
export {
    PUBLIC_SOLANA_CLUSTERS,
    type SolanaCluster,
    type SolanaHealth,
    type SolanaRpc,
} from '../rpc/solana/solanaHttp.js';
export { DEFAULT_NORI_WEBSOCKET_URL } from '../rpc/nori/noriWebsocket.js';
export {
    requestErrorCode,
    USER_REJECTED_REQUEST,
    type Eip1193EventProvider,
} from '../rpc/eth/eip1193.js';
export type { EthereumProvider } from '@nori-zk/ethereum-solana-bridge/iso-provider';
