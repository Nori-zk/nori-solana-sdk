export {
    createConnections,
    forCalls,
    forLogs,
    forSubscriptions,
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
export {
    NoEthereumHttpConfiguredError,
    NoEthereumWebsocketConfiguredError,
    NoWalletConfiguredError,
} from '../rpc/eth/errors.js';
export {
    bothReady$,
    waitingOnChanged$,
    type ConnectionName,
    type ProofRequestConnections,
} from '../proofRequest/connectedRead.js';

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
