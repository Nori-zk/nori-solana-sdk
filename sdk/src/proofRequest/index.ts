export {
    fetchProofRequestWitness,
    getProofRequestStateSnapshot,
    ProofRequestWitnessRootMismatchError,
    type ProofRequestStateSnapshot,
    type ProofRequestStateSnapshotRequest,
} from './getProofRequestStateSnapshot.js';
export { ProofRequestState } from './types.js';

// Connections: the network, the Ethereum wallet and provider, the Solana RPC.
export {
    bothReady$,
    createProofRequestConnections,
    readThroughConnections$,
    waitingOnChanged$,
    type ConnectedRead,
    type ConnectedReadClients,
    type ConnectionName,
    type ProofRequestConnections,
    type ProofRequestConnectionsOptions,
} from './connectedRead.js';
export { type HealthCheckTimings } from './rpc/healthCheckTimings.js';
export { NetworkGraph, type NetworkState } from './rpc/ystate/network.js';
export {
    createNetworkMachine,
    DEFAULT_CONNECTIVITY_PROBE_URLS,
    type NetworkMachine,
    type NetworkOptions,
} from './rpc/ystate/network.impl.js';
export {
    EthereumWalletGraph,
    type EthereumWalletState,
    type WalletInfo,
} from './rpc/eth/ystate/ethereumWallet.js';
export {
    createEthereumWalletMachine,
    type EthereumWalletMachine,
    type EthereumWalletOptions,
} from './rpc/eth/ystate/ethereumWallet.impl.js';
export {
    requestErrorCode,
    USER_REJECTED_REQUEST,
    type Eip1193EventProvider,
} from './rpc/eth/eip1193.js';
export { dataOnEntry$, stateOf$, type StartedMachine } from './ystate/dataOnEntry.js';
export {
    getEthereumProviderWithConnectivity$,
    type EthereumProviderConnectivityOptions,
    type EthereumProviderWithConnectivity,
} from './rpc/eth/ethereumProvider.js';
export {
    EthereumProviderConnectivityGraph,
    type EthereumProviderConnection,
} from './rpc/eth/ystate/ethereumProviderConnectivity.js';
export {
    getSolanaRpcWithConnectivity$,
    PUBLIC_SOLANA_CLUSTERS,
    type SolanaCluster,
    type SolanaRpc,
    type SolanaRpcConnectivityOptions,
    type SolanaRpcWithConnectivity,
} from './rpc/solana/solanaRpc.js';
export {
    SolanaRpcConnectivityGraph,
    type SolanaRpcConnection,
} from './rpc/solana/ystate/solanaRpcConnectivity.js';

// Reading machines: one proof request, a paged history, a live view.
export {
    ProofRequestStateGraph,
    type ProofRequestStateNodeUnion,
} from './ystate/proofRequest.js';
export {
    createProofRequestStateMachine,
    type FollowedProofRequest,
} from './ystate/proofRequest.impl.js';
export {
    ProofRequestHistoryGraph,
    type ProofRequestHistoryState,
} from './ystate/proofRequestHistory.js';
export {
    createProofRequestHistoryMachine,
    type ProofRequestHistoryQuery,
    type ReadRetryBackoff,
} from './ystate/proofRequestHistory.impl.js';
export {
    LatestProofRequestsGraph,
    type LatestProofRequestsState,
} from './ystate/latestProofRequests.js';
export {
    createLatestProofRequestsMachine,
    type LatestProofRequestsQuery,
} from './ystate/latestProofRequests.impl.js';
export {
    UnprocessedProofRequestStateGraph,
    type UnprocessedProofRequestStateNodeUnion,
    type UnprocessedProofRequestTopics,
} from './ystate/unprocessed.js';
export { createUnprocessedProofRequestStateMachine } from './ystate/unprocessed.impl.js';

// One-off reads.
export {
    fetchProofRequestCountsByTarget,
    fetchProofRequestHistoryPage,
    type ProofRequestCounts,
    type ProofRequestHistoryAddresses,
    type ProofRequestHistoryEntry,
    type ProofRequestHistoryPage,
    type ProofRequestHistoryRequest,
} from './proofRequestHistory.js';
export {
    fetchProofRequestsByTarget,
    type ProofRequestHistoryCursor,
    type ProofRequestHistoryOrder,
    type ProofRequestsByTargetPage,
    type ProofRequestsByTargetQuery,
} from './rpc/eth/fetchProofRequestsByTarget.js';
export type { RequestLeaf, RequestWitness } from '@nori-zk/ethereum-solana-proof-queue-utils-glam';
export { findProofQueueBatchPda } from './rpc/solana/findProofQueueBatchPda.js';
export {
    isSolanaRpcTransportFailure,
    ProofQueueBatchSearchError,
    SolanaAccountMismatchError,
    SolanaRpcTransportError,
} from './rpc/solana/errors.js';
export { fetchBridgeState } from './rpc/solana/fetchBridgeState.js';
export { fetchProofQueueBatches } from './rpc/solana/fetchProofQueueBatches.js';
export { findProofQueueBatch, type FoundProofQueueBatch } from './rpc/solana/findProofQueueBatch.js';
export { findProofQueueBatchesForRequests } from './rpc/solana/findProofQueueBatchesForRequests.js';
export { findRequestIdByTxHash, type ProofRequest } from './rpc/eth/fetchProofRequest.js';
export {
    fetchProofRequestBatch,
    type ProofRequestBatchEntry,
} from './rpc/eth/fetchProofRequestBatch.js';
export { fetchLatestBlockHeight } from './rpc/eth/fetchLatestBlockHeight.js';
export { proofRequestAge } from './rpc/eth/proofRequestAge.js';
export * from './rpc/eth/errors.js';
