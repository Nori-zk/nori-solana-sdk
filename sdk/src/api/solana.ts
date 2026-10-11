export { type NoriSolTokenBridge as SolanaBridgeState } from '../program/accounts/noriSolTokenBridge.js';
export type { SolanaSignatureStatus } from '../rpc/solana/signatureStatus.js';
export type { SolanaAccount } from '../rpc/solana/account.js';
export type { SolanaTransactionLogs } from '../rpc/solana/transactionLogs.js';
export type { SolanaWatch } from '../rpc/solana/solanaChanges.impl.js';

// Any account, and the logs of the transactions mentioning an address, each read through the Solana connection and kept current.
export { SolanaAccountGraph, type SolanaAccountState } from '../solana/account.js';
export { createSolanaAccountMachine } from '../solana/account.impl.js';
export { TransactionLogsGraph, type TransactionLogsState } from '../solana/transactionLogs.js';
export { createTransactionLogsMachine } from '../solana/transactionLogs.impl.js';

// The bridge's state and a transaction's status, each read through the Solana connection and kept current.
export { BridgeStateGraph, type BridgeStateState } from '../proofQueue/bridgeState.js';
export { createBridgeStateMachine } from '../proofQueue/bridgeState.impl.js';
export { SignatureStatusGraph, type SignatureStatusState } from '../transaction/signatureStatus.js';
export { createSignatureStatusMachine } from '../transaction/signatureStatus.impl.js';

// The bridge program's addresses.
export { proofQueueBatchAddress$, stateAddress$ } from '../rpc/solana/programAddresses.js';
export { solanaRpcRead$ } from '../rpc/solana/solanaRpcRead.js';
export { SolanaRpcTransportError } from '../rpc/solana/errors.js';

export { SolanaWalletGraph, type SolanaWalletInfo, type SolanaWalletState } from '../rpc/solana/solanaWallet.js';
export { NoSolanaWalletConfiguredError } from '../rpc/solana/errors.js';

export {
    instructionsToSendOf,
    type InstructionToSend,
    sentSolanaTransactionOf,
} from '../transaction/solanaTransaction.js';
export {
    programErrorNamesOf,
    type ProgramErrorNames,
    type SentSolanaTransaction,
    type SolanaTransactionMachineOptions,
    TOKEN_PROGRAM_ERROR_NAMES,
} from '../transaction/solanaTransaction.impl.js';
export {
    SolanaSignerTransactionGraph,
    type SolanaSignerTransactionState,
} from '../transaction/solanaSignerTransaction.js';
export { createSolanaSignerTransactionMachine } from '../transaction/solanaSignerTransaction.impl.js';
export {
    SolanaWalletTransactionGraph,
    type SolanaWalletTransactionState,
} from '../transaction/solanaWalletTransaction.js';
export { createSolanaWalletTransactionMachine } from '../transaction/solanaWalletTransaction.impl.js';

export {
    ASSOCIATED_TOKEN_PROGRAM_ADDRESS,
    associatedTokenAddress$,
    bridgedTokenAddress$,
    mintCall$,
    SPL_TOKEN_PROGRAM_ADDRESS,
    tokenAccountStorageAddress$,
} from '../rpc/solana/tokenBridgeInstructions.js';
export { MintedSoFarGraph, type MintedSoFarState } from '../tokenBridge/mintedSoFar.js';
export { createMintedSoFarMachine } from '../tokenBridge/mintedSoFar.impl.js';
export { TokenBalanceGraph, type TokenBalanceState } from '../tokenBridge/tokenBalance.js';
export { createTokenBalanceMachine } from '../tokenBridge/tokenBalance.impl.js';
