export class EthRpcTransportError extends Error {
    constructor(message: string, readonly cause: unknown) {
        super(message);
        this.name = 'EthRpcTransportError';
    }
}

export class EthCallFailedError extends Error {
    constructor(message: string, readonly cause: unknown) {
        super(message);
        this.name = 'EthCallFailedError';
    }
}

export class MalformedProofRequestError extends Error {
    constructor(readonly requestId: bigint, message: string) {
        super(message);
        this.name = 'MalformedProofRequestError';
    }
}

/** The transaction that enqueues a proof request has no receipt yet: it is not mined. */
export class ProofRequestTransactionNotMinedError extends Error {
    constructor(readonly transactionHash: string) {
        super(`Transaction ${transactionHash} is not mined yet.`);
        this.name = 'ProofRequestTransactionNotMinedError';
    }
}

export class EthDataNotFoundError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'EthDataNotFoundError';
    }
}

export type ProofRequestFailureCause =
    | EthRpcTransportError
    | EthCallFailedError
    | MalformedProofRequestError;

export type ProofRequestBatchFailure = {
    requestIds: bigint[];
    error: ProofRequestFailureCause;
};

export class ProofRequestBatchFetchError extends Error {
    constructor(readonly failures: ProofRequestBatchFailure[]) {
        const count = failures.reduce((total, failure) => total + failure.requestIds.length, 0);
        super(`Failed to fetch ${count} of the queue's proof requests.`);
        this.name = 'ProofRequestBatchFetchError';
    }
}

/** Thrown by `ethereum.wallet.ready()` when the app gave no `ethereum.wallet`. */
export class NoWalletConfiguredError extends Error {
    constructor() {
        super('No wallet is configured: give ethereum.wallet to createConnections to use one.');
        this.name = 'NoWalletConfiguredError';
    }
}

/** Thrown by `ethereum.http.ready()` when the app gave no `ethereum.http`. */
export class NoEthereumHttpConfiguredError extends Error {
    constructor() {
        super('No Ethereum http is configured: give ethereum.http to createConnections to use one.');
        this.name = 'NoEthereumHttpConfiguredError';
    }
}

/** Thrown by `ethereum.websocket.ready()` when the app gave no `ethereum.websocket`. */
export class NoEthereumWebsocketConfiguredError extends Error {
    constructor() {
        super(
            'No Ethereum websocket is configured: give ethereum.websocket to createConnections to use one.'
        );
        this.name = 'NoEthereumWebsocketConfiguredError';
    }
}
