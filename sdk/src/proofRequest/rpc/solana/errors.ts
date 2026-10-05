export class SolanaAccountMismatchError extends Error {
    constructor(readonly address: string, message: string) {
        super(message);
        this.name = 'SolanaAccountMismatchError';
    }
}

export class ProofQueueBatchSearchError extends Error {
    constructor(readonly requestId: bigint, message: string) {
        super(message);
        this.name = 'ProofQueueBatchSearchError';
    }
}
