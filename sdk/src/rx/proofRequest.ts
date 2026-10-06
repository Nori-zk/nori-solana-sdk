/**
 * The stages an unprocessed proof request waits through while the bridge
 * works towards settling it on Solana, as observed from the bridge's
 * websocket topics.
 *
 * - `WaitingForEthFinality`: The request is awaiting Ethereum chain finality before processing can begin.
 * - `WaitingForCurrentJobCompletion`: The request is included in the current bridge job but must wait for it to finalize.
 * - `WaitingForPreviousJobCompletion`: The request is not part of the current job and must wait for its job window to open.
 */
export enum BridgeProofRequestProcessingStatus {
    WaitingForEthFinality = 'WaitingForEthFinality',
    WaitingForCurrentJobCompletion = 'WaitingForCurrentJobCompletion',
    WaitingForPreviousJobCompletion = 'WaitingForPreviousJobCompletion',
}
