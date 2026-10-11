import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { type ProofRequest } from '../rpc/eth/proofRequest.js';
import { type AsNodeData } from '../utils/machines.js';

/**
 * The proof request a transaction enqueued, read through the Ethereum
 * connection (`readThroughConnectionsOf`): read at once, then again on each
 * new block until the transaction is mined. `transactionHash` is its
 * starting data; `proofRequest` (its id, target, slot key and block) is
 * `undefined` until mined, and never read again after.
 */
export const ProofRequestOfTransactionGraph = define(
    readThroughConnectionsOf({
        transactionHash: '',
        proofRequest: undefined as AsNodeData<ProofRequest> | undefined,
    })
);

/** The enqueued request's state: a node of the graph and its data. */
export type ProofRequestOfTransactionState = StateUnion<typeof ProofRequestOfTransactionGraph.nodes>;
