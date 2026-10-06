import { define } from '@yaw-rx/ystate';
import { ProofRequestState } from '../types.js';

export const ProofRequestStateGraph = define({
    nodes: {
        undetermined: {},
        unprocessed: {
            state: ProofRequestState.Unprocessed as typeof ProofRequestState.Unprocessed,
            requestId: 0n as bigint,
            requestBlockNumber: 0n as bigint,
            queueCursor: 0n as bigint,
            proofQueueBatchCount: 0n as bigint,
        },
        proofAvailable: {
            state: ProofRequestState.ProofAvailable as typeof ProofRequestState.ProofAvailable,
            requestId: 0n as bigint,
            requestBlockNumber: 0n as bigint,
            queueCursor: 0n as bigint,
            proofQueueBatchIndex: 0n as bigint,
            proofQueueBatchAddress: '' as string, // base58 proof queue batch PDA
            root: '' as string, // 0x-prefixed batch root
            inputQueueCursor: 0n as bigint,
            outputQueueCursor: 0n as bigint,
            outputBlockNumber: 0n as bigint,
            previousOutputBlockNumber: -1n as bigint, // sentinel: no previous proof queue batch (first-ever batch)
            indexInBatch: 0n as bigint,
        },
    },
    edges: {
        discoveredUnprocessed: {
            from: 'undetermined',
            to: 'unprocessed',
            on: 'checkWhetherProofRequestIsUnprocessed.next',
        },
        discoveredProofAvailable: {
            from: 'undetermined',
            to: 'proofAvailable',
            on: 'checkWhetherProofIsAvailable.next',
        },
        proofRequestCommitted: {
            from: 'unprocessed',
            to: 'proofAvailable',
            on: 'checkWhetherUnprocessedProofRequestIsCommitted.next',
        },
    },
});

export type ProofRequestStateNodeUnion = {
    [Node in keyof typeof ProofRequestStateGraph.nodes]: {
        node: Node;
        data: (typeof ProofRequestStateGraph.nodes)[Node];
    };
}[keyof typeof ProofRequestStateGraph.nodes];
