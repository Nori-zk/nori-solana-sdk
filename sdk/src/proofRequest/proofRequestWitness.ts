import { type RequestWitness } from '@nori-zk/ethereum-solana-proof-queue-utils-glam';
import { define, type StateUnion } from '@yaw-rx/ystate';
import { readThroughConnectionsOf } from '../rpc/connection/readThroughConnections.js';
import { type AsNodeData } from '../utils/machines.js';
import { type ProofAvailableProofRequestSnapshot, type VerifiedRequestWitness } from './getProofRequestStateSnapshot.js';

/**
 * A proven request's witness, read through the Ethereum connection
 * (`readThroughConnectionsOf`): its batch's requests, rebuilt into the
 * batch's Merkle tree and checked against the root committed on Solana.
 * `proofAvailable` is the proven request it is made for, its starting data.
 * `witness` is the request's leaf, bottom-up path and root, and
 * `verifiedWitness` the same in the shape the program's `mint` takes
 * (`mintCall$`), both `undefined` until read. A committed batch never
 * changes, so it is never read again.
 */
export const ProofRequestWitnessGraph = define(
    readThroughConnectionsOf({
        proofAvailable: {} as AsNodeData<ProofAvailableProofRequestSnapshot>,
        witness: undefined as RequestWitness | undefined,
        verifiedWitness: undefined as VerifiedRequestWitness | undefined,
    })
);

/** The witness's state: a node of the graph and its data. */
export type ProofRequestWitnessState = StateUnion<typeof ProofRequestWitnessGraph.nodes>;
