//! Host-side tests: proof JSON loading, chain-continuity of the example
//! series, loader error reporting, and `from_env` validation. No validator
//! required.

use {
    alloy_primitives::B256,
    nori_sp1_helios_primitives::types::ProofOutputs,
    proof_submitter::{
        load_update_proof, load_update_proofs_dir, LoadedProof, ProofFileError,
        SolanaProofSubmitter,
    },
    sp1_solana::SP1_GROTH16_PROOF_LEN,
};

const PROOFS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/example-proofs");
const SLOTS: [u64; 4] = [11_298_112, 11_298_144, 11_298_176, 11_298_208];

fn load_proofs() -> Vec<LoadedProof> {
    load_update_proofs_dir(PROOFS_DIR).expect("example proofs must parse")
}

fn outputs(proof: &LoadedProof) -> ProofOutputs {
    ProofOutputs::from_bytes(&proof.wire.sp1_public_inputs).expect("public values decode")
}

#[test]
fn example_proofs_load_and_chain() {
    let proofs = load_proofs();
    assert_eq!(proofs.len(), 4);
    let mut prev: Option<ProofOutputs> = None;
    for (i, proof) in proofs.iter().enumerate() {
        assert_eq!(proof.wire.proof.len(), SP1_GROTH16_PROOF_LEN);
        assert_eq!(proof.wire.sp1_public_inputs.len(), 220);
        let out = outputs(proof);
        assert_eq!(out.input_slot, SLOTS[i]);
        assert!(out.output_slot > out.input_slot);
        assert!(out.next_sync_committee_hash != B256::ZERO);
        if let Some(prev) = &prev {
            assert_eq!(out.input_slot, prev.output_slot);
            assert_eq!(out.input_store_hash, prev.output_store_hash);
            assert_eq!(out.input_queue_cursor, prev.output_queue_cursor);
        }
        prev = Some(out);
    }
    // Same proving program across the whole series.
    assert!(proofs
        .iter()
        .all(|p| p.program_vkey == proofs[0].program_vkey));
}

#[test]
fn load_update_proof_reports_bad_files() {
    let good = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/example-proofs/11298112-v6.1.0.json"
    ))
    .unwrap();

    let dir = std::env::temp_dir().join(format!("nori-bad-proof-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let bad_json = dir.join("bad.json");
    std::fs::write(&bad_json, "{\"proof\":{}}").unwrap();
    assert!(matches!(
        load_update_proof(&bad_json),
        Err(ProofFileError::Json(_))
    ));

    // Truncated encoded proof.
    let mut parsed: serde_json::Value = serde_json::from_str(&good).unwrap();
    parsed["proof"]["Groth16"]["encoded_proof"] = serde_json::Value::String("00ff".into());
    let short = dir.join("short.json");
    std::fs::write(&short, serde_json::to_string(&parsed).unwrap()).unwrap();
    assert!(matches!(
        load_update_proof(&short),
        Err(ProofFileError::EncodedProofLength { got: 2, .. })
    ));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn from_env_requires_url_and_payer() {
    let url = std::env::var("SOLANA_RPC_NETWORK_URL").ok();
    let payer = std::env::var("SOLANA_PAYER_KEYPAIR_PATH").ok();
    std::env::remove_var("SOLANA_RPC_NETWORK_URL");
    std::env::remove_var("SOLANA_PAYER_KEYPAIR_PATH");
    assert!(SolanaProofSubmitter::from_env().is_err());

    std::env::set_var("SOLANA_RPC_NETWORK_URL", "http://localhost:8899");
    assert!(SolanaProofSubmitter::from_env().is_err());

    match (url, payer) {
        (Some(u), Some(p)) => {
            std::env::set_var("SOLANA_RPC_NETWORK_URL", u);
            std::env::set_var("SOLANA_PAYER_KEYPAIR_PATH", p);
        }
        _ => {
            std::env::remove_var("SOLANA_RPC_NETWORK_URL");
            std::env::remove_var("SOLANA_PAYER_KEYPAIR_PATH");
        }
    }
}
