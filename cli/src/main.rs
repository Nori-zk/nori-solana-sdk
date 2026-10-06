//! Operator CLI for a deployed Nori Solana token bridge program.
//!
//! Wraps [`proof_submitter::SolanaProofSubmitter`]; run from the repo root
//! with `cargo run -p nori-cli -- <command> --help`.

use alloy_primitives::{hex, Address, B256};
use anchor_lang::prelude::Pubkey;
use anyhow::{bail, ensure, Context, Result};
use clap::{Args, Parser, Subcommand};
use proof_submitter::{load_update_proof, read_keypair_file, SolanaProofSubmitter};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::{
    io::{BufRead, Write},
    path::PathBuf,
};
use token::state::NoriSolTokenBridgeInit;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

#[derive(Parser)]
#[command(
    name = "nori-cli",
    about = "Operator commands for the Nori Solana token bridge"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send the one-off `initialize` to an already deployed token program:
    /// creates the bridge state PDA and the token mint and pins the init
    /// values (DEPLOYMENT.md §6). It cannot be repeated for a program id.
    Initialize(InitializeArgs),
}

/// RPC endpoint, payer and program. Each flag falls back to the env var the
/// proof submitter reads (`.env` in the working directory is loaded too).
#[derive(Args)]
struct Connection {
    /// Solana JSON-RPC endpoint.
    // hide_env_values: --help would otherwise print the URL's API key.
    #[arg(
        short = 'u',
        long = "url",
        env = "SOLANA_RPC_NETWORK_URL",
        hide_env_values = true
    )]
    rpc_url: String,

    /// Payer keypair file (Solana CLI `id.json` format). Pays rent and fees
    /// and becomes the bridge state's `authority`.
    #[arg(short = 'k', long, env = "SOLANA_PAYER_KEYPAIR_PATH")]
    keypair: PathBuf,

    /// Deployed token program id.
    #[arg(long, env = "NORI_SOL_TOKEN_PROGRAM_ID", default_value_t = token::id())]
    program_id: Pubkey,
}

/// `NoriSolTokenBridgeInit` fields. With `--proof`, the store hash, proof
/// queue address, latest head and queue cursor come from that proof's input
/// side (making it the first valid `update`); otherwise pass all six.
#[derive(Args)]
struct InitializeArgs {
    #[command(flatten)]
    connection: Connection,

    /// First `update` proof JSON (nori-bridge-head output).
    #[arg(long, value_name = "PATH")]
    proof: Option<PathBuf>,

    /// Execution state root at the start point (§4 `initialVerifiedStateRoot`).
    #[arg(long, value_name = "HEX32")]
    verified_state_root: B256,

    /// `NoriTokenBridge` address on Ethereum (§3 `EthBridge`).
    #[arg(long, value_name = "HEX20")]
    eth_token_bridge_address: Address,

    /// Helios store input hash at the start point (§4 `initialStoreHash`).
    #[arg(
        long,
        value_name = "HEX32",
        required_unless_present = "proof",
        conflicts_with = "proof"
    )]
    latest_helios_store_input_hash: Option<B256>,

    /// `NoriProofRequestQueue` address on Ethereum (§3 `EthQueue`).
    #[arg(
        long,
        value_name = "HEX20",
        required_unless_present = "proof",
        conflicts_with = "proof"
    )]
    eth_proof_queue_address: Option<Address>,

    /// Beacon slot at the start point; the first `update` must resume from it.
    #[arg(
        long,
        value_name = "SLOT",
        required_unless_present = "proof",
        conflicts_with = "proof"
    )]
    latest_head: Option<u64>,

    /// Proof request queue cursor at the start point (§4 `initialQueueCursor`).
    #[arg(
        long,
        value_name = "CURSOR",
        required_unless_present = "proof",
        conflicts_with = "proof"
    )]
    queue_cursor: Option<u64>,

    /// Run the checks and print the summary without sending.
    #[arg(long)]
    dry_run: bool,

    /// Send without the confirmation prompt.
    #[arg(short = 'y', long)]
    yes: bool,
}

impl InitializeArgs {
    fn init_values(&self) -> Result<NoriSolTokenBridgeInit> {
        let Some(path) = &self.proof else {
            // clap enforces these when --proof is absent.
            return Ok(NoriSolTokenBridgeInit {
                verified_state_root: self.verified_state_root.into(),
                latest_helios_store_input_hash: self
                    .latest_helios_store_input_hash
                    .context("--latest-helios-store-input-hash is required")?
                    .into(),
                eth_proof_queue_address: self
                    .eth_proof_queue_address
                    .context("--eth-proof-queue-address is required")?
                    .into(),
                eth_token_bridge_address: self.eth_token_bridge_address.into(),
                latest_head: self.latest_head.context("--latest-head is required")?,
                queue_cursor: self.queue_cursor.context("--queue-cursor is required")?,
            });
        };
        let proof =
            load_update_proof(path).with_context(|| format!("loading {}", path.display()))?;
        // initialize pins nori-elf's vkey; a proof for any other vkey could
        // never be accepted as an update.
        ensure!(
            proof.program_vkey == nori_elf::NORI_SP1_HELIOS_PROGRAM_VK,
            "{} was proven for program vkey 0x{}, but initialize pins 0x{} (nori-elf)",
            path.display(),
            hex::encode(proof.program_vkey),
            hex::encode(nori_elf::NORI_SP1_HELIOS_PROGRAM_VK),
        );
        Ok(proof.bridge_init(self.verified_state_root, self.eth_token_bridge_address)?)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    match Cli::parse().command {
        Command::Initialize(args) => initialize(args).await,
    }
}

async fn initialize(args: InitializeArgs) -> Result<()> {
    let init_values = args.init_values()?;
    let Connection {
        rpc_url,
        keypair,
        program_id,
    } = args.connection;
    let payer = read_keypair_file(&keypair.to_string_lossy())?;
    let submitter = SolanaProofSubmitter::new(rpc_url, payer, program_id);
    let client = RpcClient::new(submitter.rpc_url().to_string());
    let (state, mint, payer) = (
        submitter.state_address(),
        submitter.mint_address(),
        submitter.payer_pubkey(),
    );
    let endpoint = redact_query(submitter.rpc_url());

    let accounts = client
        .get_multiple_accounts(&[program_id, state])
        .await
        .with_context(|| format!("reading accounts from {endpoint}"))?;
    match &accounts[0] {
        None => bail!("no account at program id {program_id} on {endpoint}; deploy first"),
        Some(account) if !account.executable => {
            bail!("{program_id} on {endpoint} is not an executable program")
        }
        Some(_) => {}
    }
    if accounts[1].is_some() {
        bail!("already initialized: bridge state {state} exists on {endpoint}");
    }
    let balance = client.get_balance(&payer).await?;

    println!("Nori bridge initialize");
    println!("  rpc                             {endpoint}");
    println!("  program                         {program_id}");
    println!(
        "  payer / authority               {payer} ({} SOL)",
        balance as f64 / LAMPORTS_PER_SOL
    );
    println!("  state PDA                       {state}");
    println!("  mint PDA                        {mint}");
    println!("init values");
    println!(
        "  verified_state_root             {}",
        B256::from(init_values.verified_state_root)
    );
    println!(
        "  latest_helios_store_input_hash  {}",
        B256::from(init_values.latest_helios_store_input_hash)
    );
    println!(
        "  eth_proof_queue_address         {}",
        Address::from(init_values.eth_proof_queue_address)
    );
    println!(
        "  eth_token_bridge_address        {}",
        Address::from(init_values.eth_token_bridge_address)
    );
    println!(
        "  latest_head                     {}",
        init_values.latest_head
    );
    println!(
        "  queue_cursor                    {}",
        init_values.queue_cursor
    );
    println!(
        "  nori_bridge_vk (nori-elf)       0x{}",
        hex::encode(nori_elf::NORI_SP1_HELIOS_PROGRAM_VK)
    );
    if program_id != token::id() {
        println!(
            "warning: {program_id} differs from the declared id {}; the program rejects \
             calls unless its binary was built with a matching declare_id",
            token::id()
        );
    }

    if args.dry_run {
        println!("dry run: not sent");
        return Ok(());
    }
    if !args.yes && !confirm("Send initialize? It cannot be repeated for this program id.")? {
        bail!("aborted");
    }

    let result = submitter
        .submit_initialize(init_values)
        .await
        .context("initialize failed")?;
    println!("initialize tx {}", result.tx_hash);
    if let Some(cu) = result.cu_consumed {
        println!("compute units {cu}");
    }
    Ok(())
}

/// Endpoint without its query string, which often carries an API key.
fn redact_query(url: &str) -> &str {
    url.split_once('?').map_or(url, |(base, _)| base)
}

fn confirm(question: &str) -> Result<bool> {
    print!("{question} [y/N] ");
    std::io::stdout().flush()?;
    let mut answer = String::new();
    std::io::stdin().lock().read_line(&mut answer)?;
    Ok(matches!(answer.trim(), "y" | "Y" | "yes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{error::ErrorKind, CommandFactory};

    const FIRST_PROOF: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../proof-submitter/example-proofs/11247328-v6.1.0.json"
    );
    const STATE_ROOT: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    const BRIDGE: &str = "0x2222222222222222222222222222222222222222";
    const CONNECTION: [&str; 4] = ["--url", "http://127.0.0.1:8899", "--keypair", "id.json"];

    fn parse(extra: &[&str]) -> Result<InitializeArgs, clap::Error> {
        let mut argv = vec!["nori-cli", "initialize"];
        argv.extend(CONNECTION);
        argv.extend([
            "--verified-state-root",
            STATE_ROOT,
            "--eth-token-bridge-address",
            BRIDGE,
        ]);
        argv.extend(extra);
        Cli::try_parse_from(argv).map(|cli| match cli.command {
            Command::Initialize(args) => args,
        })
    }

    #[test]
    fn clap_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn explicit_init_values() {
        let args = parse(&[
            "--latest-helios-store-input-hash",
            "0x3333333333333333333333333333333333333333333333333333333333333333",
            "--eth-proof-queue-address",
            "0x4444444444444444444444444444444444444444",
            "--latest-head",
            "11247328",
            "--queue-cursor",
            "7",
        ])
        .unwrap();
        assert_eq!(args.connection.program_id, token::id());
        let init = args.init_values().unwrap();
        assert_eq!(init.verified_state_root.0, [0x11; 32]);
        assert_eq!(init.latest_helios_store_input_hash.0, [0x33; 32]);
        assert_eq!(init.eth_proof_queue_address.0, [0x44; 20]);
        assert_eq!(init.eth_token_bridge_address.0, [0x22; 20]);
        assert_eq!(init.latest_head, 11_247_328);
        assert_eq!(init.queue_cursor, 7);
    }

    #[test]
    fn init_values_from_proof() {
        let init = parse(&["--proof", FIRST_PROOF])
            .unwrap()
            .init_values()
            .unwrap();
        let expected = load_update_proof(FIRST_PROOF)
            .unwrap()
            .bridge_init(STATE_ROOT.parse().unwrap(), BRIDGE.parse().unwrap())
            .unwrap();
        assert_eq!(init.verified_state_root, expected.verified_state_root);
        assert_eq!(
            init.latest_helios_store_input_hash,
            expected.latest_helios_store_input_hash
        );
        assert_eq!(
            init.eth_proof_queue_address,
            expected.eth_proof_queue_address
        );
        assert_eq!(init.eth_token_bridge_address.0, [0x22; 20]);
        assert_eq!(init.latest_head, expected.latest_head);
        assert_eq!(init.queue_cursor, expected.queue_cursor);
    }

    #[test]
    fn proof_conflicts_with_proof_derived_values() {
        let err = parse(&["--proof", FIRST_PROOF, "--latest-head", "1"])
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn explicit_values_are_all_required_without_proof() {
        let err = parse(&["--latest-head", "1"]).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn redacts_query_string() {
        assert_eq!(
            redact_query("https://rpc.example/?api-key=secret"),
            "https://rpc.example/"
        );
        assert_eq!(
            redact_query("http://127.0.0.1:8899"),
            "http://127.0.0.1:8899"
        );
    }
}
