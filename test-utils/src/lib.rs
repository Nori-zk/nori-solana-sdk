//! Shared harness for tests that run against a local Surfnet validator
//! (surfpool): start a validator on free ports with kill-on-drop, fund
//! keypairs, read Solana CLI keypair files, deploy a program through the
//! Solana CLI (at its own id or a given keypair's), write accounts and move
//! the clock with surfpool's cheatcodes, send an instruction and measure it,
//! and read Anchor custom error codes out of RPC errors.
//!
//! Requires `surfpool` (and, for the deploy helpers, `solana`) on PATH.

use {
    base64::{engine::general_purpose::STANDARD as BASE64, Engine},
    solana_instruction::Instruction,
    solana_keypair::Keypair,
    solana_message::{Message, VersionedMessage},
    solana_pubkey::Pubkey,
    solana_rpc_client::{api::request::RpcRequest, nonblocking::rpc_client::RpcClient},
    solana_signer::Signer,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        net::TcpListener,
        path::Path,
        process::{Child, Command, Stdio},
        time::Duration,
    },
    tokio::time::Instant,
};

/// How long [`Surfpool::start`] waits for the validator's RPC to answer.
const SURFPOOL_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`funded_keypair`] waits for an airdrop to land.
const AIRDROP_TIMEOUT: Duration = Duration::from_secs(30);

const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// A free local TCP port (bound and released, so another process could
/// still take it before it is used).
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// A running `surfpool start --offline --no-deploy --ci` validator on free
/// ports. Killed when dropped, so a panicking test does not leak it.
pub struct Surfpool {
    child: Child,
    rpc_url: String,
    ws_url: String,
}

impl Surfpool {
    /// Start a validator and wait until its RPC answers health checks.
    ///
    /// The child is owned by the guard from the moment it spawns, so a panic
    /// while waiting (or anywhere later in the test) still kills it.
    pub async fn start() -> Self {
        let port = free_port();
        let ws_port = free_port();
        let child = Command::new("surfpool")
            .args([
                "start",
                "--offline",
                "--no-deploy",
                "--ci",
                "-p",
                &port.to_string(),
                "--ws-port",
                &ws_port.to_string(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn surfpool (is it on PATH?)");
        let surfpool = Self {
            child,
            rpc_url: format!("http://127.0.0.1:{port}"),
            ws_url: format!("ws://127.0.0.1:{ws_port}"),
        };
        assert!(
            rpc_ready(&surfpool.rpc_url, SURFPOOL_READY_TIMEOUT).await,
            "surfpool did not come up on {}",
            surfpool.rpc_url
        );
        surfpool
    }

    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }

    /// Process id of the validator, for callers that must kill it outside
    /// of drop (e.g. on Ctrl-C).
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn client(&self) -> RpcClient {
        RpcClient::new(self.rpc_url.clone())
    }
}

impl Drop for Surfpool {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Poll `get_health` until it succeeds or `timeout` elapses.
pub async fn rpc_ready(rpc_url: &str, timeout: Duration) -> bool {
    let client = RpcClient::new(rpc_url.to_owned());
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if client.get_health().await.is_ok() {
            return true;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    false
}

/// A new keypair holding at least `lamports`, airdropped and waited on by
/// balance.
pub async fn funded_keypair(client: &RpcClient, lamports: u64) -> Keypair {
    let keypair = Keypair::new();
    let signature = client
        .request_airdrop(&keypair.pubkey(), lamports)
        .await
        .expect("airdrop request");
    let deadline = Instant::now() + AIRDROP_TIMEOUT;
    loop {
        if client.get_balance(&keypair.pubkey()).await.unwrap_or(0) >= lamports {
            return keypair;
        }
        assert!(
            Instant::now() < deadline,
            "airdrop {signature} not confirmed in time"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Read a Solana CLI keypair file (JSON array of 64 bytes).
pub fn read_keypair_file(path: impl AsRef<Path>) -> Keypair {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("cannot read keypair file '{}': {e}", path.display()));
    let bytes: Vec<u8> = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("keypair file '{}' is not JSON bytes: {e}", path.display()));
    Keypair::try_from(&bytes[..])
        .unwrap_or_else(|e| panic!("keypair file '{}' is invalid: {e}", path.display()))
}

/// Deploy `program_so` with `solana program deploy`, paid by `payer`. The
/// program id is the keypair the Solana CLI finds next to the `.so`
/// (`<name>-keypair.json`).
pub fn deploy_program_with_cli(surfpool: &Surfpool, payer: &Keypair, program_so: impl AsRef<Path>) {
    deploy(surfpool, payer, program_so.as_ref(), None);
}

/// [`deploy_program_with_cli`] at the id of `program_keypair` (a Solana CLI
/// keypair file), so a program lands at its `declare_id!` wherever its
/// `.so` was built.
pub fn deploy_program_at(
    surfpool: &Surfpool,
    payer: &Keypair,
    program_so: impl AsRef<Path>,
    program_keypair: impl AsRef<Path>,
) {
    deploy(
        surfpool,
        payer,
        program_so.as_ref(),
        Some(program_keypair.as_ref()),
    );
}

fn deploy(surfpool: &Surfpool, payer: &Keypair, program_so: &Path, program_keypair: Option<&Path>) {
    let payer_path = std::env::temp_dir().join(format!(
        "nori-test-payer-{}-{}.json",
        std::process::id(),
        payer.pubkey()
    ));
    let bytes: Vec<u8> = payer.to_bytes().to_vec();
    std::fs::write(&payer_path, serde_json::to_string(&bytes).unwrap()).unwrap();
    let mut command = Command::new("solana");
    command.arg("program").arg("deploy").arg(program_so);
    if let Some(program_keypair) = program_keypair {
        command.arg("--program-id").arg(program_keypair);
    }
    let deploy = command
        .args([
            "-k",
            payer_path.to_str().unwrap(),
            "--url",
            surfpool.rpc_url(),
            "--ws",
            surfpool.ws_url(),
        ])
        .output()
        .expect("failed to run solana CLI");
    std::fs::remove_file(&payer_path).ok();
    assert!(
        deploy.status.success(),
        "program deploy failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&deploy.stdout),
        String::from_utf8_lossy(&deploy.stderr)
    );
}

/// The custom program error code in an RPC error's text
/// (`"custom program error: 0x1774"` → 6004). Panics if there is none.
pub fn custom_error_code(error_text: &str) -> u32 {
    let marker = "custom program error: 0x";
    let start = error_text
        .find(marker)
        .unwrap_or_else(|| panic!("no custom error in: {error_text}"));
    let hex = error_text[start + marker.len()..]
        .chars()
        .take_while(|c| c.is_ascii_hexdigit())
        .collect::<String>();
    u32::from_str_radix(&hex, 16).expect("hex error code")
}

/// Write an account with surfpool's `surfnet_setAccount` cheatcode: `data`,
/// owned by `owner`, rent-exempt and not executable.
pub async fn set_account(client: &RpcClient, address: &Pubkey, owner: &Pubkey, data: &[u8]) {
    let lamports = client
        .get_minimum_balance_for_rent_exemption(data.len())
        .await
        .expect("rent-exempt minimum");
    let hex: String = data.iter().map(|byte| format!("{byte:02x}")).collect();
    cheatcode(
        client,
        "surfnet_setAccount",
        serde_json::json!([
            address.to_string(),
            {
                "lamports": lamports,
                "data": hex,
                "owner": owner.to_string(),
                "executable": false,
            }
        ]),
    )
    .await;
}

/// Move the validator clock to `unix_time` (seconds) with surfpool's
/// `surfnet_timeTravel` cheatcode. Surfpool only travels forward.
pub async fn time_travel(client: &RpcClient, unix_time: i64) {
    cheatcode(
        client,
        "surfnet_timeTravel",
        serde_json::json!([{ "absoluteTimestamp": unix_time * 1000 }]),
    )
    .await;
}

async fn cheatcode(client: &RpcClient, method: &'static str, params: serde_json::Value) {
    client
        .send::<serde_json::Value>(RpcRequest::Custom { method }, params)
        .await
        .unwrap_or_else(|e| panic!("{method}: {e}"));
}

/// What a confirmed transaction cost and produced.
pub struct Sent {
    pub compute_units: u64,
    pub transaction_bytes: usize,
    pub return_data: Vec<u8>,
    pub logs: Vec<String>,
}

impl Sent {
    /// The payloads of the `Program data:` log lines, in order. An Anchor
    /// event is one of these, starting with its 8-byte discriminator.
    pub fn program_data(&self) -> Vec<Vec<u8>> {
        self.logs
            .iter()
            .filter_map(|log| log.strip_prefix("Program data: "))
            .map(|data| BASE64.decode(data).expect("base64 program data"))
            .collect()
    }
}

/// Simulate `instruction` (paid by `payer`, also signed by `signers`), then
/// send and confirm it. On failure, the error text is the simulation's logs
/// (or the RPC error), so [`custom_error_code`] reads the code from it.
pub async fn send(
    client: &RpcClient,
    payer: &Keypair,
    instruction: Instruction,
    signers: &[&Keypair],
) -> Result<Sent, String> {
    let blockhash = client
        .get_latest_blockhash()
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut all_signers = vec![payer];
    all_signers.extend(signers.iter().filter(|s| s.pubkey() != payer.pubkey()));
    let message = Message::new_with_blockhash(&[instruction], Some(&payer.pubkey()), &blockhash);
    let transaction =
        VersionedTransaction::try_new(VersionedMessage::Legacy(message), &all_signers)
            .map_err(|e| format!("{e:?}"))?;

    let simulation = client
        .simulate_transaction(&transaction)
        .await
        .map_err(|e| format!("{e:?}"))?
        .value;
    let logs = simulation.logs.unwrap_or_default();
    if simulation.err.is_some() {
        return Err(logs.join("\n"));
    }
    client
        .send_and_confirm_transaction(&transaction)
        .await
        .map_err(|e| format!("{e:?}"))?;
    Ok(Sent {
        compute_units: simulation.units_consumed.unwrap_or_default(),
        transaction_bytes: 1
            + 64 * transaction.signatures.len()
            + transaction.message.serialize().len(),
        return_data: simulation
            .return_data
            .map(|data| BASE64.decode(data.data.0).expect("base64 return data"))
            .unwrap_or_default(),
        logs,
    })
}

/// The custom program error code of an `instruction` that must fail.
pub async fn error_code(
    client: &RpcClient,
    payer: &Keypair,
    instruction: Instruction,
    signers: &[&Keypair],
) -> u32 {
    let error = send(client, payer, instruction, signers)
        .await
        .err()
        .expect("the instruction must fail");
    custom_error_code(&error)
}
