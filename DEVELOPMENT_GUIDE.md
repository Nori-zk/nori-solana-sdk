# Local Development Setup

## Dependencies

Full install instructions live here: https://solana.com/docs/intro/installation

A single installer sets up Rust, the Solana CLI, Anchor, Surfpool, Node.js, and Yarn together:

```bash
curl --proto '=https' --tlsv1.2 -sSfL https://solana-install.solana.workers.dev | bash
```

This prints its own install/progress output, which varies depending on what is already installed, so no fixed example is shown here.

The installer's bin directory needs to be on PATH for the `solana` command to resolve:

```bash
echo 'export PATH="$HOME/.local/share/solana/install/active_release/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

Both commands are silent on success: `echo` here only writes to `~/.bashrc`, and `source` produces no output unless something in `~/.bashrc` prints something.

Sanity-check that everything landed:

```bash
rustc --version && solana --version && anchor --version && surfpool --version && node --version && yarn --version
```

*Output looks like this. Exact versions will differ:*

```
rustc 1.98.0 (88d9e12ae 2026-08-18)
solana-cli 3.1.10 (src:7bc9c805; feat:1620780344, client:Agave)
anchor-cli 1.1.2
surfpool 1.5.0
v24.18.0
1.22.22
```

If any command isn't found, the dependencies doc covers troubleshooting: https://solana.com/docs/intro/installation/dependencies

## Surfpool

Surfpool is the local validator used for development. It stands in for a real Solana cluster on a local machine, allowing programs to be deployed and tested without touching devnet or mainnet.

### Start

Bring the local validator up:

```bash
surfpool start
```

This launches an interactive terminal dashboard (not plain scrollback text) alongside a local web UI and the RPC/WebSocket endpoints programs connect to, so there is no fixed stdout snippet to show here.

### Stop

Surfpool runs in the foreground, not as a detached service. Shut it down with Ctrl+C in the terminal where `surfpool start` is running.

## Anchor

Anchor is the framework used to scaffold, build, and deploy Solana programs.

### Create a new program

Scaffold a new project:

```bash
anchor init first-program
```

*Output looks like this (a git default-branch-name hint is trimmed):*

```
Initialized empty Git repository in /path/to/first-program/.git/
first-program initialized
```

Move into the new project directory. `cd` produces no output on success:

```bash
cd first-program
```

Build the program:

```bash
anchor build
```

The first run compiles every dependency, producing a long list of `Compiling X` lines; trimmed here to the final result:

```
    Finished `test` profile [unoptimized + debuginfo] target(s) in 14.99s
```

Deploy it to whichever cluster is configured (a local validator needs to be running first, see Surfpool above). Use `anchor program deploy`, not the deprecated `anchor deploy`, and point it at the built `.so`:

```bash
anchor program deploy target/deploy/first_program.so
```

*Output looks like this. The program ID differs every time, since it is derived from the generated program keypair:*

```
Program ID: 5ZZoRpJwyAhY54mcTKcHN24PdfDyQDKc1eE9DWjp7Khy
Skipping IDL deployment on localnet
```

## Solana CLI

The Solana CLI manages wallets, configuration, and interaction with a cluster (local or remote).

### Configure the target cluster

Point the CLI at the local validator:

```bash
solana config set --url localhost
```

*Output looks like this: the config file it wrote, the RPC and WebSocket URLs now pointing at the local validator, which keypair the CLI will sign with, and the commitment level used when confirming transactions:*

```
Config File: /home/<user>/.config/solana/cli/config.yml
RPC URL: http://localhost:8899 
WebSocket URL: ws://localhost:8900/ (computed)
Keypair Path: /home/<user>/.config/solana/id.json 
Commitment: confirmed 
```

### Generate a keypair

Generate a local dev keypair. This is a throwaway wallet for local testing, funded via airdrop, not a real-funds wallet:

```bash
solana-keygen new
```

*Output looks like this. The actual pubkey and seed phrase will differ each time it runs:*

```
Wrote new keypair to /home/<user>/.config/solana/id.json
======================================================================
pubkey: Ffw32UiWGknLF4mma7KF1GDoc43ic13aC5dUnPMeBghD
======================================================================
Save this seed phrase and your BIP39 passphrase to recover your new keypair:
<redacted>
```

### Check the generated address

Print the pubkey of the currently configured keypair. This matches the pubkey shown when the keypair was generated:

```bash
solana address
```

*Output looks like this. The pubkey differs every time, matching whatever keypair is currently configured:*

```
Ffw32UiWGknLF4mma7KF1GDoc43ic13aC5dUnPMeBghD
```

### Fund and check the balance

Request an airdrop of 2 SOL from the local validator's faucet:

```bash
solana airdrop 2
```

*Output looks like this. The signature differs every time, and the final line is the wallet's total balance after the airdrop, not just the 2 SOL just requested:*

```
Requesting airdrop of 2 SOL

Signature: 27yp71LgE1EQMzhFCgUCgbDLLJcYPHTX9TTbhauD4rrncxt4a6yKjCVcjKSbPEcYSWK3J8ffPwDxxKSmozxgpxbn

10002 SOL
```

Check the balance directly:

```bash
solana balance
```

*Output looks like this. The amount reflects the wallet's current total balance, not just the last airdrop:*

```
10002 SOL
```
