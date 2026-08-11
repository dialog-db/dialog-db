//! The runner shim: the process `cargo test` / `cargo nextest` invokes as
//! the wasm target runner. It answers `--list` locally, delegates
//! non-browser binaries to the stock `wasm-bindgen-test-runner`, and sends
//! browser runs to the shared daemon (spawning it on first use).

use crate::cli::RunnerCli;
use crate::daemon::default_state_dir;
use crate::protocol::{DaemonState, RunReport, RunRequest};
use crate::suite::{self, TestMode};
use anyhow::{bail, Context, Result};
use clap::Parser;
use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub fn main(args: Vec<OsString>) -> Result<()> {
    let cli = match RunnerCli::try_parse_from(&args) {
        Ok(cli) => cli,
        Err(error) => match error.kind() {
            clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => {
                print!("{error}");
                return Ok(());
            }
            _ => bail!(error),
        },
    };

    // Emscripten test binaries are passed as a `.js` file; wbg-pool does not
    // support them, so hand the invocation to the stock runner untouched.
    if cli.file.extension().unwrap_or_default() == "js" {
        return delegate(&args);
    }

    let wasm = std::fs::read(&cli.file)
        .with_context(|| format!("failed to read wasm file {}", cli.file.display()))?;
    let suite = suite::inspect(&wasm)?;

    // Browser, dedicated worker, shared worker and service worker suites all
    // run inside the pooled browser; only Node and Emscripten suites are
    // handed to the stock runner.
    if matches!(
        suite.effective_mode(),
        TestMode::Node | TestMode::Emscripten
    ) {
        return delegate(&args);
    }

    if cli.bench {
        bail!(
            "wbg-pool does not support --bench; run benchmarks with \
             wasm-bindgen-test-runner directly"
        );
    }

    if cli.list {
        let filtered = suite::filter(&suite.tests, &cli.filter_args());
        let mut stdout = std::io::stdout().lock();
        for test in filtered.to_run {
            // A closed stdout (e.g. `wbg-pool foo.wasm --list | head`) is
            // not an error worth reporting.
            if let Err(error) = writeln!(stdout, "{}: test", test.name) {
                if error.kind() == std::io::ErrorKind::BrokenPipe {
                    return Ok(());
                }
                return Err(error.into());
            }
        }
        return Ok(());
    }

    if suite.tests.is_empty() {
        println!("no tests to run!");
        return Ok(());
    }

    let timeout_secs = std::env::var("WASM_BINDGEN_TEST_TIMEOUT")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(20);

    let daemon_url = ensure_daemon()?;
    let request = RunRequest {
        binary: std::fs::canonicalize(&cli.file)?,
        args: cli.filter_args(),
        nocapture: cli.nocapture,
        timeout_secs,
    };

    let response = ureq::post(&format!("{daemon_url}/api/run"))
        .timeout(Duration::from_secs(timeout_secs + 120))
        .send_json(&request);

    let report: RunReport = match response {
        Ok(response) => response.into_json()?,
        Err(ureq::Error::Status(code, response)) => {
            let body = response.into_string().unwrap_or_default();
            bail!("wbg-pool daemon returned {code}: {body}");
        }
        Err(error) => bail!("failed to reach wbg-pool daemon: {error}"),
    };

    print!("{}", report.output);

    if !report.ok {
        if !report.console_output.is_empty() {
            println!("console output:");
            for line in report.console_output.lines() {
                println!("    {line}");
            }
        }
        eprintln!("Error: some tests failed");
        std::process::exit(1);
    }

    Ok(())
}

/// Hands the invocation to the stock `wasm-bindgen-test-runner`, preserving
/// arguments and exit status, for test modes wbg-pool does not handle.
fn delegate(args: &[OsString]) -> Result<()> {
    let runner = std::env::var_os("WBG_POOL_FALLBACK_RUNNER")
        .unwrap_or_else(|| OsString::from("wasm-bindgen-test-runner"));
    use std::os::unix::process::CommandExt;
    let error = std::process::Command::new(&runner).args(&args[1..]).exec();
    bail!(
        "this binary is not a browser-mode test suite, and delegating to {} failed: {error}",
        Path::new(&runner).display()
    )
}

/// Finds a healthy daemon or spawns one, coordinating concurrent shims
/// (nextest starts many at once) through a spawn lock file.
fn ensure_daemon() -> Result<String> {
    if let Ok(url) = std::env::var("WBG_POOL_URL") {
        return Ok(url);
    }

    let dir = default_state_dir();
    std::fs::create_dir_all(&dir)?;
    let state_path = dir.join("daemon.json");
    let lock_path = dir.join("spawn.lock");

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Some(url) = healthy_daemon(&state_path) {
            return Ok(url);
        }

        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(mut lock) => {
                let _ = write!(lock, "{}", std::process::id());
                let result = spawn_daemon(&dir)
                    .and_then(|_| wait_for_daemon(&state_path, Duration::from_secs(30)));
                let _ = std::fs::remove_file(&lock_path);
                return result;
            }
            Err(_) => {
                // Another shim is spawning the daemon; wait for it, but
                // clear the lock if its owner appears to have died.
                if let Ok(url) = wait_for_daemon(&state_path, Duration::from_secs(10)) {
                    return Ok(url);
                }
                let stale = std::fs::metadata(&lock_path)
                    .and_then(|meta| meta.modified())
                    .map(|modified| {
                        modified.elapsed().unwrap_or_default() > Duration::from_secs(30)
                    })
                    .unwrap_or(false);
                if stale {
                    let _ = std::fs::remove_file(&lock_path);
                }
            }
        }
    }
    bail!("timed out waiting for the wbg-pool daemon to start")
}

fn healthy_daemon(state_path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(state_path).ok()?;
    let state: DaemonState = serde_json::from_str(&raw).ok()?;
    let response = ureq::get(&format!("{}/api/health", state.url))
        .timeout(Duration::from_millis(500))
        .call()
        .ok()?;
    if response.status() == 200 {
        Some(state.url)
    } else {
        None
    }
}

fn wait_for_daemon(state_path: &Path, timeout: Duration) -> Result<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(url) = healthy_daemon(state_path) {
            return Ok(url);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    bail!("daemon did not become healthy within {timeout:?}")
}

fn spawn_daemon(state_dir: &PathBuf) -> Result<()> {
    let exe = std::env::current_exe().context("failed to locate the wbg-pool binary")?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(state_dir.join("daemon.log"))?;

    let mut command = std::process::Command::new(exe);
    command
        .arg("daemon")
        .arg("--state-dir")
        .arg(state_dir)
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);

    // Detach into its own session so the daemon outlives this shim and is
    // not killed alongside it by the test harness.
    unsafe {
        use std::os::unix::process::CommandExt;
        command.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }

    command
        .spawn()
        .context("failed to spawn the wbg-pool daemon")?;
    Ok(())
}
