//! The shared-browser daemon. It owns one headless Chrome instance and an
//! HTTP server; every `/api/run` request becomes a fresh tab on a fresh
//! `t-<id>.localhost` origin, which gives each test pristine origin-scoped
//! storage without paying a browser launch.

mod browser;
mod codegen;
mod harness;
mod server;

use crate::protocol::DaemonState;
use anyhow::{Context, Result};
use clap::Parser;
use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Codegen results keyed by (canonical path, fingerprint); the OnceCell
/// coalesces concurrent requests for the same binary into one codegen run.
pub type BinaryCache = tokio::sync::Mutex<
    HashMap<(PathBuf, String), Arc<tokio::sync::OnceCell<Arc<codegen::Binary>>>>,
>;

#[derive(Parser, Debug)]
#[command(
    name = "wbg-pool daemon",
    version,
    about = "Shared-browser daemon for wbg-pool"
)]
struct DaemonCli {
    /// Directory for the daemon state file (daemon.json) and log.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// Exit after this many seconds without any test activity.
    #[arg(long, default_value = "300")]
    idle_timeout: u64,
    /// Stop a running daemon instead of starting one.
    #[arg(long)]
    stop: bool,
}

pub fn main(args: Vec<OsString>) -> Result<()> {
    let cli = DaemonCli::parse_from(args);
    let state_dir = cli.state_dir.clone().unwrap_or_else(default_state_dir);

    if cli.stop {
        return stop(&state_dir);
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(state_dir, cli.idle_timeout))
}

/// Where shims and the daemon rendezvous. Overridable with WBG_POOL_DIR.
pub fn default_state_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("WBG_POOL_DIR") {
        return PathBuf::from(dir);
    }
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir).join("wbg-pool");
    }
    let uid = unsafe { libc::geteuid() };
    std::env::temp_dir().join(format!("wbg-pool-{uid}"))
}

fn stop(state_dir: &Path) -> Result<()> {
    let state_path = state_dir.join("daemon.json");
    let raw = match std::fs::read_to_string(&state_path) {
        Ok(raw) => raw,
        Err(_) => {
            println!("no running daemon found at {}", state_path.display());
            return Ok(());
        }
    };
    let state: DaemonState = serde_json::from_str(&raw)?;
    match ureq::post(&format!("{}/api/shutdown", state.url))
        .timeout(Duration::from_secs(5))
        .call()
    {
        Ok(_) => println!("stopped daemon at {}", state.url),
        Err(error) => println!(
            "daemon at {} did not respond ({error}); removing state file",
            state.url
        ),
    }
    let _ = std::fs::remove_file(&state_path);
    Ok(())
}

pub struct Daemon {
    pub port: u16,
    pub state_dir: PathBuf,
    pub work_dir: PathBuf,
    pub browser: browser::BrowserPool,
    pub binaries: BinaryCache,
    pub runs: Mutex<HashMap<u64, server::RunSlot>>,
    pub run_counter: AtomicU64,
    pub active: AtomicUsize,
    pub last_activity: Mutex<Instant>,
}

impl Daemon {
    pub fn touch(&self) {
        *self.last_activity.lock().unwrap() = Instant::now();
    }
}

async fn run(state_dir: PathBuf, idle_timeout: u64) -> Result<()> {
    std::fs::create_dir_all(&state_dir)
        .with_context(|| format!("failed to create state dir {}", state_dir.display()))?;

    let work_dir = state_dir.join(format!("work-{}", std::process::id()));
    std::fs::create_dir_all(&work_dir)?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    let daemon = std::sync::Arc::new(Daemon {
        port,
        state_dir: state_dir.clone(),
        work_dir: work_dir.clone(),
        browser: browser::BrowserPool::new(work_dir.join("browser-profile")),
        binaries: tokio::sync::Mutex::new(HashMap::new()),
        runs: Mutex::new(HashMap::new()),
        run_counter: AtomicU64::new(1),
        active: AtomicUsize::new(0),
        last_activity: Mutex::new(Instant::now()),
    });

    write_state_file(&state_dir, port)?;
    println!("wbg-pool daemon listening on http://127.0.0.1:{port}");

    let app = server::router(daemon.clone());

    let idle_daemon = daemon.clone();
    tokio::spawn(async move {
        let idle = Duration::from_secs(idle_timeout);
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let active = idle_daemon.active.load(Ordering::SeqCst);
            let last = *idle_daemon.last_activity.lock().unwrap();
            if active == 0 && last.elapsed() > idle {
                println!("idle for {idle_timeout}s, shutting down");
                shutdown(&idle_daemon).await;
            }
        }
    });

    let signal_daemon = daemon.clone();
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        shutdown(&signal_daemon).await;
    });

    axum::serve(listener, app).await?;
    Ok(())
}

pub async fn shutdown(daemon: &Daemon) -> ! {
    daemon.browser.kill().await;
    remove_state_file_if_ours(&daemon.state_dir);
    let _ = std::fs::remove_dir_all(&daemon.work_dir);
    std::process::exit(0)
}

fn write_state_file(state_dir: &Path, port: u16) -> Result<()> {
    let state = DaemonState {
        url: format!("http://127.0.0.1:{port}"),
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let path = state_dir.join("daemon.json");
    let tmp = state_dir.join(format!("daemon.json.{}", std::process::id()));
    std::fs::write(&tmp, serde_json::to_vec_pretty(&state)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

fn remove_state_file_if_ours(state_dir: &Path) {
    let path = state_dir.join("daemon.json");
    let ours = std::fs::read_to_string(&path)
        .ok()
        .and_then(|raw| serde_json::from_str::<DaemonState>(&raw).ok())
        .map(|state| state.pid == std::process::id())
        .unwrap_or(false);
    if ours {
        let _ = std::fs::remove_file(&path);
    }
}
