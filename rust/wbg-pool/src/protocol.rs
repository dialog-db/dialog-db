//! Types shared between the runner shim (client) and the daemon (server).

use crate::suite::FilterArgs;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// State file the daemon writes so shims can find it, stored as
/// `daemon.json` inside the state directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonState {
    pub url: String,
    pub pid: u32,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRequest {
    /// Absolute path to the wasm test binary.
    pub binary: PathBuf,
    pub args: FilterArgs,
    pub nocapture: bool,
    /// Wall-clock budget for this invocation, in seconds.
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub ok: bool,
    /// The libtest-style output the harness wrote to `#output`.
    pub output: String,
    /// Captured `console.*` output (`#console_output`), printed on failure.
    pub console_output: String,
    pub timed_out: bool,
}
