//! Types shared between the runner shim (client) and the daemon (server).

use crate::suite::FilterArgs;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Names the environment variables the shim hands to the test: a comma
/// separated list. A wasm test has no process environment, so the daemon
/// defines each named variable on [`ENV_GLOBAL`] before the suite loads.
pub const PASS_ENV: &str = "WBG_POOL_PASS_ENV";

/// The global object the passed variables are defined on, in the scope the
/// suite runs in (the page, or its worker).
pub const ENV_GLOBAL: &str = "__wbg_pool_env";

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
    /// The variables [`PASS_ENV`] names that are set in the shim's
    /// environment, by name.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
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
