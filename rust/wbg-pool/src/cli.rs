//! Command line interface mirroring `wasm-bindgen-test-runner` so that
//! `wbg-pool` can be used as a drop-in `target.wasm32-unknown-unknown.runner`.
//! The flag set (and its filtering semantics in [`crate::suite`]) must stay
//! byte-compatible with what libtest-style harnesses such as `cargo test`
//! and `cargo nextest` pass to target runners.

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "wbg-pool", version, about, long_about = None)]
pub struct RunnerCli {
    #[arg(
        index = 1,
        help = "The wasm test binary to run. `cargo test` passes this argument for you."
    )]
    pub file: PathBuf,
    #[arg(long, help = "Run benchmarks")]
    pub bench: bool,
    #[arg(long, conflicts_with = "ignored", help = "Run ignored tests")]
    pub include_ignored: bool,
    #[arg(long, conflicts_with = "include_ignored", help = "Run ignored tests")]
    pub ignored: bool,
    #[arg(long, help = "Exactly match filters rather than by substring")]
    pub exact: bool,
    #[arg(
        long,
        value_name = "FILTER",
        help = "Skip tests whose names contain FILTER (this flag can be used multiple times)"
    )]
    pub skip: Vec<String>,
    #[arg(long, help = "List all tests and benchmarks")]
    pub list: bool,
    #[arg(
        long,
        help = "don't capture `console.*()` of each task, allow printing directly"
    )]
    pub nocapture: bool,
    #[arg(
        long,
        value_name = "terse",
        help = "Configure formatting of output (accepted for libtest compatibility)"
    )]
    pub format: Option<String>,
    #[arg(
        index = 2,
        value_name = "FILTER",
        help = "The FILTER string is tested against the name of all tests, and only those tests \
                whose names contain the filter are run."
    )]
    pub filter: Option<String>,
}

impl RunnerCli {
    pub fn filter_args(&self) -> crate::suite::FilterArgs {
        crate::suite::FilterArgs {
            filter: self.filter.clone(),
            exact: self.exact,
            skip: self.skip.clone(),
            ignored: self.ignored,
            include_ignored: self.include_ignored,
        }
    }
}
