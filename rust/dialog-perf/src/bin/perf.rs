#![cfg(not(target_arch = "wasm32"))]

//! # dialog-perf
//!
//! The engine's cost matrix CLI.
//!
//! ```text
//! perf list                                  # the scenarios and their sizes
//! perf run rule-ranked                       # one scenario (JSON to stdout)
//! perf sweep --out-dir target/perf-current   # every scenario under callgrind
//! perf compare perf/baseline target/perf-current   # regression gate
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use dialog_perf::{SweepConfig, Tolerance, catalog, compare_dirs, run, sweep};

/// The engine's cost matrix: counters and instruction counts per scenario.
#[derive(Debug, Parser)]
#[command(name = "perf", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// List the scenarios, their baseline sizes and what they measure.
    List,
    /// Run one scenario in this process and print its report as JSON.
    Run(RunArgs),
    /// Run every scenario in a child process, under callgrind unless told
    /// otherwise, writing one report each.
    Sweep(SweepArgs),
    /// Compare two sweep directories and fail on regressions.
    Compare(CompareArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    /// The scenario's name, as `list` prints it.
    scenario: String,

    /// Run at this size instead of the catalog's.
    #[arg(long)]
    size: Option<usize>,
}

#[derive(Debug, Args)]
struct SweepArgs {
    /// Directory to write the reports into.
    #[arg(long, default_value = "target/perf-current")]
    out_dir: PathBuf,

    /// Only these scenarios (comma-separated).
    #[arg(long, value_delimiter = ',')]
    only: Vec<String>,

    /// Skip callgrind: counters alone, no instruction counts.
    #[arg(long)]
    no_callgrind: bool,
}

#[derive(Debug, Args)]
struct CompareArgs {
    /// Baseline sweep directory (e.g. `perf/baseline`).
    baseline: PathBuf,

    /// New sweep directory (e.g. `target/perf-current`).
    new: PathBuf,

    /// Instruction growth allowed, in percent.
    #[arg(long, default_value_t = Tolerance::default().instructions_pct)]
    threshold: f64,
}

fn main() -> Result<ExitCode> {
    match Cli::parse().command {
        Command::List => {
            for spec in catalog() {
                println!("{:<20} size {:>5}  {}", spec.name, spec.size, spec.about);
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Run(args) => {
            let report = run(&args.scenario, args.size)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(ExitCode::SUCCESS)
        }
        Command::Sweep(args) => {
            let reports = sweep(&SweepConfig {
                out_dir: args.out_dir.clone(),
                only: args.only,
                callgrind: !args.no_callgrind,
            })?;
            eprintln!(
                "{} report(s) written to {}",
                reports.len(),
                args.out_dir.display()
            );
            Ok(ExitCode::SUCCESS)
        }
        Command::Compare(args) => {
            let tolerance = Tolerance {
                instructions_pct: args.threshold,
                ..Tolerance::default()
            };
            let comparison = compare_dirs(&args.baseline, &args.new, &tolerance)?;
            for line in &comparison.summary {
                println!("{line}");
            }
            if !comparison.improvements.is_empty() {
                println!(
                    "\n{} improvement(s) past the allowance; re-baseline to hold them:",
                    comparison.improvements.len()
                );
                for line in &comparison.improvements {
                    println!("  {line}");
                }
            }
            if comparison.passed() {
                println!("\nno regressions");
                Ok(ExitCode::SUCCESS)
            } else {
                println!(
                    "\n{} regression(s) (instructions over {:.0}%, steps exact, blocks within two):",
                    comparison.regressions.len(),
                    args.threshold
                );
                for line in &comparison.regressions {
                    println!("  {line}");
                }
                Ok(ExitCode::FAILURE)
            }
        }
    }
}
