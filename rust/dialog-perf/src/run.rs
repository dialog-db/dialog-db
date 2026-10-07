//! Run one scenario in this process: setup, then the measured phase
//! between two snapshots of the counters.

use std::time::Instant;

use anyhow::{Context as _, Result, anyhow};

use crate::counters::{Counters, since};
use crate::report::Report;
use crate::scenario::spec;

/// Run `scenario` at `size` (the catalog's when `None`) and report what
/// its measured phase did. Installs the counting subscriber, so one
/// process runs one scenario.
pub fn run(scenario: &str, size: Option<usize>) -> Result<Report> {
    let spec = spec(scenario).ok_or_else(|| anyhow!("no scenario named {scenario}"))?;
    let size = size.unwrap_or(spec.size);
    let counters = Counters::install();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building the runtime")?;

    let mut prepared = runtime
        .block_on(spec.prepare(size))
        .with_context(|| format!("preparing {scenario}"))?;
    let before = counters.snapshot(prepared.env().tally());
    prepared.env().mark("measure");

    let started = Instant::now();
    let mut outcome = Err(anyhow!("the measured phase did not run"));
    crate::measured(|| outcome = runtime.block_on(prepared.measure()));
    let elapsed = started.elapsed();
    let outcome = outcome.with_context(|| format!("measuring {scenario}"))?;

    let after = counters.snapshot(prepared.env().tally());
    eprintln!("{scenario} (size {size}): measured phase {elapsed:?}");
    Ok(Report {
        scenario: scenario.to_string(),
        size,
        profile: Report::profile().to_string(),
        outcome,
        counters: since(&before, &after),
        instructions: None,
        volatile: spec.volatile,
    })
}
