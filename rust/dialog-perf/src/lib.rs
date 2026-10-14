#![cfg(not(target_arch = "wasm32"))]
#![warn(missing_docs)]

//! # dialog-perf
//!
//! The engine's cost matrix: one scenario per path an application drives
//! through dialog (a bulk commit, a write that succeeds a standing claim,
//! a transaction read between its writes, a point read, a join, a rule of
//! each shape, a subscription's re-poll), each measured two ways that do
//! not drift between machines or runs:
//!
//! - **counters**: the archive blocks a scenario reads and writes, and
//!   the engine's own steps (rule resolutions, rule hydrations, cell
//!   settlements), counted from the tracing spans the engine emits. These
//!   are exact: a change that moves one is a change in what the engine
//!   does, not in how fast the machine ran.
//! - **instructions**: the instructions the measured phase retires under
//!   callgrind, which is what the wall clock measures without the noise.
//!   Collection toggles on the symbol [`measured`] exports, so a
//!   scenario's setup (seeding, rule installation, cache warm-up) is not
//!   in the number.
//!
//! Every scenario reports its outcome (rows returned, facts committed) so
//! a baseline documents the workload and a regression in what a query
//! answers is as visible as one in what it costs.
//!
//! ## Running
//!
//! ```text
//! cargo run -p dialog-perf --release -- list
//! cargo run -p dialog-perf --release -- run rule-ranked
//! cargo run -p dialog-perf --release -- sweep --out-dir target/perf-current
//! cargo run -p dialog-perf --release -- compare perf/baseline target/perf-current
//! ```
//!
//! `sweep` runs each scenario in a child process under valgrind and
//! writes one report per scenario; `compare` fails on growth past the
//! baseline. `perf:gate` in the nix dev shell runs both, and CI runs it
//! on every pull request. Re-baseline by sweeping into `perf/baseline`
//! and committing the reports with the numbers in the pull request.

pub mod compare;
pub mod counters;
pub mod env;
pub mod report;
pub mod run;
pub mod scenario;
pub mod sweep;

pub use compare::{Comparison, Tolerance, compare_dirs};
pub use report::Report;
pub use run::run;
pub use scenario::{Outcome, Prepared, Spec, catalog, spec};
pub use sweep::{SweepConfig, sweep};

use std::ffi::c_void;

/// Run `phase` as the measured part of a scenario.
///
/// The call goes through [`dialog_perf_measured`], an exported symbol
/// callgrind toggles collection on (`--toggle-collect=dialog_perf_measured`
/// with `--collect-atstart=no`), so only the instructions retired inside
/// `phase` are counted. Outside valgrind this is a plain call.
pub fn measured<F: FnOnce()>(phase: F) {
    let mut phase = Some(phase);
    let mut call = move || {
        if let Some(phase) = phase.take() {
            phase();
        }
    };
    let mut dynamic: &mut dyn FnMut() = &mut call;
    let context: *mut &mut dyn FnMut() = &mut dynamic;
    dialog_perf_measured(context.cast::<c_void>());
}

/// The symbol callgrind toggles collection on; see [`measured`].
///
/// `phase` is a pointer to a `&mut dyn FnMut()` that [`measured`] keeps
/// alive for the duration of this call. Not part of the crate's API:
/// it is exported unmangled so a profiler can name it.
#[doc(hidden)]
#[unsafe(no_mangle)]
#[inline(never)]
pub extern "C" fn dialog_perf_measured(phase: *mut c_void) {
    // SAFETY: `measured` is the only caller and passes a pointer to a
    // `&mut dyn FnMut()` that lives on its stack frame for this call.
    let phase = unsafe { &mut *phase.cast::<&mut dyn FnMut()>() };
    phase();
}
