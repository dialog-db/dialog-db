#![cfg(not(target_arch = "wasm32"))]
#![warn(missing_docs)]

//! # Dialog Soak
//!
//! A sync/join soak harness: measures what replication actually costs a
//! client over a *simulated* network — in round trips, transferred bytes,
//! and modeled wall-clock time — so regressions in join/sync performance
//! are visible as numbers instead of anecdotes.
//!
//! ## How it works
//!
//! A "server" replica seeds a space of configurable shape and pushes it to
//! a local-directory remote (the [`Fs`](dialog_remote_fs::Fs) transport).
//! A fresh "client" replica then joins that space through the same remote,
//! phase by phase, mirroring the shape of a real application join (tonk's
//! space join): adopt the head, probe the content the join validates, read
//! the membership roster, commit and push a claim, render a first page of
//! content, re-run that query warm, and finally materialize everything
//! (`download`).
//!
//! The [`Fs`] transport meters every remote effect (count + payload bytes)
//! and, when a [`NetworkShape`](dialog_remote_fs::simulation::NetworkShape)
//! is configured, delays each one by its modeled cost: per-request
//! authorization (the access-service redeem a UCAN remote pays per
//! object), round-trip latency, and serialization on a shared
//! bandwidth-limited link. The harness runs under tokio's paused test
//! clock, so the modeled time is measured deterministically and the run
//! completes in real seconds regardless of the simulated latencies.
//!
//! ## Running
//!
//! ```text
//! cargo run -p dialog-soak --release -- --network mobile
//! DIALOG_TREE_FANOUT_N=5 cargo run -p dialog-soak --release -- --network mobile
//! ```
//!
//! See `scripts/soak.sh` for the sweep the regression baseline uses.
//!
//! [`Fs`]: dialog_remote_fs::Fs

pub mod join;
pub mod report;

pub use join::{JoinScenario, run_join};
pub use report::{PhaseReport, Report, TallyRows};
