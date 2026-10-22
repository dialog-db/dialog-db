#![cfg(not(target_arch = "wasm32"))]
#![warn(missing_docs)]

//! # Dialog Diagnose
//!
//! A Terminal User Interface (TUI) for debugging and inspecting Dialog databases.
//! This crate provides interactive tools to explore database contents, view facts,
//! and navigate the probabilistic search tree structure that underpins Dialog DB.
//!
//! ## Features
//!
//! - **Facts Viewer**: Browse and inspect database facts in a table format
//! - **Tree Explorer**: Navigate the probabilistic search tree structure with expandable nodes
//! - **Interactive Navigation**: Keyboard-driven interface for efficient exploration
//! - **CSV Import**: Load data from CSV files for analysis
//! - **Context Readout**: Profile the causal context a branch head publishes
//!
//! ## Usage
//!
//! The diagnose tool is typically run as a binary:
//!
//! ```bash
//! cargo run --bin diagnose -- <csv_file>
//! ```
//!
//! Or with the tree view enabled by default:
//!
//! ```bash
//! cargo run --bin diagnose -- --tree <csv_file>
//! ```
//!
//! Or, to profile the causal context of a branch head (the dag-cbor
//! `revision` cell of a branch, as a filesystem replica stores it under
//! `memory/<space>/branch/<name>/revision`):
//!
//! ```bash
//! cargo run --bin diagnose -- --head <revision_file>
//! ```
//!
//! ## Key Bindings
//!
//! - `q` - Quit the application
//! - `f` or `F` - Switch to Facts table view
//! - `t` or `T` - Switch to Tree explorer view
//! - `Up`/`Down` - Navigate within the current view
//! - `Enter` - In Tree view, expand/collapse selected node

mod cli;
pub use cli::*;

mod context;
pub use context::*;

mod source;
pub use source::*;

mod state;
pub use state::*;

mod widgets;
pub use widgets::*;

mod promise;
pub use promise::*;
