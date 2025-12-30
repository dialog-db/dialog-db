#![warn(missing_docs)]

//! This crate constitutes a library of light weight helpers that are shared
//! across multiple other crates. Their chief quality is that they have
//! virtually zero dependencies.

mod sync;
pub use sync::*;

mod hash;
pub use hash::*;

/// Async utilities for cross-platform task management.
pub mod r#async;
pub use r#async::*;

/// Algebraic effects system for capability-based programming.
pub mod fx;
