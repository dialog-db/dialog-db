#![warn(missing_docs)]

//! This crate constitutes a library of light weight helpers that are shared
//! across multiple other crates. Their cheif quality is that they have
//! virtually zero dependencies.

mod sync;
pub use sync::*;

mod hash;
pub use hash::*;

/// Async utilities for cross-platform task management.
pub mod r#async;
pub use r#async::*;
