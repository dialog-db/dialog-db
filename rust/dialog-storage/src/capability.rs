//! Capability commands for archive and memory operations.
//!
//! This module defines command types for storage operations. The [`Effect`] and
//! [`Provider`] traits are re-exported from [`dialog_common`].
//!
//! # Example
//!
//! ```ignore
//! use dialog_storage::capability::{Provider, archive};
//!
//! async fn archive_reader<P>(provider: &P) -> ...
//! where
//!     P: Provider<archive::Get> + Provider<archive::List>
//! { ... }
//! ```

// The archive, memory, and storage capability modules require the S3 feature
// because they depend on types from dialog_s3_credentials.
#[cfg(feature = "s3")]
pub mod archive;
#[cfg(feature = "s3")]
pub mod memory;
#[cfg(feature = "s3")]
pub mod storage;

pub use dialog_common::capability::*;
