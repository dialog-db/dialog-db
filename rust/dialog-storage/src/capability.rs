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

pub mod archive;
pub mod memory;

pub use archive::{ArchiveError, Delete, Get, List, ListResult, Put};
pub use memory::{MemoryError, Precondition, Publication, Publish, Resolution, Resolve};

// Re-export Effect and Provider from dialog_common
pub use dialog_common::{Effect, Provider};
