//! Capability commands for storage operations.
//!
//! This module re-exports the capability hierarchy and effect types from
//! [`dialog_effects`], providing archive, memory, and storage operations.
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

// Re-export all capability types from dialog-effects
pub use dialog_effects::archive;
pub use dialog_effects::memory;
pub use dialog_effects::storage;

// Re-export capability primitives
pub use dialog_capability::*;
