//! Capability-based repository system.
//!
//! This module provides a repository abstraction built on top of the
//! capability-based effect system (`dialog-capability` / `dialog-effects`).
//!
//! - [`archive`] — CAS adapter bridging capabilities with prolly tree storage
//! - [`branch`] — Branch operations (open, load, commit, select, reset, pull)
//! - [`cell`] — Transactional memory cells with edition tracking
//! - [`revision`] — Revision tracking and logical timestamps

/// CAS adapter bridging capabilities with prolly tree's ContentAddressedStorage.
pub mod archive;
/// Capability-based branch operations (command pattern).
pub mod branch;
/// Cell descriptor for typed memory cell operations.
pub mod cell;
/// Credentials for signing and identity management.
pub mod credentials;
/// Repository error types.
pub mod error;
/// Node reference type for tree root hashes.
pub mod node_reference;
/// Occurence logical timestamp type.
pub mod occurence;
/// Revision type and edition tracking.
pub mod revision;

/// Remote site identifier.
pub type Site = String;

pub use branch::{BranchId, BranchState, UpstreamState};
pub use error::{OperationKind, RepositoryError};
pub use node_reference::NodeReference;
pub use occurence::Occurence;
pub use revision::{Edition, Revision};
