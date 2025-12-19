#![warn(missing_docs)]

//! This crate provides a key-value store implemented as a prolly tree,
//! utilizing a flexible content-addressed block storage backend. This prolly
//! tree is designed to be the foundation of a passive database, providing on
//! demand partial replication.
//!
//! In order to use it, first construct a [`dialog_storage::Storage`] and then initialize
//! a [`Tree`] with it:
//!
//! ```rust
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use dialog_storage::{Storage, MemoryStorageBackend, CborEncoder};
//! use dialog_prolly_tree::{Tree, GeometricDistribution};
//! use dialog_storage::Blake3Hash;
//!
//! let storage = Storage {
//!     encoder: CborEncoder,
//!     backend: MemoryStorageBackend::default()
//! };
//!
//! // Create a tree with geometric distribution (branch factor 254) and Blake3 hashes
//! let mut tree = Tree::<GeometricDistribution, Vec<u8>, Vec<u8>, Blake3Hash, _>::new(storage);
//!
//! // Store a key-value pair
//! tree.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
//!
//! // Get the hash of the tree root (if any)
//! println!("{:?}", tree.hash());
//! # Ok(())
//! # }
//! ```

mod block;
pub use block::*;

mod entry;
pub use entry::*;

mod distribution;
pub use distribution::*;

mod key;
pub use key::*;

mod error;
pub use error::*;

mod node;
pub use node::*;

mod tree;
pub use tree::*;

mod reference;
pub use reference::*;

mod adopter;
pub use adopter::*;

/// Differential synchronization module for computing and applying tree differences.
///
/// This module provides functionality for:
/// - Computing the difference between two trees (`differentiate`)
/// - Applying changes to a tree (`integrate`)
/// - Deterministic conflict resolution for concurrent changes.
pub mod differential;
pub use differential::*;

/// Helpers for testing and development.
///
/// This module provides utilities for creating deterministic tree structures
/// for testing, including the `tree_spec!` macro and `DistributionSimulator`.
#[cfg(any(test, feature = "helpers"))]
mod helpers;
#[cfg(any(test, feature = "helpers"))]
pub use helpers::*;
