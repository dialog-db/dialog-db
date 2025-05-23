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
//! // Create a tree with branch factor 32, 32-byte hashes, geometric distribution
//! let mut tree = Tree::<32, 32, GeometricDistribution, Vec<u8>, Vec<u8>, Blake3Hash, _>::new(storage);
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
