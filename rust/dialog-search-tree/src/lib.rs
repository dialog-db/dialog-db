#![deny(missing_docs)]

//! A content-addressed search tree implementation.
//!
//! This crate provides [`PersistentTree`], a persistent key-value store backed
//! by a prolly tree with content-addressed storage. Trees support efficient
//! lookups, insertions, deletions, and range queries while maintaining
//! structural sharing across versions.
//!
//! Trees are immutable data structures. A [`PersistentTree`] is read-only;
//! mutations go through [`PersistentTree::edit`], which returns a
//! [`TransientTree`] that applies a batch of [`insert`](TransientTree::insert)
//! and [`delete`](TransientTree::delete) operations in memory. Sealing the batch
//! with [`persist`](TransientTree::persist) yields a new [`PersistentTree`]
//! rather than modifying the original in place. This enables:
//!
//! - **Version History**: Keep multiple versions of the tree simultaneously
//! - **Efficient Copying**: Trees share unchanged nodes through content
//!   addressing
//! - **Safe Concurrency**: Multiple readers can access different versions
//!   without conflicts
//!
//! The tree uses a three-tier storage architecture:
//!
//! 1. **Delta Buffer**: Newly created or modified nodes are held in an
//!    in-memory delta buffer. Reads will check the delta before accessing
//!    storage.
//!
//! 2. **Node Cache**: Nodes retrieved from storage are cached in memory to
//!    avoid redundant storage operations. The cache is shared across tree
//!    versions.
//!
//! 3. **Environment**: nodes the cache does not hold are loaded by their
//!    [`Blake3Hash`](dialog_common::Blake3Hash) through the [`LoadBlock`] command the caller's environment
//!    provides. Where the bytes come from is the environment's concern; the
//!    tree checks each loaded block against the hash it asked for.
//!
//! Tree modifications (insert, delete) accumulate in a caller-owned [`Delta`].
//! Each [`persist`](TransientTree::persist) writes new nodes into that delta,
//! and you call [`Delta::flush`] and store the returned buffers to persist
//! changes. Unflushed changes remain queryable but are lost when the delta is
//! dropped. [`MemoryBlocks`] provides [`LoadBlock`] over blocks held in memory.
//!
//! Basic usage:
//!
//! ```
//! # tokio_test::block_on(async {
//! use dialog_search_tree::{Delta, MemoryBlocks, PersistentTree};
//!
//! let storage = MemoryBlocks::new();
//! let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
//! let mut delta = Delta::zero();
//!
//! // Insert entries
//! tree = tree.edit().insert([0, 0, 0, 1], vec![1, 2, 3], &storage).await.unwrap().persist(&mut delta).unwrap();
//!
//! // Flush the persisted nodes into storage so reads can resolve them
//! storage.flush(&mut delta);
//!
//! // Retrieve entries
//! let value = tree.get(&[0, 0, 0, 1], &storage).await.unwrap();
//! assert_eq!(value, Some(vec![1, 2, 3]));
//! # })
//! ```
//!
//! Persisting changes with flush:
//!
//! ```
//! # tokio_test::block_on(async {
//! use dialog_search_tree::{Delta, MemoryBlocks, PersistentTree};
//!
//! let storage = MemoryBlocks::new();
//! let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
//! let mut delta = Delta::zero();
//!
//! // Make several modifications. Each persist writes its new nodes into the
//! // caller-owned delta rather than into storage, so flush after each one to
//! // store the new nodes before the next edit descends into them.
//! for i in 0..10u32 {
//!     tree = tree.edit().insert(i.to_le_bytes(), vec![i as u8], &storage).await.unwrap().persist(&mut delta).unwrap();
//!     storage.flush(&mut delta);
//! }
//!
//! let root_hash = tree.root().clone();
//!
//! // After flushing, the tree can be reconstructed from its root hash
//! // by loading nodes from storage as needed
//! let tree = PersistentTree::<[u8; 4], Vec<u8>>::from_hash(root_hash);
//! assert_eq!(tree.get(&5u32.to_le_bytes(), &storage).await.unwrap(), Some(vec![5]));
//! assert_eq!(tree.get(&9u32.to_le_bytes(), &storage).await.unwrap(), Some(vec![9]));
//! # })
//! ```
//!
//! Working with multiple tree versions:
//!
//! ```
//! # tokio_test::block_on(async {
//! use dialog_search_tree::{Delta, MemoryBlocks, PersistentTree};
//!
//! let storage = MemoryBlocks::new();
//! let tree_v1 = PersistentTree::<[u8; 4], Vec<u8>>::empty();
//! let mut delta = Delta::zero();
//!
//! // Create version 1 with some data. Each persist writes its new nodes into
//! // the delta, so flush after each one before the next edit descends into them.
//! let tree_v1 = tree_v1.edit().insert([0, 0, 0, 1], vec![1], &storage).await.unwrap().persist(&mut delta).unwrap();
//! storage.flush(&mut delta);
//! let tree_v1 = tree_v1.edit().insert([0, 0, 0, 2], vec![2], &storage).await.unwrap().persist(&mut delta).unwrap();
//! storage.flush(&mut delta);
//!
//! // Create version 2 by modifying version 1
//! // Note: tree_v1 remains unchanged
//! let tree_v2 = tree_v1.edit().insert([0, 0, 0, 3], vec![3], &storage).await.unwrap().persist(&mut delta).unwrap();
//! storage.flush(&mut delta);
//!
//! // Both versions can be queried independently
//! assert_eq!(tree_v1.get(&[0, 0, 0, 3], &storage).await.unwrap(), None);
//! assert_eq!(tree_v2.get(&[0, 0, 0, 3], &storage).await.unwrap(), Some(vec![3]));
//!
//! // Both versions see the shared data
//! assert_eq!(tree_v1.get(&[0, 0, 0, 1], &storage).await.unwrap(), Some(vec![1]));
//! assert_eq!(tree_v2.get(&[0, 0, 0, 1], &storage).await.unwrap(), Some(vec![1]));
//! # })
//! ```

mod accessor;
pub use accessor::*;

pub use dialog_common::Buffer;

mod kv;
pub use kv::*;

mod component;
pub use component::*;

mod manifest;
pub use manifest::*;

mod link;
pub use link::*;

mod scale;
pub use scale::*;

mod entry;
pub use entry::*;

mod node;
pub use node::*;

mod load;
pub use load::*;

mod memory;
pub use memory::*;

mod traversal;
pub use traversal::*;

mod tree;
mod validate;
pub use tree::*;

mod hitchhiker;
pub use hitchhiker::*;

mod delta;
pub use delta::*;

mod differential;
pub use differential::*;

mod cache;
pub use cache::*;

mod error;
pub use error::*;

mod distribution;
pub use distribution::*;

mod hashed;
pub use hashed::{Hashed, LazyHash, Separator};

mod encoding;
pub use encoding::*;

mod walker;
pub use walker::*;

/// Helpers for testing and development.
///
/// This module provides utilities for creating deterministic tree structures
/// for testing, including the `tree_spec!` macro and `DistributionSimulator`.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
