#![deny(missing_docs)]

//! A content-addressed search tree implementation.
//!
//! This crate provides [`PersistentTree`], a persistent key-value store backed
//! by a probabilistic search tree with content-addressed storage. Trees support efficient
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
//!    in-memory delta buffer until the caller flushes them into storage.
//!
//! 2. **Node Cache**: the environment a tree reads through may hold a
//!    [`NodeCache`] ([`NodeCache::of`]), so a node is checked once and read
//!    from memory after that, by every tree and every version read through
//!    that environment. The tree itself holds no nodes, and a persist keeps
//!    the nodes it seals in the same cache.
//!
//! 3. **Environment**: a tree is a root under a catalog
//!    ([`CatalogScope`](dialog_effects::archive::prelude::CatalogScope)), and
//!    a node the cache does not hold is the block its
//!    [`Blake3Hash`](dialog_common::Blake3Hash) names in that catalog, read by
//!    performing [`Load`](dialog_effects::archive::Load) on the environment
//!    ([`load_node`]). Where the bytes come from (the local archive, or a
//!    remote the environment hydrates from) is the environment's concern;
//!    the tree checks each loaded block against the hash it asked for.
//!
//! Tree modifications (insert, delete) accumulate in a caller-owned [`Delta`].
//! Each [`persist`](TransientTree::persist) writes new nodes into that delta,
//! and you call [`Delta::flush`] and store the returned buffers to persist
//! changes. Unflushed changes remain queryable but are lost when the delta is
//! dropped.
//!
//! Basic usage, over a volatile store:
//!
//! ```
//! # tokio_test::block_on(async {
//! use dialog_capability::{Subject, did};
//! use dialog_effects::archive::prelude::ArchiveScope;
//! use dialog_search_tree::{Delta, PersistentTree};
//! use dialog_storage::provider::Volatile;
//!
//! let storage = Volatile::new();
//! let catalog = ArchiveScope::new(Subject::from(did!("key:z6MkTree"))).index();
//! let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty_in(catalog.clone());
//! let mut delta = Delta::zero();
//!
//! // Insert entries
//! tree = tree.edit().insert([0, 0, 0, 1], vec![1, 2, 3], &storage).await.unwrap().persist(&mut delta, &storage).unwrap();
//!
//! // Flush the persisted nodes into the catalog so reads can resolve them
//! let blocks: Vec<_> = delta.flush().map(|(_, block)| block).collect();
//! catalog.import(blocks).perform(&storage).await.unwrap();
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
//! use dialog_capability::{Subject, did};
//! use dialog_effects::archive::prelude::ArchiveScope;
//! use dialog_search_tree::{Delta, PersistentTree};
//! use dialog_storage::provider::Volatile;
//!
//! let storage = Volatile::new();
//! let catalog = ArchiveScope::new(Subject::from(did!("key:z6MkTree"))).index();
//! let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty_in(catalog.clone());
//! let mut delta = Delta::zero();
//!
//! // Make several modifications. Each persist writes its new nodes into the
//! // caller-owned delta rather than into storage, so flush after each one to
//! // store the new nodes before the next edit descends into them.
//! for i in 0..10u32 {
//!     tree = tree.edit().insert(i.to_le_bytes(), vec![i as u8], &storage).await.unwrap().persist(&mut delta, &storage).unwrap();
//!     let blocks: Vec<_> = delta.flush().map(|(_, block)| block).collect();
//!     catalog.import(blocks).perform(&storage).await.unwrap();
//! }
//!
//! let root_hash = tree.root().clone();
//!
//! // After flushing, the tree can be reconstructed from its root hash in
//! // its catalog, loading nodes from storage as needed
//! let tree = PersistentTree::<[u8; 4], Vec<u8>>::at(catalog, root_hash);
//! assert_eq!(tree.get(&5u32.to_le_bytes(), &storage).await.unwrap(), Some(vec![5]));
//! assert_eq!(tree.get(&9u32.to_le_bytes(), &storage).await.unwrap(), Some(vec![9]));
//! # })
//! ```
//!
//! Working with multiple tree versions:
//!
//! ```
//! # tokio_test::block_on(async {
//! use dialog_capability::{Subject, did};
//! use dialog_effects::archive::prelude::ArchiveScope;
//! use dialog_search_tree::{Delta, PersistentTree};
//! use dialog_storage::provider::Volatile;
//!
//! let storage = Volatile::new();
//! let catalog = ArchiveScope::new(Subject::from(did!("key:z6MkTree"))).index();
//! let flush = async |delta: &mut Delta<_, _>| {
//!     let blocks: Vec<_> = delta.flush().map(|(_, block)| block).collect();
//!     catalog.import(blocks).perform(&storage).await.unwrap();
//! };
//! let tree_v1 = PersistentTree::<[u8; 4], Vec<u8>>::empty_in(catalog.clone());
//! let mut delta = Delta::zero();
//!
//! // Create version 1 with some data. Each persist writes its new nodes into
//! // the delta, so flush after each one before the next edit descends into them.
//! let tree_v1 = tree_v1.edit().insert([0, 0, 0, 1], vec![1], &storage).await.unwrap().persist(&mut delta, &storage).unwrap();
//! flush(&mut delta).await;
//! let tree_v1 = tree_v1.edit().insert([0, 0, 0, 2], vec![2], &storage).await.unwrap().persist(&mut delta, &storage).unwrap();
//! flush(&mut delta).await;
//!
//! // Create version 2 by modifying version 1
//! // Note: tree_v1 remains unchanged
//! let tree_v2 = tree_v1.edit().insert([0, 0, 0, 3], vec![3], &storage).await.unwrap().persist(&mut delta, &storage).unwrap();
//! flush(&mut delta).await;
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

/// What a tree's read bound names, for this crate's modules to import at
/// once without colliding with what they import themselves.
mod reads {
    pub use dialog_capability::Provider;
    pub use dialog_common::Holds;
    pub use dialog_effects::archive::{Get, Load};
}

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
