#![deny(missing_docs)]

//! A content-addressed search tree implementation.
//!
//! This crate provides [`Tree`], a persistent key-value store backed by a
//! prolly tree with content-addressed storage. Trees support efficient lookups,
//! insertions, deletions, and range queries while maintaining structural
//! sharing across versions.
//!
//! Trees are immutable data structures. Operations like [`Tree::insert`] and
//! [`Tree::delete`] return a new [`Tree`] instance rather than modifying in
//! place. This enables:
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
//! 3. **Content-Addressed Storage**: Persistent storage where nodes are keyed
//!    by their [`Blake3Hash`]. Storage is only accessed when a node is not
//!    found in the delta or cache.
//!
//! Tree modifications (insert, delete) accumulate in the delta buffer. You must
//! call [`Tree::flush`] and store the returned buffers to persist changes.
//! Unflushed changes remain queryable but are lost when the tree is dropped.
//!
//! Basic usage:
//!
//! ```
//! # tokio_test::block_on(async {
//! use dialog_search_tree::{Tree, ContentAddressedStorage};
//! use dialog_storage::MemoryStorageBackend;
//!
//! let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
//! let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();
//!
//! // Insert entries
//! tree = tree.insert([0, 0, 0, 1], vec![1, 2, 3], &storage).await.unwrap();
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
//! use dialog_search_tree::{Tree, ContentAddressedStorage};
//! use dialog_storage::MemoryStorageBackend;
//!
//! let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
//! let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();
//!
//! // Make several modifications
//! for i in 0..10u32 {
//!     tree = tree.insert(i.to_le_bytes(), vec![i as u8], &storage).await.unwrap();
//! }
//!
//! // Changes are queryable immediately, even before flushing
//! assert_eq!(tree.get(&5u32.to_le_bytes(), &storage).await.unwrap(), Some(vec![5]));
//!
//! // Flush the delta to get buffers that need to be persisted
//! let root_hash = tree.root().clone();
//! for (hash, buffer) in tree.flush() {
//!     storage.store(buffer.as_ref().to_vec(), &hash).await.unwrap();
//! }
//!
//! // After flushing, the tree can be reconstructed from its root hash
//! // by loading nodes from storage as needed
//! let tree = Tree::<[u8; 4], Vec<u8>>::from_hash(root_hash);
//! assert_eq!(tree.get(&5u32.to_le_bytes(), &storage).await.unwrap(), Some(vec![5]));
//! assert_eq!(tree.get(&9u32.to_le_bytes(), &storage).await.unwrap(), Some(vec![9]));
//! # })
//! ```
//!
//! Working with multiple tree versions:
//!
//! ```
//! # tokio_test::block_on(async {
//! use dialog_search_tree::{Tree, ContentAddressedStorage};
//! use dialog_storage::MemoryStorageBackend;
//!
//! let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
//! let tree_v1 = Tree::<[u8; 4], Vec<u8>>::empty();
//!
//! // Create version 1 with some data
//! let tree_v1 = tree_v1.insert([0, 0, 0, 1], vec![1], &storage).await.unwrap();
//! let tree_v1 = tree_v1.insert([0, 0, 0, 2], vec![2], &storage).await.unwrap();
//!
//! // Create version 2 by modifying version 1
//! // Note: tree_v1 remains unchanged
//! let tree_v2 = tree_v1.insert([0, 0, 0, 3], vec![3], &storage).await.unwrap();
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

mod buffer;
pub use buffer::*;

mod kv;
pub use kv::*;

mod link;
pub use link::*;

mod entry;
pub use entry::*;

mod node;
pub use node::*;

mod body;
pub use body::*;

mod storage;
pub use storage::*;

mod tree;
pub use tree::*;

mod delta;
pub use delta::*;

mod cache;
pub use cache::*;

mod error;
pub use error::*;

mod distribution;
pub use distribution::*;

mod encoding;
pub use encoding::*;

mod walker;
pub use walker::*;

mod shaper;
pub use shaper::*;

mod compare;
pub use compare::*;
