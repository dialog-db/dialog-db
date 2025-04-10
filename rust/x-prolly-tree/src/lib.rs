#![warn(missing_docs)]

//! This crate provides a key-value store implemented as a prolly tree,
//! utilizing a flexible content-addressed block storage backend. This prolly
//! tree is designed to be the foundation of a passive database, providing on
//! demand partial replication.
//!
//! In order to use it, first construct a [`x_storage::Storage`] and then initialize
//! a [`Tree`] with it:
//!
//! ```ignore
//! use x_storage::{Storage, MemoryStorageBackend};
//! use x_prolly_tree::{BasicEncoder, Tree};
//!
//! let storage = Storage {
//!     encoder: BasicEncoder,
//!     backend: MemoryStorageBackend::default()
//! };
//!
//! let tree = Tree::<32, 32, GeometricDistribution, _, _, _, _>::new(storage);
//!
//! tree.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
//!
//! println!("{:?}", tree.hash());
//! ```

mod encoder;
pub use encoder::*;

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
