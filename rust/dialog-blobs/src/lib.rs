//! Content-addressed blob storage.
//!
//! This crate provides a purpose-built blob storage layer that differs
//! from the general-purpose filesystem provider in `dialog_storage` in
//! several ways:
//!
//! - **Streaming interface**: Blobs are read and written as async streams,
//!   enabling support for large files without loading entire contents
//!   into memory.
//!
//! - **Fastest storage backend per platform**: On native targets, this uses
//!   direct filesystem operations. On web targets, this uses the Origin
//!   Private File System (OPFS) rather than IndexedDB. OPFS provides
//!   significantly better performance for large file access and supports
//!   efficient streaming.
//!
//! - **Sharded directory structure**: Blobs are stored in a 3-level directory
//!   tree derived from the first 6 characters of their base58-encoded hash
//!   (e.g., `Ab/Cd/Ef/AbCdEfGh...`). This prevents any single directory from
//!   accumulating too many entries, which can degrade filesystem performance.
//!
//! - **Cross-platform WASM support**: The same API works on both native and
//!   web targets with platform-appropriate implementations.
//!
//! The existing content-addressed storage in `dialog_storage` is designed
//! for smaller blocks and uses IndexedDB on web, which makes efficient
//! chunk streaming comparatively slow and complicated.
//!
//! # Examples
//!
//! Store a blob and read it back:
//!
//! ```
//! use dialog_blobs::{BlobStorage, Vfs};
//! use futures_util::stream;
//! use futures_util::StreamExt;
//!
//! # async fn run() {
//! #     #[cfg(not(target_arch = "wasm32"))]
//! #     let _dir = tempfile::tempdir().unwrap();
//! #     #[cfg(not(target_arch = "wasm32"))]
//! #     let vfs = Vfs::new(_dir.path().to_path_buf());
//! #     #[cfg(target_arch = "wasm32")]
//! #     let vfs = Vfs::new(format!("doctest/{}", ulid::Ulid::new()));
//! let mut storage = BlobStorage::new(vfs);
//!
//! // Store a blob from a stream of byte chunks
//! let hash = storage
//!     .put(stream::iter(vec![b"hello world".to_vec()]))
//!     .await
//!     .unwrap();
//!
//! // Retrieve it by hash
//! let mut reader = storage.get(hash).await.unwrap().expect("blob exists");
//! let mut buf = Vec::new();
//! while let Some(chunk) = reader.next().await {
//!     buf.extend_from_slice(&chunk.unwrap());
//! }
//! assert_eq!(buf, b"hello world");
//! # }
//! # #[cfg(not(target_arch = "wasm32"))]
//! # tokio::runtime::Runtime::new().unwrap().block_on(run());
//! # #[cfg(target_arch = "wasm32")]
//! # wasm_bindgen_futures::spawn_local(run());
//! ```
//!
//! A missing blob returns `None`:
//!
//! ```
//! use dialog_blobs::{BlobStorage, Vfs};
//! use dialog_common::Blake3Hash;
//!
//! # async fn run() {
//! #     #[cfg(not(target_arch = "wasm32"))]
//! #     let _dir = tempfile::tempdir().unwrap();
//! #     #[cfg(not(target_arch = "wasm32"))]
//! #     let vfs = Vfs::new(_dir.path().to_path_buf());
//! #     #[cfg(target_arch = "wasm32")]
//! #     let vfs = Vfs::new(format!("doctest/{}", ulid::Ulid::new()));
//! let storage = BlobStorage::new(vfs);
//!
//! let missing = Blake3Hash::from([0u8; 32]);
//! assert!(storage.get(missing).await.unwrap().is_none());
//! # }
//! # #[cfg(not(target_arch = "wasm32"))]
//! # tokio::runtime::Runtime::new().unwrap().block_on(run());
//! # #[cfg(target_arch = "wasm32")]
//! # wasm_bindgen_futures::spawn_local(run());
//! ```

#![warn(missing_docs)]

mod error;
pub use error::*;

mod vfs;
pub use vfs::*;

mod blob_storage;
pub use blob_storage::*;
