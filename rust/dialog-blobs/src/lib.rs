//! Local blob storage backed by the filesystem.
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

#[cfg(feature = "archive")]
mod provider;
