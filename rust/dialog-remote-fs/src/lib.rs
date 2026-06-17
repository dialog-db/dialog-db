//! Local-filesystem remote backend for dialog-db.
//!
//! This crate provides the [`Fs`] site type for syncing dialog repository data
//! to a user-picked local directory. It is a thin **credential-resolution
//! wrapper**: it maps an [`FsAddress`] to a registered directory and delegates
//! every capability to `dialog_storage`'s isomorphic
//! [`FileSystem`](dialog_storage::provider::FileSystem) provider, which does
//! all of the filesystem I/O (layout, atomic writes, CAS locking). On native
//! that provider is backed by `tokio::fs`; in the browser by the
//! [File System Access API][fsapi]. The on-disk format is identical on both
//! targets, so a directory written from the browser is a valid native vault
//! and vice versa.
//!
//! Unlike [`dialog-remote-s3`](https://docs.rs/dialog-remote-s3), there is no
//! over-the-wire authorization step: access is granted directly by the host
//! (the user's filesystem permission grant for the chosen directory). The
//! site/authorization pattern is preserved for structural parity with the S3
//! remote, but [`FsAuthorization`] is a unit marker.
//!
//! # Registering a directory
//!
//! Before any invocation targeting an [`FsAddress`] fires, the host must
//! register the directory it names. On native, register a path; in the browser,
//! register a [`web_sys::FileSystemDirectoryHandle`] (from `showDirectoryPicker()`
//! or `navigator.storage.getDirectory()`):
//!
//! ```no_run
//! # #[cfg(not(target_arch = "wasm32"))]
//! # fn main() -> anyhow::Result<()> {
//! use dialog_remote_fs::{FsAddress, register_directory};
//!
//! // The id is opaque; consumers typically use the vault's subject DID.
//! let id = "did:key:z6MkExample";
//! register_directory(id, std::path::PathBuf::from("/path/to/vault"))?;
//!
//! // An FsAddress with the same id now resolves to that directory.
//! let _address = FsAddress::new(id);
//! # Ok(())
//! # }
//! # #[cfg(target_arch = "wasm32")]
//! # fn main() {}
//! ```
//!
//! [fsapi]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API

#![warn(missing_docs)]

mod error;
pub mod fs;
pub mod registry;

pub use error::FsError;
pub use fs::*;
pub use registry::{register_directory, unregister_directory};
