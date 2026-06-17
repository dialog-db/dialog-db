//! Local-filesystem remote backend for dialog-db.
//!
//! This crate provides the [`Fs`] site type for syncing dialog repository data
//! to a local directory that backs a space. An [`FsAddress`] names that
//! directory; at authorize time the [`Fs`] fork opens it, **verifies it really
//! is the space for the invocation's subject** — its stored `credential/key/self`
//! DID must match — and hands the resolved
//! `dialog_storage` [`FileSystem`](dialog_storage::provider::FileSystem) to the
//! provider, which performs the capability against it. This is the local
//! analogue of [`dialog-remote-s3`](https://docs.rs/dialog-remote-s3)'s presign
//! endpoint: it checks the request, then yields the means to act. All filesystem
//! I/O — layout, atomic writes, CAS locking — lives in that isomorphic provider
//! (`tokio::fs` on native, the [File System Access API][fsapi] in the browser).
//!
//! # Addressing a directory
//!
//! The address resolves to a directory per target:
//!
//! - **native**: a `file:` URL, opened directly.
//! - **web**: an IndexedDB database name holding the directory's
//!   `FileSystemDirectoryHandle`. Register it once (typically right after
//!   `showDirectoryPicker()`) with
//!   [`register_web_directory`](dialog_storage::provider::register_web_directory);
//!   afterwards the database is the durable, self-contained address.
//!
//! ```no_run
//! # #[cfg(not(target_arch = "wasm32"))]
//! # fn example() {
//! use dialog_remote_fs::FsAddress;
//!
//! // Native: the address is the directory's file: URL.
//! let _address = FsAddress::new("file:///path/to/vault");
//! # }
//! ```
//!
//! [fsapi]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API

#![warn(missing_docs)]

pub mod fs;
pub mod helpers;

pub use fs::*;
