//! Local-filesystem remote backend for dialog-db.
//!
//! This crate provides the [`Fs`] site type for syncing dialog repository data
//! to a local directory that backs a space. An [`FsAddress`] names that
//! directory; at authorize time the [`Fs`] fork opens it, checks that the
//! directory's stored `credential/key/self` DID matches the invocation's
//! subject, and hands the resolved `dialog_storage`
//! [`FileSystem`](dialog_storage::provider::FileSystem) to the provider, which
//! performs the capability against it. This mirrors the *shape* of
//! [`dialog-remote-s3`](https://docs.rs/dialog-remote-s3)'s presign step — check,
//! then yield the means to act — locally, with no network. All filesystem I/O
//! (layout, atomic writes, CAS locking) lives in that isomorphic provider
//! (`tokio::fs` on native, the [File System Access API][fsapi] in the browser).
//!
//! # Trust model
//!
//! The directory's identity is **self-asserted**: `credential/key/self` is just
//! a file in the directory, so whoever can write the directory can choose the
//! DID it claims. The subject check therefore confirms only that the address
//! points at the space the caller means to act on — it catches a mis-pointed
//! address (wrong vault for this subject), **not** an adversary who controls the
//! directory's contents. The trust boundary is the host's grant of the
//! directory itself (a chosen path on native, a user-picked
//! `FileSystemDirectoryHandle` on the web); this crate trusts that grant and
//! does not defend a shared directory against other writers.
//!
//! Access is **all-or-nothing**: once the subject matches, every effect
//! (`Get`/`Put`/`Resolve`/`Publish`/`Retract`) is permitted. There is no
//! read-only-vs-read-write distinction at this layer.
//!
//! # Preconditions
//!
//! The target directory must already be a space — i.e. created with
//! [`Repository::create`](dialog_repository) (which writes `credential/key/self`)
//! — before any `Fs` invocation. The site does **not** bootstrap an empty
//! directory: with no credential present, [`authorize`](FsFork) denies. This is
//! the same "the vault must already exist" precondition that native
//! `Repository::open` has.
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
