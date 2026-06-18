//! Local-filesystem remote backend for dialog-db.
//!
//! This crate provides the [`Fs`] site type for syncing dialog repository data
//! to a local directory that backs a space. An [`FsAddress`] names that
//! directory; at authorize time the [`Fs`] fork (1) proves the operator holds a
//! delegation for the requested effect, (2) opens the directory and checks that
//! its stored `credential/key/self` DID matches the invocation's subject, then
//! (3) hands the resolved `dialog_storage`
//! [`FileSystem`](dialog_storage::provider::FileSystem) to the provider, which
//! performs the capability against it. This mirrors
//! [`dialog-remote-ucan-s3`](https://docs.rs/dialog-remote-ucan-s3): the same
//! `Identify` + delegation-proof step a UCAN fork runs before redeeming an
//! invocation, done locally against the operator's own stored delegations with
//! no network. All filesystem I/O (layout, atomic writes, CAS locking) lives in
//! that isomorphic provider (`tokio::fs` on native, the
//! [File System Access API][fsapi] in the browser).
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
//! Access is **per-effect**: the operator must hold a delegation covering the
//! invoked command. Because the proof matches on command prefix, a delegation
//! granting only `/archive/get` authorizes reads but not `/archive/put` writes
//! — the same read-vs-write gating a UCAN remote enforces. A self-owned
//! operator (it *is* the subject) is authorized for every effect.
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
//! The address is a [`Location`](dialog_effects::storage::Location) — the same
//! target-agnostic locator the rest of the system uses to open storage. It
//! resolves to a platform directory through
//! [`FileSystem::open`](dialog_storage::provider::FileSystem), so one address
//! works on native (a path under the platform layout) and the web (an OPFS
//! subdirectory) alike.
//!
//! ```no_run
//! # fn example() {
//! use dialog_effects::storage::Location;
//! use dialog_remote_fs::FsAddress;
//!
//! let _address = FsAddress::new(Location::temp("my-vault"));
//! # }
//! ```
//!
//! [fsapi]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API

#![warn(missing_docs)]

pub mod fs;

pub use fs::*;
