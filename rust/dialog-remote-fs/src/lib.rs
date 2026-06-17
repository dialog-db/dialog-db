//! Local-filesystem remote backend for dialog-db.
//!
//! This crate provides the [`Fs`] site type for syncing dialog repository data
//! to a host-granted local directory. It is a thin **credential-resolution
//! wrapper**: an [`FsAddress`] names a directory grant, which is stored as a
//! site credential. At authorize time the [`Fs`] fork loads that
//! [`Grant`](dialog_effects::credential::Grant), resolves it into a
//! `dialog_storage` [`FileSystem`](dialog_storage::provider::FileSystem), and
//! delegates every capability to it. All filesystem I/O — layout, atomic writes,
//! CAS locking — lives in that isomorphic provider; on native it is backed by
//! `tokio::fs`, in the browser by the [File System Access API][fsapi].
//!
//! Unlike [`dialog-remote-s3`](https://docs.rs/dialog-remote-s3), there is no
//! over-the-wire authorization step: access is granted directly by the host
//! (the user's filesystem permission grant for the chosen directory). That
//! grant *is* the credential — a path on native, a structured-cloneable
//! `FileSystemDirectoryHandle` on the web (which persists in IndexedDB across
//! sessions, so the directory survives a reload without re-prompting).
//!
//! # Granting a directory
//!
//! Before any invocation targeting an [`FsAddress`] fires, the host saves a
//! [`Grant`](dialog_effects::credential::Grant) for it through the credential
//! capability, the same way an S3 secret is saved:
//!
//! ```no_run
//! # #[cfg(not(target_arch = "wasm32"))]
//! # async fn example<Env>(env: &Env, profile: dialog_capability::Subject) -> anyhow::Result<()>
//! # where Env: dialog_capability::Provider<dialog_effects::credential::Save<dialog_effects::credential::Grant>> + dialog_common::ConditionalSync {
//! use dialog_effects::credential::Grant;
//! use dialog_effects::credential::prelude::*;
//! use dialog_remote_fs::FsAddress;
//!
//! let address = FsAddress::new("did:key:z6MkExample");
//! profile
//!     .credential()
//!     .site(address.clone())
//!     .save_grant(Grant::path("/path/to/vault"))
//!     .perform(env)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! On the web, `Grant::handle(directory_handle)` wraps a handle from
//! `showDirectoryPicker()` (or `navigator.storage.getDirectory()`); saving it
//! persists the handle in IndexedDB.
//!
//! [fsapi]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API

#![warn(missing_docs)]

pub mod fs;
pub mod helpers;

pub use fs::*;
