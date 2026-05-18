//! Local-filesystem remote backend for dialog-db.
//!
//! This crate provides the [`Fs`] site type for syncing dialog repository
//! data to a local filesystem directory. In a browser context the directory
//! is reached through the
//! [File System Access API](https://developer.mozilla.org/en-US/docs/Web/API/File_System_Access_API)
//! (Chromium-only); on native targets it can be backed by `std::fs` for
//! parity / tests.
//!
//! Unlike [`dialog-remote-s3`](https://docs.rs/dialog-remote-s3), there is
//! no over-the-wire authorization step — access is granted directly by the
//! host (the user's filesystem permission grant for the chosen directory).
//! The site/authorization/permit/invocation pattern is preserved for
//! structural parity with the S3 remote.
//!
//! Byte-compatible on-disk format with `dialog-storage`'s native FS
//! provider:
//!
//! ```text
//! {root}/archive/{catalog}/{base58(digest)}
//! {root}/memory/{space}/{cell}
//! {root}/credential/key/{address}
//! {root}/certificate/{audience}/{subject}/{issuer}.{hash}
//! ```

#![warn(missing_docs)]

mod error;
pub mod fs;
pub mod request;

pub use error::FsError;
pub use fs::*;
pub use request::{FsRequest, IntoRequest};
pub use request::{archive, memory};
