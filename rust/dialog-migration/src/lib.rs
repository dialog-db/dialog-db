#![cfg_attr(not(target_arch = "wasm32"), warn(missing_docs))]
// Native-only: migrations read and write replicas on disk. The crate
// compiles to nothing on wasm32 so the workspace's wasm builds are
// unaffected.
#![cfg(not(target_arch = "wasm32"))]

//! Migrations that move a dialog replica from one representation to
//! another.
//!
//! Each migration is behind a feature of its own, so it is built, and its
//! dependencies pulled in, only by those who need to run it. The
//! `dialog-migrate` binary runs whichever migrations it was built with.
//!
//! - `archive-file` ([`archive_file`]): a replica the `fs` provider wrote,
//!   a file per block, into the single-file archive.

#[cfg(feature = "archive-file")]
pub mod archive_file;
