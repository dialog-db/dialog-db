#![cfg_attr(not(target_arch = "wasm32"), warn(missing_docs))]
// Native-only: one file through positioned reads and writes. The crate
// compiles to nothing on wasm32 so the workspace's wasm builds are
// unaffected.
#![cfg(not(target_arch = "wasm32"))]

//! A dialog archive kept in one append-only file.
//!
//! [`FileArchive`] answers the archive effects (`Get`, `Put`, `Import`) the
//! way the `fs` provider does, from one file instead of a file per block.
//! The tree, queries and sync read and write it as they do any archive.
//!
//! It builds `research/single-file-format/design.md` up to its fifth step: an
//! artifact tree node is kept as a frame over the file's tokens, and a read
//! realizes the node's block from its frame. Writers in any number of
//! processes take turns on the file's lock. Serving the tree a node read
//! straight from its rows, without realizing a block, is the step after.
//!
//! ```no_run
//! # use dialog_archive_file::FileArchive;
//! # use dialog_common::Buffer;
//! # fn main() -> Result<(), dialog_effects::archive::ArchiveError> {
//! let archive = FileArchive::open("replica.dialog")?;
//! let block = Buffer::from(b"a block".to_vec());
//! archive.append("index", std::slice::from_ref(&block))?;
//! assert!(archive.get("index", block.blake3_hash())?.is_some());
//! # Ok(())
//! # }
//! ```

mod archive;
mod file;
mod hash;
mod locks;
mod node;
mod resource;
mod rows;
mod tokens;
#[cfg(all(feature = "trace", unix))]
pub mod trace;

pub use archive::{CHAIN, FileArchive, Options, Stats};
pub use resource::{FILE_NAME, OpenError, file_at};
