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
//! This is the first step of `ideas/single-file-format-design.md`: every
//! block is stored whole. Frames, the compact form for nodes nobody reads,
//! come later and change what a record can hold, not how the archive is
//! used.
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
mod resource;

pub use archive::{CHECKPOINT_EVERY, FileArchive, Options};
pub use resource::{FILE_NAME, OpenError, file_at};
