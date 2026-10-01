#![warn(missing_docs)]

//! Storage for dialog: capability providers for the effects a replica
//! performs against its archive, memory, blobs, credentials and
//! certificates, over volatile memory, the file system, or IndexedDB (see
//! [`provider`]), plus the codec and hashing those effects share.

extern crate self as dialog_storage;

pub mod capability;
pub mod emulator;
pub use emulator::*;

pub mod resource;

pub mod dup_audit;
pub use dup_audit::{DUPLICATE_SETS, TOTAL_SETS};

mod encoder;
pub use encoder::*;

mod error;
pub use error::*;

mod storage;
pub use storage::*;

mod hash;
pub use hash::*;

#[cfg(any(test, feature = "helpers"))]
mod helpers;
#[cfg(any(test, feature = "helpers"))]
pub use helpers::*;
