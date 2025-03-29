#![warn(missing_docs)]

//! This crate contains generalized API for constructing content addressed
//! storage from different backends and encoding schemes.
//!
//! In order to use it, first select or implement an [Encoder], and then select
//! or implement a [StorageBackend]. When you have selected these things, you
//! can construct a [Storage]:
//!
//! ```ignore
//! let encoder = /* Some encoder */;
//! let backend = /* Some storage backend */;
//! let storage = Storage {
//!     encoder,
//!     backend
//! };
//! ```
//!
//! The prepared `storage` will automatically implement
//! [ContentAddressedStorage] for bounds-matching encoders and storage backends.

mod encoder;
pub use encoder::*;

mod error;
pub use error::*;

mod storage;
pub use storage::*;

mod hash;
pub use hash::*;
