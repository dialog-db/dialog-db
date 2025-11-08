#![warn(missing_docs)]

//! This crate contains generalized API for constructing content addressed
//! storage from different backends and encoding schemes.
//!
//! In order to use it, first select or implement an [Encoder], and then select
//! or implement a [StorageBackend]. When you have selected these things, you
//! can construct a [Storage]:
//!
//! ```rust
//! use dialog_storage::{Storage, CborEncoder, MemoryStorageBackend};
//!
//! // Create a CBOR encoder for serialization/deserialization
//! let encoder = CborEncoder;
//!
//! // Create an in-memory storage backend with explicit types
//! // Using [u8; 32] as the key type and Vec<u8> as the value type
//! let backend = MemoryStorageBackend::<[u8; 32], Vec<u8>>::default();
//!
//! // Combine them into a Storage instance
//! let storage = Storage {
//!     encoder,
//!     backend
//! };
//! ```
//!
//! The prepared `storage` will automatically implement
//! [ContentAddressedStorage] for bounds-matching encoders and storage backends.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod web;

mod encoder;
pub use encoder::*;

mod error;
pub use error::*;

mod storage;
pub use storage::*;

mod hash;
pub use hash::*;

/// S3-safe key encoding functions.
///
/// Keys are treated as `/`-delimited paths. Each path component is checked:
/// - If it contains only safe characters (alphanumeric, `-`, `_`, `.`), it's kept as-is
/// - Otherwise, it's base58-encoded and prefixed with `!`
///
/// The `!` character is used as a prefix because it's in AWS S3's "safe for use" list.
///
/// Examples:
/// - `"remote/main"` → `"remote/main"` (all components safe)
/// - `"remote/user@example"` → `"remote/!<base58>"` (@ is unsafe)
/// - `"foo/bar/baz"` → `"foo/bar/baz"` (all safe)
pub mod key_encoding {
    pub use crate::{decode_s3_key as decode, encode_s3_key as encode};
}

#[cfg(any(test, feature = "helpers"))]
mod helpers;
#[cfg(any(test, feature = "helpers"))]
pub use helpers::*;

/// S3 test server for integration testing
#[cfg(all(any(test, feature = "test-utils"), not(target_arch = "wasm32")))]
pub mod s3_test_server {
    pub use crate::s3::{Service, start};
}
