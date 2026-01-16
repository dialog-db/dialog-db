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

extern crate self as dialog_storage;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod web;

mod encoder;
pub use encoder::*;

mod error;
pub use error::*;

mod storage;
pub use storage::*;
#[cfg(feature = "s3")]
pub use storage::s3;

mod hash;
pub use hash::*;

#[cfg(any(test, feature = "helpers"))]
mod helpers;
#[cfg(any(test, feature = "helpers"))]
pub use helpers::*;
