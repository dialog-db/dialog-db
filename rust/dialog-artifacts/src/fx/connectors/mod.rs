//! Connectors for opening storage backends from addresses.
//!
//! This module provides connector implementations that create storage backends
//! from local and remote addresses. These are used by `Site` to open connections
//! on demand.

mod rest;
pub use rest::*;

#[cfg(not(target_arch = "wasm32"))]
mod fs;
#[cfg(not(target_arch = "wasm32"))]
pub use fs::*;

#[cfg(target_arch = "wasm32")]
mod indexeddb;
#[cfg(target_arch = "wasm32")]
pub use indexeddb::*;
