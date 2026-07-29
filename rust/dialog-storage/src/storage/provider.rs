//! Capability-based storage providers.
//!
//! This module provides implementations of the [`Provider`] trait from
//! `dialog-capability` for executing storage effects defined in `dialog-effects`.
//!
//! # Available Providers
//!
//! - [`FileSystem`] - Filesystem-based storage for native environments
//! - [`IndexedDb`] - IndexedDB-based storage for WASM environments
//! - [`Volatile`] - In-memory storage for testing
//!
//! # Architecture
//!
//! Each provider manages resources keyed by subject DID. For the filesystem,
//! each subject maps to a directory. For IndexedDB, each subject maps to a
//! separate database. For volatile storage, each subject maps to in-memory
//! hash maps. Providers lazily create resources on first access and cache
//! them for subsequent operations.
//!
//! [`Provider`]: dialog_capability::Provider
//! [`FileSystem`]: fs::FileSystem
//! [`IndexedDb`]: indexeddb::IndexedDb
//! [`Volatile`]: volatile::Volatile

// The filesystem provider is isomorphic: native is backed by `tokio::fs`,
// the browser by the File System Access API. It compiles on every target.
pub mod fs;
pub use fs::*;

// The DCAA provider serves the archive effects from one append-only
// `.dialog` file per catalog (`notes/dcaa.md`). Native-only: it needs
// real file offsets and fdatasync.
#[cfg(not(target_arch = "wasm32"))]
pub mod dcaa;
#[cfg(not(target_arch = "wasm32"))]
pub use dcaa::Dcaa;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod indexeddb;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use indexeddb::*;

pub mod volatile;
pub use volatile::*;

pub mod space;
pub use space::{Space, SpaceProvider};

pub mod storage;
pub use storage::Storage;
