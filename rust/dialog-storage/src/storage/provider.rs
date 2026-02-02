//! Capability-based storage providers.
//!
//! This module provides implementations of the [`Provider`] trait from
//! `dialog-capability` for executing storage effects defined in `dialog-effects`.
//!
//! # Available Providers
//!
//! - [`FileSystem`] - Filesystem-based storage for native environments
//! - [`IndexedDb`] - IndexedDB-based storage for WASM environments
//!
//! # Architecture
//!
//! Each provider manages resources keyed by subject DID. For the filesystem,
//! each subject maps to a directory. For IndexedDB, each subject maps to a
//! separate database. Providers lazily create resources on first access and
//! cache them for subsequent operations.
//!
//! [`Provider`]: dialog_capability::Provider
//! [`FileSystem`]: fs::FileSystem
//! [`IndexedDb`]: indexeddb::IndexedDb

#[cfg(not(target_arch = "wasm32"))]
pub mod fs;

#[cfg(not(target_arch = "wasm32"))]
pub use fs::*;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod indexeddb;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use indexeddb::*;
