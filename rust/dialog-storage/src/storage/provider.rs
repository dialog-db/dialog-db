//! Capability-based storage providers.
//!
//! This module provides implementations of the [`Provider`] trait from
//! `dialog-capability` for executing storage effects defined in `dialog-effects`.
//!
//! # Available Providers
//!
//! - [`IndexedDb`] - IndexedDB-based storage for WASM environments
//!
//! # Architecture
//!
//! Each provider manages resources keyed by subject DID. For IndexedDB, each
//! subject maps to a separate database. The provider lazily opens databases
//! on first access and caches them for subsequent operations.
//!
//! [`Provider`]: dialog_capability::Provider
//! [`IndexedDb`]: indexeddb::IndexedDb

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod indexeddb;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use indexeddb::IndexedDb;
