//! Dialog effects - capability hierarchy types for storage operations.
//!
//! This crate defines the domain-specific capability hierarchies used by Dialog
//! for storage operations. It provides the structural types (attenuations, policies,
//! and effects) that form capability chains.
//!
//! # Capability Domains
//!
//! - [`storage`]: Key-value storage operations (`Storage`, `Store`, `Get`, `Set`, `Delete`, `List`)
//! - [`memory`]: CAS memory cells (`Memory`, `Space`, `Cell`, `Resolve`, `Publish`, `Retract`)
//! - [`archive`]: Content-addressed archive (`Archive`, `Catalog`, `Get`, `Put`)
//!
//! # Example
//!
//! ```
//! use dialog_effects::storage::{Storage, Store, Get};
//! use dialog_capability::{did, Subject};
//!
//! // Build a capability to get a value from the "index" store
//! let get_capability = Subject::from(did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
//!     .attenuate(Storage)              // Domain: storage operations
//!     .attenuate(Store::new("index"))  // Policy: only the "index" store
//!     .invoke(Get::new(b"my-key"));    // Effect: get this specific key
//! ```

#![warn(missing_docs)]

pub mod archive;
pub mod memory;
pub mod storage;

// Re-export capability primitives for convenience
pub use dialog_capability::{Attenuation, Capability, Effect, Policy, Subject};
