//! Dialog effects - capability hierarchy types for storage operations.
//!
//! This crate defines the domain-specific capability hierarchies used by Dialog
//! for storage operations. It provides the structural types (attenuations, policies,
//! and effects) that form capability chains.
//!
//! # Capability Domains
//!
//! - [`storage`]: Location-based storage operations (`Storage`, `Location`, `Mount`, `Load`, `Save`)
//! - [`memory`]: CAS memory cells (`Memory`, `Space`, `Cell`, `Resolve`, `Publish`, `Retract`)
//! - [`archive`]: Content-addressed archive (`Archive`, `Catalog`, `Get`, `Put`)
//!
//! # Example
//!
//! ```
//! use dialog_effects::archive::{Archive, Catalog, Get};
//! use dialog_capability::{did, Subject};
//! use dialog_common::Blake3Hash;
//!
//! // Build a capability to get content from the "index" catalog
//! let digest = Blake3Hash::hash(b"hello");
//! let get_capability = Subject::from(did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
//!     .attenuate(Archive)              // Domain: archive operations
//!     .attenuate(Catalog::new("index"))  // Policy: only the "index" catalog
//!     .invoke(Get::new(digest));         // Effect: get this specific digest
//! ```

#![warn(missing_docs)]
#![warn(clippy::absolute_paths)]
#![warn(clippy::default_trait_access)]
#![warn(clippy::fallible_impl_from)]
#![warn(clippy::panicking_unwrap)]
#![warn(clippy::unused_async)]
#![deny(clippy::partial_pub_fields)]
#![deny(clippy::unnecessary_self_imports)]
#![cfg_attr(not(test), warn(clippy::large_futures))]
#![cfg_attr(not(test), deny(clippy::panic))]

pub mod access;
pub mod archive;
pub mod authority;
pub mod credential;
pub mod memory;
pub mod space;
pub mod storage;

// Re-export capability primitives for convenience
pub use dialog_capability::{Attenuation, Capability, Effect, Policy, Subject};
