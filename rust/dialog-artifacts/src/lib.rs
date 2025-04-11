#![warn(missing_docs)]

//! This package embodies a data storage primitive called [`Artifacts`]. [`Artifacts`]
//! is a triple store backed by indexes that are represented as prolly trees.
//!
//! To make use of [`Artifacts`] via the Rust API:
//!
//! ```ignore
//! use std::str::FromStr;
//! use dialog_storage::MemoryStorageBackend;
//! use dialog_facts::{Entity, Attribute, Value, Artifacts, Artifact};
//!
//! // Substitute with your storage backend of choice:
//! let storage_backend = MemoryStorageBackend::default();
//! let mut artifacts = Artifacts::new(storage_backend);
//!
//! artifacts.commit([
//!     Artifact {
//!         the: Attribute::from_str("profile/name"),
//!         of: Entity::new(),
//!         is: Value::String("Foo Bar".into())
//!     }
//! ]).await?;
//!
//! let artifact_stream = facts.select(FactSelector::default()
//!     .the(Attribute::from_str("profile/name")));
//!
//! let artifacts = fact_stream.filter_map(|fact| fact.ok())
//!     .collect::earVec<Fact>>().await;
//! ```

mod data;
pub use data::*;

mod artifacts;
pub use artifacts::*;

mod reference;
pub use reference::*;

mod error;
pub use error::*;

mod state;
pub use state::*;

mod constants;
pub use constants::*;

mod key;
pub use key::*;

#[cfg(any(test, feature = "helpers"))]
mod helpers;
#[cfg(any(test, feature = "helpers"))]
pub use helpers::*;
