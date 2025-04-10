#![warn(missing_docs)]

//! This package embodies a data storage primitive called [`Facts`]. [`Facts`]
//! is a triple store backed by indexes that are represented as prolly trees.
//!
//! To make use of [`Facts`] via the Rust API:
//!
//! ```ignore
//! use std::str::FromStr;
//! use x_storage::MemoryStorageBackend;
//! use x_facts::{Entity, Attribute, Value, Facts, Fact};
//!
//! // Substitute with your storage backend of choice:
//! let storage_backend = MemoryStorageBackend::default();
//! let mut facts = Facts::new(storage_backend);
//!
//! facts.commit([
//!     Fact {
//!         the: Attribute::from_str("profile/name"),
//!         of: Entity::new(),
//!         is: Value::String("Foo Bar".into())
//!     }
//! ]).await?;
//!
//! let fact_stream = facts.select(FactSelector::default()
//!     .the(Attribute::from_str("profile/name")));
//!
//! let facts = fact_stream.filter_map(|fact| fact.ok())
//!     .collect::<Vec<Fact>>().await;
//! ```

mod data;
pub use data::*;

mod fact;
pub use fact::*;

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
