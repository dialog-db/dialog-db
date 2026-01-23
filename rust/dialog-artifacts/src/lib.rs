#![warn(missing_docs)]

//! This package embodies a data storage primitive called [`Artifacts`]. [`Artifacts`]
//! is a triple store backed by indexes that are represented as prolly trees.
//!
//! To make use of [`Artifacts`] via the Rust API:
//!
//! ```rust
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::str::FromStr;
//! use dialog_artifacts::{Entity, Attribute, Value, Artifacts, Artifact, ArtifactSelector, Instruction, ArtifactStore, ArtifactStoreMut, MemoryStorageBackend};
//! use futures_util::{StreamExt, stream};
//!
//! // Substitute with your storage backend of choice:
//! let storage_backend = MemoryStorageBackend::<[u8; 32], Vec<u8>>::default();
//! let mut artifacts = Artifacts::anonymous(storage_backend).await?;
//!
//! // Create an artifact
//! let artifact = Artifact {
//!     the: Attribute::from_str("profile/name")?,
//!     of: Entity::new()?,
//!     is: Value::String("Foo Bar".into()),
//!     cause: None
//! };
//!
//! // Create a stream of instructions and commit
//! let instructions = stream::iter(vec![Instruction::Assert(artifact)]);
//! artifacts.commit(instructions).await?;
//!
//! // Query the artifacts
//! let artifact_stream = artifacts.select(ArtifactSelector::new()
//!     .the(Attribute::from_str("profile/name")?));
//!
//! let results = artifact_stream.filter_map(|fact| async move { fact.ok() })
//!     .collect::<Vec<_>>().await;
//! # Ok(())
//! # }
//! ```

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod web;

/// Core artifacts types and functionality for operating on artifacts
pub mod artifacts;
pub use artifacts::*;

mod platform;
/// Replica abstraction for dialog
pub mod replica;
pub use platform::*;
mod reference;
pub use reference::*;

mod error;
pub use error::*;

mod state;
pub use state::*;

mod constants;

mod key;
pub use key::*;

mod uri;
pub use uri::*;

#[cfg(any(test, feature = "helpers"))]
mod helpers;
#[cfg(any(test, feature = "helpers"))]
pub use helpers::*;
