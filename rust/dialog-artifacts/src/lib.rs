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

//! This package embodies a data storage primitive called [`Artifacts`]. [`Artifacts`]
//! is a triple store backed by indexes that are represented as prolly trees.
//!
//! To make use of [`Artifacts`] via the Rust API:
//!
//! ```rust
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::str::FromStr;
//! use dialog_storage::MemoryStorageBackend;
//! use dialog_artifacts::{Entity, Attribute, Value, Artifacts, Artifact, ArtifactSelector, Instruction, ArtifactStore, ArtifactStoreMut};
//! use futures_util::{StreamExt, stream};
//!
//! // Substitute with your storage backend of choice:
//! let storage_backend = MemoryStorageBackend::default();
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

mod uri;
pub use uri::*;

/// Concrete environment composition for the repository layer.
pub mod environment;
pub use environment::*;

/// Profile configuration for opening an environment.
pub mod profile;
pub use profile::{Operator, Profile};

/// Remote dispatch for fork invocations.
pub mod remote;
pub use remote::Remote;

/// Capability-based repository system.
mod repository;
pub use repository::{
    Branch, BranchName, NodeReference, Occurence, RemoteAddress, RemoteBranch, RemoteRepository,
    RemoteSite, Repository, Revision,
};

#[cfg(any(test, feature = "helpers"))]
mod helpers;
#[cfg(any(test, feature = "helpers"))]
pub use helpers::*;
