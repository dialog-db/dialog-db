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

//! This package embodies the data model of dialog: [`Artifact`]s (facts of
//! the form "the attribute of an entity is a value") and the indexes that
//! store them, which are search trees keyed so that entity-, attribute- and
//! value-ordered scans are all range reads (see [`tree`]).
//!
//! Applications store artifacts through a branch of a repository, which adds
//! version control on top of the index writes here. Working with an index
//! directly:
//!
//! ```rust
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::str::FromStr;
//! use dialog_artifacts::tree::{ArtifactTree, ArtifactTreeExt, spill_cache};
//! use dialog_artifacts::{
//!     ArchiveDelta, Artifact, ArtifactSelector, Attribute, Entity, Instruction, Value,
//! };
//! use dialog_search_tree::MemoryBlocks;
//! use futures_util::{StreamExt, stream};
//!
//! // Any environment that loads blocks and blobs will do; this one keeps
//! // them in memory.
//! let blocks = MemoryBlocks::new();
//! let mut index = ArtifactTree::empty();
//! let mut delta = ArchiveDelta::zero();
//!
//! // Assert an artifact: the new nodes (and any spilled value) are staged
//! // in the delta, then written out.
//! let artifact = Artifact {
//!     the: Attribute::from_str("profile/name")?,
//!     of: Entity::new()?,
//!     is: Value::String("Foo Bar".into()),
//!     cause: None,
//! };
//! index
//!     .apply(&blocks, &mut delta, stream::iter(vec![Instruction::Assert(artifact)]))
//!     .await?;
//! delta.flush_into(&blocks);
//!
//! // Query the index
//! let selector = ArtifactSelector::new().the(Attribute::from_str("profile/name")?);
//! let results = index
//!     .scan(blocks.clone(), spill_cache(), selector)
//!     .filter_map(|view| async move { view.ok() })
//!     .collect::<Vec<_>>()
//!     .await;
//! # Ok(())
//! # }
//! ```

mod archive;
pub use archive::*;

mod artifacts;
pub use artifacts::*;

pub mod history;

mod reference;
pub use reference::*;

mod error;
pub use error::*;

/// Format-agnostic export trait for artifacts.
pub mod exporter;
pub use exporter::Exporter;

/// Format-agnostic import trait for artifacts.
pub mod importer;
pub use importer::Importer;

mod state;
pub use state::*;

mod blob_index;
pub use blob_index::*;

mod collection;
pub use collection::*;

mod spill;
pub use spill::*;

mod constants;
pub use constants::*;

mod key;
pub use key::*;

/// Shared tree-ops on the artifact prolly tree.
mod buffered;
pub use buffered::*;

pub mod inspect;
pub mod merge;
pub mod position;
pub mod tree;

pub use dialog_capability::identity::{
    ENTITY_LENGTH, Entity, IdentityError, Revision, SealedTree, TreeReference, Uri,
};

/// Test helpers for generating deterministic test data.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
