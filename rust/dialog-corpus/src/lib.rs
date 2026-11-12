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

//! This package embodies the data model of dialog: [`Fact`]s (facts of
//! the form "the attribute of an entity is a value") and the indexes that
//! store them, which are search trees keyed so that entity-, attribute- and
//! value-ordered scans are all range reads (see [`tree`]).
//!
//! Applications store facts through a branch of a repository, which adds
//! version control on top of the index writes here. Working with an index
//! directly:
//!
//! ```rust
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::str::FromStr;
//! use dialog_corpus::tree::{Corpus, CorpusExt, spill_cache};
//! use dialog_corpus::{
//!     ArchiveDelta, Fact, Selector, Entity, Instruction, Relation, Value,
//! };
//! use dialog_search_tree::helpers::{Blocks, test_subject};
//! use futures_util::{StreamExt, stream};
//!
//! // Any environment that answers the corpus's node reads and spilled-value
//! // loads will do; this one keeps everything in memory. The corpus lives
//! // in a catalog of a subject's archive.
//! let blocks = Blocks::new();
//! let mut corpus = Corpus::empty_in(test_subject());
//! let mut delta = ArchiveDelta::zero();
//!
//! // Assert a fact: the new nodes (and any spilled value) are staged
//! // in the delta, then written out.
//! let fact = Fact {
//!     the: Relation::from_str("profile/name")?,
//!     of: Entity::new()?,
//!     is: Value::String("Foo Bar".into()),
//!     cause: None,
//! };
//! corpus
//!     .apply(&blocks, &mut delta, stream::iter(vec![Instruction::Assert(fact, dialog_corpus::Pick::All)]))
//!     .await?;
//! // A commit writes the staged nodes and spilled values where they belong;
//! // this stand-in keeps them in the blocks.
//! delta.flush_into(&blocks);
//!
//! // Query the corpus
//! let selector = Selector::new().the(Relation::from_str("profile/name")?);
//! let results = corpus
//!     .scan(blocks.clone(), spill_cache(), selector)
//!     .filter_map(|view| async move { view.ok() })
//!     .collect::<Vec<_>>()
//!     .await;
//! # Ok(())
//! # }
//! ```

mod archive;
pub use archive::*;

/// What a read bound names, for this crate's modules to import at once
/// without colliding with what they import themselves.
mod reads {
    pub use crate::tree::CorpusNode;
    pub use dialog_capability::Provider;
    pub use dialog_common::{ConditionalSend, ConditionalSync};
    pub use dialog_effects::blob;
    pub use dialog_search_tree::nodes;
}

mod model;
pub use model::*;

pub mod history;

pub mod identity;

mod reference;
pub use reference::*;

mod error;
pub use error::*;

/// Format-agnostic export trait for facts.
pub mod exporter;
pub use exporter::Exporter;

/// Format-agnostic import trait for facts.
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

/// Shared tree-ops on the corpus.
mod buffered;
pub use buffered::*;

pub mod inspect;
pub mod merge;
pub mod position;
pub mod tree;

pub use dialog_capability::identity::{
    ENTITY_LENGTH, Entity, IdentityError, Revision, TreeReference, Uri,
};

/// Test helpers for generating deterministic test data.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
