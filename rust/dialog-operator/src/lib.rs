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

//! Operator layer for Dialog-DB.
//!
//! This crate provides the capability-based operator system: authority
//! credentials, profiles, operator builders, storage dispatch, and remote
//! fork dispatch that together form the operational layer above the core
//! artifact store.

// Re-export core artifact types for convenience.
pub use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, Artifacts, Attribute,
    AttributeKey, Cause, Datum, DialogArtifactsError, Entity, EntityKey, FromKey, Instruction, Key,
    KeyView, KeyViewConstruct, KeyViewMut, State, Value, ValueKey,
};

/// Authority — opened profile with signers and authority chain.
pub mod authority;
pub use authority::Authority;

/// Profile — named identity with signing credential.
pub mod profile;

/// Operator — operating environment built from a profile.
pub mod operator;
pub use operator::Operator;

/// Remote dispatch for fork invocations.
pub mod remote;
pub use remote::Remote;

/// Test helpers for setting up profiles, operators, and test data.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
