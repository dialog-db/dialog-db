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
//! This crate provides the identity and operating environment layer:
//! authority (identity + signing), profiles, operators, storage dispatch,
//! and remote fork dispatch.

// Re-export core artifact types for convenience.
pub use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, Artifacts, Attribute,
    AttributeKey, Cause, Datum, DialogArtifactsError, Entity, EntityKey, FromKey, Instruction, Key,
    KeyView, KeyViewConstruct, KeyViewMut, State, Value, ValueKey,
};

/// Authority — profile and operator signers for identity and signing.
pub mod authority;
pub use authority::Authority;
