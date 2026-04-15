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

//! Repository layer for Dialog-DB.
//!
//! This crate provides the capability-based repository system built on top
//! of the operator layer (`dialog-operator`). It re-exports operator types
//! and adds the repository abstraction with branches, remotes, and archives.

// Re-export everything from dialog-operator for backwards compatibility.
pub use dialog_operator::{
    Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, Artifacts, Attribute,
    AttributeKey, Authority, Cause, Datum, DialogArtifactsError, Entity, EntityKey, FromKey,
    Instruction, Key, KeyView, KeyViewConstruct, KeyViewMut, Network, Operator, State, Value,
    ValueKey,
};

/// Authority — opened profile with signers and authority chain.
pub use dialog_operator::authority;

/// Profile — named identity with signing credential.
pub use dialog_operator::profile;

/// Operator — operating environment built from a profile.
pub use dialog_operator::operator;

/// Network dispatch for fork invocations.
pub use dialog_operator::network;

/// Capability-based repository system.
mod repository;
pub use repository::branch::BranchReference;
pub use repository::memory::MemoryExt;
pub use repository::{
    Branch, BranchName, CreateRemote, CreateRepository, LoadBranch, LoadRemote, LoadRepository,
    OpenBranch, OpenRepository, RemoteAddress, RemoteName, RemoteReference, RemoteRepository,
    Repository, RepositoryExt, SiteAddress, UpstreamState,
};

/// Test helpers for setting up profiles, operators, repositories, and test data.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
