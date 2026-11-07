//! Reading a branch's tree through archive capabilities.
//!
//! A tree reads its nodes by performing the node layer's requests
//! ([`dialog_search_tree::nodes`]) on the environment with the branch's
//! catalog, and its spilled values by performing
//! [`blob::Load`](dialog_effects::blob::Load); a reader of blocks performs
//! [`Load`](dialog_effects::archive::Load). What is here is how an
//! environment answers those:
//!
//! - [`hydrate`] -- a local miss hydrates from where the subject's blocks
//!   live, and a node read is answered from the cache the environment
//!   holds, its store, or a hydrated block
//! - [`routes`] -- where that is, as the subject's open branches register

/// A local read miss hydrates from the routes registered for its subject.
pub mod hydrate;
pub use hydrate::*;

/// Where a subject's blocks hydrate from, as its open branches register.
pub mod routes;
pub use routes::{Routes, Routing};

mod persist;
pub(crate) use persist::persist;

use dialog_artifacts::tree::{ArtifactNodes, ArtifactWrites};
use dialog_capability::Provider;
use dialog_common::{ConditionalSync, Holds};
use dialog_effects::archive::{Get, Load};
use dialog_effects::blob::Load as BlobLoad;

/// What a tree read or commit needs of the environment it goes through, as
/// one trait object: the node layer's reads and write, the loads of both
/// lanes, the local read of a block, and the handles it holds.
///
/// The tree code a read or a commit runs is generic over the environment
/// it reads through. Read through each concrete environment, that code is
/// copied once per environment type, in every crate that names one: a
/// peer's storage and mode, and each environment an embedder builds
/// around it. Read through this object (`&dyn TreeEnv`, which performs
/// every command on the environment it borrows), the copies collapse
/// into one.
///
/// Nothing here asks `'static` of the environment, and must not. The
/// object's lifetime is a region, which rustc erases when it checks that
/// a future holding the object is `Send` for every lifetime it borrows;
/// an outlives bound on it then cannot be shown, and every provider that
/// commits stops being `Send` (rust-lang/rust#100013).
pub(crate) trait TreeEnv:
    ArtifactNodes
    + ArtifactWrites
    + Provider<Get>
    + Provider<Load>
    + Provider<BlobLoad>
    + Holds
    + ConditionalSync
{
}

impl<T> TreeEnv for T where
    T: ArtifactNodes
        + ArtifactWrites
        + Provider<Get>
        + Provider<Load>
        + Provider<BlobLoad>
        + Holds
        + ConditionalSync
        + ?Sized
{
}

/// `env` as the [`TreeEnv`] object a tree reads through.
pub(crate) fn erased<Env: TreeEnv>(env: &Env) -> &dyn TreeEnv {
    env
}
