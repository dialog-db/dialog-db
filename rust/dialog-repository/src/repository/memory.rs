//! Memory capabilities: cells, publish/resolve commands, and caching.
use crate::{BranchReference, RemoteReference};
use dialog_capability::Subject;
use dialog_effects::memory::prelude::MemoryExt;

mod cell;
pub use cell::*;

mod publish;
pub use publish::*;

mod resolve;
pub use resolve::*;

/// The branch every other branch is recorded in.
///
/// A branch exists as a set of memory cells; the fact that it exists
/// lives here, so that branches can be listed without asking a backend
/// to enumerate anything.
///
/// The registry is not recorded in itself. It does not have to be: its
/// own fact is synthesized into every query's overlay, so a listing
/// sees it while nothing about it is ever stored. That dissolves the
/// recursion rather than special-casing it.
pub const REGISTRY: &str = "meta";

/// Extension trait for repository memory navigation.
///
/// Extends [`MemoryExt`] with repository-specific helpers for
/// addressing branches and remotes by name.
pub trait RepositoryMemoryExt: MemoryExt {
    /// Access a branch scoped to `branch/{name}`.
    fn branch(&self, name: impl Into<String>) -> BranchReference;

    /// Access a remote scoped to `remote/{name}`.
    fn remote(&self, name: impl Into<String>) -> RemoteReference;
}

impl RepositoryMemoryExt for Subject {
    fn branch(&self, name: impl Into<String>) -> BranchReference {
        let name = name.into();
        self.clone().memory().space(format!("branch/{name}")).into()
    }

    fn remote(&self, name: impl Into<String>) -> RemoteReference {
        let name = name.into();
        self.clone().memory().space(format!("remote/{name}")).into()
    }
}
