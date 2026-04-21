//! Memory capabilities: cells, publish/resolve commands, and caching.

mod cell;
pub use cell::*;

mod publish;
pub use publish::*;

mod resolve;
pub use resolve::*;

use dialog_capability::Subject;
use dialog_effects::memory::prelude::{MemoryExt, MemorySubjectExt};

use crate::{BranchReference, RemoteReference};

/// Extension trait for repository memory navigation.
///
/// Extends [`MemorySubjectExt`] with repository-specific helpers for
/// addressing branches and remotes by name.
pub trait RepositoryMemoryExt: MemorySubjectExt {
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
