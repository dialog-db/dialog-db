//! Extension trait on [`Subject`] for repository memory navigation.

use dialog_capability::Subject;
use dialog_effects::memory::prelude::{MemoryExt as _, MemorySubjectExt};

use super::branch::BranchReference;
use super::branch::name::BranchName;
use super::remote::RemoteName;
use super::remote::RemoteReference;

mod cell;
pub use cell::*;

/// Extension trait on [`Subject`] for repository memory navigation.
///
/// Provides `.branch("name")` and `.remote("name")` methods that
/// create scoped references for repository operations.
pub trait MemoryExt {
    /// Access branch scoped to `branch/{name}`.
    fn branch(&self, branch: impl Into<BranchName>) -> BranchReference;

    /// Access remote scoped to `remote/{name}`.
    fn remote(&self, name: impl Into<RemoteName>) -> RemoteReference;
}

impl MemoryExt for Subject {
    fn branch(&self, branch: impl Into<BranchName>) -> BranchReference {
        let name: BranchName = branch.into();
        let space = self.clone().memory().space(format!("branch/{}", name));
        space.into()
    }

    fn remote(&self, name: impl Into<RemoteName>) -> RemoteReference {
        let name: RemoteName = name.into();
        let space = self
            .clone()
            .memory()
            .space(format!("remote/{}", name.as_str()));
        space.into()
    }
}
