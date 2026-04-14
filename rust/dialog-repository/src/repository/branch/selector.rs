use crate::{BranchName, LoadBranch, OpenBranch, Repository};
use dialog_varsig::Principal;

/// A reference to a named branch within a repository.
///
/// Call `.open()` to open (create if missing) or `.load()` to load (fail if missing).
pub struct BranchReference<'a, C: Principal> {
    name: BranchName,
    repository: &'a Repository<C>,
}

impl<'a, C: Principal> BranchReference<'a, C> {
    /// Returns the name of this branch.
    pub fn name(&self) -> &BranchName {
        &self.name
    }

    /// Open the branch, creating it if it doesn't exist.
    pub fn open(self) -> OpenBranch {
        OpenBranch::new(
            self.repository.credential.did(),
            self.repository.memory.clone(),
            self.repository.memory.branch(self.name()),
        )
    }

    /// Load the branch, returning an error if it doesn't exist.
    pub fn load(self) -> LoadBranch {
        LoadBranch::new(
            self.repository.credential.did(),
            self.repository.memory.clone(),
            self.repository.memory.branch(self.name()),
        )
    }
}

impl<C: Principal> Repository<C> {
    /// Get a branch reference for the given name.
    ///
    /// Call `.open()` or `.load()` on the returned reference.
    pub fn branch(&self, name: impl Into<BranchName>) -> BranchReference<'_, C> {
        BranchReference {
            repository: self,
            name: name.into(),
        }
    }
}
