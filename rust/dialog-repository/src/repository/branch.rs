use dialog_capability::{Did, Subject};
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::Blake3Hash;

use std::fmt::{Debug, Formatter, Result as FmtResult};

use dialog_artifacts::Datum;

use crate::repository::RemoteReference;
use crate::{Key, State};

/// Branch state, identifiers, and upstream descriptors.
pub mod state;

mod claims;
mod commit;
mod edit;
mod fetch;
#[cfg(all(test, feature = "integration-tests"))]
mod integration_tests;
mod load;
mod novelty;
mod open;
mod pull;
mod push;
mod reference;
mod reset;
mod select;
mod set_upstream;

pub use load::LoadBranch;
pub use open::OpenBranch;
pub use reference::*;

use super::archive::Archive;
use super::cell::Cell;
use super::memory::{BranchMemory, Memory, RemoteMemory};

pub use super::occurence::Occurence;
use super::revision::Revision;
pub use state::{BranchName, UpstreamState};

/// Type alias for the prolly tree index.
pub type Index = Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash>;

/// A branch represents a named line of development within a repository.
///
/// Holds a [`BranchMemory`] (scoped to `branch/{name}`) plus separate cells
/// for revision and upstream state.
pub struct Branch {
    subject: Did,
    memory: Memory,
    branch_memory: BranchMemory,
    revision: Cell<Option<Revision>>,
    upstream: Cell<Option<UpstreamState>>,
}

impl Debug for Branch {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Branch")
            .field("name", self.branch_memory.name())
            .finish_non_exhaustive()
    }
}

impl Branch {
    /// Returns the branch name.
    pub fn name(&self) -> &BranchName {
        self.branch_memory.name()
    }

    /// Returns the current revision of this branch, or `None` if the branch
    /// has no commits yet (equivalent to an orphan branch in git).
    pub fn revision(&self) -> Option<Revision> {
        self.revision.get().flatten()
    }

    /// Returns the upstream state.
    pub fn upstream(&self) -> Option<UpstreamState> {
        self.upstream.get().flatten()
    }

    /// Returns the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Returns the branch-scoped memory namespace.
    pub fn branch_memory(&self) -> &BranchMemory {
        &self.branch_memory
    }

    /// Logical time on this branch, or `None` if the branch has no commits.
    pub fn occurence(&self) -> Option<Occurence> {
        self.revision().map(Into::into)
    }

    /// Pre-attenuated archive capability for this branch's subject.
    pub fn archive(&self) -> Archive {
        Archive::new(Subject::from(self.subject.clone()))
    }

    /// Get a remote reference by name.
    pub fn remote(&self, name: impl Into<super::remote::RemoteName>) -> RemoteReference {
        let name = name.into();
        let space = self.memory.space(&format!("remote/{}", name.as_str()));
        RemoteReference::new(RemoteMemory::from(space), self.subject.clone())
    }
}
