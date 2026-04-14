use dialog_capability::{Did, Subject};
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::Blake3Hash;

use std::fmt::{Debug, Formatter, Result as FmtResult};

use dialog_artifacts::Datum;

use crate::repository::RemoteSelector;
use crate::{Key, State};

/// Branch state, identifiers, and upstream descriptors.
pub mod state;

mod commit;
mod edit;
mod fetch;
mod index;
#[cfg(all(test, feature = "integration-tests"))]
mod integration_tests;
mod load;
mod novelty;
mod open;
mod pull;
mod push;
mod reset;
mod select;
mod selector;
mod set_upstream;

pub use load::LoadBranch;
pub use open::OpenBranch;
pub use selector::*;

use super::archive::Archive;
use super::cell::Cell;
use super::memory::{Memory, Site, Trace};

pub use super::occurence::Occurence;
use super::revision::Revision;
pub use state::{BranchName, UpstreamState};

/// Type alias for the prolly tree index.
pub type Index = Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash>;

/// A branch represents a named line of development within a repository.
///
/// Holds a `Trace` (scoped to `trace/{branch}/local`) plus separate cells
/// for revision and upstream state.
pub struct Branch {
    subject: Did,
    memory: Memory,
    trace: Trace,
    revision: Cell<Option<Revision>>,
    upstream: Cell<Option<UpstreamState>>,
}

impl Debug for Branch {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Branch")
            .field("name", self.trace.name())
            .finish_non_exhaustive()
    }
}

impl Branch {
    /// Returns the branch name.
    pub fn name(&self) -> &BranchName {
        self.trace.name()
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

    /// Returns the trace capability for this branch.
    pub fn trace(&self) -> &Trace {
        &self.trace
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
    pub fn remote(&self, name: impl Into<super::remote::RemoteName>) -> RemoteSelector {
        let name = name.into();
        let space = self.memory.space(&format!("remote/{}", name.as_str()));
        RemoteSelector::new(Site::from(space), self.subject.clone())
    }
}
