use dialog_capability::{Capability, Did, Subject};
use dialog_effects::archive as archive_fx;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::Blake3Hash;

use std::fmt::{Debug, Formatter, Result as FmtResult};

use dialog_artifacts::Datum;

use crate::{Key, State};

/// Branch name newtype.
pub mod name;
/// Upstream tracking state.
pub mod upstream;

mod claims;
mod commit;
mod fetch;
#[cfg(all(test, feature = "integration-tests"))]
mod integration_tests;
mod load;
mod novelty;
mod open;
mod pull;
mod push;
pub mod reference;
mod reset;
mod select;
mod set_upstream;
mod transaction;

pub use load::LoadBranch;
pub use open::OpenBranch;
pub use reference::BranchReference;

use super::memory::Cell;

use super::revision::Revision;
pub use name::BranchName;
pub use upstream::UpstreamState;

/// Type alias for the prolly tree index.
pub type Index = Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash>;

/// A branch represents a named line of development within a repository.
///
/// Holds a [`BranchReference`] (scoped to `branch/{name}`) plus separate
/// cells for revision and upstream state.
pub struct Branch {
    reference: BranchReference,
    revision: Cell<Option<Revision>>,
    upstream: Cell<Option<UpstreamState>>,
}

impl Debug for Branch {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Branch")
            .field("name", &self.reference.name())
            .finish_non_exhaustive()
    }
}

impl Branch {
    /// Returns the branch name.
    pub fn name(&self) -> BranchName {
        self.reference.name()
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
        self.reference.subject()
    }

    /// Archive capability for this branch's subject.
    pub fn archive(&self) -> Capability<archive_fx::Archive> {
        Subject::from(self.subject().clone()).archive()
    }
}
