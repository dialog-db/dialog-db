use dialog_capability::{Did, Subject};
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_query::query::Application;
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
mod session;
mod set_upstream;
mod transaction;

pub use load::LoadBranch;
pub use open::OpenBranch;
pub use reference::BranchReference;

use super::archive::Archive;
use super::cell::Cell;

pub use super::occurence::Occurence;
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
    memory: BranchReference,
    revision: Cell<Option<Revision>>,
    upstream: Cell<Option<UpstreamState>>,
}

impl Debug for Branch {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Branch")
            .field("name", &self.memory.name())
            .finish_non_exhaustive()
    }
}

impl Branch {
    /// Returns the branch name.
    pub fn name(&self) -> BranchName {
        self.memory.name()
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
        self.memory.subject()
    }

    /// Logical time on this branch, or `None` if the branch has no commits.
    pub fn occurence(&self) -> Option<Occurence> {
        self.revision().map(Into::into)
    }

    /// Archive capability for this branch's subject.
    pub fn archive(&self) -> Archive {
        Archive::new(Subject::from(self.subject().clone()))
    }

    /// Query with an application. Shortcut for `branch.query().select(query)`.
    pub fn select<Q: Application>(&self, query: Q) -> session::SelectQuery<'_, Q> {
        session::SelectQuery::new(self, query)
    }
}
