use super::memory::Cell;
use crate::Revision;
use dialog_artifacts::{Datum, Key, State};
use dialog_capability::{Capability, Did, Subject};
use dialog_effects::archive::Archive;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_query::query::Application;
use dialog_storage::Blake3Hash;

mod claims;
pub use claims::*;

mod commit;
pub use commit::*;

mod fetch;
pub use fetch::*;

mod load;
pub use load::*;

mod open;
pub use open::*;

mod pull;
pub use pull::*;

mod push;
pub use push::*;

mod reference;
pub use reference::*;

mod reset;
pub use reset::*;

mod select;
pub use select::*;

mod session;
pub use session::*;

mod set_upstream;
pub use set_upstream::*;

mod transaction;
pub use transaction::*;

mod upstream;
pub use upstream::*;

#[cfg(all(test, feature = "integration-tests"))]
mod integration_tests;

/// Type alias for the search tree index.
pub type Index = Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash>;

/// A branch represents a named line of development within a repository.
///
/// Holds a [`BranchReference`] (scoped to `branch/{name}`) plus cells
/// for the branch's latest revision and optional upstream tracking.
#[derive(Debug, Clone)]
pub struct Branch {
    reference: BranchReference,
    revision: Cell<Revision>,
    upstream: Cell<Upstream>,
}

impl Branch {
    /// Returns the branch name.
    pub fn name(&self) -> &str {
        self.reference.name()
    }

    /// Returns the current revision of this branch, or `None` if the branch
    /// has no commits yet (equivalent to an orphan branch in git).
    pub fn revision(&self) -> Option<Revision> {
        self.revision.content()
    }

    /// Returns the upstream state, or `None` if no upstream is configured.
    pub fn upstream(&self) -> Option<Upstream> {
        self.upstream.content()
    }

    /// Returns the DID of the host repository.
    pub fn of(&self) -> &Did {
        self.reference.of()
    }

    /// The subject (repository) this branch lives in.
    pub fn subject(&self) -> Subject {
        self.reference.subject()
    }

    /// Archive capability for this branch's subject.
    pub fn archive(&self) -> Capability<Archive> {
        self.subject().archive()
    }

    /// Query with an application. Shortcut for `branch.query().select(query)`.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery::new(self, query)
    }
}
