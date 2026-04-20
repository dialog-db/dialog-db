use dialog_artifacts::{Datum, Key, State};
use dialog_capability::{Capability, Did, Subject};
use dialog_effects::archive::Archive;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::Blake3Hash;

mod claims;
mod commit;
mod fetch;
mod load;
mod open;
mod reference;
mod reset;
mod select;
mod transaction;
mod upstream;

pub use claims::*;
pub use commit::*;
pub use fetch::*;
pub use load::*;
pub use open::*;
pub use reference::*;
pub use reset::*;
pub use select::*;
pub use transaction::*;
pub use upstream::*;

use super::memory::Cell;
use super::revision::Revision;

/// Type alias for the prolly tree index.
pub type Index = Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash>;

/// A branch represents a named line of development within a repository.
///
/// Holds a [`BranchReference`] (scoped to `branch/{name}`) plus cells
/// for the branch's latest revision and optional upstream tracking.
#[derive(Debug, Clone)]
pub struct Branch {
    reference: BranchReference,
    revision: Cell<Revision>,
    upstream: Cell<UpstreamState>,
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
    pub fn upstream(&self) -> Option<UpstreamState> {
        self.upstream.content()
    }

    /// Returns the subject DID.
    pub fn subject(&self) -> &Did {
        self.reference.subject()
    }

    /// Archive capability for this branch's subject.
    pub fn archive(&self) -> Capability<Archive> {
        Subject::from(self.subject().clone()).archive()
    }
}
