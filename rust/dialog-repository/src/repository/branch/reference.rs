use dialog_capability::{Did, Subject};

use crate::{Branch, Cell, LoadBranch, OpenBranch, Revision, Tracking};
use dialog_effects::memory::prelude::SpaceScope;

/// A reference to a named branch within a repository's memory.
///
/// Wraps `SpaceScope` scoped to `branch/{name}`.
/// Use `.open()` or `.load()` to create a command, then `.perform(&env)`.
#[derive(Debug, Clone)]
pub struct BranchReference(SpaceScope);

/// The branch an opened [`Branch`] is: to hand on, so whoever it is
/// handed to opens the same branch.
impl From<&Branch> for BranchReference {
    fn from(branch: &Branch) -> Self {
        branch.reference.clone()
    }
}

impl From<SpaceScope> for BranchReference {
    fn from(space: SpaceScope) -> Self {
        Self(space)
    }
}

impl BranchReference {
    /// The DID of the repository this branch belongs to.
    pub fn of(&self) -> &Did {
        self.0.subject()
    }

    /// The subject (repository) this branch belongs to.
    pub fn subject(&self) -> Subject {
        Subject::from(self.of().clone())
    }

    /// The branch name, extracted from the space path.
    pub fn name(&self) -> &str {
        self.0.space_name().strip_prefix("branch/").unwrap_or("")
    }

    /// Open the branch, creating it if it doesn't exist.
    pub fn open(self) -> OpenBranch {
        self.into()
    }

    /// Load the branch, returning an error if it doesn't exist.
    pub fn load(self) -> LoadBranch {
        self.into()
    }

    /// The cell holding this branch's latest [`Revision`].
    pub fn revision(&self) -> Cell<Revision> {
        self.cell("revision")
    }

    /// The cell recording this branch's upstreams as last resolved, and
    /// how far it has synced with each.
    pub fn tracking(&self) -> Cell<Tracking> {
        self.cell("tracking")
    }

    /// The cell releases before peers kept this branch's upstreams in.
    /// Read only by the upgrade that carries them into facts.
    pub(crate) fn legacy_upstream(&self) -> Cell<super::upstream::legacy::Upstreams> {
        self.cell("upstream")
    }

    /// The cell holding this branch's induction watermark: the last
    /// [`Revision`] through which inductive rules have evaluated.
    /// Replica-local (it lives in the local store and never
    /// replicates): each replica catches its rules up over
    /// `(watermark, head]` as its own head advances.
    pub fn induction(&self) -> Cell<Revision> {
        self.cell("induction")
    }

    /// Create a typed cell within this branch's space.
    pub fn cell<T>(&self, cell_name: impl Into<String>) -> Cell<T> {
        self.0.clone().cell(cell_name).into()
    }
}
