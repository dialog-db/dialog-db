use dialog_capability::{Capability, Did, Policy};
use dialog_effects::memory::prelude::SpaceExt;
use dialog_effects::memory::{Cell as CellCapability, Space};

use crate::repository::branch::{LoadBranch, OpenBranch, UpstreamState};
use crate::repository::memory::Cell;
use crate::repository::revision::Revision;

/// A reference to a named branch within a repository's memory.
///
/// Wraps `Capability<Space>` scoped to `branch/{name}`.
/// Use `.open()` or `.load()` to create a command, then `.perform(&env)`.
#[derive(Debug, Clone)]
pub struct BranchReference(Capability<Space>);

impl From<Capability<Space>> for BranchReference {
    fn from(space: Capability<Space>) -> Self {
        Self(space)
    }
}

impl BranchReference {
    /// The subject DID this branch belongs to.
    pub fn subject(&self) -> &Did {
        self.0.subject()
    }

    /// The branch name, extracted from the space path.
    pub fn name(&self) -> &str {
        Space::of(&self.0)
            .space
            .strip_prefix("branch/")
            .unwrap_or("")
    }

    /// Open the branch, creating it if it doesn't exist.
    pub fn open(&self) -> OpenBranch {
        OpenBranch::new(self.clone())
    }

    /// Load the branch, returning an error if it doesn't exist.
    pub fn load(&self) -> LoadBranch {
        LoadBranch::new(self.clone())
    }

    /// The cell holding this branch's latest [`Revision`].
    pub fn revision(&self) -> Cell<Revision> {
        self.cell("revision")
    }

    /// The cell holding this branch's [`UpstreamState`], if any.
    pub fn upstream(&self) -> Cell<UpstreamState> {
        self.cell("upstream")
    }

    /// Create a typed cell within this branch's space.
    pub fn cell<T>(&self, cell_name: impl Into<String>) -> Cell<T> {
        self.cell_capability(cell_name).into()
    }

    /// Return the raw cell capability without wrapping in [`Cell<T>`].
    pub fn cell_capability(&self, cell_name: impl Into<String>) -> Capability<CellCapability> {
        self.0.clone().cell(cell_name)
    }
}
