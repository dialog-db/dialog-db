use dialog_capability::{Capability, Did, Policy};
use dialog_effects::memory as fx;
use dialog_effects::memory::prelude::SpaceExt;

use crate::repository::branch::name::BranchName;
use crate::repository::branch::{LoadBranch, OpenBranch};
use crate::repository::cell::Cell;

/// A reference to a named branch within a repository's memory.
///
/// Wraps `Capability<fx::Space>` scoped to `branch/{name}`.
/// Use `.open()` or `.load()` to create a command, then `.perform(&env)`.
#[derive(Debug, Clone)]
pub struct BranchReference(Capability<fx::Space>);

impl From<Capability<fx::Space>> for BranchReference {
    fn from(space: Capability<fx::Space>) -> Self {
        Self(space)
    }
}

impl BranchReference {
    /// The subject DID this branch belongs to.
    pub fn subject(&self) -> &Did {
        self.0.subject()
    }

    /// The branch name, extracted from the space path.
    pub fn name(&self) -> BranchName {
        fx::Space::of(&self.0)
            .space
            .strip_prefix("branch/")
            .unwrap_or("")
            .into()
    }

    /// Open the branch, creating it if it doesn't exist.
    pub fn open(&self) -> OpenBranch {
        OpenBranch::new(self.clone())
    }

    /// Load the branch, returning an error if it doesn't exist.
    pub fn load(&self) -> LoadBranch {
        LoadBranch::new(self.clone())
    }

    /// Create a typed cell within this branch's space.
    pub fn cell<T>(&self, cell_name: impl Into<String>) -> Cell<T> {
        Cell::from_capability(self.cell_capability(cell_name))
    }

    /// Return the raw cell capability without wrapping in [`Cell<T>`].
    pub fn cell_capability(&self, cell_name: impl Into<String>) -> Capability<fx::Cell> {
        self.0.clone().cell(cell_name)
    }
}
