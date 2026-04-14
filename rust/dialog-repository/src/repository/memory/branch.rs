use dialog_capability::Capability;
use dialog_effects::memory as fx;

use crate::repository::branch::BranchName;
use crate::repository::cell::Cell;

/// Branch-scoped memory namespace (`branch/{name}`).
///
/// Carries the branch name and provides typed cell accessors
/// for revision and upstream state.
#[derive(Debug, Clone)]
pub struct BranchMemory {
    name: BranchName,
    space: Capability<fx::Space>,
}

impl BranchMemory {
    pub(super) fn new(name: BranchName, space: Capability<fx::Space>) -> Self {
        Self { name, space }
    }

    /// The branch name this memory is scoped to.
    pub fn name(&self) -> &BranchName {
        &self.name
    }

    /// Create a typed cell within this branch's space.
    pub fn cell<T>(&self, cell_name: impl Into<String>) -> Cell<T> {
        Cell::from_capability(self.cell_capability(cell_name))
    }

    /// Return the raw cell capability without wrapping in [`Cell<T>`].
    pub fn cell_capability(&self, cell_name: impl Into<String>) -> Capability<fx::Cell> {
        self.space.clone().attenuate(fx::Cell::new(cell_name))
    }
}
