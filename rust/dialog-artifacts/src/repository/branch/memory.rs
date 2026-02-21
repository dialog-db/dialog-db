use dialog_capability::Subject;
use dialog_effects::memory as fx;

use super::state::BranchId;
use crate::repository::cell::Cell;

/// Pre-attenuated memory capability (`Subject → Memory → Space("local")`),
/// scoped to a specific branch.
///
/// The cell name is the branch id, so this targets the branch's state cell
/// at `local/{branch_id}`.
#[derive(Debug, Clone)]
pub struct Memory {
    space: dialog_capability::Capability<fx::Space>,
    branch_id: BranchId,
}

impl Memory {
    /// Create a new memory capability scoped to the given branch.
    pub fn new(subject: Subject, branch_id: BranchId) -> Self {
        let space = subject
            .attenuate(fx::Memory)
            .attenuate(fx::Space::new("local"));
        Self { space, branch_id }
    }

    /// A [`Cell`] for this branch's state.
    pub fn cell<T>(&self) -> Cell<T> {
        let capability = self
            .space
            .clone()
            .attenuate(fx::Cell::new(self.branch_id.to_string()));
        Cell::from_capability(capability)
    }
}
