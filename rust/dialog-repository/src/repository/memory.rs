//! Memory capabilities: cells, publish/resolve commands, and caching.

mod cell;
mod publish;
mod resolve;

pub use cell::*;
pub use publish::*;
pub use resolve::*;

use dialog_capability::Subject;
use dialog_effects::memory::prelude::{MemoryExt, MemorySubjectExt};

use super::branch::BranchReference;

/// Extension trait for repository memory navigation.
///
/// Extends [`MemorySubjectExt`] with repository-specific helpers for
/// addressing branches (and, in follow-ups, remotes) by name.
pub trait RepositoryMemoryExt: MemorySubjectExt {
    /// Access a branch scoped to `branch/{name}`.
    fn branch(&self, name: impl Into<String>) -> BranchReference;
}

impl RepositoryMemoryExt for Subject {
    fn branch(&self, name: impl Into<String>) -> BranchReference {
        let name = name.into();
        self.clone().memory().space(format!("branch/{name}")).into()
    }
}
