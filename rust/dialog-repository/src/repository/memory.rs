//! Memory capability wrappers for structured cell access.
//!
//! ```text
//! branch/{name}/revision                  Cell<Revision>
//! branch/{name}/upstream                  Cell<Option<UpstreamState>>
//! remote/{name}/address                   Cell<RemoteAddress>
//! remote/{name}/branch/{branch}/revision  Cell<Revision>
//! ```

mod branch;
mod remote;

pub use branch::BranchMemory;
pub use remote::RemoteMemory;

use dialog_capability::{Capability, Subject};
use dialog_effects::memory as fx;

use super::branch::BranchName;

/// Pre-attenuated memory capability (`Subject -> Memory`).
///
/// Provides structured access to branch and remote memory namespaces.
#[derive(Debug, Clone)]
pub struct Memory(Capability<fx::Memory>);

impl Memory {
    /// Create a new memory capability for the given subject.
    pub fn new(subject: Subject) -> Self {
        Self(subject.attenuate(fx::Memory))
    }

    /// Access branch memory scoped to `branch/{name}`.
    pub fn branch(&self, branch: impl Into<BranchName>) -> BranchMemory {
        let name: BranchName = branch.into();
        let space = self
            .0
            .clone()
            .attenuate(fx::Space::new(format!("branch/{}", name)));
        BranchMemory::new(name, space)
    }

    /// Attenuate to a specific space within this memory.
    pub(crate) fn space(&self, name: &str) -> Space {
        Space(self.0.clone().attenuate(fx::Space::new(name)))
    }
}

/// A memory capability attenuated to a specific space.
///
/// Used internally to construct [`RemoteMemory`].
#[derive(Debug, Clone)]
pub(crate) struct Space(pub(crate) Capability<fx::Space>);
