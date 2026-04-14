use dialog_capability::{Capability, Policy, Subject};
use dialog_effects::memory as fx;

use crate::RemoteAddress;

use super::branch::BranchName;
use super::cell::Cell;

/// Pre-attenuated memory capability (`Subject → Memory`).
///
/// Wraps `Capability<fx::Memory>` so callers can further attenuate with
/// structured helpers:
///
/// ```text
/// branch/{name}/revision                  Cell<Revision>
/// branch/{name}/upstream                  Cell<Option<UpstreamState>>
/// remote/{name}/address                   Cell<RemoteAddress>
/// remote/{name}/branch/{branch}/revision  Cell<Revision>
/// ```
#[derive(Debug, Clone)]
pub struct Memory(Capability<fx::Memory>);

impl Memory {
    /// Create a new memory capability for the given subject.
    pub fn new(subject: Subject) -> Self {
        Self(subject.attenuate(fx::Memory))
    }

    /// Attenuate to a specific space within this memory.
    pub fn space(&self, name: &str) -> Space {
        Space(self.0.clone().attenuate(fx::Space::new(name)))
    }

    /// Access branch trace state — `trace/{branch}/local/...`
    pub fn trace(&self, branch: impl Into<BranchName>) -> Trace {
        let name: BranchName = branch.into();
        let space = self
            .0
            .clone()
            .attenuate(fx::Space::new(format!("trace/{}/local", name)));
        Trace { name, space }
    }
}

/// Branch trace — a `Capability<fx::Space>` scoped to a branch's local
/// namespace (`trace/{branch}/local`).
///
/// Carries the branch name and provides typed cell accessors.
#[derive(Debug, Clone)]
pub struct Trace {
    name: BranchName,
    space: Capability<fx::Space>,
}

impl Trace {
    /// The branch name this trace is scoped to.
    pub fn name(&self) -> &BranchName {
        &self.name
    }

    /// Create a typed cell within this trace space.
    pub fn cell<T>(&self, cell_name: impl Into<String>) -> Cell<T> {
        Cell::from_capability(self.cell_capability(cell_name))
    }

    /// Return the raw cell capability without wrapping in [`Cell<T>`].
    pub fn cell_capability(&self, cell_name: impl Into<String>) -> Capability<fx::Cell> {
        self.space.clone().attenuate(fx::Cell::new(cell_name))
    }
}

/// A memory capability attenuated to a specific space.
#[derive(Debug, Clone)]
pub struct Space(Capability<fx::Space>);

impl Space {
    /// Create a [`Cell`] within this space.
    pub fn cell<T>(&self, name: impl Into<String>) -> Cell<T> {
        Cell::from_capability(self.cell_capability(name))
    }

    /// Return the raw cell capability without wrapping in [`Cell<T>`].
    pub fn cell_capability(&self, name: impl Into<String>) -> Capability<fx::Cell> {
        self.0.clone().attenuate(fx::Cell::new(name))
    }
}

#[derive(Debug, Clone)]
pub struct Site(Capability<fx::Space>);

impl From<Space> for Site {
    fn from(space: Space) -> Self {
        Self(space.0)
    }
}

impl Site {
    /// Return name for this site, strips out remote/ prefix.
    pub fn name(&self) -> &str {
        fx::Space::of(&self.0)
            .space
            .strip_prefix("remote/")
            .unwrap_or("")
    }

    /// The underlying space capability.
    pub fn capability(&self) -> Capability<fx::Space> {
        self.0.clone()
    }

    /// Cell for the site address.
    pub fn address(&self) -> Cell<RemoteAddress> {
        Cell::from_capability(self.cell_capability("address"))
    }

    /// Create a [`Cell`] within this site space.
    pub fn cell<T>(&self, name: impl Into<String>) -> Cell<T> {
        Cell::from_capability(self.cell_capability(name))
    }

    /// Return the raw cell capability without wrapping in [`Cell<T>`].
    pub fn cell_capability(&self, name: impl Into<String>) -> Capability<fx::Cell> {
        self.0.clone().attenuate(fx::Cell::new(name))
    }
}
