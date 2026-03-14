use dialog_capability::{Capability, Subject};
use dialog_effects::memory as fx;

use super::branch::BranchName;
use super::cell::Cell;
use super::credentials::Credentials;

/// Pre-attenuated memory capability (`Subject → Memory`).
///
/// Wraps `Capability<fx::Memory>` so callers can further attenuate with
/// structured helpers:
///
/// ```text
/// trace/{branch}/local/revision        Cell<Revision>
/// trace/{branch}/local/upstream        Cell<Option<UpstreamState>>
/// trace/{branch}/remote/{site}/revision Cell<Revision>
/// credential/{audience}/{site}          Cell<RemoteAddress>
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

    /// Create a session for the given issuer — `credential/{issuer.did()}/...`
    pub fn credentials(&self, issuer: Credentials) -> Authorization {
        let space = self
            .0
            .clone()
            .attenuate(fx::Space::new(format!("credential/{}", issuer.did())));
        Authorization { issuer, space }
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

/// Issuer session — `Credentials` + `Capability<fx::Space>` scoped to
/// `credential/{issuer.did()}`.
///
/// Carries the issuer credentials and provides typed cell accessors
/// for credential storage.
#[derive(Debug, Clone)]
pub struct Authorization {
    issuer: Credentials,
    space: Capability<fx::Space>,
}

impl Authorization {
    /// The issuer credentials.
    pub fn issuer(&self) -> &Credentials {
        &self.issuer
    }

    /// The issuer's DID.
    pub fn did(&self) -> dialog_capability::Did {
        self.issuer.did()
    }

    /// Create a typed cell within this session's credential space.
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
