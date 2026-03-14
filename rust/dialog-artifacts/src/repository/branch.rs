use dialog_capability::{Did, Subject};
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::Blake3Hash;

use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::artifacts::selector::Constrained;
use crate::artifacts::{ArtifactSelector, Datum};
use crate::{Key, State};

/// Branch state, identifiers, and upstream descriptors.
pub mod state;

mod commit;
#[cfg(test)]
mod e2e_tests;
mod fetch;
mod load;
mod novelty;
mod open;
mod pull;
mod push;
mod reset;
mod select;
mod set_upstream;

pub use commit::Commit;
pub use fetch::Fetch;
pub use load::Load;
pub use open::Open;
pub use pull::{Pull, PullLocal};
pub use push::Push;
pub use reset::Reset;
pub use select::Select;
pub use set_upstream::SetUpstream;

use super::archive::Archive;
use super::cell::{Cell, CellOr};
use super::credentials::Credentials;
use super::memory::{Authorization, Memory, Trace};
pub use super::occurence::Occurence;
use super::revision::Revision;
pub use state::{BranchName, UpstreamState};

/// Type alias for the prolly tree index.
pub type Index = Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash>;

/// A branch represents a named line of development within a repository.
///
/// Holds a `Trace` (scoped to `trace/{branch}/local`) plus separate cells
/// for revision and upstream state.
pub struct Branch {
    subject: Did,
    memory: Memory,
    session: Authorization,
    trace: Trace,
    revision: CellOr<Revision>,
    upstream: Cell<Option<UpstreamState>>,
}

impl Debug for Branch {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Branch")
            .field("name", self.trace.name())
            .field("issuer", &self.session.did())
            .finish_non_exhaustive()
    }
}

impl Branch {
    /// Returns the branch name.
    pub fn name(&self) -> &BranchName {
        self.trace.name()
    }

    /// Returns the DID of the authority issuing changes on this branch.
    pub fn did(&self) -> Did {
        self.session.did()
    }

    /// Returns the current revision of this branch.
    pub fn revision(&self) -> Revision {
        self.revision.get()
    }

    /// Returns the upstream state.
    pub fn upstream(&self) -> Option<UpstreamState> {
        self.upstream.get().flatten()
    }

    /// Returns the issuer credentials.
    pub fn issuer(&self) -> &Credentials {
        self.session.issuer()
    }

    /// Returns the authorization (credentials + scoped credential space).
    pub fn authorization(&self) -> &Authorization {
        &self.session
    }

    /// Returns the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Returns the trace capability for this branch.
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// Logical time on this branch
    pub fn occurence(&self) -> Occurence {
        self.revision.read_with(|rev| rev.clone().into())
    }

    /// Pre-attenuated archive capability for this branch's subject.
    pub fn archive(&self) -> Archive {
        Archive::new(Subject::from(self.subject.clone()))
    }
}

impl Branch {
    /// Load a sibling branch by name (shares this branch's memory and credentials).
    pub fn load_branch(&self, name: impl Into<BranchName>) -> Load {
        let trace = self.memory.trace(name);
        Load::new(self.session.clone(), self.subject.clone(), self.memory.clone(), trace)
    }

    /// Load a remote site by name (shares this branch's memory).
    pub fn load_remote(&self, name: impl Into<super::remote::SiteName>) -> super::remote::site::Load {
        super::remote::site::Load::new(name, self.memory.space("site"))
    }

    /// Create a command to commit instructions to this branch.
    pub fn commit<I>(&self, instructions: I) -> Commit<'_, I> {
        Commit::new(self, instructions)
    }

    /// Create a command to select artifacts from this branch.
    pub fn select(&self, selector: ArtifactSelector<Constrained>) -> Select {
        Select::new(self.subject().clone(), self.revision(), selector)
    }

    /// Create a command to reset the branch to a given revision.
    pub fn reset(&self, revision: Revision) -> Reset<'_> {
        Reset::new(self, revision)
    }

    /// Create a command to pull changes from a local upstream revision.
    ///
    /// This performs a three-way merge using an explicitly provided
    /// upstream revision. For auto-dispatching based on the branch's
    /// configured upstream, use [`pull_upstream`](Branch::pull_upstream).
    pub fn pull(&self, upstream_revision: Revision) -> PullLocal<'_> {
        PullLocal::new(self, upstream_revision)
    }

    /// Create a command to pull from the configured upstream.
    ///
    /// Reads the branch's upstream and dispatches to local or remote
    /// pull logic automatically.
    pub fn pull_upstream(&self) -> Pull<'_> {
        Pull::new(self)
    }

    /// Create a command to fetch the upstream branch's current revision.
    ///
    /// Does NOT modify local state — only reads from upstream.
    pub fn fetch(&self) -> Fetch<'_> {
        Fetch::new(self)
    }

    /// Create a command to push local changes to the upstream branch.
    ///
    /// Reads the upstream configuration from branch state and dispatches
    /// to local or remote push logic.
    pub fn push(&self) -> Push<'_> {
        Push::new(self)
    }

    /// Create a command to set the upstream for this branch.
    ///
    /// Accepts both `UpstreamState` and `RemoteBranch` directly via
    /// `impl Into<UpstreamState>`.
    pub fn set_upstream(&self, upstream: impl Into<UpstreamState>) -> SetUpstream<'_> {
        SetUpstream::new(self, upstream.into())
    }
}

#[cfg(test)]
mod tests {
    use dialog_capability::Did;

    use super::super::credentials::Credentials;

    pub fn test_subject() -> Did {
        "did:test:branch-cap".parse().unwrap()
    }

    pub async fn test_issuer() -> Credentials {
        Credentials::from_passphrase("test").await.unwrap()
    }
}
