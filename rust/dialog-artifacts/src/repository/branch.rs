use dialog_capability::{Did, Subject};
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::Blake3Hash;

use std::fmt::{Debug, Formatter, Result as FmtResult};

use crate::artifacts::selector::Constrained;
use crate::artifacts::{ArtifactSelector, Datum};
use crate::{Key, State};

/// Pre-attenuated archive capability for content-addressed storage.
pub mod archive;
/// Pre-attenuated memory capability for cell storage.
pub mod memory;
/// Branch state, identifiers, and upstream descriptors.
pub mod state;

mod advance;
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

pub use advance::Advance;
pub use commit::Commit;
pub use fetch::Fetch;
pub use load::Load;
pub use open::Open;
pub use pull::{Pull, PullLocal};
pub use push::Push;
pub use reset::Reset;
pub use select::Select;
pub use set_upstream::SetUpstream;

use super::cell::CellOr;
use super::credentials::Credentials;
use super::node_reference::NodeReference;
use super::revision::Revision;
pub use state::{BranchName, BranchState, UpstreamState};

/// Type alias for the prolly tree index.
pub type Index = Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash>;

/// A branch represents a named line of development within a repository.
///
/// Wraps a `Cell<BranchState>` (transactional memory cell) plus issuer
/// credentials. The subject DID and branch name are derived from the cell's
/// capability chain and cached state respectively.
pub struct Branch {
    name: BranchName,
    issuer: Credentials,
    cell: CellOr<BranchState>,
}

impl Debug for Branch {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("Branch")
            .field("name", &self.name)
            .field("issuer", &self.issuer.did())
            .finish_non_exhaustive()
    }
}

impl Branch {
    /// Returns the branch name.
    pub fn name(&self) -> BranchName {
        self.name.clone()
    }

    /// Returns the DID of the authority issuing changes on this branch.
    pub fn did(&self) -> Did {
        self.issuer.did()
    }

    /// Returns the current branch state (cloned).
    fn state(&self) -> BranchState {
        self.cell.get()
    }

    /// Returns the current revision of this branch.
    pub fn revision(&self) -> Revision {
        self.cell.read_with(|state| state.revision.clone())
    }

    /// Returns the base tree reference for this branch.
    pub fn base(&self) -> NodeReference {
        self.cell.read_with(|state| state.base.clone())
    }

    /// Returns the issuer.
    pub fn issuer(&self) -> &Credentials {
        &self.issuer
    }

    /// Returns the subject DID.
    pub fn subject(&self) -> &Did {
        self.cell.subject()
    }

    /// Logical time on this branch
    pub fn occurence(&self) -> super::occurence::Occurence {
        self.cell.read_with(|state| state.revision.clone().into())
    }

    /// Pre-attenuated archive capability for this branch's subject.
    pub fn archive(&self) -> archive::Archive {
        archive::Archive::new(Subject::from(self.subject().clone()))
    }
}

impl Branch {
    /// Create a command to open (load or create) a branch.
    pub fn open(name: impl Into<BranchName>, issuer: Credentials, subject: Did) -> Open {
        Open::new(name.into(), issuer, subject)
    }

    /// Create a command to load an existing branch (error if not found).
    pub fn load(name: impl Into<BranchName>, issuer: Credentials, subject: Did) -> Load {
        Load::new(name.into(), issuer, subject)
    }

    /// Create a command to commit instructions to this branch.
    pub fn commit<I>(self, instructions: I) -> Commit<I> {
        Commit::new(self, instructions)
    }

    /// Create a command to select artifacts from this branch.
    pub fn select(&self, selector: ArtifactSelector<Constrained>) -> Select {
        Select::new(self.subject().clone(), self.state(), selector)
    }

    /// Create a command to reset the branch to a given revision.
    pub fn reset(self, revision: Revision) -> Reset {
        Reset::new(self, revision)
    }

    /// Create a command to advance the branch to a new revision with an
    /// explicit base tree. Used after merge operations where `base` should
    /// be set to the upstream's tree (what we synced from) while `revision`
    /// is the merged result.
    pub fn advance(self, revision: Revision, base: NodeReference) -> Advance {
        Advance::new(self, revision, base)
    }

    /// Create a command to pull changes from a local upstream revision.
    ///
    /// This performs a three-way merge using an explicitly provided
    /// upstream revision. For auto-dispatching based on the branch's
    /// configured upstream, use [`pull_upstream`](Branch::pull_upstream).
    pub fn pull(self, upstream_revision: Revision) -> PullLocal {
        PullLocal::new(self, upstream_revision)
    }

    /// Create a command to pull from the configured upstream.
    ///
    /// Reads `branch.state().upstream` and dispatches to local or remote
    /// pull logic automatically.
    pub fn pull_upstream(self) -> Pull {
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
