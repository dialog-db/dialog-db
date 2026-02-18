use dialog_capability::{Did, Subject};
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::Blake3Hash;
use std::fmt::Debug;

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
mod load;
mod novelty;
mod open;
mod pull;
mod reset;
mod select;

pub use advance::Advance;
pub use commit::Commit;
pub use load::Load;
pub use novelty::novelty;
pub use open::Open;
pub use pull::PullLocal;
pub use reset::Reset;
pub use select::Select;

use super::archive::ContentAddressedStore;
use super::cell::CellOr;
use super::credentials::Credentials;
use super::node_reference::NodeReference;
use super::revision::Revision;
pub use state::{BranchId, BranchState, UpstreamState};

/// Type alias for the prolly tree index backed by capability-based archive.
pub type Index<Env> =
    Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash, ContentAddressedStore<Env>>;

/// A branch represents a named line of development within a repository.
///
/// Wraps a `Cell<BranchState>` (transactional memory cell) plus issuer
/// credentials. The subject DID and branch id are derived from the cell's
/// capability chain and cached state respectively.
pub struct Branch {
    issuer: Credentials,
    cell: CellOr<BranchState>,
}

impl Debug for Branch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cell.read_with(|state| {
            f.debug_struct("Branch")
                .field("id", &state.id)
                .field("issuer", &self.issuer.did())
                .finish_non_exhaustive()
        })
    }
}

impl Branch {
    /// Returns the branch identifier.
    pub fn id(&self) -> BranchId {
        self.cell.read_with(|state| state.id.clone())
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

    /// Returns a description of this branch.
    pub fn description(&self) -> String {
        self.cell.read_with(|state| state.description.clone())
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
    pub fn open(id: impl Into<BranchId>, issuer: Credentials, subject: Did) -> Open {
        Open {
            id: id.into(),
            issuer,
            subject,
        }
    }

    /// Create a command to load an existing branch (error if not found).
    pub fn load(id: impl Into<BranchId>, issuer: Credentials, subject: Did) -> Load {
        Load {
            id: id.into(),
            issuer,
            subject,
        }
    }

    /// Create a command to commit instructions to this branch.
    pub fn commit<I>(self, instructions: I) -> Commit<I> {
        Commit {
            branch: self,
            instructions,
        }
    }

    /// Create a command to select artifacts from this branch.
    pub fn select(&self, selector: ArtifactSelector<Constrained>) -> Select {
        Select {
            subject: self.subject().clone(),
            state: self.state(),
            selector,
        }
    }

    /// Create a command to reset the branch to a given revision.
    pub fn reset(self, revision: Revision) -> Reset {
        Reset {
            branch: self,
            revision,
        }
    }

    /// Create a command to advance the branch to a new revision with an
    /// explicit base tree. Used after merge operations where `base` should
    /// be set to the upstream's tree (what we synced from) while `revision`
    /// is the merged result.
    pub fn advance(self, revision: Revision, base: NodeReference) -> Advance {
        Advance {
            branch: self,
            revision,
            base,
        }
    }

    /// Create a command to pull changes from a local upstream branch.
    ///
    /// This performs a three-way merge:
    /// 1. Loads the upstream tree (their changes)
    /// 2. Computes local changes since last pull
    /// 3. Integrates local changes into upstream tree
    /// 4. Creates a new revision
    pub fn pull(self, upstream_revision: Revision) -> PullLocal {
        PullLocal {
            branch: self,
            upstream_revision,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use dialog_capability::Did;

    use super::super::credentials::Credentials;

    pub fn test_subject() -> Did {
        "did:test:branch-cap".parse().unwrap()
    }

    pub async fn test_issuer() -> Credentials {
        Credentials::from_passphrase("test").await.unwrap()
    }
}
