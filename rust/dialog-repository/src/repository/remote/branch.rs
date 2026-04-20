mod fetch;
mod load;
mod open;
mod publish;
mod reference;

pub use fetch::*;
pub use load::*;
pub use open::*;
pub use publish::*;
pub use reference::*;

use crate::repository::branch::BranchReference;
use crate::repository::memory::Cell;
use crate::repository::remote::{RemoteAddress, RemoteRepository};
use crate::repository::revision::Revision;

/// A loaded remote branch.
///
/// Produced by opening or loading a [`LoadedRemoteBranchReference`].
/// Carries the already-loaded [`RemoteRepository`] it lives at, the
/// branch capability at the remote's subject, and the two cells used
/// at runtime: the local snapshot cache and the in-memory handle to
/// the remote's revision cell used by fork-based resolve/publish.
#[derive(Debug, Clone)]
pub struct RemoteBranch {
    /// The loaded remote repository this branch lives at (site +
    /// retained address).
    repository: RemoteRepository,
    /// Names the branch at the remote repository's subject. Capability
    /// chain: `{remote_did}/memory/branch/{branch_name}`.
    branch: BranchReference,
    /// Local snapshot of the last-known remote edition. Lives under
    /// the enclosing repo's subject at
    /// `memory/remote/{name}/branch/{branch}/revision`. Primed on
    /// open/load; updated on fetch/publish.
    cache: Cell<RemoteEdition>,
    /// In-memory handle to the remote's own branch revision cell.
    /// Lives under the remote repo's subject at
    /// `memory/branch/{branch}/revision`. Reads and writes through
    /// this cell cross the network via `.fork(address.site())`.
    upstream: Cell<Revision>,
}

impl RemoteBranch {
    /// Construct from the loaded repository + branch and the two cells.
    pub(super) fn new(
        repository: RemoteRepository,
        branch: BranchReference,
        cache: Cell<RemoteEdition>,
        upstream: Cell<Revision>,
    ) -> Self {
        Self {
            repository,
            branch,
            cache,
            upstream,
        }
    }

    /// The loaded remote repository this branch lives at.
    pub fn repository(&self) -> &RemoteRepository {
        &self.repository
    }

    /// The branch capability at the remote repository's subject.
    pub fn branch(&self) -> &BranchReference {
        &self.branch
    }

    /// The branch name.
    pub fn name(&self) -> &str {
        self.branch.name()
    }

    /// The full remote address (site + subject).
    pub fn address(&self) -> RemoteAddress {
        self.repository.address()
    }

    /// The cached remote revision, if it was resolved.
    pub fn revision(&self) -> Option<Revision> {
        self.cache.content().map(|edition| edition.content)
    }

    /// The local snapshot cache cell (holds the last-known remote
    /// edition).
    pub fn cache(&self) -> &Cell<RemoteEdition> {
        &self.cache
    }

    /// The upstream revision cell — an in-memory handle to the remote
    /// branch's own revision cell, used for fork-based resolve/publish.
    pub fn upstream(&self) -> &Cell<Revision> {
        &self.upstream
    }

    /// Fetch the latest revision from the remote.
    pub fn fetch(&self) -> FetchRemoteBranch<'_> {
        FetchRemoteBranch::new(self)
    }

    /// Publish a revision to the remote.
    pub fn publish(&self, revision: Revision) -> PublishRemoteBranch<'_> {
        PublishRemoteBranch::new(self, revision)
    }
}

impl From<&RemoteBranch> for RemoteBranchReference {
    fn from(branch: &RemoteBranch) -> Self {
        RemoteBranchReference::new(branch.repository.site().clone(), branch.branch.clone())
    }
}

impl From<&LoadedRemoteBranchReference> for RemoteBranchReference {
    fn from(loaded: &LoadedRemoteBranchReference) -> Self {
        RemoteBranchReference::new(loaded.repository.site().clone(), loaded.branch.clone())
    }
}
