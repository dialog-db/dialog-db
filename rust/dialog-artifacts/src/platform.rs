use crate::replica::{BranchId, Revision};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RemoteError {
    /// Resolving a branch failed
    #[error("Failed to resolve branch {0}")]
    ResolveError(ResolveError),
}

#[derive(Error, Debug, Clone)]
pub enum ResolveError {
    /// Error produced
    #[error("Branch not found")]
    BranchNotFound { branch: BranchId },
    #[error("Failed to resolve branch revision for {branch} from storage due to: {cause}")]
    StorageError { branch: BranchId, cause: String },
}

#[derive(Error, Debug, Clone)]
pub enum FetchError {
    /// Error produced
    #[error("Branch not found")]
    BranchNotFound { branch: BranchId },
    #[error("Failed to read branch revision for {branch} due to: {cause}")]
    NetworkError { branch: BranchId, cause: String },
}

/// Represents a platform API for working with remotes and local cache
/// for those remotes.
trait JournalBackend {
    /// Resolves revision for the given branch for remote. It should resolve
    /// from the local cache as opposed to actual remote node. To fetch from
    /// the actual remote node, `fetch` should be called.
    async fn resolve(&self, branch: &BranchId) -> Result<Revision, RemoteError>;

    /// Fetches revision for the given branch from the actual remote node.
    async fn fetch(&self, branch: &BranchId) -> Result<Revision, RemoteError>;

    async fn push(&mut self, branch: BranchId, revision: Revision) -> Result<(), RemoteError>;
}
