use dialog_artifacts::DialogArtifactsError;
use dialog_credentials::Ed25519SignerError;
use dialog_effects::authority::AuthorityError;
use dialog_effects::memory::{MemoryError, Version};
use dialog_effects::storage::StorageError;
use dialog_prolly_tree::DialogProllyTreeError;
use dialog_storage::DialogStorageError;
use std::io;
use thiserror::Error;

/// The umbrella error type for the repository API.
///
/// Each variant wraps a command-specific error type. Callers doing
/// multiple operations can `?` them into a single
/// `Result<_, RepositoryError>` without juggling per-command error
/// types. Pattern match on variants (or use
/// [`source()`](std::error::Error::source)) when specific handling is
/// needed.
///
/// Downstream crates layer additional variants on this same pattern
/// for branch/remote/sync operations.
#[derive(Error, Debug)]
pub enum RepositoryError {
    /// Open-repository command failed.
    #[error(transparent)]
    Open(#[from] OpenRepositoryError),

    /// Load-repository command failed.
    #[error(transparent)]
    Load(#[from] LoadRepositoryError),

    /// Create-repository command failed.
    #[error(transparent)]
    Create(#[from] CreateRepositoryError),

    /// Load-branch command failed.
    #[error(transparent)]
    LoadBranch(#[from] LoadBranchError),

    /// Commit command failed.
    #[error(transparent)]
    Commit(#[from] CommitError),

    /// Cell publish failed (outside a command context).
    #[error(transparent)]
    Publish(#[from] PublishError),

    /// Cell resolve failed (outside a command context).
    #[error(transparent)]
    Resolve(#[from] ResolveError),

    /// A verifier-only credential was used where a signer was required.
    #[error(transparent)]
    SignerRequired(#[from] SignerRequiredError),
}

/// Attempted to use a verifier-only credential where a signer was
/// required.
#[derive(Error, Debug)]
#[error("Expected signer credential, got verifier-only")]
pub struct SignerRequiredError;

/// Errors returned by the open repository command.
#[derive(Error, Debug)]
pub enum OpenRepositoryError {
    /// Generating a new signer for the fresh repository failed.
    #[error("Failed to generate signer for new repository: {0}")]
    Signer(#[from] Ed25519SignerError),

    /// Backend storage failed during load-or-create.
    #[error("Storage failed during open: {0}")]
    Storage(#[from] StorageError),
}

/// Errors returned by the load repository command.
#[derive(Error, Debug)]
pub enum LoadRepositoryError {
    /// Backend storage failed during load.
    #[error("Storage failed during load: {0}")]
    Storage(#[from] StorageError),
}

/// Errors returned by the create repository command.
#[derive(Error, Debug)]
pub enum CreateRepositoryError {
    /// Generating a new signer for the repository failed.
    #[error("Failed to generate signer for new repository: {0}")]
    Signer(#[from] Ed25519SignerError),

    /// Backend storage failed during create.
    #[error("Storage failed during create: {0}")]
    Storage(#[from] StorageError),
}

/// Errors returned by cell resolve operations.
#[derive(Error, Debug)]
pub enum ResolveError {
    /// CAS edition mismatch — the backing store saw a different edition.
    #[error("Version mismatch: expected {expected:?}, got {actual:?}")]
    VersionMismatch {
        /// The edition we held locally.
        expected: Option<Version>,
        /// The edition the backing store actually had.
        actual: Option<Version>,
    },

    /// Storage backend failure.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Authorization denied.
    #[error("Authorization error: {0}")]
    Authorization(String),

    /// IO failure.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Failed to decode the resolved bytes.
    #[error("Decode error: {0}")]
    Decode(String),
}

impl From<MemoryError> for ResolveError {
    fn from(error: MemoryError) -> Self {
        match error {
            MemoryError::VersionMismatch { expected, actual } => {
                Self::VersionMismatch { expected, actual }
            }
            MemoryError::Storage(message) => Self::Storage(message),
            MemoryError::Authorization(message) => Self::Authorization(message),
            MemoryError::Io(error) => Self::Io(error),
        }
    }
}

/// Errors returned by cell publish operations.
#[derive(Error, Debug)]
pub enum PublishError {
    /// CAS edition mismatch — another writer won the race.
    #[error("Version mismatch: expected {expected:?}, got {actual:?}")]
    VersionMismatch {
        /// The edition we held locally.
        expected: Option<Version>,
        /// The edition the backing store actually had.
        actual: Option<Version>,
    },

    /// Storage backend failure.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Authorization denied.
    #[error("Authorization error: {0}")]
    Authorization(String),

    /// IO failure.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Failed to encode the value before publishing.
    #[error("Encode error: {0}")]
    Encode(String),
}

impl From<MemoryError> for PublishError {
    fn from(error: MemoryError) -> Self {
        match error {
            MemoryError::VersionMismatch { expected, actual } => {
                Self::VersionMismatch { expected, actual }
            }
            MemoryError::Storage(message) => Self::Storage(message),
            MemoryError::Authorization(message) => Self::Authorization(message),
            MemoryError::Io(error) => Self::Io(error),
        }
    }
}

// Temporary: `archive/local.rs` uses `DialogStorageError` from the
// prolly-tree integration; keep `From<DialogStorageError>` alias so
// call sites that haven't been typed yet still compile.
impl From<DialogStorageError> for PublishError {
    fn from(error: DialogStorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<DialogStorageError> for ResolveError {
    fn from(error: DialogStorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

/// Errors returned by the load branch command.
#[derive(Error, Debug)]
pub enum LoadBranchError {
    /// The branch has no revision yet (nothing to load).
    #[error("Branch {name} not found")]
    NotFound {
        /// The branch name.
        name: String,
    },

    /// Failed to resolve the branch's cells.
    #[error("Failed to resolve branch cells: {0}")]
    Resolve(#[from] ResolveError),
}

/// Errors specific to a commit operation.
#[derive(Error, Debug)]
pub enum CommitError {
    /// A search-tree operation during commit failed.
    #[error("Tree operation failed during commit: {0}")]
    Tree(#[from] DialogProllyTreeError),

    /// An artifact decode during commit failed.
    #[error("Artifact decode failed during commit: {0}")]
    Artifact(#[from] DialogArtifactsError),

    /// Identifying the current authority for the new revision failed.
    #[error("Failed to identify authority for commit: {0}")]
    Authority(#[from] AuthorityError),

    /// Publishing the new revision failed.
    #[error("Failed to publish new revision: {0}")]
    Publish(#[from] PublishError),
}
