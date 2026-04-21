use std::io;

use dialog_artifacts::DialogArtifactsError;
use dialog_credentials::Ed25519SignerError;
use dialog_effects::archive::ArchiveError;
use dialog_effects::authority::AuthorityError;
use dialog_effects::memory::{MemoryError, Version};
use dialog_effects::storage::StorageError;
use dialog_prolly_tree::DialogProllyTreeError;
use dialog_storage::DialogStorageError;
use thiserror::Error;

use super::tree::TreeReference;

/// The common error type used by repository operations.
#[derive(Error, Debug)]
pub enum RepositoryError {
    /// Branch with the given name was not found.
    #[error("Branch {name} not found")]
    BranchNotFound {
        /// The name of the branch that was not found.
        name: String,
    },

    /// Branch has no configured upstream.
    #[error("Branch {name} has no upstream")]
    BranchHasNoUpstream {
        /// The name of the branch that has no upstream.
        name: String,
    },

    /// Branch upstream is set to itself.
    #[error("Upstream of local {name} is set to itself")]
    BranchUpstreamIsItself {
        /// Branch name.
        name: String,
    },

    /// Remote repository not found.
    #[error("Remote {remote} not found")]
    RemoteNotFound {
        /// Remote site name.
        remote: String,
    },

    /// Remote repository already exists.
    #[error("Remote {remote} already exists")]
    RemoteAlreadyExists {
        /// Remote site name.
        remote: String,
    },

    /// Pushing a revision failed.
    #[error("Pushing revision failed: {cause}")]
    PushFailed {
        /// The underlying error message.
        cause: String,
    },

    /// Identifying the current authority failed.
    #[error("Identify failed: {0}")]
    Identify(#[from] AuthorityError),

    /// A memory effect (publish/resolve) failed.
    #[error(transparent)]
    Memory(#[from] MemoryError),

    /// Scaffolding: commands that haven't yet been converted to their
    /// own typed error still produce `RepositoryError`, and any
    /// [`PublishError`] they bubble up lands here. Remove this variant
    /// once every `cell.publish(...).perform(env)` call site lives in
    /// a command with its own typed error.
    #[error(transparent)]
    TempPublish(#[from] PublishError),

    /// An archive effect (get/put) failed.
    #[error(transparent)]
    Archive(#[from] ArchiveError),

    /// A storage-backed operation failed.
    #[error(transparent)]
    Storage(#[from] DialogStorageError),

    /// An operator-space storage effect failed.
    #[error(transparent)]
    OperatorStorage(#[from] StorageError),

    /// Key generation or signing failed.
    #[error(transparent)]
    Signer(#[from] Ed25519SignerError),

    /// An artifact operation (tree manipulation, keyview, etc.) failed.
    #[error(transparent)]
    Artifact(#[from] DialogArtifactsError),

    /// A prolly tree operation failed.
    #[error(transparent)]
    Tree(#[from] DialogProllyTreeError),

    /// Repository not found.
    #[error("Repository '{0}' not found")]
    NotFound(String),

    /// Repository already exists.
    #[error("Repository '{0}' already exists")]
    AlreadyExists(String),

    /// Attempted to use a verifier-only credential where a signer is required.
    #[error("Expected signer credential, got verifier-only")]
    SignerRequired,

    /// Invalid internal state (should never happen in normal operation).
    #[error("Invalid state: {message}")]
    InvalidState {
        /// Description of the invalid state.
        message: String,
    },
}

/// Errors specific to a push operation.
#[derive(Error, Debug)]
pub enum PushError {
    /// Branch has no configured upstream to push to.
    #[error("Branch {branch} has no upstream")]
    BranchHasNoUpstream {
        /// The local branch with no configured upstream.
        branch: String,
    },

    /// Push was rejected because the upstream has advanced since the
    /// last sync. The local branch must integrate upstream changes
    /// (e.g. via `pull`) before pushing again.
    #[error(
        "Non-fast-forward push of branch {branch}: expected upstream tree {expected:?}, found {actual:?}"
    )]
    NonFastForward {
        /// The local branch whose push was rejected.
        branch: String,
        /// The tree we recorded as the upstream's last-known state
        /// (the divergence point).
        expected: TreeReference,
        /// The tree the upstream is actually at now.
        actual: TreeReference,
    },

    /// A cell publish during push failed.
    #[error(transparent)]
    Publish(#[from] PublishError),

    /// A prolly-tree operation during push failed.
    #[error(transparent)]
    Tree(#[from] DialogProllyTreeError),

    /// Underlying repository operation (storage, network, capability,
    /// etc.) failed during push.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
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

