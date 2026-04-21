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

    /// Scaffolding: same as `TempPublish` but for resolve. Remove once
    /// every `cell.resolve(...).perform(env)` call site lives in a
    /// command with its own typed error.
    #[error(transparent)]
    TempResolve(#[from] ResolveError),

    /// Scaffolding: bubbles `LoadBranchError` through commands that
    /// still return `RepositoryError`. Remove once every `.load()`
    /// caller has its own typed error.
    #[error(transparent)]
    TempLoadBranch(#[from] LoadBranchError),

    /// Scaffolding: bubbles `LoadRemoteError` through commands that
    /// still return `RepositoryError`. Remove once every remote load
    /// caller has its own typed error.
    #[error(transparent)]
    TempLoadRemote(#[from] LoadRemoteError),

    /// Scaffolding: bubbles `OpenRemoteBranchError` through commands
    /// that still return `RepositoryError`.
    #[error(transparent)]
    TempOpenRemoteBranch(#[from] OpenRemoteBranchError),

    /// Scaffolding: bubbles `LoadRemoteBranchError` through commands
    /// that still return `RepositoryError`.
    #[error(transparent)]
    TempLoadRemoteBranch(#[from] LoadRemoteBranchError),

    /// Scaffolding: bubbles `FetchRemoteBranchError` through commands
    /// that still return `RepositoryError`.
    #[error(transparent)]
    TempFetchRemoteBranch(#[from] FetchRemoteBranchError),

    /// Scaffolding: bubbles `PublishRemoteBranchError` through commands
    /// that still return `RepositoryError`.
    #[error(transparent)]
    TempPublishRemoteBranch(#[from] PublishRemoteBranchError),

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

/// Errors returned by the open remote branch command.
#[derive(Error, Debug)]
pub enum OpenRemoteBranchError {
    /// Loading the remote (to resolve its address) failed.
    #[error(transparent)]
    LoadRemote(#[from] LoadRemoteError),

    /// Resolving the local snapshot cache failed.
    #[error(transparent)]
    Resolve(#[from] ResolveError),
}

/// Errors returned by the fetch remote branch command.
#[derive(Error, Debug)]
pub enum FetchRemoteBranchError {
    /// Resolving the upstream revision from the remote failed.
    #[error(transparent)]
    Resolve(#[from] ResolveError),

    /// Persisting the fetched revision to the local cache failed.
    #[error(transparent)]
    Publish(#[from] PublishError),
}

/// Errors returned by the publish remote branch command.
#[derive(Error, Debug)]
pub enum PublishRemoteBranchError {
    /// Publishing the revision to the upstream failed.
    #[error(transparent)]
    Publish(#[from] PublishError),

    /// The upstream cell has no edition after publish — this should
    /// not happen in normal operation.
    #[error("Upstream cell missing edition after publish")]
    MissingEdition,
}

/// Errors returned by the load remote branch command.
#[derive(Error, Debug)]
pub enum LoadRemoteBranchError {
    /// The remote branch has no cached revision locally (never
    /// fetched).
    #[error("Remote branch {name} not found")]
    NotFound {
        /// The branch name.
        name: String,
    },

    /// Opening the remote branch (to resolve address + cache) failed.
    #[error(transparent)]
    Open(#[from] OpenRemoteBranchError),
}

/// Errors returned by the load remote command.
#[derive(Error, Debug)]
pub enum LoadRemoteError {
    /// The remote has no recorded address (never created).
    #[error("Remote {name} not found")]
    NotFound {
        /// The remote name.
        name: String,
    },

    /// Failed to resolve the remote's address cell.
    #[error(transparent)]
    Resolve(#[from] ResolveError),
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
    #[error(transparent)]
    Resolve(#[from] ResolveError),
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

    /// A cell resolve during push failed.
    #[error(transparent)]
    Resolve(#[from] ResolveError),

    /// Loading the configured remote failed.
    #[error(transparent)]
    LoadRemote(#[from] LoadRemoteError),

    /// Opening the remote branch failed.
    #[error(transparent)]
    OpenRemoteBranch(#[from] OpenRemoteBranchError),

    /// Fetching the upstream revision from the remote failed.
    #[error(transparent)]
    FetchRemoteBranch(#[from] FetchRemoteBranchError),

    /// Publishing the revision to the remote upstream failed.
    #[error(transparent)]
    PublishRemoteBranch(#[from] PublishRemoteBranchError),

    /// Uploading novel blocks to the remote archive failed.
    #[error(transparent)]
    Upload(#[from] UploadError),

    /// A prolly-tree operation during push failed.
    #[error(transparent)]
    Tree(#[from] DialogProllyTreeError),
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

/// Errors returned by the remote archive upload command.
#[derive(Error, Debug)]
pub enum UploadError {
    /// Failed to walk the local tree to enumerate novel nodes.
    #[error("Failed to enumerate novel tree nodes: {0}")]
    Tree(#[from] DialogProllyTreeError),

    /// Failed to read a block from the local archive before uploading.
    #[error("Failed to read block from local archive: {0}")]
    LocalRead(ArchiveError),

    /// Failed to write a block to the remote archive.
    #[error("Failed to write block to remote archive: {0}")]
    RemoteWrite(ArchiveError),
}

