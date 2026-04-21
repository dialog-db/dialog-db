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

/// The umbrella error type for the repository API.
///
/// Each variant wraps a command-specific error type. Callers doing
/// multiple operations (e.g. `push` then `pull`) can `?` both into a
/// single `Result<_, RepositoryError>` without juggling per-command
/// error types. Pattern match on variants or use `downcast` via
/// [`source()`](std::error::Error::source) when specific handling is
/// needed.
#[derive(Error, Debug)]
pub enum RepositoryError {
    /// Open-repository command failed.
    #[error(transparent)]
    OpenRepository(#[from] OpenRepositoryError),

    /// Load-repository command failed.
    #[error(transparent)]
    LoadRepository(#[from] LoadRepositoryError),

    /// Create-repository command failed.
    #[error(transparent)]
    CreateRepository(#[from] CreateRepositoryError),

    /// Load-branch command failed.
    #[error(transparent)]
    LoadBranch(#[from] LoadBranchError),

    /// Commit command failed.
    #[error(transparent)]
    Commit(#[from] CommitError),

    /// Set-upstream command failed.
    #[error(transparent)]
    SetUpstream(#[from] SetUpstreamError),

    /// Fetch command failed.
    #[error(transparent)]
    Fetch(#[from] FetchError),

    /// Push command failed.
    #[error(transparent)]
    Push(#[from] PushError),

    /// Pull command failed.
    #[error(transparent)]
    Pull(#[from] PullError),

    /// Load-remote command failed.
    #[error(transparent)]
    LoadRemote(#[from] LoadRemoteError),

    /// Create-remote command failed.
    #[error(transparent)]
    CreateRemote(#[from] CreateRemoteError),

    /// Open-remote-branch command failed.
    #[error(transparent)]
    OpenRemoteBranch(#[from] OpenRemoteBranchError),

    /// Load-remote-branch command failed.
    #[error(transparent)]
    LoadRemoteBranch(#[from] LoadRemoteBranchError),

    /// Fetch-remote-branch command failed.
    #[error(transparent)]
    FetchRemoteBranch(#[from] FetchRemoteBranchError),

    /// Publish-remote-branch command failed.
    #[error(transparent)]
    PublishRemoteBranch(#[from] PublishRemoteBranchError),

    /// Upload command (novel blocks to remote archive) failed.
    #[error(transparent)]
    Upload(#[from] UploadError),

    /// Cell publish failed (outside a command context).
    #[error(transparent)]
    Publish(#[from] PublishError),

    /// Cell resolve failed (outside a command context).
    #[error(transparent)]
    Resolve(#[from] ResolveError),

    /// Select command failed to load its tree (the stream itself yields
    /// `DialogArtifactsError` per-item, which is surfaced through the
    /// stream).
    #[error(transparent)]
    Select(#[from] DialogProllyTreeError),

    /// A verifier-only credential was used where a signer was required.
    #[error(transparent)]
    SignerRequired(#[from] SignerRequiredError),
}

/// Errors returned by the open remote branch command.
#[derive(Error, Debug)]
pub enum OpenRemoteBranchError {
    /// Loading the remote (to resolve its address) failed.
    #[error("Failed to load remote during open: {0}")]
    LoadRemote(#[from] LoadRemoteError),

    /// Resolving the local snapshot cache failed.
    #[error("Failed to resolve snapshot cache during open: {0}")]
    Resolve(#[from] ResolveError),
}

/// Errors returned by the fetch remote branch command.
#[derive(Error, Debug)]
pub enum FetchRemoteBranchError {
    /// Resolving the upstream revision from the remote failed.
    #[error("Failed to resolve upstream revision from remote: {0}")]
    Resolve(#[from] ResolveError),

    /// Persisting the fetched revision to the local cache failed.
    #[error("Failed to persist fetched revision to local cache: {0}")]
    Publish(#[from] PublishError),
}

/// Errors returned by the publish remote branch command.
#[derive(Error, Debug)]
pub enum PublishRemoteBranchError {
    /// Publishing the revision to the upstream failed.
    #[error("Failed to publish revision to upstream: {0}")]
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
    #[error("Remote branch {name} not found in local cache")]
    NotFound {
        /// The branch name.
        name: String,
    },

    /// Opening the remote branch (to resolve address + cache) failed.
    #[error("Failed to open remote branch during load: {0}")]
    Open(#[from] OpenRemoteBranchError),
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

    /// The created repository couldn't be used as a signer (should
    /// not happen — freshly generated credentials always have a key).
    #[error("Expected signer credential after create, got verifier-only")]
    SignerRequired,
}

/// Errors returned by the create remote command.
#[derive(Error, Debug)]
pub enum CreateRemoteError {
    /// A remote with this name already exists.
    #[error("Remote {name} already exists")]
    AlreadyExists {
        /// The remote name.
        name: String,
    },

    /// Failed to resolve the remote's address cell to check for
    /// existing record.
    #[error("Failed to resolve remote address cell: {0}")]
    Resolve(#[from] ResolveError),

    /// Failed to publish the new remote's address.
    #[error("Failed to publish remote address: {0}")]
    Publish(#[from] PublishError),
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
    #[error("Failed to resolve remote address cell: {0}")]
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
    #[error("Failed to resolve branch cells: {0}")]
    Resolve(#[from] ResolveError),
}

/// Errors specific to setting a branch's upstream.
#[derive(Error, Debug)]
pub enum SetUpstreamError {
    /// Upstream was set to the same branch it would advance, which
    /// would create a cycle.
    #[error("Upstream of local branch {branch} cannot be itself")]
    UpstreamIsItself {
        /// The branch name.
        branch: String,
    },

    /// Publishing the new upstream state failed.
    #[error("Failed to publish upstream state: {0}")]
    Publish(#[from] PublishError),
}

/// Errors specific to a branch fetch operation.
#[derive(Error, Debug)]
pub enum FetchError {
    /// Branch has no configured upstream to fetch from.
    #[error("Branch {branch} has no upstream to fetch from")]
    BranchHasNoUpstream {
        /// The local branch with no configured upstream.
        branch: String,
    },

    /// Loading the local upstream branch failed.
    #[error("Failed to load upstream branch: {0}")]
    LoadBranch(#[from] LoadBranchError),

    /// Loading the configured remote failed.
    #[error("Failed to load remote: {0}")]
    LoadRemote(#[from] LoadRemoteError),

    /// Opening the remote branch failed.
    #[error("Failed to open remote branch: {0}")]
    OpenRemoteBranch(#[from] OpenRemoteBranchError),

    /// Fetching from the remote failed.
    #[error("Failed to fetch from remote: {0}")]
    FetchRemoteBranch(#[from] FetchRemoteBranchError),
}

/// Errors specific to a commit operation.
#[derive(Error, Debug)]
pub enum CommitError {
    /// A prolly-tree operation during commit failed.
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

/// Errors specific to a pull operation.
#[derive(Error, Debug)]
pub enum PullError {
    /// Branch has no configured upstream to pull from.
    #[error("Branch {branch} has no upstream to pull from")]
    BranchHasNoUpstream {
        /// The local branch with no configured upstream.
        branch: String,
    },

    /// Loading the local upstream branch failed.
    #[error("Failed to load upstream branch: {0}")]
    LoadBranch(#[from] LoadBranchError),

    /// Loading the configured remote failed.
    #[error("Failed to load remote: {0}")]
    LoadRemote(#[from] LoadRemoteError),

    /// Opening the remote branch failed.
    #[error("Failed to open remote branch: {0}")]
    OpenRemoteBranch(#[from] OpenRemoteBranchError),

    /// Fetching the upstream revision from the remote failed.
    #[error("Failed to fetch from remote: {0}")]
    FetchRemoteBranch(#[from] FetchRemoteBranchError),

    /// A cell publish during pull failed.
    #[error("Failed to publish merged revision: {0}")]
    Publish(#[from] PublishError),

    /// Identifying the current authority for the merge revision failed.
    #[error("Failed to identify authority for merge: {0}")]
    Authority(#[from] AuthorityError),

    /// A prolly-tree operation during pull failed.
    #[error("Tree operation failed during pull: {0}")]
    Tree(#[from] DialogProllyTreeError),

    /// Streaming a block during replication failed.
    #[error("Block streaming failed during pull: {0}")]
    Storage(#[from] DialogStorageError),

    /// An artifact decode during pull failed.
    #[error("Artifact decode failed during pull: {0}")]
    Artifact(#[from] DialogArtifactsError),
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
    #[error("Failed to publish during push: {0}")]
    Publish(#[from] PublishError),

    /// A cell resolve during push failed.
    #[error("Failed to resolve during push: {0}")]
    Resolve(#[from] ResolveError),

    /// Loading the configured remote failed.
    #[error("Failed to load remote during push: {0}")]
    LoadRemote(#[from] LoadRemoteError),

    /// Opening the remote branch failed.
    #[error("Failed to open remote branch during push: {0}")]
    OpenRemoteBranch(#[from] OpenRemoteBranchError),

    /// Fetching the upstream revision from the remote failed.
    #[error("Failed to fetch upstream during push: {0}")]
    FetchRemoteBranch(#[from] FetchRemoteBranchError),

    /// Publishing the revision to the remote upstream failed.
    #[error("Failed to publish to remote upstream: {0}")]
    PublishRemoteBranch(#[from] PublishRemoteBranchError),

    /// Uploading novel blocks to the remote archive failed.
    #[error("Failed to upload novel blocks: {0}")]
    Upload(#[from] UploadError),

    /// A prolly-tree operation during push failed.
    #[error("Tree operation failed during push: {0}")]
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

