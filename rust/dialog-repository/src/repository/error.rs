use crate::TreeReference;
use dialog_artifacts::DialogArtifactsError;
use dialog_capability::access::AuthorizeError;
use dialog_common::Blake3Hash;
use dialog_credentials::Ed25519SignerError;
use dialog_effects::Rejection;
use dialog_effects::archive::ArchiveError;
use dialog_effects::authority::AuthorityError;
use dialog_effects::blob::BlobError;
use dialog_effects::memory::{MemoryError, Version};
use dialog_effects::storage::StorageError;
use dialog_search_tree::DialogSearchTreeError;
use dialog_storage::DialogStorageError;
use std::io;
use thiserror::Error;

/// Errors returned by the open remote branch command.
#[derive(Error, Debug)]
pub enum OpenRemoteBranchError {
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
    /// A search-tree operation during commit failed.
    #[error("Tree operation failed during commit: {0}")]
    Tree(#[from] DialogSearchTreeError),

    /// An artifact decode during commit failed.
    #[error("Artifact decode failed during commit: {0}")]
    Artifact(#[from] DialogArtifactsError),

    /// Identifying the current authority for the new revision failed.
    #[error("Failed to identify authority for commit: {0}")]
    Authority(#[from] AuthorityError),

    /// Publishing the new revision failed.
    #[error("Failed to publish new revision: {0}")]
    Publish(#[from] PublishError),

    /// Ingesting a blob's bytes into the store failed.
    #[error("Blob ingest failed during write: {0}")]
    Blob(#[from] BlobError),

    /// A cell resolve during commit failed.
    #[error("Failed to resolve during commit: {0}")]
    Resolve(#[from] ResolveError),

    /// Commit-time induction still emitted novelty or transients after
    /// the round bound — a self-feeding cascade (e.g. a transient
    /// ping-pong between two rules, or an unguarded rule deriving from
    /// its own durable output).
    #[error("Inductive rules did not settle within {0} rounds")]
    InductionDivergence(u32),

    /// Evaluating or loading an inductive rule during commit failed.
    #[error("Commit-time induction failed: {0}")]
    Induction(String),
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

    /// Pull targeted the branch itself.
    #[error("Branch {branch} cannot pull from itself")]
    UpstreamIsItself {
        /// The branch name.
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

    /// A cell resolve during pull failed.
    #[error("Failed to resolve during pull: {0}")]
    Resolve(#[from] ResolveError),

    /// Identifying the current authority for the merge revision failed.
    #[error("Failed to identify authority for merge: {0}")]
    Authority(#[from] AuthorityError),

    /// A search-tree operation during pull failed.
    #[error("Tree operation failed during pull: {0}")]
    Tree(#[from] DialogSearchTreeError),

    /// Streaming a block during replication failed.
    #[error("Block streaming failed during pull: {0}")]
    Storage(#[from] DialogStorageError),

    /// An artifact decode during pull failed.
    #[error("Artifact decode failed during pull: {0}")]
    Artifact(#[from] DialogArtifactsError),

    /// Materializing the adopted head after the pull failed
    /// ([`Pull::download`](crate::Pull::download)).
    #[error("Download after pull failed: {0}")]
    Download(#[from] DownloadError),
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

    /// Push targeted the branch itself.
    #[error("Branch {branch} cannot push to itself")]
    UpstreamIsItself {
        /// The branch name.
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

    /// A search-tree operation during push failed.
    #[error("Tree operation failed during push: {0}")]
    Tree(#[from] DialogSearchTreeError),

    /// Reading the blob differential or a blob record from the index failed.
    #[error("Artifact operation failed during push: {0}")]
    Artifact(#[from] DialogArtifactsError),

    /// Shipping a newly-referenced blob to the remote failed.
    #[error("Blob operation failed during push: {0}")]
    Blob(#[from] BlobError),

    /// Reading a spilled value block from the local archive during push failed.
    #[error("Storage operation failed during push: {0}")]
    Storage(#[from] DialogStorageError),

    /// A remote archive operation (e.g. writing a spilled value block) during
    /// push failed.
    #[error("Remote archive operation failed during push: {0}")]
    Archive(#[from] ArchiveError),
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

    /// The request was not authorized.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

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
            MemoryError::Rejected(error) => Self::Rejected(error),
            MemoryError::Authorization(error) => Self::Authorization(error),
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

    /// The request was not authorized.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

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
            MemoryError::Rejected(error) => Self::Rejected(error),
            MemoryError::Authorization(error) => Self::Authorization(error),
        }
    }
}

/// Errors returned by the remote archive upload command.
#[derive(Error, Debug)]
pub enum UploadError {
    /// Failed to walk the local tree to enumerate novel nodes.
    #[error("Failed to enumerate novel tree nodes: {0}")]
    Tree(#[from] DialogSearchTreeError),

    /// Failed to read a block from the local archive before uploading.
    #[error("Failed to read block from local archive: {0}")]
    LocalRead(#[source] ArchiveError),

    /// Failed to write a block to the remote archive.
    #[error("Failed to write block to remote archive: {0}")]
    RemoteWrite(#[source] ArchiveError),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unused_async)]

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_capability::access::AuthorizeError;

    use super::*;

    fn revoked() -> AuthorizeError {
        AuthorizeError::Revoked {
            subject: dialog_capability::did!(
                "did:key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"
            ),
        }
    }

    /// Confirm the reason is still reachable as a value, not as text.
    ///
    /// `#[error(transparent)]` delegates `source()` straight past the
    /// variant, so the wrapper chain does not expose the
    /// [`AuthorizeError`] as a source -- it is reachable by matching, and
    /// that is the property worth pinning: previously each hop rendered
    /// the reason with `to_string()`, so the only way to recover it was
    /// to parse the message.
    fn assert_revoked(rendered: &str) {
        assert!(
            rendered.contains("revoked"),
            "the reason survives the wrappers, got {rendered}"
        );
    }

    #[dialog_common::test]
    async fn it_preserves_memory_reasons_through_pull_and_push() {
        let pull = PullError::FetchRemoteBranch(FetchRemoteBranchError::Resolve(
            ResolveError::from(MemoryError::Authorization(revoked())),
        ));
        let push = PushError::PublishRemoteBranch(PublishRemoteBranchError::Publish(
            PublishError::from(MemoryError::Authorization(revoked())),
        ));

        // Structural: the reason is a value at the end of the chain.
        let PullError::FetchRemoteBranch(FetchRemoteBranchError::Resolve(
            ResolveError::Authorization(reason),
        )) = &pull
        else {
            panic!("expected an authorization reason, got {pull:?}");
        };
        assert!(matches!(reason, AuthorizeError::Revoked { .. }));

        assert_revoked(&pull.to_string());
        assert_revoked(&push.to_string());
    }

    #[dialog_common::test]
    async fn it_preserves_archive_reasons_through_pull_and_push() {
        let storage = DialogStorageError::from(ArchiveError::Authorization(revoked()));
        let pull = PullError::Tree(DialogSearchTreeError::from(storage));
        let push = PushError::Upload(UploadError::RemoteWrite(ArchiveError::Authorization(
            revoked(),
        )));

        assert_revoked(&pull.to_string());
        assert_revoked(&push.to_string());
    }

    #[dialog_common::test]
    async fn it_preserves_blob_reasons_through_push() {
        let push = PushError::Blob(BlobError::Authorization(revoked()));
        let PushError::Blob(BlobError::Authorization(reason)) = &push else {
            panic!("expected an authorization reason, got {push:?}");
        };
        assert!(matches!(reason, AuthorizeError::Revoked { .. }));
        assert_revoked(&push.to_string());
    }

    #[dialog_common::test]
    async fn it_preserves_artifact_reasons_through_push() {
        let tree = DialogSearchTreeError::from(DialogStorageError::Authorization(revoked()));
        let push = PushError::Artifact(DialogArtifactsError::from(tree));
        let PushError::Artifact(DialogArtifactsError::Authorization(reason)) = &push else {
            panic!("the conversion flattened the reason: {push:?}");
        };
        assert!(matches!(reason, AuthorizeError::Revoked { .. }));
        assert_revoked(&push.to_string());
    }
}

/// Errors from materializing a branch's content locally
/// ([`Branch::download`](crate::Branch::download)).
#[derive(Error, Debug)]
pub enum DownloadError {
    /// Loading the branch's configured remote failed.
    #[error("Failed to load remote for download: {0}")]
    LoadRemote(#[from] LoadRemoteError),

    /// The materializing walk failed.
    #[error("Download walk failed: {0}")]
    Snapshot(#[from] SnapshotError),
}

/// Errors from snapshot export and import.
///
/// The digest-mismatch variants are the point of this type. A snapshot may
/// cross a trust boundary -- a source that fetched from a remote, or bytes
/// read from a file -- so content that does not hash to the address it
/// claims must be refused rather than stored. Both carry what was expected
/// and what arrived, so a caller can say which content was corrupt.
#[derive(Error, Debug)]
pub enum SnapshotError {
    /// A block's content did not hash to the digest it declared.
    ///
    /// Storing it anyway would not fail loudly: content-addressed bytes
    /// simply land at a different address, unreferenced, and surface much
    /// later as a missing node far from the cause.
    #[error("Block content does not match its address: expected {expected}, got {actual}")]
    BlockDigestMismatch {
        /// The address the block declared.
        expected: Blake3Hash,
        /// The address its content actually hashes to.
        actual: Blake3Hash,
    },

    /// A blob's content did not hash to the digest it declared.
    ///
    /// Detected at `finish`, once the last chunk has been written: a blob
    /// arrives in pieces, so it cannot be checked on arrival the way a
    /// block can.
    #[error("Blob content does not match its address: expected {expected}, got {actual}")]
    BlobDigestMismatch {
        /// The address the blob declared.
        expected: Blake3Hash,
        /// The address its content actually hashes to.
        actual: Blake3Hash,
    },

    /// The revision references a block this store does not hold.
    ///
    /// Only raised when the export was asked for a complete snapshot; a
    /// sparse one records the gap and carries on.
    #[error("Revision references block {digest}, which is not present")]
    MissingBlock {
        /// The address that could not be read.
        digest: Blake3Hash,
    },

    /// A spilled-value key carried a malformed content reference.
    #[error("Spilled-value reference is not 32 bytes: {0:?}")]
    InvalidSpillReference(Vec<u8>),

    /// The revision references a blob this store does not hold.
    #[error("Revision references blob {digest}, which is not present")]
    MissingBlob {
        /// The address that could not be read.
        digest: Blake3Hash,
    },

    /// Reading or writing an archive block failed.
    #[error(transparent)]
    Archive(#[from] ArchiveError),

    /// Reading or writing a blob failed.
    #[error(transparent)]
    Blob(#[from] BlobError),

    /// Walking the revision's tree failed.
    #[error(transparent)]
    Tree(#[from] DialogSearchTreeError),

    /// Reading a referenced artifact failed.
    #[error(transparent)]
    Artifact(#[from] DialogArtifactsError),

    /// Accessing the archive backend failed.
    #[error(transparent)]
    Storage(#[from] DialogStorageError),
}
