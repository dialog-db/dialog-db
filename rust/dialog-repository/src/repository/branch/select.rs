use base58::ToBase58;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::ArtifactTreeExt as _;
use dialog_artifacts::{Artifact, ArtifactSelector, DialogArtifactsError};
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_effects::memory::Resolve;
use dialog_search_tree::DialogSearchTreeError;
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::Stream;

use crate::{
    Branch, EMPTY_TREE_HASH, Index, NetworkedIndex, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt, Upstream,
};

/// Command struct for selecting artifacts from a branch.
pub struct Select<'a> {
    branch: &'a Branch,
    selector: ArtifactSelector<Constrained>,
}

impl<'a> Select<'a> {
    /// Create a select command for the given branch and artifact selector.
    pub fn new(branch: &'a Branch, selector: ArtifactSelector<Constrained>) -> Self {
        Self { branch, selector }
    }

    fn tree_hash(&self) -> Blake3Hash {
        self.branch
            .revision()
            .as_ref()
            .map(|rev| *rev.tree.hash())
            .unwrap_or(EMPTY_TREE_HASH)
    }

    /// The catalog (archive index) scoped to this branch's subject.
    pub fn catalog(&self) -> Capability<Catalog> {
        self.branch.subject().archive().index()
    }
}

impl Select<'_> {
    /// Execute the select, using fallback to remote if the branch has
    /// a remote upstream.
    ///
    /// The per-item error type remains [`DialogArtifactsError`] because
    /// stream items surface artifact-decoding errors that the caller may
    /// want to inspect directly.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>>, DialogSearchTreeError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        // Load the remote if the branch tracks one so the networked
        // index can fall back to it for blocks missing locally. Failing
        // to load the remote (e.g. no credentials) is non-fatal — the
        // local archive alone may still satisfy the query.
        let remote = match self.branch.upstream() {
            Some(Upstream::Remote { remote: name, .. }) => self
                .branch
                .subject()
                .remote(name)
                .load()
                .perform(env)
                .await
                .ok(),
            _ => None,
        };

        let store = NetworkedIndex::new(env, self.catalog(), remote);
        self.execute(store).await
    }

    /// Execute the select against the given content-addressed store.
    ///
    /// Unlike [`perform`](Self::perform) this does not pick a store for
    /// you — useful when callers (e.g. query sessions) want to supply a
    /// custom one such as a pre-configured [`NetworkedIndex`].
    pub async fn execute<'s, S>(
        self,
        store: S,
    ) -> Result<
        impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's,
        DialogSearchTreeError,
    >
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's,
    {
        // Tree hydration is lazy (nodes load on demand during the scan),
        // but unreachable branches should fail here rather than midway
        // through the stream, so probe the root block eagerly. Through a
        // `NetworkedIndex` this also replicates and caches the root
        // locally, so the scan's own root read stays local.
        let tree_hash = self.tree_hash();
        if tree_hash != EMPTY_TREE_HASH {
            store.get(&tree_hash).await?.ok_or_else(|| {
                DialogSearchTreeError::Node(format!(
                    "Blob not found in storage: {}",
                    tree_hash.to_base58(),
                ))
            })?;
        }

        let tree = Index::from_hash(NodeHash::from(tree_hash));

        // EAV/AEV/VAE dispatch + per-entry filtering lives in the shared
        // `ArtifactTreeExt::scan` so branch scans and Changes-overlay
        // scans agree on key order — that adjacency invariant is what
        // the cardinality-one sliding window relies on.
        Ok(tree.scan(store, self.selector))
    }
}
