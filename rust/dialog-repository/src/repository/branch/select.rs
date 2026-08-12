use base58::ToBase58;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::ArtifactTreeExt as _;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactView, ArtifactViewStream as _, DialogArtifactsError,
};
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_effects::memory::Resolve;
use dialog_search_tree::{Buffer, DialogSearchTreeError};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::Stream;

use crate::{
    Branch, EMPTY_TREE_HASH, Index, NetworkedIndex, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt,
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

impl<'a> Select<'a> {
    /// Materialize every row: the select's streams yield owned
    /// [`Artifact`]s instead of borrowed-access [`ArtifactView`]s.
    ///
    /// `select(..).to_owned().perform(..)` is the drop-in spelling for
    /// consumers of the pre-view API; prefer reading fields off the views
    /// where the rows never leave the caller's scope.
    // to_owned takes `self` because the select statement is a builder the
    // terminal perform/execute consumes; there is no `&self` version to
    // convert from.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_owned(self) -> SelectOwned<'a> {
        SelectOwned(self)
    }
}

impl Select<'_> {
    /// Execute the select, using fallback to remote if the branch has
    /// a remote upstream.
    ///
    /// Rows stream as borrowed-access [`ArtifactView`]s; chain
    /// [`to_owned`](Self::to_owned) before this call for owned
    /// [`Artifact`]s. The per-item error type remains
    /// [`DialogArtifactsError`] because stream items surface
    /// artifact-decoding errors that the caller may want to inspect
    /// directly.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<impl Stream<Item = Result<ArtifactView, DialogArtifactsError>>, DialogSearchTreeError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        // Load a remote if the branch tracks one so the networked
        // index can fall back to it for blocks missing locally. Failing
        // to load the remote (e.g. no credentials) is non-fatal — the
        // local archive alone may still satisfy the query.
        let upstreams = self.branch.upstreams();
        let remote = match upstreams.remote_name() {
            Some(name) => self
                .branch
                .subject()
                .remote(name.to_string())
                .load()
                .perform(env)
                .await
                .ok(),
            None => None,
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
        impl Stream<Item = Result<ArtifactView, DialogArtifactsError>> + 's,
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
        //
        // Route the probe through the shared node cache, not a raw
        // `store.get`: the root is the single most-reused block, and a
        // multi-premise query re-selects the same branch once per outer
        // binding. A raw probe would re-fetch the root from the backend on
        // every one of those selects (defeating the cache); `get_or_fetch`
        // makes the first select warm the cache and the rest hit it, while
        // still fetching (and, through `NetworkedIndex`, replicating) on a
        // genuine miss and failing fast when the root is truly absent.
        let tree_hash = self.tree_hash();
        let node_cache = self.branch.node_cache();
        if tree_hash != EMPTY_TREE_HASH {
            node_cache
                .get_or_fetch(&NodeHash::from(tree_hash), async |hash| {
                    store
                        .get(hash.as_bytes())
                        .await
                        .map(|maybe| maybe.map(Buffer::from))
                })
                .await?
                .ok_or_else(|| {
                    DialogSearchTreeError::Node(format!(
                        "Blob not found in storage: {}",
                        tree_hash.to_base58(),
                    ))
                })?;
        }

        let tree = Index::from_hash_with_cache(NodeHash::from(tree_hash), node_cache);

        // EAV/AEV/VAE dispatch + per-entry filtering lives in the shared
        // `ArtifactTreeExt::scan` so branch scans and Changes-overlay
        // scans agree on key order — that adjacency invariant is what
        // the cardinality-one sliding window relies on.
        Ok(tree.scan(store, self.branch.spill_cache(), self.selector))
    }
}

/// A [`Select`] whose streams materialize every row into an owned
/// [`Artifact`] — the explicit opt-in produced by [`Select::to_owned`].
///
/// The query pipeline above the branch scan (`ArtifactStream`, the k-way
/// merge, the `Changes` overlay) still traffics in owned `Artifact`s, so it
/// ingests through this form. Threading views through that pipeline — so a
/// query only materializes rows that survive its filters — is the follow-up.
pub struct SelectOwned<'a>(Select<'a>);

impl SelectOwned<'_> {
    /// The catalog (archive index) scoped to this branch's subject.
    pub fn catalog(&self) -> Capability<Catalog> {
        self.0.catalog()
    }

    /// [`Select::perform`], with every row materialized.
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
        Ok(self.0.perform(env).await?.owned())
    }

    /// [`Select::execute`], with every row materialized.
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
        Ok(self.0.execute(store).await?.owned())
    }
}
