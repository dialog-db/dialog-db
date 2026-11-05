use base58::ToBase58;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::ArtifactTreeExt as _;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, ArtifactView, Datum, DialogArtifactsError, Key,
    State,
};
use dialog_capability::Provider;
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{ConditionalSend, ConditionalSync, Holds};
use dialog_effects::archive::Get;
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::blob::Load as BlobLoad;
use dialog_search_tree::{DialogSearchTreeError, Manifest, load_node};
use dialog_storage::Blake3Hash;
use futures_util::Stream;

use dialog_effects::archive::prelude::CatalogScope;

use crate::repository::archive::erased;
use crate::repository::source::SourceRef;
use crate::{Branch, Index};

/// Command struct for selecting artifacts from a branch or a snapshot.
pub struct Select<'a> {
    source: SourceRef<'a>,
    selector: ArtifactSelector<Constrained>,
}

impl<'a> Select<'a> {
    /// Create a select command for the given branch and artifact selector.
    pub fn new(branch: &'a Branch, selector: ArtifactSelector<Constrained>) -> Self {
        Self::from_source(SourceRef::from(branch), selector)
    }

    /// Create a select command for the given line (branch or snapshot)
    /// and artifact selector.
    pub(crate) fn from_source(
        source: SourceRef<'a>,
        selector: ArtifactSelector<Constrained>,
    ) -> Self {
        Self { source, selector }
    }

    fn tree_hash(&self) -> Option<Blake3Hash> {
        self.source.root()
    }

    /// The catalog (archive index) scoped to this line's subject.
    pub fn catalog(&self) -> CatalogScope {
        self.source.catalog()
    }
}

/// The format [`Manifest`] of a line's tree: the manifest its root node
/// carries, or, for a line with no tree yet, the one its empty tree holds
/// (the format its first commit creates the tree under). Rows scanned from
/// the line are keyed under it, so anything compared against them — an
/// overlay row, a retracted fact, a demanded range — is keyed under it too.
pub(crate) async fn line_manifest<Env>(
    source: SourceRef<'_>,
    env: &Env,
) -> Result<Manifest, DialogArtifactsError>
where
    Env: Provider<ArchiveLoad> + Holds + ConditionalSync + ?Sized,
{
    Ok(source.tree().manifest(&env).await?)
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
    /// Execute the select, using fallback to remote if the line is a
    /// branch with a remote upstream.
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
        Env: Provider<Get> + Provider<ArchiveLoad> + Provider<BlobLoad> + Holds + ConditionalSync,
    {
        // A block missing locally hydrates through the env, from the
        // remotes the subject's open branches track. A remote that failed
        // to load fails such a read with the load failure as its cause
        // instead of a bare not-found, while what the local archive holds
        // still reads.
        self.execute(erased(env)).await
    }

    /// The tree this select scans, with its root block probed eagerly.
    async fn probed_tree<S>(&self, store: &S) -> Result<Index, DialogSearchTreeError>
    where
        S: Provider<ArchiveLoad> + Holds + ConditionalSync,
    {
        // Tree hydration is lazy (nodes load on demand during the scan),
        // but unreachable branches should fail here rather than midway
        // through the stream, so probe the root block eagerly. The load
        // hydrates a root the replica lacks from where the subject's blocks
        // live, replicating it locally.
        //
        // The probe reads like every tree read: the root is the single
        // most-reused block, and a multi-premise query re-selects the same
        // branch once per outer binding, so the first select keeps it in
        // the node cache the env holds and the rest (and the scan's own
        // root read) are answered from there, while a genuine miss still
        // fetches and a truly absent root fails fast.
        let catalog = self.catalog();
        Ok(match self.tree_hash() {
            Some(tree_hash) => {
                let root = NodeHash::from(tree_hash);
                load_node::<Key, State<Datum>, S>(store, &catalog, &root)
                    .await?
                    .ok_or_else(|| {
                        DialogSearchTreeError::Node(format!(
                            "Block not found in storage: {}",
                            tree_hash.to_base58(),
                        ))
                    })?;
                Index::at(catalog, root)
            }
            // No revision means no tree to probe or scan: the select runs
            // over the empty index and yields nothing.
            None => Index::empty_in(catalog),
        })
    }

    /// Execute the select against the given content-addressed store.
    ///
    /// Unlike [`perform`](Self::perform) this reads through the given
    /// environment as it is, by value: a borrowed environment (`&env`) or
    /// the trait object a query session reads through.
    pub async fn execute<'s, S>(
        self,
        store: S,
    ) -> Result<
        impl Stream<Item = Result<ArtifactView, DialogArtifactsError>> + 's + use<'s, S>,
        DialogSearchTreeError,
    >
    where
        S: Provider<ArchiveLoad>
            + Provider<BlobLoad>
            + Holds
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        let tree = self.probed_tree(&store).await?;
        // EAV/AEV/VAE dispatch + per-entry filtering lives in the shared
        // `ArtifactTreeExt::scan` so branch scans and Changes-overlay
        // scans agree on key order — that adjacency invariant is what
        // the cardinality-one sliding window relies on.
        Ok(tree.scan(store, self.source.spill_cache(), self.selector))
    }

    /// [`execute`](Self::execute), with the scan stream built directly in
    /// its box. The stream is several KiB of state; returned by value it
    /// was copied through each future and result it passed on the way to
    /// the box a query environment keeps it in.
    pub(crate) async fn execute_boxed<'s, S>(
        self,
        store: S,
    ) -> Result<ArtifactStream<'s>, DialogSearchTreeError>
    where
        S: Provider<ArchiveLoad>
            + Provider<BlobLoad>
            + Holds
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        let tree = self.probed_tree(&store).await?;
        Ok(Box::pin(tree.scan(
            store,
            self.source.spill_cache(),
            self.selector,
        )))
    }

    /// Estimate this selector's range size, picking a store the same way
    /// [`perform`](Self::perform) does. See [`estimate`](Self::estimate).
    pub async fn estimate_perform<Env>(self, env: &Env) -> Result<Option<u64>, DialogArtifactsError>
    where
        Env: Provider<Get> + Provider<ArchiveLoad> + Provider<BlobLoad> + Holds + ConditionalSync,
    {
        self.estimate(erased(env)).await
    }

    /// An advisory upper-bound estimate of how many artifacts this selector's
    /// range spans, read from the range's edge paths (see
    /// [`ArtifactTreeExt::estimate`](dialog_artifacts::tree::ArtifactTreeExt::estimate)).
    ///
    /// A couple of blocks per level rather than a scan, for a planner
    /// comparing scan sizes.
    /// This estimates against the line's tree only; it ignores any pending
    /// `Changes` overlay, whose in-memory edits are small relative to the tree
    /// and do not change the order-of-magnitude answer a strategy choice
    /// needs. Returns `None` for an empty tree.
    pub async fn estimate<S>(self, store: S) -> Result<Option<u64>, DialogArtifactsError>
    where
        S: Provider<ArchiveLoad>
            + Provider<BlobLoad>
            + Holds
            + ConditionalSend
            + ConditionalSync
            + Clone,
    {
        let Some(tree_hash) = self.tree_hash() else {
            return Ok(None);
        };
        let tree = Index::at(self.catalog(), NodeHash::from(tree_hash));
        tree.estimate(store, self.selector).await
    }

    /// [`execute`](Self::execute) materializing every row from the scan's
    /// own key parse — the fast path behind [`SelectOwned`], which by
    /// definition materializes everything and would otherwise pay a second
    /// key walk per row through the view's `to_owned`.
    async fn execute_owned<'s, S>(
        self,
        store: S,
    ) -> Result<
        impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's,
        DialogSearchTreeError,
    >
    where
        S: Provider<ArchiveLoad>
            + Provider<BlobLoad>
            + Holds
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        let tree = self.probed_tree(&store).await?;
        Ok(tree.scan_owned(store, self.source.spill_cache(), self.selector))
    }
}

/// A [`Select`] whose streams materialize every row into an owned
/// [`Artifact`] — the explicit opt-in produced by [`Select::to_owned`].
///
/// The query pipeline itself traffics in borrowed-access views
/// (`ArtifactStream` yields [`ArtifactView`]s; the k-way merge and the
/// `Changes` overlay operate on them, and the engine materializes only the
/// rows its filters admit). This form is for CONSUMERS that want every row
/// owned — a collect-everything read, an export — where materializing from
/// the scan's own key parse beats a per-row view + `to_owned` round trip.
pub struct SelectOwned<'a>(Select<'a>);

impl SelectOwned<'_> {
    /// The catalog (archive index) scoped to this line's subject.
    pub fn catalog(&self) -> CatalogScope {
        self.0.catalog()
    }

    /// [`Select::perform`], with every row materialized from the scan's
    /// own key parse (see [`Select::execute_owned`]).
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>>, DialogSearchTreeError>
    where
        Env: Provider<Get> + Provider<ArchiveLoad> + Provider<BlobLoad> + Holds + ConditionalSync,
    {
        self.execute(erased(env)).await
    }

    /// [`Select::execute`], with every row materialized from the scan's
    /// own key parse (see [`Select::execute_owned`]).
    pub async fn execute<'s, S>(
        self,
        store: S,
    ) -> Result<
        impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's,
        DialogSearchTreeError,
    >
    where
        S: Provider<ArchiveLoad>
            + Provider<BlobLoad>
            + Holds
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        self.0.execute_owned(store).await
    }
}

#[cfg(test)]
mod number_tests;
#[cfg(test)]
mod tests;
