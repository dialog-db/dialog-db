use base58::ToBase58;
use dialog_capability::Provider;
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{ConditionalSend, ConditionalSync, Holds};
use dialog_corpus::selector::Constrained;
use dialog_corpus::tree::CorpusExt as _;
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Datum, DialogCorpusError, Fact, FactStream, FactView, Key, Selector, State};
use dialog_effects::archive::Get;
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::blob::Load as BlobLoad;
use dialog_search_tree::nodes;
use dialog_search_tree::{DialogSearchTreeError, Manifest, load_node};
use dialog_storage::Blake3Hash;
use futures_util::Stream;

use dialog_effects::archive::prelude::CatalogScope;

use crate::repository::archive::erased;
use crate::repository::source::{Live, View};
use crate::{Branch, Index};

/// Command struct for selecting artifacts from a branch or a snapshot.
///
/// A select reads one revision: the one its line stood at when the
/// select was made (see [`View`]). A commit that lands on the branch
/// afterwards is read by the next select, not by this one.
pub struct Select<'a> {
    view: View<'a>,
    selector: Selector<Constrained>,
}

impl<'a> Select<'a> {
    /// Create a select command for the given branch and artifact
    /// selector, over the branch as it stands now.
    pub fn new(branch: &'a Branch, selector: Selector<Constrained>) -> Self {
        Self::from_view(Live::from(branch).pin(), selector)
    }

    /// Create a select command for the given pinned line (branch or
    /// snapshot) and artifact selector.
    pub(crate) fn from_view(view: View<'a>, selector: Selector<Constrained>) -> Self {
        Self { view, selector }
    }

    fn tree_hash(&self) -> Option<Blake3Hash> {
        self.view.root()
    }

    /// The catalog (archive index) scoped to this line's subject.
    pub fn catalog(&self) -> CatalogScope {
        self.view.catalog()
    }
}

/// The format [`Manifest`] of a line's tree: the manifest its root node
/// carries, or, for a line with no tree yet, the one its empty tree holds
/// (the format its first commit creates the tree under). Rows scanned from
/// the line are keyed under it, so anything compared against them — an
/// overlay row, a retracted fact, a demanded range — is keyed under it too.
pub(crate) async fn line_manifest<Env>(
    view: &View<'_>,
    env: &Env,
) -> Result<Manifest, DialogCorpusError>
where
    Env: Provider<ArchiveLoad>
        + Provider<nodes::Write<CorpusNode>>
        + ConditionalSync
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + ConditionalSync
        + Holds
        + ConditionalSync
        + ?Sized,
{
    Ok(view.tree().manifest(&env).await?)
}

impl<'a> Select<'a> {
    /// Materialize every row: the select's streams yield owned
    /// [`Fact`]s instead of borrowed-access [`FactView`]s.
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
    /// Rows stream as borrowed-access [`FactView`]s; chain
    /// [`to_owned`](Self::to_owned) before this call for owned
    /// [`Fact`]s. The per-item error type remains
    /// [`DialogCorpusError`] because stream items surface
    /// fact-decoding errors that the caller may want to inspect
    /// directly.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<impl Stream<Item = Result<FactView, DialogCorpusError>>, DialogSearchTreeError>
    where
        Env: Provider<Get>
            + Provider<ArchiveLoad>
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Holds
            + ConditionalSync,
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
        S: Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Holds
            + ConditionalSync,
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
        impl Stream<Item = Result<FactView, DialogCorpusError>> + 's + use<'s, S>,
        DialogSearchTreeError,
    >
    where
        S: Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Holds
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        let tree = self.probed_tree(&store).await?;
        // EAV/AEV/VAE dispatch + per-entry filtering lives in the shared
        // `CorpusExt::scan` so branch scans and Changes-overlay
        // scans agree on key order — that adjacency invariant is what
        // the cardinality-one sliding window relies on.
        Ok(tree.scan(store, self.view.spill_cache(), self.selector))
    }

    /// [`execute`](Self::execute), with the scan stream built directly in
    /// its box. The stream is several KiB of state; returned by value it
    /// was copied through each future and result it passed on the way to
    /// the box a query environment keeps it in.
    pub(crate) async fn execute_boxed<'s, S>(
        self,
        store: S,
    ) -> Result<FactStream<'s>, DialogSearchTreeError>
    where
        S: Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
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
            self.view.spill_cache(),
            self.selector,
        )))
    }

    /// [`execute_boxed`](Self::execute_boxed) in reverse: the same rows,
    /// the greatest key first (see
    /// [`CorpusExt::scan_reverse`](dialog_corpus::tree::CorpusExt::scan_reverse)).
    pub(crate) async fn execute_reverse_boxed<'s, S>(
        self,
        store: S,
    ) -> Result<FactStream<'s>, DialogSearchTreeError>
    where
        S: Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<BlobLoad>
            + Holds
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        let tree = self.probed_tree(&store).await?;
        Ok(Box::pin(tree.scan_reverse(
            store,
            self.view.spill_cache(),
            self.selector,
        )))
    }

    /// Estimate this selector's range size, picking a store the same way
    /// [`perform`](Self::perform) does. See [`estimate`](Self::estimate).
    pub async fn estimate_perform<Env>(self, env: &Env) -> Result<Option<u64>, DialogCorpusError>
    where
        Env: Provider<Get>
            + Provider<ArchiveLoad>
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Holds
            + ConditionalSync,
    {
        self.estimate(erased(env)).await
    }

    /// An advisory upper-bound estimate of how many facts this selector's
    /// range spans, read from the range's edge paths (see
    /// [`CorpusExt::estimate`](dialog_corpus::tree::CorpusExt::estimate)).
    ///
    /// A couple of blocks per level rather than a scan, for a planner
    /// comparing scan sizes.
    /// This estimates against the line's tree only; it ignores any pending
    /// `Changes` overlay, whose in-memory edits are small relative to the tree
    /// and do not change the order-of-magnitude answer a strategy choice
    /// needs. Returns `None` for an empty tree.
    pub async fn estimate<S>(self, store: S) -> Result<Option<u64>, DialogCorpusError>
    where
        S: Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
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
    ) -> Result<impl Stream<Item = Result<Fact, DialogCorpusError>> + 's, DialogSearchTreeError>
    where
        S: Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Holds
            + ConditionalSend
            + ConditionalSync
            + Clone
            + 's,
    {
        let tree = self.probed_tree(&store).await?;
        Ok(tree.scan_owned(store, self.view.spill_cache(), self.selector))
    }
}

/// A [`Select`] whose streams materialize every row into an owned
/// [`Fact`] — the explicit opt-in produced by [`Select::to_owned`].
///
/// The query pipeline itself traffics in borrowed-access views
/// (`FactStream` yields [`FactView`]s; the k-way merge and the
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
    ) -> Result<impl Stream<Item = Result<Fact, DialogCorpusError>>, DialogSearchTreeError>
    where
        Env: Provider<Get>
            + Provider<ArchiveLoad>
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Holds
            + ConditionalSync,
    {
        self.execute(erased(env)).await
    }

    /// [`Select::execute`], with every row materialized from the scan's
    /// own key parse (see [`Select::execute_owned`]).
    pub async fn execute<'s, S>(
        self,
        store: S,
    ) -> Result<impl Stream<Item = Result<Fact, DialogCorpusError>> + 's, DialogSearchTreeError>
    where
        S: Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
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
