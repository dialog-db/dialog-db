use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, AttributeKey, Datum, DialogArtifactsError, EntityKey, Key,
    KeyViewConstruct, KeyViewMut, MatchCandidate, State, ValueKey,
};
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_effects::memory::Resolve;
use dialog_prolly_tree::{EMPT_TREE_HASH, Entry, Tree};
use dialog_storage::{Blake3Hash, ContentAddressedStorage, DialogStorageError};
use futures_util::Stream;
use std::ops::Range;

use dialog_prolly_tree::DialogProllyTreeError;

use crate::{
    Branch, Index, NetworkedIndex, RemoteSite, RepositoryArchiveExt as _, RepositoryMemoryExt,
    Upstream,
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
            .unwrap_or(EMPT_TREE_HASH)
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
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>>, DialogProllyTreeError>
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
        DialogProllyTreeError,
    >
    where
        S: ContentAddressedStorage<Hash = Blake3Hash, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's,
    {
        // Load the branch's search tree. Tree loading may have to hit
        // the network through `store` (NetworkedIndex) when the root is
        // remote-only, which is why this is async and fallible up front.
        let tree: Index = Tree::from_hash(&self.tree_hash(), &store).await?;

        let selector = self.selector;

        // The tree indexes every datum under three parallel keys:
        // `entity/…`, `attribute/…`, and `value/…`. We pick the prefix
        // that matches the most specific constraint on the selector so
        // `stream_range` can narrow the scan to the minimum key range;
        // any remaining constraints are checked per-entry via
        // `matches_selector`.
        Ok(async_stream::try_stream! {
            if selector.entity().is_some() {
                let start = <EntityKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <EntityKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();
                let stream = tree.stream_range(Range { start, end }, &store);
                tokio::pin!(stream);
                for await item in stream {
                    let entry: Entry<Key, State<Datum>> = item?;
                    // Filter out entries in the range that don't
                    // satisfy the full selector, and skip retracted
                    // entries (`State::Removed`) — only `Added` datums
                    // surface as artifacts.
                    if entry.matches_selector(&selector)
                        && let Entry { value: State::Added(datum), .. } = entry
                    {
                        yield Artifact::try_from(datum)?;
                    }
                }
            } else if selector.value().is_some() {
                let start = <ValueKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <ValueKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();
                let stream = tree.stream_range(Range { start, end }, &store);
                tokio::pin!(stream);
                for await item in stream {
                    let entry: Entry<Key, State<Datum>> = item?;
                    if entry.matches_selector(&selector)
                        && let Entry { value: State::Added(datum), .. } = entry
                    {
                        yield Artifact::try_from(datum)?;
                    }
                }
            } else if selector.attribute().is_some() {
                let start = <AttributeKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <AttributeKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();
                let stream = tree.stream_range(Range { start, end }, &store);
                tokio::pin!(stream);
                for await item in stream {
                    let entry: Entry<Key, State<Datum>> = item?;
                    if entry.matches_selector(&selector)
                        && let Entry { value: State::Added(datum), .. } = entry
                    {
                        yield Artifact::try_from(datum)?;
                    }
                }
            } else {
                // `Constrained` is a type-state marker that guarantees
                // the selector has at least one of entity/value/
                // attribute set, so this branch is unreachable.
                unreachable!("ArtifactSelector will always have at least one field specified")
            };
        })
    }
}
