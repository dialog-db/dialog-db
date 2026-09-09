use crate::repository::source::SourceRef;
use crate::{
    Branch, CommitError, EMPTY_TREE_HASH, Index, NetworkedIndex, PublishError, RemoteSite,
    RepositoryArchiveExt as _, Revision, Snapshot, TreeReference, origin_of,
};
use dialog_artifacts::history::{
    Context, Edition, Origin, RevisionRecord, TreeHistory, Version, context_of, extend_skips,
};
use dialog_artifacts::tree::WriteScope;
use dialog_artifacts::{Datum, DialogArtifactsError, Entity, Instruction, Key, State};
use dialog_capability::{Did, Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::prelude::CatalogExt as _;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, OperatorExt};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::Delta;
use futures_util::Stream;

/// Command that commits a stream of changes (assert/retract) to a branch
/// or a snapshot.
///
/// Created by [`Branch::commit`] or [`Snapshot::commit`]. Execute with
/// `.perform(&env)`.
pub struct Commit<'a, Changes> {
    source: SourceRef<'a>,
    changes: Changes,
    allow_empty: bool,
    canonicalize: bool,
    scope: WriteScope,
    entries: Vec<(Key, State<Datum>)>,
}

impl<'a, Changes> Commit<'a, Changes> {
    pub(crate) fn new(source: impl Into<SourceRef<'a>>, changes: Changes) -> Self {
        Self {
            source: source.into(),
            changes,
            allow_empty: false,
            canonicalize: false,
            scope: WriteScope::Application,
            entries: Vec::new(),
        }
    }

    /// Permit reserved `dialog.*` attributes in the change stream.
    ///
    /// For machinery-written facts (delegation records); application
    /// commits keep the default [`WriteScope::Application`] rejection.
    pub(crate) fn machinery(mut self) -> Self {
        self.scope = WriteScope::Machinery;
        self
    }

    /// Append pre-built machinery entries (blob-index edits) to the same
    /// batch, so they seal, persist, and publish with the commit's data in
    /// one revision. Entries make the commit non-empty even when the change
    /// stream is all no-ops.
    pub(crate) fn with_entries(mut self, entries: Vec<(Key, State<Datum>)>) -> Self {
        self.entries = entries;
        self
    }

    /// Flush the write buffers to the leaves before publishing, so the
    /// revision names the *canonical* tree for its fact set.
    ///
    /// A commit buffers by default: writes land in bounded per-node buffers
    /// instead of reshaping the tree, which is what keeps an interactive commit
    /// cheap. The published root still identifies its content exactly (a node's
    /// hash covers its buffers as well as its links), so a buffered head reads,
    /// diffs, pushes, and verifies like any other.
    ///
    /// What buffering gives up is *canonicality*: two replicas holding the same
    /// facts hash differently if they buffered and flushed at different points.
    /// Nothing breaks, but they no longer recognize each other as equal by root
    /// comparison, so a fast-forward check finds work where there is none.
    ///
    /// Canonicalize when that matters:
    ///
    /// - importing a dataset, so the result is history-independent and two
    ///   importers of the same data converge on the same root;
    /// - at a checkpoint two replicas are expected to agree on bit for bit;
    /// - before a long quiet period, so the stored form is the compact one.
    ///
    /// ```no_run
    /// # use dialog_repository::Branch;
    /// # use dialog_artifacts::Instruction;
    /// # use futures_util::stream;
    /// # async fn example<Env>(branch: &Branch, env: &Env, changes: Vec<Instruction>)
    /// # -> anyhow::Result<()>
    /// # where Env: dialog_capability::Provider<dialog_effects::memory::Resolve> {
    /// # let _ = (branch, env, changes);
    /// // branch.commit(stream::iter(changes)).canonicalize().perform(env).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn canonicalize(mut self) -> Self {
        self.canonicalize = true;
        self
    }

    /// Mint a revision even when the change stream leaves the indexes
    /// untouched (analogous to git's `--allow-empty`).
    ///
    /// By default such a commit is a no-op: the branch keeps its current
    /// revision. With `allow_empty` the revision is minted anyway — its
    /// DAG edge, skip links, and attribute claims land in the tree, so
    /// the lineage advances even though the data did not. Useful for
    /// marking a point in history or forcing a sync point.
    pub fn allow_empty(mut self) -> Self {
        self.allow_empty = true;
        self
    }
}

impl Branch {
    /// Commit a stream of changes to this branch.
    pub fn commit<Changes>(&self, changes: Changes) -> Commit<'_, Changes> {
        Commit::new(self, changes)
    }
}

impl Snapshot {
    /// Commit a stream of changes to this snapshot: the same [`Commit`]
    /// a branch runs. The minted revision is persisted and the snapshot
    /// advances to it; no branch head moves. Mirrors [`Branch::commit`].
    pub fn commit<Changes>(&self, changes: Changes) -> Commit<'_, Changes> {
        Commit::new(self, changes)
    }
}

impl<Changes> Commit<'_, Changes>
where
    Changes: Stream<Item = Instruction> + ConditionalSend,
{
    /// Execute the commit, returning the newly-published [`Revision`].
    ///
    /// Load the line's current search tree, apply every change in the
    /// stream to the three (entity / attribute / value) indexes, then
    /// adopt the new [`Revision`] with the updated logical clock: a
    /// branch publishes it to its revision cell, a snapshot advances its
    /// own head.
    #[tracing::instrument(skip_all, name = "commit")]
    pub async fn perform<Env>(self, env: &Env) -> Result<Revision, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        match self.source {
            SourceRef::Branch(branch) => self.perform_on_branch(branch, env).await,
            SourceRef::Snapshot(snapshot) => self.perform_on_snapshot(snapshot, env).await,
        }
    }

    /// The branch path: build on the checkpointed head, publish CAS'd
    /// against it.
    async fn perform_on_branch<Env>(
        self,
        branch: &Branch,
        env: &Env,
    ) -> Result<Revision, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        // Checkpoint the head: capture the version we build this commit on top
        // of, so the publish below CAS's against it. A concurrent commit or
        // pull that advances the head while we apply changes then makes this
        // publish fail with `VersionMismatch` rather than silently overwriting
        // it — the caller refreshes and retries. See `Cell::checkpoint`.
        let head = branch.revision.checkpoint();
        let base_revision = branch.revision();
        let base_version = branch.revision.edition().map(|edition| edition.version);

        let minted = Mint {
            source: SourceRef::from(branch),
            base: base_revision,
            changes: self.changes,
            entries: self.entries,
            scope: self.scope,
            allow_empty: self.allow_empty,
            canonicalize: self.canonicalize,
        }
        .perform(env, |profile, issuer| {
            branch.commit_identity(profile, issuer)
        })
        .await?;

        let Minted {
            revision,
            record,
            context,
        } = match minted {
            // A no-op verdict is only true of the head it was judged
            // against, and this early return never reaches the publish CAS
            // below — so re-read the head before reporting "nothing to do".
            // If another writer advanced it, the same instructions may not be
            // no-ops against the current head (a re-assertion of a value the
            // head has since retracted, for example): fail with the same
            // `VersionMismatch` any other stale write gets, so the caller
            // refreshes and retries against the fresh snapshot.
            Outcome::Unchanged(base) => {
                branch.revision.resolve().perform(env).await?;
                let actual = branch.revision.edition().map(|edition| edition.version);
                if actual != base_version {
                    return Err(PublishError::VersionMismatch {
                        expected: base_version,
                        actual,
                    }
                    .into());
                }
                return Ok(base);
            }
            Outcome::Minted(minted) => *minted,
        };

        head.publish(revision.clone(), env).await?;

        // Seed the memos with what was just published: the next commit's
        // skip-table walk starts at this very record, and later pulls
        // through this handle answer the context from memory. Seeded only
        // once the head has actually advanced — a publish that lost its
        // CAS is re-minted under the same version with a different
        // parent, and a memo entry from the losing attempt would be wrong.
        let version = revision.version();
        branch.records().insert(version, record);
        branch.contexts().insert(version, context);

        Ok(revision)
    }

    /// The snapshot path: build on the head the snapshot holds, mint on
    /// the snapshot's own lineage, and advance the head in place.
    async fn perform_on_snapshot<Env>(
        self,
        snapshot: &Snapshot,
        env: &Env,
    ) -> Result<Revision, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        // The head this commit builds on and the line it mints on, read
        // together. The lineage is inherited from the snapshot's earlier
        // commits, or allocated now for its first: random rather than
        // derived, because two clones of one base must not share it.
        let (base, lineage) = snapshot.head();
        let lineage = match lineage {
            Some(lineage) => lineage,
            None => Entity::new()?,
        };

        let minted = Mint {
            source: SourceRef::from(snapshot),
            base: Some(base.clone()),
            changes: self.changes,
            entries: self.entries,
            scope: self.scope,
            allow_empty: self.allow_empty,
            canonicalize: self.canonicalize,
        }
        .perform(env, |_, issuer| {
            (lineage.clone(), origin_of(&lineage, issuer))
        })
        .await?;

        let Minted {
            revision,
            record,
            context,
        } = match minted {
            Outcome::Unchanged(base) => return Ok(base),
            Outcome::Minted(minted) => *minted,
        };

        // Adopt the revision: the in-memory CAS against the head this
        // commit built on, refused if a commit through this same handle
        // advanced it in the meantime.
        snapshot.advance(&base, revision.clone(), lineage)?;

        // Seed the memos once the head has moved, as the branch path
        // does: the next commit's skip-table walk starts at this record,
        // and the context memo answers the successor's context without
        // an ancestry walk.
        let version = revision.version();
        snapshot.caches().records.insert(version, record);
        snapshot.caches().contexts.insert(version, context);

        Ok(revision)
    }
}

/// A revision minted by [`Mint`] and persisted, but not yet adopted:
/// the branch adopts it through its head cell, a snapshot by value.
pub(crate) struct Minted {
    /// The signed head, naming the persisted tree root.
    pub(crate) revision: Revision,
    /// Its signed in-tree record, for the verified-record memo.
    pub(crate) record: RevisionRecord,
    /// Its causal context, for the context memo.
    pub(crate) context: Context,
}

/// How a [`Mint`] came out.
pub(crate) enum Outcome {
    /// The batch left the indexes untouched and `allow_empty` was not
    /// asked for: nothing was minted or persisted, and the line keeps
    /// the revision it had. Only possible when there was a base
    /// revision to keep.
    Unchanged(Revision),
    /// A revision was minted and its blocks persisted.
    Minted(Box<Minted>),
}

/// The commit procedure shared by every line: apply a change batch to
/// the tree at `base`, mint the successor revision under the line's
/// identity, sign it, and persist its blocks. What it deliberately does
/// not do is adopt the result — a branch publishes it CAS'd against
/// its head cell, a snapshot takes it by value — so the caller owns the
/// step that makes the revision visible.
pub(crate) struct Mint<'a, Changes> {
    /// The line the commit builds on: where its store, caches, and
    /// remote fallback come from.
    pub(crate) source: SourceRef<'a>,
    /// The revision the batch applies on top of, captured by the caller
    /// (a branch checkpoints it for its CAS).
    pub(crate) base: Option<Revision>,
    /// The instruction stream to apply.
    pub(crate) changes: Changes,
    /// Pre-built machinery entries riding the same batch. See
    /// [`Commit::with_entries`].
    pub(crate) entries: Vec<(Key, State<Datum>)>,
    /// Which attributes the stream may write. See [`Commit::machinery`].
    pub(crate) scope: WriteScope,
    /// Mint even when the batch changes nothing. See
    /// [`Commit::allow_empty`].
    pub(crate) allow_empty: bool,
    /// Flush buffers to the leaves first. See [`Commit::canonicalize`].
    pub(crate) canonicalize: bool,
}

impl<Changes> Mint<'_, Changes>
where
    Changes: Stream<Item = Instruction> + ConditionalSend,
{
    /// Run the commit procedure. `line` names the entity and origin the
    /// revision is minted under, given the profile and issuer the
    /// environment identifies as — a branch's content-derived branch
    /// entity, a snapshot's own lineage.
    pub(crate) async fn perform<Env>(
        self,
        env: &Env,
        line: impl FnOnce(&Did, &Did) -> (Entity, Origin),
    ) -> Result<Outcome, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let source = self.source;
        let base_revision = self.base;
        let changes = self.changes;

        // If the line tracks a remote upstream, commits must be able
        // to read remote-only blocks on demand (pull only merges the
        // tree metadata, not every block). `NetworkedIndex` falls back
        // to the remote when a block is missing locally. A remote that
        // fails to LOAD is not silently dropped: on a partial replica
        // that store is the only place by-reference blocks can hydrate
        // from, so the failure is carried into the fallback and surfaces
        // — with its cause — on the first read that needed it, while a
        // commit every block of which is local proceeds untouched.
        let remote = source.fallback(env).await;
        let mut store = NetworkedIndex::new(env, source.archive().index(), remote);

        // Discover who we are up front: the revision is attributed to the
        // profile / operator, and the commit's `Version` — the identifier
        // every datum and history record it writes is tagged with — derives
        // from the issuer and the line. The line comes from the caller,
        // not the identity chain.
        let authority = Identify.perform(env).await?;
        let issuer = authority.did();
        let profile = authority.profile().clone();

        let edition = base_revision
            .as_ref()
            .map(|base| base.edition.successor())
            .unwrap_or(Edition::GENESIS);
        let (line_entity, origin) = line(&profile, &issuer);
        let version = Version::new(origin, edition);

        // Walk forward from the base revision's tree root, or from the
        // empty tree if the line has no commits yet.
        let base_tree_hash = base_revision
            .as_ref()
            .map(|rev| *rev.tree.hash())
            .unwrap_or(EMPTY_TREE_HASH);

        // Read through the line's shared node cache: the commit's
        // supersession scans and history reads then hit blocks earlier
        // commits and queries already fetched (and blocks the persist below
        // seeds), instead of re-fetching everything into a cache that dies
        // with this commit.
        let mut tree =
            Index::from_hash_with_cache(NodeHash::from(base_tree_hash), source.node_cache());

        // Drain the change stream into the tree. EAV/AEV/VAE writes,
        // cardinality-one supersession, retraction — and, because the
        // writes are version-tagged, each instruction's history record,
        // whose cause lists the versions of the exact claims the write
        // superseded — all live in the shared instruction semantics so the
        // key layout stays uniform. Data and history land in the same tree:
        // one root covers both.
        // Writes go through the node buffers: a commit appends its ops instead
        // of rebuilding and re-hashing the leaves they belong in, and the
        // reshape happens later, amortized, when a buffer overflows. The
        // published root still identifies its content exactly (a node's hash
        // covers its buffers), so the head, pull, and push paths are unchanged.
        //
        // Nothing is persisted yet: the batch stays open so the revision
        // record minted below (which needs the batch's outcome and signs over
        // its content, so it cannot ride the instruction stream) is appended
        // to the SAME buffered tree, and one seal below covers data and
        // record together. A no-op batch is simply dropped, leaving the delta
        // untouched and the tree root unchanged.
        //
        // `canonicalize()` on the builder flushes to the leaves at seal time,
        // for callers that want the history-independent form (see
        // `Commit::canonicalize`).
        let mut delta = Delta::zero();
        let batch = dialog_artifacts::BufferedBatch::apply_reusing(
            source.spine(),
            &tree,
            &mut store,
            Some(version),
            changes,
            self.scope,
        )
        .await?;
        // Machinery entries count as changes: a commit carrying only a
        // blob-index edit still advances the head.
        let changed = batch.changed() || !self.entries.is_empty();

        // A batch that left the indexes untouched (e.g. a transaction
        // re-asserting metadata that is already in place) is a no-op:
        // keep the current revision rather than minting one that differs
        // only by edition (unless `allow_empty` asks for one). Only a
        // line with no revision at all still publishes, to establish
        // its genesis.
        if !changed
            && !self.allow_empty
            && let Some(base) = base_revision
        {
            return Ok(Outcome::Unchanged(base));
        }

        // Mint the revision (the placeholder tree root is replaced below,
        // after its own records are in the tree) and record its DAG edge on
        // the line entity, its skip links, plus its attribute claims on the
        // revision entity, in the same batch delta. None of those records
        // depend on the final root — a root cannot appear inside itself.
        //
        // The skip table (logarithmic leaps through the revision DAG for
        // `common_ancestor` — see `dialog_artifacts::history::extend_skips`)
        // is lifted from the parent's recorded table, read out of the base
        // tree through the line's shared node cache.
        let parent = base_revision.as_ref().map(Revision::version);
        let skips = match &parent {
            Some(parent) => {
                let history = TreeHistory::from_root_with_cache(
                    &base_tree_hash,
                    store.clone(),
                    source.node_cache(),
                )
                .with_record_cache(source.records());
                extend_skips(&history, parent).await?
            }
            None => Vec::new(),
        };

        // The new head's causal context: the parent's plus this commit's
        // own version. Sourced from the parent head's published context,
        // the line's memo, or (once, for lineages minted before heads
        // carried contexts) the ancestry walk — so every head published
        // from here on carries its watermark for peers to read.
        let contexts = source.contexts();
        let base_context = base_revision.as_ref().and_then(|base| base.context.clone());
        let context = {
            let mut context = match (&parent, base_context) {
                (None, _) => Context::new(),
                (Some(_), Some(context)) => context,
                (Some(parent), None) => match contexts.cached(parent).await {
                    Some(context) => context,
                    None => {
                        let history = TreeHistory::from_root_with_cache(
                            &base_tree_hash,
                            store.clone(),
                            source.node_cache(),
                        )
                        .with_record_cache(source.records());
                        context_of(parent, &history).await?
                    }
                },
            };
            context.record(version);
            context
        };

        let mut revision = match base_revision {
            Some(base) => base.advance(TreeReference::default(), line_entity.clone(), issuer),
            None => Revision::new(TreeReference::default(), line_entity.clone(), issuer),
        };
        debug_assert_eq!(revision.version(), version);
        // Sign the record before it enters the tree: the issuer's signature
        // covers everything the revision states about itself, and readers
        // (`TreeHistory::revision_record`) refuse records that don't verify
        // against the slot they were found at.
        let mut record = revision.record(&profile, parent.into_iter().collect(), skips);
        record.signature = Attest::new(record.payload()?).perform(env).await?;
        debug_assert_eq!(record.version(), version);
        // The record's key carries its value through the tree's own
        // inline-vs-spill threshold, so build its entries under the manifest
        // the batch captured from the tree rather than assuming the default.
        // The entries are appended to the batch's still open buffered tree:
        // they ride the same buffered write as the data, so the record costs
        // a buffer append instead of a second canonical spine-to-leaf edit.
        let entries = record.entries(batch.manifest())?;
        // The caller's machinery entries (blob-index edits) ride the same
        // batch as the revision record, so one seal covers data, record,
        // and entries together.
        let batch = batch.record(&store, self.entries).await?;
        let batch = batch.record(&store, entries).await?;

        // ONE seal covers the data writes and the record entries, into the
        // batch delta (flushing buffers to the leaves first when the caller
        // asked to canonicalize).
        tree = batch.seal(&store, &mut delta, self.canonicalize).await?;

        // Persist the tree's pending nodes before referencing the root in
        // a revision; a revision must only point at durable blocks. The
        // whole flush travels as one `Import` invocation; block buffers are
        // reference-counted, so nothing is copied on the way in, and
        // providers with native batching persist it in a single round trip
        // (one IndexedDB transaction).
        source
            .archive()
            .index()
            .import(delta.flush().map(|(_, buffer)| buffer))
            .perform(env)
            .await
            .map_err(DialogArtifactsError::from)?;

        revision.tree = TreeReference::from(*tree.root().as_bytes());
        revision.context = Some(context.clone());
        // Sign the head last: only now are the tree root and context
        // final, and the head signature is what binds them to the issuer
        // (the in-tree record cannot contain the root of the tree it
        // lives in). A replica adopting this head verifies it first —
        // see `Pull`.
        revision.signature = Attest::new(revision.payload()).perform(env).await?;

        Ok(Outcome::Minted(Box::new(Minted {
            revision,
            record,
            context,
        })))
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::TreeReference;
    use crate::helpers::test_repo;
    use anyhow::Result;
    use dialog_operator::helpers::test_operator_with_profile;

    use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
    use futures_util::{StreamExt, stream};

    /// Two commits in sequence (no refresh between) with different entities on
    /// the same attribute: both facts must survive.
    ///
    /// Regression for the variable-key `Replace` supersede-scan bug: the
    /// scan's upper bound lost its entity (the max sentinel's `0xFF` filler
    /// made the bound unparseable after `set_entity`, so `set_attribute`'s
    /// rebuild fell back to max parts), widening the scan to every entity
    /// sorting after the new one — here the first-committed `alice`, whose
    /// fact was deleted as a "superseded prior".
    #[dialog_common::test]
    async fn it_keeps_both_facts_across_two_commits() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // alice sorts AFTER bob; alice is committed first, then bob.
        let alice = "did:key:z6MkQmQKzPsjyUz49pvaxYdiiZEuQXyNqeBkS88GTrvqnov";
        let bob = "did:key:z6MkDiL3ZaJ4V7VSdQruLenZLA4RNbu6cErR5m8K5Wj99wTF";

        branch
            .commit(stream::iter(vec![Instruction::Replace(Artifact {
                the: "person/name".parse()?,
                of: alice.parse()?,
                is: Value::String("Alice".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        branch
            .commit(stream::iter(vec![Instruction::Replace(Artifact {
                the: "person/name".parse()?,
                of: bob.parse()?,
                is: Value::String("Bob".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let results: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().the("person/name".parse()?))
            .to_owned()
            .perform(&operator)
            .await?
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;

        assert_eq!(results.len(), 2, "both facts must survive two commits");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_and_selects() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: Value::String("Alice".to_string()),
            cause: None,
        };

        let instructions = stream::iter(vec![Instruction::Assert(artifact.clone())]);

        let revision = branch.commit(instructions).perform(&operator).await?;
        assert_ne!(revision.tree, TreeReference::default());

        // Select should find the artifact
        let selector = ArtifactSelector::new().the("user/name".parse()?);
        let stream = branch
            .claims()
            .select(selector)
            .to_owned()
            .perform(&operator)
            .await?;
        tokio::pin!(stream);

        let results: Vec<_> = stream.filter_map(|r| async { r.ok() }).collect().await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].the, artifact.the);
        assert_eq!(results[0].is, artifact.is);

        Ok(())
    }

    /// A commit made through one handle, while another handle to the same
    /// branch commits from a stale snapshot, must not be silently lost — the
    /// stale commit fails loudly, and refreshing then re-committing reconciles.
    ///
    /// `commit` checkpoints the head it builds on, then publishes CAS'd against
    /// that version. Two independent handles (the shape the service worker hits
    /// once `/transact` no longer serializes writes under a single lock) both
    /// snapshot the same head; one commits and advances storage, so the other's
    /// publish CAS fails with a `VersionMismatch` rather than overwriting the
    /// first commit with a tree built from the now-stale snapshot. Recovery is
    /// refresh + re-commit.
    #[dialog_common::test]
    async fn it_fails_a_commit_racing_another_then_reconciles_on_refresh() -> Result<()> {
        use crate::PublishError;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // Two independent handles to the same branch — both snapshot the same
        // (empty) head at open time.
        let writer_a = repo.branch("main").open().perform(&operator).await?;
        let writer_b = repo.branch("main").open().perform(&operator).await?;

        // A commits first, advancing the head in storage. B's cache is now
        // stale.
        writer_a
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:a".parse()?,
                is: Value::String("Alice".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        // B commits from its stale snapshot — must fail loudly, not silently
        // drop A's commit.
        let raced = writer_b
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:b".parse()?,
                is: Value::String("Bob".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await;
        assert!(
            matches!(
                raced,
                Err(crate::CommitError::Publish(
                    PublishError::VersionMismatch { .. }
                ))
            ),
            "a commit racing another must fail with a version mismatch; got {raced:?}"
        );

        // Recovery: refresh B's view of the head, then re-commit.
        writer_b.refresh(&operator).await?;
        writer_b
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:b".parse()?,
                is: Value::String("Bob".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        // Both commits survive.
        let fresh = repo.branch("main").open().perform(&operator).await?;
        let results: Vec<_> = fresh
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .to_owned()
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            results.len(),
            2,
            "both racing commits must survive recovery"
        );

        Ok(())
    }
}

#[cfg(test)]
mod history_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use anyhow::Result;
    use dialog_operator::helpers::test_operator_with_profile;

    use dialog_artifacts::history::{
        Causality, History as _, HistorySelector, causality, common_ancestor,
    };
    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::{TryStreamExt as _, stream};

    fn title(of: &str, value: &str) -> Artifact {
        Artifact {
            the: "post/title".parse().unwrap(),
            of: of.parse().unwrap(),
            is: Value::String(value.to_string()),
            cause: None,
        }
    }

    /// Commits record claim lineage into the history index: a replacement's
    /// record supersedes the claim it replaced, detectable via the tiered
    /// conflict detection, and every revision's DAG edge is recorded.
    #[dialog_common::test]
    async fn it_records_claim_lineage_across_commits() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let first = branch
            .commit(stream::iter(vec![Instruction::Assert(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;

        // Refresh so the next commit builds on the published head.
        branch.refresh(&operator).await?;
        let second = branch
            .commit(stream::iter(vec![Instruction::Replace(title(
                "post:1", "Hi",
            ))]))
            .perform(&operator)
            .await?;

        branch.refresh(&operator).await?;
        let history = branch.history(&operator);

        // Both claims are recorded, and the replacement's cause lists the
        // version of the claim it superseded.
        let records = history
            .select(HistorySelector::All)
            .try_collect::<Vec<_>>()
            .await?;
        let claims: Vec<_> = records
            .iter()
            .filter(|(_, record)| record.claim().the.to_string() == "post/title")
            .collect();
        assert_eq!(claims.len(), 2);
        let (first_version, first_record) = claims[0];
        let (second_version, second_record) = claims[1];
        assert_eq!(*first_version, first.version());
        assert_eq!(*second_version, second.version());
        assert!(first_record.claim().cause.is_genesis());
        assert!(second_record.claim().cause.contains(&first.version()));

        // Tier 1 conflict detection over the branch's durable history.
        assert_eq!(
            causality(
                (second_record.claim(), second_version),
                (first_record.claim(), first_version),
                &history
            )
            .await?,
            Causality::Supersedes
        );

        // Each revision's record is retrievable from the tree — parents,
        // attribution, and lineage as one atomic fact on the revision
        // entity.
        let record = history
            .revision_record(&second.version())
            .await?
            .expect("the revision record is retrievable");
        assert!(history.revision_record(&first.version()).await?.is_some());
        assert_eq!(record.branch, second.branch.clone());
        assert_eq!(record.parents, vec![first.version()]);
        assert_eq!(record.issuer, second.issuer.to_string());
        assert_eq!(record.authority, profile.did().to_string());
        assert_eq!(
            common_ancestor(&second.version(), &first.version(), &history).await?,
            Some(first.version())
        );

        // A third commit records its skip table: the level-1 link leaps two
        // first-parent steps back, from the third revision to the first.
        branch.refresh(&operator).await?;
        let third = branch
            .commit(stream::iter(vec![Instruction::Replace(title(
                "post:1", "Hello",
            ))]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let history = branch.history(&operator);
        let record = history
            .revision_record(&third.version())
            .await?
            .expect("the third revision's record is retrievable");
        assert_eq!(record.skips, vec![first.version()]);

        Ok(())
    }

    /// A commit signs what it publishes: the head revision verifies as
    /// issued by the session's operator key, and any tampering with a
    /// signed field — the tree root above all, since it is what a replica
    /// adopts on pull — breaks verification.
    #[dialog_common::test]
    async fn it_signs_the_published_head() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let revision = branch
            .commit(stream::iter(vec![Instruction::Assert(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;

        assert_eq!(revision.issuer, operator.did());
        revision.verify()?;

        let mut swapped_root = revision.clone();
        swapped_root.tree = crate::TreeReference::from([9u8; 32]);
        assert!(
            swapped_root.verify().is_err(),
            "a head with a swapped tree root must not verify"
        );

        // Reattributing the head to another principal (here the profile,
        // a real key the operator does not hold) fails too: the signature
        // is not by the newly-named issuer.
        let mut reattributed = revision.clone();
        reattributed.issuer = profile.did();
        assert!(
            reattributed.verify().is_err(),
            "a reattributed head must not verify"
        );

        Ok(())
    }

    /// The `dialog.` namespace is reserved for version-control machinery:
    /// user instructions cannot assert, replace, or retract under it, so
    /// lineage cannot be corrupted through the ordinary write path.
    #[dialog_common::test]
    async fn it_rejects_writes_to_the_reserved_dialog_namespace() -> Result<()> {
        use dialog_artifacts::DialogArtifactsError;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let forged = Artifact {
            the: "dialog.db/revision".parse()?,
            of: "forged:revision".parse()?,
            is: Value::String("lies".to_string()),
            cause: None,
        };
        for instruction in [
            Instruction::Assert(forged.clone()),
            Instruction::Replace(forged.clone()),
            Instruction::Retract(forged),
        ] {
            let result = branch
                .commit(stream::iter(vec![instruction]))
                .perform(&operator)
                .await;
            assert!(
                matches!(
                    result,
                    Err(crate::CommitError::Artifact(
                        DialogArtifactsError::ReservedAttribute(_)
                    ))
                ),
                "writes to the reserved namespace must be refused: {result:?}"
            );
        }

        Ok(())
    }

    /// A commit whose instruction stream is empty (or entirely no-op)
    /// leaves the branch exactly where it was: same revision, same
    /// edition, no new history.
    #[dialog_common::test]
    async fn it_keeps_the_revision_for_an_empty_commit() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let first = branch
            .commit(stream::iter(vec![Instruction::Assert(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let unchanged = branch
            .commit(stream::iter(Vec::<Instruction>::new()))
            .perform(&operator)
            .await?;
        assert_eq!(unchanged, first, "an empty commit mints no revision");
        assert_eq!(branch.revision(), Some(first.clone()));

        // `allow_empty` mints the revision anyway: the lineage advances —
        // DAG edge and all — even though the data did not.
        let empty = branch
            .commit(stream::iter(Vec::<Instruction>::new()))
            .allow_empty()
            .perform(&operator)
            .await?;
        assert_eq!(empty.edition, first.edition.successor());
        assert_eq!(branch.revision(), Some(empty.clone()));
        assert_ne!(
            empty.tree, first.tree,
            "the empty revision's own records still land in the tree"
        );

        branch.refresh(&operator).await?;
        let history = branch.history(&operator);
        let record = history
            .revision_record(&empty.version())
            .await?
            .expect("the empty revision's record is retrievable");
        assert!(record.parents.contains(&first.version()));

        Ok(())
    }

    /// A commit whose only instructions retract facts that were never
    /// asserted is a no-op end to end: retract-of-absent changes no index,
    /// so the branch keeps its revision and mints no new edition.
    #[dialog_common::test]
    async fn it_keeps_the_revision_when_a_commit_only_retracts_absent_facts() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let first = branch
            .commit(stream::iter(vec![Instruction::Assert(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let unchanged = branch
            .commit(stream::iter(vec![Instruction::Retract(title(
                "post:9", "Never",
            ))]))
            .perform(&operator)
            .await?;
        assert_eq!(
            unchanged, first,
            "a commit that only retracts absent facts mints no revision"
        );
        assert_eq!(branch.revision(), Some(first.clone()));

        Ok(())
    }

    /// A no-op commit from a stale handle must not silently succeed: the
    /// instructions were judged no-op against a snapshot another writer
    /// has since superseded, so "nothing to do" may be wrong at the
    /// current head. Here the stale handle re-asserts a value the head
    /// has retracted in the meantime — succeeding silently would lose
    /// the re-assertion.
    ///
    /// The no-op verdict is reported only after re-reading the head: a
    /// resolve + compare rather than a same-value publish, so concurrent
    /// no-ops don't bump the cell version and fail each other spuriously,
    /// while a genuinely stale snapshot surfaces the usual
    /// `VersionMismatch` and the caller refreshes and retries.
    #[dialog_common::test]
    async fn it_does_not_treat_a_stale_snapshot_as_a_noop() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let seed = repo.branch("main").open().perform(&operator).await?;
        seed.commit(stream::iter(vec![Instruction::Assert(title(
            "post:1", "Hej",
        ))]))
        .perform(&operator)
        .await?;

        // Both handles snapshot the head with the title asserted.
        let retractor = repo.branch("main").open().perform(&operator).await?;
        let reasserter = repo.branch("main").open().perform(&operator).await?;

        // One handle retracts the title, advancing the head.
        retractor
            .commit(stream::iter(vec![Instruction::Retract(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;

        // The other re-asserts the same value from its stale snapshot,
        // where the write looks like a cardinality-one no-op. It must not
        // report success while leaving the title retracted at the head.
        let raced = reasserter
            .commit(stream::iter(vec![Instruction::Replace(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await;

        use futures_util::StreamExt as _;
        let head = repo.branch("main").load().perform(&operator).await?;
        let reasserted = !head
            .claims()
            .select(
                dialog_artifacts::ArtifactSelector::new()
                    .the("post/title".parse()?)
                    .of("post:1".parse()?),
            )
            .to_owned()
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .is_empty();

        assert!(
            raced.is_err() || reasserted,
            "a stale no-op either fails loudly (so the caller refreshes \
             and retries) or lands the re-assertion; it silently did \
             neither: {raced:?}"
        );

        Ok(())
    }

    /// Data committed through a branch is tagged with the revision's
    /// version, so later commits can derive what they supersede.
    #[dialog_common::test]
    async fn it_tags_committed_data_with_the_revision_version() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let revision = branch
            .commit(stream::iter(vec![Instruction::Assert(title(
                "post:1", "Hej",
            ))]))
            .perform(&operator)
            .await?;

        // Read the datum back through the artifact tree and check the tag.
        use crate::{Index, NetworkedIndex, RepositoryArchiveExt as _};
        use dialog_artifacts::tree::ArtifactTreeExt as _;
        use dialog_common::Blake3Hash as NodeHash;

        let store = NetworkedIndex::new(&operator, branch.archive().index(), None);
        let tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
        let data = tree
            .select_data(store, &"post:1".parse()?, &"post/title".parse()?)
            .await?;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].version, Some(revision.version()));

        Ok(())
    }
}
