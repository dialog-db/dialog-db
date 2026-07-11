use dialog_artifacts::DialogArtifactsError;
use dialog_artifacts::tree::ArtifactTreeExt as _;
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::CatalogExt as _;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, OperatorExt};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::{ContentAddressedStorage as TreeStorage, Delta};

use crate::{
    Branch, Checkpoint, EMPTY_TREE_HASH, Index, NetworkedIndex, PublishError, PullError,
    RemoteSite, RepositoryArchiveExt as _, RepositoryMemoryExt, Revision, TreeReference, Upstream,
    UpstreamBranch,
};

/// Command struct for pulling from upstream (auto-dispatches local/remote).
pub struct Pull<'a> {
    branch: &'a Branch,
    from: Option<Upstream>,
}

impl<'a> Pull<'a> {
    fn new(branch: &'a Branch) -> Self {
        Self { branch, from: None }
    }

    /// Pull from the given branch instead of the default upstream.
    ///
    /// Accepts either a `&Branch` or a `&RemoteBranch` — the same inputs as
    /// [`Branch::set_upstream`]. If the target is already tracked, its
    /// recorded sync base drives the merge; otherwise the merge runs from
    /// the empty base (correct, just unable to skip anything) and a
    /// successful pull starts tracking the target — without changing the
    /// default upstream — so the next pull from it is incremental.
    pub fn from(mut self, source: impl Into<UpstreamBranch>) -> Self {
        self.from = Some(Upstream::from(source.into()));
        self
    }
}

impl Branch {
    /// Pull from the configured upstream.
    ///
    /// Targets the default upstream; chain [`Pull::from`] to pull from
    /// another tracked (or brand-new) upstream instead.
    pub fn pull(&self) -> Pull<'_> {
        Pull::new(self)
    }
}

impl<'a> Pull<'a> {
    /// Execute the pull operation: [`prepare`](Self::prepare) the merge, then
    /// [`commit`](PreparedPull::commit) it.
    ///
    /// The one-shot form. To hold an exclusive lock over only the (instant)
    /// cell-advancing step while the (network-bound) fetch + rebase run
    /// lock-free, drive the two phases separately:
    ///
    /// ```ignore
    /// let prepared = branch.pull().prepare(&env).await?; // fetch + rebase, no cell writes
    /// let revision = prepared.commit(&env).await?;       // advance the cells
    /// ```
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, PullError>
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
        self.prepare(env).await?.commit(env).await
    }

    /// Phase one: fetch the upstream, rebase local changes onto it, and persist
    /// the merged tree's blocks — **without** writing any branch cell.
    ///
    /// All the network and CPU work lives here (resolve/fetch upstream,
    /// differentiate, integrate, import), so a caller can run it under a shared
    /// lock concurrently with everything else. The returned [`PreparedPull`]
    /// carries the merged revision and a checkpoint of the head it rebased on;
    /// [`PreparedPull::commit`] does the instant cell advance and can be run
    /// under a brief exclusive lock.
    pub async fn prepare<Env>(self, env: &Env) -> Result<PreparedPull<'a>, PullError>
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
        let branch = self.branch;

        // Select the upstream entry to pull from: the default when no
        // explicit source was given, otherwise the tracked entry for that
        // target — or, for a target not tracked yet, a fresh entry whose
        // empty sync base makes the merge run from scratch.
        let upstreams = branch.upstreams();
        let upstream = match self.from {
            None => upstreams.default_upstream().cloned().ok_or_else(|| {
                PullError::BranchHasNoUpstream {
                    branch: branch.name().to_string(),
                }
            })?,
            Some(target) => {
                if let Upstream::Local { branch: name, .. } = &target
                    && name == branch.name()
                {
                    return Err(PullError::UpstreamIsItself {
                        branch: branch.name().to_string(),
                    });
                }
                upstreams.find(&target).cloned().unwrap_or(target)
            }
        };

        // Resolve the upstream's current revision and, when the
        // upstream is remote, keep a handle so the merge can fall back
        // to the remote archive for blocks that aren't local.
        let (upstream_revision, remote) = match &upstream {
            Upstream::Local { branch: id, .. } => {
                let upstream_branch = branch
                    .subject()
                    .branch(id.clone())
                    .load()
                    .perform(env)
                    .await?;
                (upstream_branch.revision(), None)
            }
            Upstream::Remote {
                remote: name,
                branch: branch_name,
                ..
            } => {
                let remote = branch
                    .subject()
                    .remote(name.clone())
                    .load()
                    .perform(env)
                    .await?;
                let upstream = remote
                    .branch(branch_name.clone())
                    .open()
                    .perform(env)
                    .await?;
                (upstream.fetch().perform(env).await?, Some(remote))
            }
        };

        // Upstream has never received a revision yet — nothing to
        // merge in, so the pull is a no-op.
        let Some(upstream_revision) = upstream_revision else {
            return Ok(PreparedPull::NoOp);
        };

        // The trust boundary: this head was minted elsewhere. Before
        // adopting its tree (fast-forward) or merging it in, check that
        // the signature is the named issuer's — a forged or tampered head
        // (wrong tree root, reattributed issuer, adjusted edition) is
        // rejected here, before any of its blocks are walked.
        upstream_revision.verify()?;

        // `base` is the upstream tree at our last sync point with this
        // particular upstream (the divergence marker). If it equals the
        // upstream's current tree, the upstream hasn't moved and there's
        // nothing to pull.
        let base = upstream.tree().clone();

        if base == upstream_revision.tree {
            return Ok(PreparedPull::NoOp);
        }

        // Checkpoint the head cell up front, capturing the version we read the
        // local revision at. The merge below is computed from this snapshot;
        // the commit phase publishes through this checkpoint, CAS'ing against
        // *this* version. So a commit that advances the head between now and
        // the cell write makes that publish fail rather than silently adopt the
        // new version and drop the commit (see `Cell::checkpoint`).
        let head = branch.revision.checkpoint();
        let local_revision = branch.revision();
        let local_tree_hash = local_revision
            .as_ref()
            .map(|revision| *revision.tree.hash())
            .unwrap_or(EMPTY_TREE_HASH);

        // `NetworkedIndex` reads from the local archive first and,
        // when the upstream is remote, falls back to the remote
        // archive for blocks that haven't been replicated. With
        // `remote: None` it degrades to a plain local index.
        let mut store = NetworkedIndex::new(env, branch.archive().index(), remote);

        // The three trees: last-sync base, local current, and the
        // upstream revision we're merging in. Hydration is lazy; blocks
        // load on demand as the differential walks them.
        let base_tree = Index::from_hash(NodeHash::from(*base.hash()));
        let local = Index::from_hash(NodeHash::from(local_tree_hash));
        let mut merged = Index::from_hash(NodeHash::from(*upstream_revision.tree.hash()));

        // Replay local changes (base → local) on top of the upstream
        // tree to produce the merged tree. The differential only reads
        // blocks on paths where base and local actually differ.
        let tree_store = TreeStorage::new(TreeStorageBridge(store.clone()));
        let local_changes = base_tree.differentiate(&local, &tree_store, &tree_store);
        let mut delta = Delta::zero();
        merged = Box::pin(merged.edit().integrate(local_changes, &tree_store))
            .await?
            .persist(&mut delta)?;

        let merged_tree = TreeReference::from(*merged.root().as_bytes());

        let new_revision = match local_revision {
            // Merging produced the upstream tree verbatim (fast-forward):
            // adopt the upstream revision — there's nothing novel to
            // attribute. History lives in the same tree, so trees being
            // identical means histories are identical too: any novel local
            // record would have made the roots differ.
            _ if merged_tree == upstream_revision.tree => upstream_revision.clone(),
            // Branch has no prior revision; adopt the upstream
            // revision directly (its identity still applies).
            None => upstream_revision.clone(),
            // Real three-way merge: mint a revision attributed to the
            // current authority combining both sides. The merged tree
            // already unions the two sides' recorded history (the local
            // side's records rode the differential like any other entries);
            // the merge's own DAG edge and attribute claims — cause listing
            // both parents — are recorded on top, so conflict detection
            // keeps working across the sync boundary. The placeholder tree
            // root is replaced once those records are in the tree.
            Some(local) => {
                let authority = Identify.perform(env).await?;
                let mut revision = local.merge(
                    &upstream_revision,
                    TreeReference::default(),
                    branch.of().clone(),
                    branch.name(),
                    authority.did(),
                    authority.profile().clone(),
                );
                // A merge records no skip table: a skip chain must never
                // cross a revision with more than one parent, or leaping
                // it would lose the ancestry entering through the other
                // parent (see `dialog_artifacts::history::skip`). Sign the
                // record before it enters the tree, and the head once the
                // merged root is final — same order as `Commit`.
                let mut record = revision.record(
                    vec![local.version(), upstream_revision.version()],
                    Vec::new(),
                );
                record.signature = Attest::new(record.payload()?).perform(env).await?;
                merged
                    .record(&mut store, &mut delta, record.entries()?)
                    .await?;
                revision.tree = TreeReference::from(*merged.root().as_bytes());
                revision.signature = Attest::new(revision.payload()).perform(env).await?;

                revision
            }
        };

        // Persist the merged tree's pending nodes to the local archive
        // before referencing its root in a revision. The whole flush
        // travels as one `Import` invocation: block buffers are
        // reference-counted (nothing is copied on the way in) and
        // providers with native batching persist it in a single round
        // trip.
        branch
            .archive()
            .index()
            .import(delta.flush().map(|(_, buffer)| buffer))
            .perform(env)
            .await
            .map_err(DialogArtifactsError::from)?;

        Ok(PreparedPull::Merged(Box::new(Merged {
            branch,
            head,
            new_revision,
            sync: upstream.with_tree(upstream_revision.tree),
            base,
        })))
    }
}

/// A rebased pull awaiting its cell advance — the output of
/// [`Pull::prepare`].
///
/// All the network + CPU work is already done and the merged tree's blocks are
/// persisted locally; [`commit`](Self::commit) does only the (instant) cell
/// publishes. Splitting the two lets a caller hold an exclusive lock over just
/// the cell-advancing step while the prepare ran lock-free.
pub enum PreparedPull<'a> {
    /// Nothing to pull — upstream is empty or hasn't moved since the last sync.
    /// `commit` is a no-op returning `Ok(None)`.
    NoOp,
    /// A merge to land: advance the head to `new_revision` and the sync-base
    /// marker to `upstream_tree`. Boxed so the no-op variant stays small.
    Merged(Box<Merged<'a>>),
}

/// The payload of a [`PreparedPull::Merged`] — a rebased merge ready to land.
pub struct Merged<'a> {
    /// The branch whose cells the commit advances.
    branch: &'a Branch,
    /// Checkpoint of the head captured at prepare time — the commit publishes
    /// through it, CAS'ing against the version the merge built on, so a write
    /// that landed in between fails rather than clobbers.
    head: Checkpoint<Revision>,
    /// The merged revision to publish as the new head.
    new_revision: Revision,
    /// The upstream entry just pulled from, its sync base already advanced
    /// to the tree merged in — the tracking state to upsert.
    sync: Upstream,
    /// The sync base the merge actually ran from — what the pulled entry's
    /// tree looked like at prepare time. Lets the commit phase detect
    /// whether a concurrent write advanced this same entry in the meantime.
    base: TreeReference,
}

impl PreparedPull<'_> {
    /// Phase two: advance the branch cells — the head to the merged revision
    /// and the sync-base marker to the merged upstream tree.
    ///
    /// Instant (no network): just two cell CAS publishes. A caller can hold an
    /// exclusive lock over only this. On a head-version mismatch (a commit
    /// advanced the head since prepare) the publish fails so the caller can
    /// refresh and re-pull. A no-op prepare returns `Ok(None)`.
    pub async fn commit<Env>(self, env: &Env) -> Result<Option<Revision>, PullError>
    where
        Env: Provider<Publish> + Provider<Resolve> + ConditionalSync + 'static,
    {
        let Merged {
            branch,
            head,
            new_revision,
            sync,
            base,
        } = match self {
            PreparedPull::NoOp => return Ok(None),
            PreparedPull::Merged(merged) => *merged,
        };

        // Publish the merged revision as the branch's new head, through the
        // checkpoint — so the CAS is against the version we merged from. If a
        // commit advanced the head while we were merging, this fails with
        // `VersionMismatch` instead of silently overwriting that commit (our
        // merge was computed from a now-stale snapshot, so it must not land).
        // On success the checkpoint updates the shared cache, so the branch
        // handle sees the new head. The caller refreshes and re-pulls to
        // reconcile a mismatch.
        head.publish(new_revision.clone(), env).await?;

        // Advance the pulled upstream's recorded sync base to the tree we
        // just merged in, so the next pull/push against it uses that as the
        // divergence marker. An upstream pulled explicitly for the first
        // time gets tracked here (appended, not made the default).
        // Checkpointed just before the write, so its CAS is against the
        // marker as it stands now.
        //
        // The head publish above and this write are not one atomic step,
        // and other syncs write this cell too — a concurrent pull, a push
        // to another upstream, a set_upstream. On a version mismatch we
        // re-read the cell: if our entry is untouched (the concurrent
        // write was about a different entry), fold our advance into the
        // current state and publish once more; if our own entry moved, a
        // concurrent sync of this same upstream already established a
        // consistent (head, base) pair — clobbering it back would regress
        // the base — so we yield and return the head as it now stands.
        let marker = branch.upstream.checkpoint();
        let mut upstreams = branch.upstreams();
        upstreams.upsert(sync.clone());
        let publish = marker.publish(upstreams, env).await;

        if let Err(PublishError::VersionMismatch { .. }) = publish {
            branch.upstream.resolve().perform(env).await?;
            let marker = branch.upstream.checkpoint();
            let mut upstreams = branch.upstreams();
            let ours_untouched = match upstreams.find(&sync) {
                None => true,
                Some(entry) => *entry.tree() == base,
            };
            if !ours_untouched {
                return Ok(branch.revision());
            }
            upstreams.upsert(sync);
            match marker.publish(upstreams, env).await {
                // The cell is contended; give up on the marker advance —
                // the merge itself landed, the next pull is just heavier.
                Err(PublishError::VersionMismatch { .. }) => return Ok(branch.revision()),
                other => other?,
            }
        } else {
            publish?;
        }

        Ok(Some(new_revision))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use anyhow::Result;

    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_pulls_from_local_upstream_no_changes() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:seed".parse()?,
            is: Value::String("Seed".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let pulled = feature.pull().perform(&operator).await?;
        assert!(pulled.is_some());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_upstream_changes_without_local_changes() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let pulled = feature.pull().perform(&operator).await?;
        assert!(pulled.is_some());
        let feature_rev = feature
            .revision()
            .expect("feature should have a revision after pull");
        assert_eq!(feature_rev.tree, main_revision.tree);
        Ok(())
    }

    /// A branch can pull from an upstream other than its default: the
    /// merge lands, the target starts being tracked with its own sync base
    /// (so re-pulling it is a no-op), and the default stays put.
    #[dialog_common::test]
    async fn it_pulls_from_a_non_default_upstream_and_tracks_it() -> Result<()> {
        use crate::Upstream;
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        let dev = repo.branch("dev").open().perform(&operator).await?;
        dev.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/email".parse()?,
            of: "user:dev".parse()?,
            is: Value::String("dev@test.com".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;

        // Explicit pull from a branch that is not the default upstream.
        let merged = feature.pull().from(&dev).perform(&operator).await?;
        assert!(merged.is_some(), "pull from a second upstream merges");

        // Both data sets are visible on the feature branch.
        let emails = feature
            .claims()
            .select(ArtifactSelector::new().the("user/email".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await;
        assert_eq!(emails.len(), 1, "dev's data arrived via the explicit pull");

        // Dev is now tracked (with its own sync base), main stays default.
        let upstreams = feature.upstreams();
        assert_eq!(upstreams.iter().count(), 2);
        assert!(matches!(
            upstreams.default_upstream(),
            Some(Upstream::Local { branch, .. }) if branch == "main"
        ));

        // Dev hasn't moved since, so re-pulling it is a no-op.
        let again = feature.pull().from(&dev).perform(&operator).await?;
        assert!(
            again.is_none(),
            "tracked sync base makes the re-pull a no-op"
        );

        // Pulling from the branch itself is refused.
        let selfish = feature.pull().from(&feature).perform(&operator).await;
        assert!(matches!(
            selfish,
            Err(crate::PullError::UpstreamIsItself { .. })
        ));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_pulls_and_merges_with_both_sides_changed() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let pulled = feature.pull().perform(&operator).await?;
        assert!(pulled.is_some());
        let feature_rev = feature
            .revision()
            .expect("feature should have a revision after merge");
        assert_ne!(feature_rev.tree, main_revision.tree);
        Ok(())
    }

    /// A commit made through one branch handle, while a pull is being computed
    /// through *another handle to the same branch*, must not be silently lost —
    /// the pull fails loudly, and refreshing then re-pulling reconciles both
    /// changes.
    ///
    /// Each `open()` of a branch produces an independent handle whose revision
    /// cell caches the head it saw at open time. `pull` checkpoints its handle's
    /// head up front and, after merging, publishes the result CAS'd against the
    /// checkpointed version. If another handle commits in between, the storage
    /// head advances past the checkpoint, so the publish fails with a
    /// `VersionMismatch` rather than overwriting the commit with a tree built
    /// from the stale snapshot.
    ///
    /// This is the real shape in the service worker: the auto-sync pull and a
    /// local commit run against handles that don't share a revision-cache view.
    /// The recovery is exactly what a consumer does: `refresh` the handle to
    /// pick up the current head, then re-pull — the re-pull merges from the
    /// now-current snapshot and reuses the blocks the first attempt already
    /// fetched.
    #[dialog_common::test]
    async fn it_fails_a_pull_racing_a_commit_then_reconciles_on_refresh() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // Upstream `main` has a change to pull in.
        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        // Two independent handles to the same `feature` branch — like the two
        // call sites in the worker that don't share a revision-cache view. Both
        // snapshot the same (empty) feature head at open time.
        let feature_pull = repo.branch("feature").open().perform(&operator).await?;
        feature_pull.set_upstream(&main).perform(&operator).await?;
        let feature_commit = repo.branch("feature").open().perform(&operator).await?;

        // A local commit lands through the *other* handle, advancing the
        // feature head in storage. `feature_pull`'s cache is now stale.
        feature_commit
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        // The pull handle, unaware of that commit, pulls from upstream. It must
        // fail loudly (version mismatch) rather than silently drop the commit.
        let raced = feature_pull.pull().perform(&operator).await;
        assert!(
            matches!(
                raced,
                Err(crate::PullError::Publish(
                    crate::PublishError::VersionMismatch { .. }
                ))
            ),
            "a pull racing a commit must fail with a version mismatch, not drop the commit; got {raced:?}"
        );

        // Recovery: refresh the stale handle to pick up the current head, then
        // re-pull. This reconciles upstream with the raced commit.
        feature_pull.refresh(&operator).await?;
        feature_pull.pull().perform(&operator).await?;

        // Both changes are now present on the branch.
        let feature = repo.branch("feature").open().perform(&operator).await?;
        let committed: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("user/email".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(committed.len(), 1, "the raced commit must survive recovery");
        assert_eq!(committed[0].is, Value::String("feature@test.com".into()));

        let pulled: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            pulled.len(),
            1,
            "the pulled upstream change must survive too"
        );

        Ok(())
    }

    /// Driving the two phases explicitly (`prepare` then `commit`) lands the
    /// same result as the one-shot `perform`. This is the split a consumer uses
    /// to run the network-bound prepare lock-free and hold an exclusive lock
    /// over only the instant cell advance.
    #[dialog_common::test]
    async fn it_pulls_in_two_phases_prepare_then_commit() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
        let main_revision = main.revision().expect("main should have a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        // Phase one: fetch + rebase, no cell writes yet — the head is unchanged.
        let prepared = feature.pull().prepare(&operator).await?;
        assert!(
            feature.revision().is_none(),
            "prepare must not advance the head"
        );

        // Phase two: advance the cells.
        let pulled = prepared.commit(&operator).await?;
        assert!(pulled.is_some(), "commit should land the merged revision");
        assert_eq!(
            feature
                .revision()
                .expect("feature has a revision after commit")
                .tree,
            main_revision.tree
        );

        Ok(())
    }

    /// A no-op pull (upstream hasn't moved) prepares to `NoOp` and commits to
    /// `Ok(None)` without touching the cells.
    #[dialog_common::test]
    async fn it_prepares_a_noop_when_upstream_has_not_moved() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:main".parse()?,
            is: Value::String("Main data".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        // First pull lands main's change.
        feature.pull().perform(&operator).await?;

        // Upstream hasn't moved since, so a second pull is a no-op.
        let pulled = feature
            .pull()
            .prepare(&operator)
            .await?
            .commit(&operator)
            .await?;
        assert!(pulled.is_none(), "a no-op pull commits to None");

        Ok(())
    }
}

#[cfg(test)]
mod history_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use anyhow::Result;

    use dialog_artifacts::history::{Causality, History as _, causality, common_ancestor};
    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::stream;

    fn assert_one(the: &str, of: &str, value: &str) -> Instruction {
        Instruction::Assert(Artifact {
            the: the.parse().unwrap(),
            of: of.parse().unwrap(),
            is: Value::String(value.to_string()),
            cause: None,
        })
    }

    /// Convergence: the same fact asserted concurrently on two branches
    /// (with different editions, so the stored datums differ in version
    /// metadata) must quiesce under mutual pulls — bounded rounds until
    /// both pulls are no-ops — and land both replicas on the same tree.
    /// `integrate` resolves the contended slot by a deterministic,
    /// antisymmetric rule (hash race for Added vs Added), so whichever
    /// side integrates first, both converge on the same bytes instead of
    /// re-imposing their own copy forever.
    #[dialog_common::test]
    async fn it_quiesces_after_concurrent_identical_asserts() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let a = repo.branch("a").open().perform(&operator).await?;
        let b = repo.branch("b").open().perform(&operator).await?;

        // A filler commit on `a` skews the editions, so the two copies
        // of X carry different version metadata — a genuinely contended
        // slot, not byte-identical data.
        a.commit(stream::iter(vec![assert_one("filler/x", "f:1", "pad")]))
            .perform(&operator)
            .await?;
        a.commit(stream::iter(vec![assert_one("post/title", "post:1", "Hej")]))
            .perform(&operator)
            .await?;
        b.commit(stream::iter(vec![assert_one("post/title", "post:1", "Hej")]))
            .perform(&operator)
            .await?;

        let mut quiesced = false;
        for _ in 0..4 {
            let pulled_a = a.pull().from(&b).perform(&operator).await?;
            let pulled_b = b.pull().from(&a).perform(&operator).await?;
            if pulled_a.is_none() && pulled_b.is_none() {
                quiesced = true;
                break;
            }
        }
        assert!(quiesced, "mutual pulls must reach a fixed point");
        assert_eq!(
            a.revision().map(|r| r.tree),
            b.revision().map(|r| r.tree),
            "both replicas converge on the same tree"
        );
        Ok(())
    }

    /// A retraction must survive reconciliation with a replica that still
    /// holds the fact — including a replica we have NEVER synced with,
    /// where the merge runs from the empty base and the differential
    /// carries no memory of the deletion. Only the `State::Removed`
    /// tombstone in the active index makes the deletion a first-class
    /// write that replays over the peer's stale copy; if tombstones were
    /// retired in favor of evicting retracted keys (retaining them in
    /// history only), the empty-base leg of this test would resurrect
    /// the fact.
    #[dialog_common::test]
    async fn it_does_not_resurrect_a_deleted_fact_on_pull() -> Result<()> {
        use dialog_query::attribute::The;
        use dialog_query::query::Output as _;
        use dialog_query::{AttributeQuery, Claim, Term, the};

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // The fact everyone starts from.
        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Hej",
        )]))
        .perform(&operator)
        .await?;

        // Two downstreams sync it: `feature` (the deleter) and `peer`
        // (a replica that keeps holding the fact and that `feature`
        // never tracks).
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;

        let peer = repo.branch("peer").open().perform(&operator).await?;
        peer.set_upstream(&main).perform(&operator).await?;
        peer.pull().perform(&operator).await?;

        let titles = |branch: &crate::Branch| {
            let branch = branch.clone();
            let operator = &operator;
            async move {
                let rows: Vec<Claim> = branch
                    .query()
                    .select(AttributeQuery::from(
                        Term::<The>::from(the!("post/title"))
                            .of(Term::<dialog_artifacts::Entity>::var("of"))
                            .is(Term::<String>::var("is")),
                    ))
                    .perform(operator)
                    .try_vec()
                    .await?;
                anyhow::Ok(rows.len())
            }
        };
        assert_eq!(titles(&feature).await?, 1, "the fact syncs to feature");

        // Feature deletes the fact.
        feature
            .commit(stream::iter(vec![Instruction::Retract(Artifact {
                the: "post/title".parse()?,
                of: "post:1".parse()?,
                is: Value::String("Hej".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        assert_eq!(titles(&feature).await?, 0, "the retraction takes locally");

        // Leg 1 — tracked pull. Main moves (unrelated commit) so a real
        // merge runs; the sync base covers the deleted fact.
        main.commit(stream::iter(vec![assert_one(
            "user/name",
            "user:1",
            "Alice",
        )]))
        .perform(&operator)
        .await?;
        feature.pull().perform(&operator).await?;
        assert_eq!(
            titles(&feature).await?,
            0,
            "a tracked merge must not resurrect the deleted fact"
        );

        // Leg 2 — untracked pull: `peer` still holds the fact and
        // `feature` has no sync base for it, so the local replay is the
        // only carrier of the deletion.
        assert_eq!(titles(&peer).await?, 1, "peer still holds the fact");
        feature.pull().from(&peer).perform(&operator).await?;
        assert_eq!(
            titles(&feature).await?,
            0,
            "an empty-base merge with a stale peer must not resurrect the deleted fact"
        );

        // Leg 3 — confluence: the peer pulls the deleter and reaches the
        // same verdict. Deletion wins the concurrent contest in *both*
        // integration directions, so the replicas agree instead of each
        // re-imposing its own copy.
        peer.pull().from(&feature).perform(&operator).await?;
        assert_eq!(
            titles(&peer).await?,
            0,
            "the deletion also propagates to the replica that held the fact"
        );

        Ok(())
    }

    /// Pulling merges recorded claim lineage across the sync boundary: the
    /// upstream's history records are adopted, the merge's DAG edge lists
    /// both parents, and supersession established on one branch against
    /// claims committed on the other is detectable afterwards.
    #[dialog_common::test]
    async fn it_merges_history_across_a_pull() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // Main commits a title; feature adopts it via fast-forward pull —
        // the recorded history root travels with the adopted revision.
        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Hej",
        )]))
        .perform(&operator)
        .await?;
        let first = main.revision().expect("main has a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;
        assert_eq!(
            feature.revision().map(|r| r.tree),
            Some(first.tree.clone()),
            "fast-forward adoption carries the upstream tree, history included"
        );

        // Feature replaces the title: its record's cause lists the version
        // of main's claim, because the pulled data is version-tagged.
        feature
            .commit(stream::iter(vec![Instruction::Replace(Artifact {
                the: "post/title".parse()?,
                of: "post:1".parse()?,
                is: Value::String("Hi".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        let replacement = feature.revision().expect("feature has a revision");

        // Meanwhile main commits something else, so the next pull is a real
        // three-way merge rather than a fast-forward.
        main.commit(stream::iter(vec![assert_one(
            "user/name",
            "user:1",
            "Alice",
        )]))
        .perform(&operator)
        .await?;
        let concurrent = main.revision().expect("main has a revision");

        let merged = feature
            .pull()
            .perform(&operator)
            .await?
            .expect("pull merges");

        let history = feature.history(&operator);

        // Main's concurrent claim was adopted into feature's history.
        assert_eq!(
            history
                .claims_at(
                    &concurrent.version(),
                    &"user:1".parse()?,
                    &"user/name".parse()?
                )
                .await?
                .len(),
            1,
            "the upstream's records are adopted across the pull"
        );

        // The supersession feature established over main's claim is
        // detectable from the merged history.
        let title_claims: Vec<_> = history
            .records()
            .await?
            .into_iter()
            .filter(|(_, record)| record.claim().the.to_string() == "post/title")
            .collect();
        assert_eq!(title_claims.len(), 2);
        let (hej_version, hej) = &title_claims[0];
        let (hi_version, hi) = &title_claims[1];
        assert_eq!(*hej_version, first.version());
        assert_eq!(*hi_version, replacement.version());
        assert_eq!(
            causality(
                (hi.claim(), hi_version),
                (hej.claim(), hej_version),
                &history
            )
            .await?,
            Causality::Supersedes
        );

        // The merge's record lists both parents, and the two lineages
        // meet at main's first revision.
        let record = history
            .revision_record(&merged.version())
            .await?
            .expect("the merge's record is retrievable");
        assert!(record.parents.contains(&replacement.version()));
        assert!(record.parents.contains(&concurrent.version()));
        assert_eq!(
            common_ancestor(&replacement.version(), &concurrent.version(), &history).await?,
            Some(first.version())
        );

        // The supersession holds in the merged *data* region too: the
        // replaced value must not resurrect when the deletion crosses the
        // sync boundary through the differential.
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;
        let titles: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("post/title".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(titles.len(), 1, "the superseded value must not resurrect");
        assert_eq!(titles[0].is, Value::String("Hi".to_string()));

        // Skip tables regrow after the merge without ever crossing it: the
        // first commit on top of a merge has no table (its parent has
        // several parents), the next one leaps to the merge and stops.
        let after_merge = feature
            .commit(stream::iter(vec![assert_one("post/tag", "post:1", "a")]))
            .perform(&operator)
            .await?;
        feature.refresh(&operator).await?;
        let next = feature
            .commit(stream::iter(vec![assert_one("post/tag", "post:2", "b")]))
            .perform(&operator)
            .await?;
        feature.refresh(&operator).await?;
        let history = feature.history(&operator);
        assert!(
            history
                .revision_record(&after_merge.version())
                .await?
                .expect("the record is retrievable")
                .skips
                .is_empty(),
            "a commit on top of a merge records no skip table"
        );
        assert_eq!(
            history
                .revision_record(&next.version())
                .await?
                .expect("the record is retrievable")
                .skips,
            vec![merged.version()],
            "the regrown chain leaps to the merge and stops there"
        );

        Ok(())
    }

    /// `Branch::log` walks the committed history newest-first across a
    /// merge: both lineages list, the merge leads with its two parents,
    /// the limit trims from the newest end, and every entry carries its
    /// signed attribution.
    #[dialog_common::test]
    async fn it_logs_history_across_a_merge() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // A fresh branch has nothing to log.
        let main = repo.branch("main").open().perform(&operator).await?;
        assert!(main.log(&operator, usize::MAX).await?.is_empty());

        // Shared base, then divergence: feature replaces the title while
        // main commits something unrelated, and feature pulls the merge.
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Hej",
        )]))
        .perform(&operator)
        .await?;
        let base = main.revision().expect("main has a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;
        feature
            .commit(stream::iter(vec![Instruction::Replace(Artifact {
                the: "post/title".parse()?,
                of: "post:1".parse()?,
                is: Value::String("Hi".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        let ours = feature.revision().expect("feature has a revision");

        main.commit(stream::iter(vec![assert_one(
            "user/name",
            "user:1",
            "Alice",
        )]))
        .perform(&operator)
        .await?;
        let theirs = main.revision().expect("main has a revision");

        let merged = feature
            .pull()
            .perform(&operator)
            .await?
            .expect("pull merges");

        let entries = feature.log(&operator, usize::MAX).await?;
        let versions: Vec<_> = entries.iter().map(|(version, _)| *version).collect();
        // Newest first: the merge, then the two concurrent revisions
        // (deterministic tie-break by origin), then the shared base.
        let mut concurrent = [ours.version(), theirs.version()];
        concurrent.sort();
        assert_eq!(
            versions,
            vec![
                merged.version(),
                concurrent[1],
                concurrent[0],
                base.version(),
            ]
        );

        // The merge's record leads with both parents, and every entry
        // carries the signed attribution of the identity that minted it.
        assert_eq!(entries[0].1.parents.len(), 2);
        for (_, record) in &entries {
            assert_eq!(record.issuer, operator.did().to_string());
            assert_eq!(record.authority, profile.did().to_string());
        }

        // The limit trims from the newest end.
        let top = feature.log(&operator, 1).await?;
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].0, merged.version());

        Ok(())
    }

    /// A merge revision's transitive ancestry unions both parents'
    /// histories: the derived RevisionAncestor concept reaches ours,
    /// theirs, and the shared base — the base exactly once, even
    /// though both paths converge on it.
    #[dialog_common::test]
    async fn it_derives_merge_ancestry_across_both_parents() -> Result<()> {
        use crate::schema;
        use dialog_query::query::Output as _;
        use dialog_query::{Query, Term};

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // Same shape as the log test: shared base, divergence, merge.
        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Hej",
        )]))
        .perform(&operator)
        .await?;
        let base = main.revision().expect("main has a revision");

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;
        feature
            .commit(stream::iter(vec![assert_one("post/body", "post:1", "...")]))
            .perform(&operator)
            .await?;
        let ours = feature.revision().expect("feature has a revision");

        main.commit(stream::iter(vec![assert_one(
            "user/name",
            "user:1",
            "Alice",
        )]))
        .perform(&operator)
        .await?;
        let theirs = main.revision().expect("main has a revision");

        let merged = feature
            .pull()
            .perform(&operator)
            .await?
            .expect("pull merges");

        let mut reachable: Vec<_> = feature
            .query()
            .select(Query::<schema::RevisionAncestor> {
                this: merged.entity().into(),
                ancestor: Term::var("ancestor"),
            })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|row| row.ancestor.0)
            .collect();
        reachable.sort();
        let mut expected = vec![base.entity(), ours.entity(), theirs.entity()];
        expected.sort();
        assert_eq!(
            reachable, expected,
            "the merge reaches both parents and the base once"
        );

        Ok(())
    }

    /// Pull is the trust boundary: an upstream head that does not carry a
    /// valid signature by its named issuer is rejected before any of its
    /// tree is adopted or merged. Here the "upstream" advertises a forged
    /// head — attributed to the operator's own DID, but without its key's
    /// signature — and the pull refuses it.
    #[dialog_common::test]
    async fn it_refuses_to_pull_a_forged_head() -> Result<()> {
        use crate::{Revision, TreeReference};
        use dialog_artifacts::DialogArtifactsError;
        use dialog_artifacts::history::Edition;
        use std::collections::HashSet;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // A branch whose head is planted rather than committed: attributed
        // to a real issuer DID, pointing at an arbitrary tree, but not
        // signed by that issuer's key.
        let evil = repo.branch("evil").open().perform(&operator).await?;
        let forged = Revision {
            subject: evil.of().clone(),
            issuer: operator.did(),
            authority: profile.did(),
            branch: "evil".into(),
            tree: TreeReference::from([9u8; 32]),
            cause: HashSet::new(),
            edition: Edition::GENESIS,
            signature: Vec::new(),
        };
        evil.reset(forged).perform(&operator).await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&evil).perform(&operator).await?;
        let pulled = feature.pull().perform(&operator).await;

        assert!(
            matches!(
                pulled,
                Err(crate::PullError::Artifact(
                    DialogArtifactsError::InvalidSignature(_)
                ))
            ),
            "pulling a forged head must fail verification; got {pulled:?}"
        );
        assert!(
            feature.revision().is_none(),
            "nothing of the forged head may be adopted"
        );

        Ok(())
    }

    /// Concurrent replacements of the same cardinality-one fact on two
    /// branches: the merge surfaces the conflict rather than silently
    /// dropping a side. Both values stand in the merged data region, both
    /// history records are present, and the tiered conflict detection
    /// reports them concurrent — resolution is deferred to whoever asks.
    #[dialog_common::test]
    async fn it_surfaces_concurrent_replacements_after_a_merge() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // A shared base: both branches see title = "Base".
        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Base",
        )]))
        .perform(&operator)
        .await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;

        // Both sides replace it, without seeing each other.
        let replace = |value: &str| -> Result<Instruction> {
            Ok(Instruction::Replace(Artifact {
                the: "post/title".parse()?,
                of: "post:1".parse()?,
                is: Value::String(value.to_string()),
                cause: None,
            }))
        };
        main.commit(stream::iter(vec![replace("MainSide")?]))
            .perform(&operator)
            .await?;
        let theirs = main.revision().expect("main has a revision");
        feature
            .commit(stream::iter(vec![replace("FeatureSide")?]))
            .perform(&operator)
            .await?;
        let ours = feature.revision().expect("feature has a revision");

        feature
            .pull()
            .perform(&operator)
            .await?
            .expect("pull merges");

        // Neither side is dropped: the merged tree carries both claims at
        // the cardinality-one (entity, attribute).
        let titles: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("post/title".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let mut values: Vec<_> = titles.iter().map(|artifact| artifact.is.clone()).collect();
        values.sort_by_key(|value| value.to_utf8());
        assert_eq!(
            values,
            vec![
                Value::String("FeatureSide".to_string()),
                Value::String("MainSide".to_string()),
            ],
            "a merge surfaces concurrent values instead of dropping one"
        );

        // ... and the recorded lineage proves they are concurrent.
        let history = feature.history(&operator);
        let ours_claims = history
            .claims_at(&ours.version(), &"post:1".parse()?, &"post/title".parse()?)
            .await?;
        let theirs_claims = history
            .claims_at(
                &theirs.version(),
                &"post:1".parse()?,
                &"post/title".parse()?,
            )
            .await?;
        assert_eq!((ours_claims.len(), theirs_claims.len()), (1, 1));
        assert_eq!(
            causality(
                (&ours_claims[0], &ours.version()),
                (&theirs_claims[0], &theirs.version()),
                &history
            )
            .await?,
            Causality::Concurrent
        );

        Ok(())
    }

    /// A retraction made strictly after the retracted assertion — no
    /// concurrency on the fact at all — must survive a three-way merge:
    /// the tombstone rides the differential like any other change, and the
    /// merged tree must not resurrect the retracted value.
    #[dialog_common::test]
    async fn it_propagates_a_retraction_across_a_merge() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // A shared base: both branches see the title.
        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Hej",
        )]))
        .perform(&operator)
        .await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;

        // Feature retracts the title — causally after the assertion.
        feature
            .commit(stream::iter(vec![Instruction::Retract(Artifact {
                the: "post/title".parse()?,
                of: "post:1".parse()?,
                is: Value::String("Hej".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        // Main commits something unrelated, so the pull is a real merge.
        main.commit(stream::iter(vec![assert_one(
            "user/name",
            "user:1",
            "Alice",
        )]))
        .perform(&operator)
        .await?;

        feature
            .pull()
            .perform(&operator)
            .await?
            .expect("pull merges");

        let titles: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("post/title".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await;
        assert!(
            titles.is_empty(),
            "a causal retraction must not resurrect in a merge: {titles:?}"
        );

        Ok(())
    }

    /// A pull landing while another handle advanced the upstream cell for a
    /// *different* target must not clobber that advance — the commit phase
    /// re-reads and folds its own entry in.
    #[dialog_common::test]
    async fn it_folds_tracking_updates_racing_from_another_handle() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![assert_one(
            "user/name",
            "user:1",
            "Alice",
        )]))
        .perform(&operator)
        .await?;
        let backup = repo.branch("backup").open().perform(&operator).await?;

        // Two handles to the same branch, each with its own cell caches.
        let puller = repo.branch("feature").open().perform(&operator).await?;
        puller.set_upstream(&main).perform(&operator).await?;
        puller
            .commit(stream::iter(vec![assert_one(
                "user/email",
                "user:1",
                "alice@test.com",
            )]))
            .perform(&operator)
            .await?;
        let pusher = repo.branch("feature").open().perform(&operator).await?;

        // The pull is prepared from one handle; before it commits, the
        // other handle pushes to a different target, advancing the shared
        // upstream cell underneath it.
        let prepared = puller.pull().prepare(&operator).await?;
        pusher.push().to(&backup).perform(&operator).await?;
        let merged = prepared.commit(&operator).await?;
        assert!(merged.is_some(), "the racing pull still lands");

        // Both tracking advances survive: the pull's sync base for main and
        // the push's tracking entry for backup.
        let fresh = repo.branch("feature").open().perform(&operator).await?;
        let upstreams = fresh.upstreams();
        assert_eq!(upstreams.iter().count(), 2);
        let main_head = repo
            .branch("main")
            .load()
            .perform(&operator)
            .await?
            .revision()
            .expect("main has a revision");
        assert!(
            upstreams.iter().any(|entry| matches!(
                entry,
                crate::Upstream::Local { branch, tree } if branch == "main" && *tree == main_head.tree
            )),
            "the pull's sync-base advance survives the race"
        );
        assert!(
            upstreams.iter().any(
                |entry| matches!(entry, crate::Upstream::Local { branch, .. } if branch == "backup")
            ),
            "the racing push's tracking entry survives the pull"
        );

        Ok(())
    }
}
