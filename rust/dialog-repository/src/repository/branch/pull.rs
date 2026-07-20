use crate::RevisionExt as _;
use std::sync::{Arc, Mutex};

use dialog_artifacts::DialogArtifactsError;
use dialog_artifacts::FromKey as _;
use dialog_artifacts::history::Context;
use dialog_artifacts::merge;
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

/// Below this divergence mass (summed edition excess, roughly commits),
/// a merge routes to the direct replay or screen instead of the graft:
/// tiny deltas fragment into per-key spans, and the stitch's seam work
/// then exceeds simply walking the few entries.
const SMALL_DIVERGENCE: u64 = 8;

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
        Box::pin(self.prepare(env)).await?.commit(env).await
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
        // Surface head verification as an artifact error, the shape callers
        // (and the forged-head test) match on.
        upstream_revision
            .verify()
            .map_err(dialog_artifacts::DialogArtifactsError::from)?;

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

        // The three trees: last-sync base, the upstream revision we're
        // merging in, and the local tree the merge integrates onto.
        // Hydration is lazy; blocks load on demand as the differential
        // walks them.
        let base_tree =
            Index::from_hash_with_cache(NodeHash::from(*base.hash()), branch.node_cache());
        let upstream_tree = Index::from_hash_with_cache(
            NodeHash::from(*upstream_revision.tree.hash()),
            branch.node_cache(),
        );
        let mut merged =
            Index::from_hash_with_cache(NodeHash::from(local_tree_hash), branch.node_cache());

        // The receiver's causal context: the per-origin watermark of the
        // local head's ancestry. This is the merge's memory of every
        // claim it has ever incorporated — the observed-remove screen
        // rejects incoming stale copies of claims the context has seen
        // but the cache no longer holds (deleted facts carry no
        // tombstone; see `notes/version-control.md`).
        //
        // Answered from the branch memo, or from the watermark the head
        // itself published (heads carry their context under the head
        // signature), or — only for lineages minted before heads carried
        // contexts — by the O(ancestry) walk, once.
        let contexts = branch.contexts();
        let local_context = match &local_revision {
            Some(revision) => match contexts.cached(&revision.version()).await {
                Some(context) => context,
                None => match &revision.context {
                    Some(context) => {
                        contexts.insert(revision.version(), context.clone());
                        context.clone()
                    }
                    None => {
                        let history = branch.history(env);
                        contexts.context_of(&revision.version(), &history).await?
                    }
                },
            },
            None => Context::new(),
        };

        // The upstream published its watermark with its head: two frugal
        // paths can short-circuit the tree merge entirely, both gated on
        // comparing the two contexts (O(#origins), no reads).
        if let Some(theirs) = &upstream_revision.context {
            // Nothing new: everything the upstream has seen, we have
            // seen. Every claim live there is live or covered here
            // already, so the merge would change nothing. Keep the head
            // and advance only the sync base, so future diffs start
            // from the upstream's current tree.
            if local_context.includes(theirs) {
                if let Some(current) = &local_revision {
                    return Ok(PreparedPull::Merged(Box::new(Merged {
                        branch,
                        head,
                        new_revision: current.clone(),
                        sync: upstream.with_tree(upstream_revision.tree.clone()),
                        base,
                    })));
                }
            }
            // Fast-forward adoption: we have no novelty of our own (our
            // tree is exactly the sync base) and the upstream has seen
            // everything we have. Nothing we know could contradict what
            // survived its screen, so its tree is adopted by root: no
            // diff, no block reads, no import. Blocks hydrate lazily on
            // demand like any partially replicated region — this is what
            // keeps pull cost independent of upstream churn in regions
            // we never touch, and what makes adopting a deep history
            // free.
            else if *base.hash() == local_tree_hash && theirs.includes(&local_context) {
                contexts.insert(upstream_revision.version(), theirs.clone());
                return Ok(PreparedPull::Merged(Box::new(Merged {
                    branch,
                    head,
                    new_revision: upstream_revision.clone(),
                    sync: upstream.with_tree(upstream_revision.tree.clone()),
                    base,
                })));
            }
            // The graft merge, for tracked pulls where at least one
            // side's delta is substantial: partition the key space by
            // each side's node-level divergence from the sync base,
            // stitch the merged tree from whole subtrees of the
            // unilaterally-changed spans (adopted by hash, unread), and
            // do real merge work only where both sides changed. Cost is
            // the intersection of the two change sets plus coverage and
            // seams, independent of either side's bulk.
            //
            // Tiny deltas skip the graft: a couple of commits fragment
            // into as many divergence spans as they have keys, and the
            // stitch pays edge-spine lifts per piece that a direct
            // replay of so few entries never touches. Below the
            // threshold the direct paths (replay ours or screen theirs,
            // whichever side is smaller) are strictly cheaper; the
            // graft's economics need bulk on both sides.
            else if let Some(local) = &local_revision
                && base != TreeReference::default()
                && local_context
                    .divergence(theirs)
                    .min(theirs.divergence(&local_context))
                    > SMALL_DIVERGENCE
            {
                let tree_store = TreeStorage::new(TreeStorageBridge(store.clone()));
                let base_tree =
                    Index::from_hash_with_cache(NodeHash::from(*base.hash()), branch.node_cache());
                let local_tree = Index::from_hash_with_cache(
                    NodeHash::from(local_tree_hash),
                    branch.node_cache(),
                );
                let upstream_tree = Index::from_hash_with_cache(
                    NodeHash::from(*upstream_revision.tree.hash()),
                    branch.node_cache(),
                );

                // Node-level divergence spans of each side against the
                // base: conservative supersets read from the pruned diff
                // frontiers, no entry enumeration.
                let full = merge::full_scope();
                let ours_spans = merge::spans_from_bounds(
                    dialog_search_tree::TreeDifference::compute_within(
                        &base_tree,
                        &local_tree,
                        &tree_store,
                        &tree_store,
                        &full,
                    )
                    .await?
                    .divergent_bounds(),
                );
                let theirs_spans = merge::spans_from_bounds(
                    dialog_search_tree::TreeDifference::compute_within(
                        &base_tree,
                        &upstream_tree,
                        &tree_store,
                        &tree_store,
                        &full,
                    )
                    .await?
                    .divergent_bounds(),
                );
                let pieces = merge::partition_spans(&ours_spans, &theirs_spans);
                let contested: Vec<_> = pieces
                    .iter()
                    .filter(|(_, source)| *source == merge::SpanSource::Contested)
                    .map(|(span, _)| span.clone())
                    .collect();

                // Stitch: each unilaterally-changed span adopts the
                // changed side's subtree; unchanged space is identical
                // in all three trees. Contested spans start from the
                // BIGGER divergence's content and take the smaller
                // side's screened changes below, so the entry-level work
                // inside contested spans also tracks the smaller side.
                let ours_smaller =
                    local_context.divergence(theirs) <= theirs.divergence(&local_context);
                let contested_substrate = if ours_smaller {
                    &upstream_tree
                } else {
                    &local_tree
                };
                let stitch_pieces = pieces
                    .iter()
                    .map(|(span, source)| dialog_search_tree::Piece::Range {
                        source: match source {
                            merge::SpanSource::Ours => &local_tree,
                            merge::SpanSource::Contested => contested_substrate,
                            merge::SpanSource::Theirs => &upstream_tree,
                        },
                        range: span.clone(),
                    })
                    .collect();
                let mut stitched =
                    dialog_search_tree::TransientTree::stitch(stitch_pieces, &tree_store).await?;

                // Contested spans: apply the smaller side's screened
                // delta onto the substrate. Data adds screen by the
                // SUBSTRATE side's watermark (an add it observed is
                // either already present or was covered by its log);
                // removes stay byte-guarded; history and coverage
                // entries are append-only and observed copies are
                // already in the substrate, so the same screen passes
                // exactly the novel ones.
                if !contested.is_empty() {
                    let (changed_side, screen_context) = if ours_smaller {
                        (&local_tree, theirs.clone())
                    } else {
                        (&upstream_tree, local_context.clone())
                    };
                    let changes = base_tree.differentiate_within(
                        changed_side,
                        &contested,
                        &tree_store,
                        &tree_store,
                    );
                    let screened = merge::screen_data(changes, screen_context);
                    stitched = Box::pin(stitched.integrate(screened, &tree_store)).await?;
                }

                // Coverage repair: every covering record either side
                // minted since the base retires the covered claims still
                // live in the stitched tree. Both coverage deltas are
                // scoped diffs over the compact coverage region, so this
                // costs deletions-and-replacements, never churn. Repair
                // is version-exact, so its order relative to the
                // contested integrate is immaterial: a re-assert mints a
                // fresh version no coverage names.
                let coverage_scope = merge::coverage_scope();
                for (from, to) in [(&base_tree, &local_tree), (&base_tree, &upstream_tree)] {
                    let coverage =
                        from.differentiate_within(to, &coverage_scope, &tree_store, &tree_store);
                    futures_util::pin_mut!(coverage);
                    while let Some(change) = futures_util::StreamExt::next(&mut coverage).await {
                        let dialog_search_tree::Change::Add(entry) = change? else {
                            continue;
                        };
                        let dialog_artifacts::State::Added(record) = &entry.value else {
                            continue;
                        };
                        if record.supersedes.is_empty() {
                            continue;
                        }
                        // Scan the covered slot in the stitched tree and
                        // collect the claims whose versions the record
                        // names; delete them at all three orderings.
                        let mut retire = Vec::new();
                        {
                            let candidates = stitched
                                .stream_range(merge::coverage_range(&entry.key)?, &tree_store);
                            futures_util::pin_mut!(candidates);
                            while let Some(candidate) =
                                futures_util::StreamExt::next(&mut candidates).await
                            {
                                let candidate = candidate?;
                                if let dialog_artifacts::State::Added(datum) = &candidate.value
                                    && let Some(version) = datum.version
                                    && record.supersedes.contains(&version)
                                {
                                    retire.push(candidate.key);
                                }
                            }
                        }
                        for key in retire {
                            let entity_key = dialog_artifacts::EntityKey(key);
                            let attribute_key =
                                dialog_artifacts::AttributeKey::from_key(&entity_key);
                            let value_key = dialog_artifacts::ValueKey::from_key(&entity_key);
                            for key in [
                                entity_key.into_key(),
                                attribute_key.into_key(),
                                value_key.into_key(),
                            ] {
                                stitched = stitched.delete(&key, &tree_store).await?;
                            }
                        }
                    }
                }

                let mut delta = Delta::zero();
                let mut merged = stitched.persist(&mut delta)?;
                let merged_tree = TreeReference::from(*merged.root().as_bytes());

                // Head selection mirrors the other merge paths, so
                // mutual pulls quiesce.
                if merged_tree == upstream_revision.tree {
                    contexts.insert(upstream_revision.version(), theirs.clone());
                    return Ok(PreparedPull::Merged(Box::new(Merged {
                        branch,
                        head,
                        new_revision: upstream_revision.clone(),
                        sync: upstream.with_tree(upstream_revision.tree.clone()),
                        base,
                    })));
                }
                if merged_tree == local.tree {
                    return Ok(PreparedPull::Merged(Box::new(Merged {
                        branch,
                        head,
                        new_revision: local.clone(),
                        sync: upstream.with_tree(upstream_revision.tree.clone()),
                        base,
                    })));
                }

                let authority = Identify.perform(env).await?;
                let mut revision = local.merge(
                    &upstream_revision,
                    TreeReference::default(),
                    branch.of().clone(),
                    branch.name(),
                    authority.did(),
                    authority.profile().clone(),
                );
                let mut record = revision.record(
                    vec![local.version(), upstream_revision.version()],
                    Vec::new(),
                );
                record.signature = Attest::new(record.payload()?).perform(env).await?;
                merged
                    .record(&mut store, &mut delta, record.entries()?)
                    .await?;
                revision.tree = TreeReference::from(*merged.root().as_bytes());
                let mut context = local_context.clone();
                context.merge(theirs);
                context.record(revision.version());
                revision.context = Some(context.clone());
                revision.signature = Attest::new(revision.payload()).perform(env).await?;
                contexts.insert(revision.version(), context);

                branch
                    .archive()
                    .index()
                    .import(delta.flush().map(|(_, buffer)| buffer))
                    .perform(env)
                    .await
                    .map_err(DialogArtifactsError::from)?;

                return Ok(PreparedPull::Merged(Box::new(Merged {
                    branch,
                    head,
                    new_revision: revision,
                    sync: upstream.with_tree(upstream_revision.tree.clone()),
                    base,
                })));
            }
            // Reverse replay, for first contact when we are the smaller
            // side: no sync base exists, so the graft has no third tree
            // to partition against. Adopt their tree as the substrate
            // and replay our (whole, but smaller) delta onto it.
            //
            // This is the same screened merge with the roles swapped,
            // which is what makes it exact in every case. The screen
            // rules only ever consult the RECEIVER's state, and here the
            // receiver is the upstream: our data delta runs through R1
            // against THEIR published watermark (an add they have
            // observed is either already live there, a no-op, or was
            // covered by their log, where applying it would resurrect a
            // deletion; our fresh claims are above their watermark and
            // pass as news), our removes stay byte-guarded against their
            // tree (R2), and our history delta screens their tree
            // directly (R3: a fact we adopted after the sync base and
            // then covered nets to nothing in our data diff, so only our
            // covering record can retire their live copy). Their novelty
            // needs no screen at all: nothing they minted unseen by us
            // can have been covered by us.
            //
            // Direction is chosen by comparing the two watermarks'
            // divergence masses (summed per-origin edition excess over
            // the other side; editions count writes, so the excess is a
            // zero-read proxy for delta size): replay our delta onto
            // their tree when ours is the smaller side, screen their
            // delta onto our tree otherwise. Reads then track the
            // smaller divergence, never the larger side's churn — in
            // both directions of asymmetry. A replica that adopted a
            // bulky third upstream screens a small tracked upstream's
            // delta in rather than replaying the adopted bulk out; a
            // small replica facing a churning upstream replays only
            // what it holds. This applies on tracked and first-contact
            // pulls alike.
            else if let Some(local) = &local_revision
                && local_context.divergence(theirs) <= theirs.divergence(&local_context)
            {
                let tree_store = TreeStorage::new(TreeStorageBridge(store.clone()));
                let base_tree =
                    Index::from_hash_with_cache(NodeHash::from(*base.hash()), branch.node_cache());
                let local_tree = Index::from_hash_with_cache(
                    NodeHash::from(local_tree_hash),
                    branch.node_cache(),
                );
                let mut merged = Index::from_hash_with_cache(
                    NodeHash::from(*upstream_revision.tree.hash()),
                    branch.node_cache(),
                );
                let upstream_snapshot = Index::from_hash_with_cache(
                    NodeHash::from(*upstream_revision.tree.hash()),
                    branch.node_cache(),
                );

                let history_scope = merge::history_scope();
                let data_scope = merge::data_scope();
                let history_changes = base_tree.differentiate_within(
                    &local_tree,
                    &history_scope,
                    &tree_store,
                    &tree_store,
                );
                let data_changes = base_tree.differentiate_within(
                    &local_tree,
                    &data_scope,
                    &tree_store,
                    &tree_store,
                );
                let screen_store = TreeStorage::new(TreeStorageBridge(store.clone()));
                let screened_history =
                    merge::screen_history(history_changes, upstream_snapshot, screen_store);
                let screened_data = merge::screen_data(data_changes, theirs.clone());
                let screened = futures_util::StreamExt::chain(screened_history, screened_data);

                let mut delta = Delta::zero();
                merged = Box::pin(merged.edit().integrate(screened, &tree_store))
                    .await?
                    .persist(&mut delta)?;
                let merged_tree = TreeReference::from(*merged.root().as_bytes());

                // The replay can degenerate, and the head selection must
                // mirror the screened path's arms or mutual pulls mint
                // merge revisions forever instead of quiescing. Nothing
                // effective replayed (their tree stands): adopt their
                // head. Nothing of theirs was new (our tree stands):
                // keep our head and advance only the sync base. In both
                // arms the integrate produced no new nodes, so there is
                // nothing to import.
                if merged_tree == upstream_revision.tree {
                    contexts.insert(upstream_revision.version(), theirs.clone());
                    return Ok(PreparedPull::Merged(Box::new(Merged {
                        branch,
                        head,
                        new_revision: upstream_revision.clone(),
                        sync: upstream.with_tree(upstream_revision.tree.clone()),
                        base,
                    })));
                }
                if merged_tree == local.tree {
                    return Ok(PreparedPull::Merged(Box::new(Merged {
                        branch,
                        head,
                        new_revision: local.clone(),
                        sync: upstream.with_tree(upstream_revision.tree.clone()),
                        base,
                    })));
                }

                // Mint the merge revision exactly as the screened path
                // does; its context needs no derivation at all: the
                // merged ancestry is both parents', and both watermarks
                // are in hand.
                let authority = Identify.perform(env).await?;
                let mut revision = local.merge(
                    &upstream_revision,
                    TreeReference::default(),
                    branch.of().clone(),
                    branch.name(),
                    authority.did(),
                    authority.profile().clone(),
                );
                let mut record = revision.record(
                    vec![local.version(), upstream_revision.version()],
                    Vec::new(),
                );
                record.signature = Attest::new(record.payload()?).perform(env).await?;
                merged
                    .record(&mut store, &mut delta, record.entries()?)
                    .await?;
                revision.tree = TreeReference::from(*merged.root().as_bytes());
                let mut context = local_context.clone();
                context.merge(theirs);
                context.record(revision.version());
                revision.context = Some(context.clone());
                revision.signature = Attest::new(revision.payload()).perform(env).await?;
                contexts.insert(revision.version(), context);

                branch
                    .archive()
                    .index()
                    .import(delta.flush().map(|(_, buffer)| buffer))
                    .perform(env)
                    .await
                    .map_err(DialogArtifactsError::from)?;

                return Ok(PreparedPull::Merged(Box::new(Merged {
                    branch,
                    head,
                    new_revision: revision,
                    sync: upstream.with_tree(upstream_revision.tree.clone()),
                    base,
                })));
            }
        }

        // Integrate the *upstream's* changes since the sync base onto
        // the local tree in two screened passes — history first, so
        // incoming coverage records retire the claims they supersede
        // (R3) before any data change can contest those slots; then the
        // data regions under the context screen (R1), with the tree's
        // byte-guarded removes as R2 throughout. Local novelty is
        // preserved by construction — the merge starts from the local
        // tree — and each differential only reads blocks on paths where
        // base and upstream actually differ within its region.
        let tree_store = TreeStorage::new(TreeStorageBridge(store.clone()));
        let screen_store = TreeStorage::new(TreeStorageBridge(store.clone()));
        let local_snapshot =
            Index::from_hash_with_cache(NodeHash::from(local_tree_hash), branch.node_cache());

        // History changes are screened + emitted first, data changes
        // second; chaining them into one stream integrated in a single
        // pass keeps that order (so R3's coverage removes precede the
        // R1 data adds that would otherwise contest the same slots)
        // without an intermediate persist. `integrate` applies changes
        // in stream order.
        let history_scope = merge::history_scope();
        let data_scope = merge::data_scope();
        let history_changes = base_tree.differentiate_within(
            &upstream_tree,
            &history_scope,
            &tree_store,
            &tree_store,
        );
        let data_changes =
            base_tree.differentiate_within(&upstream_tree, &data_scope, &tree_store, &tree_store);
        let screened_history = merge::screen_history(history_changes, local_snapshot, screen_store);
        // Fold the version of every revision record riding the delta
        // into `observed` while the data differential streams anyway.
        // Those records are exactly the upstream-ancestry revisions we
        // may lack (records at or below the sync base arrived with the
        // pulls that established it), so `local_context + observed` is
        // the context of a head that adopts or merges this upstream —
        // derived at zero extra reads, in place of the ancestry walk.
        let observed = Arc::new(Mutex::new(Context::new()));
        let observed_data = merge::observe_revisions(data_changes, observed.clone());
        let screened_data = merge::screen_data(observed_data, local_context.clone());
        let screened = futures_util::StreamExt::chain(screened_history, screened_data);

        let mut delta = Delta::zero();
        merged = Box::pin(merged.edit().integrate(screened, &tree_store))
            .await?
            .persist(&mut delta)?;

        let merged_tree = TreeReference::from(*merged.root().as_bytes());

        // The merged head's context, derived incrementally: the local
        // context plus every revision that rode the delta (folded by
        // `observe_revisions` while the differential streamed). The
        // records in the delta are exactly the upstream-ancestry
        // revisions we may have lacked, so this equals the ancestry walk
        // without paying it.
        let merged_context = {
            let mut context = local_context;
            context.merge(
                &observed
                    .lock()
                    .expect("the revision observer mutex is never poisoned"),
            );
            context
        };

        let had_local_head = local_revision.is_some();
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
            // The upstream had nothing we lack (its novelty was already
            // in our ancestry, or was screened out as covered): the
            // local head stands. No revision is minted — only the sync
            // base advances, so the next pull from this upstream is
            // incremental.
            Some(current) if merged_tree == current.tree => current.clone(),
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
                // The minted head publishes its watermark: the merged
                // context plus its own version, signed with the rest of
                // the head so peers can adopt it without walking.
                let mut context = merged_context.clone();
                context.record(revision.version());
                revision.context = Some(context);
                revision.signature = Attest::new(revision.payload()).perform(env).await?;

                revision
            }
        };

        // Remember the new head's context: the merged context plus the
        // head itself. Exact for every arm — an adopted head's ancestry
        // is covered by the sync base (whose records entered the local
        // ancestry with the pulls that established it) plus the delta; a
        // minted merge adds its own version; an unchanged head folds
        // only versions it already had. The next pull answers from the
        // memo instead of paying the ancestry walk. The one shape where
        // base records may be in neither side is a branch with no head
        // but a stale nonempty sync base — an inconsistent state; skip
        // the memo and let the next pull derive by the walk.
        if had_local_head || base == TreeReference::default() {
            let mut context = merged_context;
            context.record(new_revision.version());
            contexts.insert(new_revision.version(), context);
        }

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
    use crate::RevisionExt as _;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo, unique_name};
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
        a.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Hej",
        )]))
        .perform(&operator)
        .await?;
        b.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Hej",
        )]))
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
    /// where the merge runs from the empty base and the data differential
    /// carries no remove for the fact. There is no tombstone: the peer's
    /// stale copy is rejected by the causal-context screen (R1 — the
    /// claim is in our ancestry but no longer live, so re-applying it
    /// would resurrect a deletion), and our own retract record's coverage
    /// (R3) is what carries the deletion to the peer in the reverse
    /// direction.
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
        // The region clusters by origin, not causal order; locate the two
        // claims by version.
        let (hej_version, hej) = title_claims
            .iter()
            .find(|(version, _)| *version == first.version())
            .expect("the original claim is in the merged history");
        let (hi_version, hi) = title_claims
            .iter()
            .find(|(version, _)| *version == replacement.version())
            .expect("the replacement claim is in the merged history");
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
            context: None,
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

    /// Observed-remove semantics over a cardinality-many attribute, the
    /// full Alice / Bob / Mallory / Jordan scenario from
    /// `notes/version-control.md`. Bob's assertion is retracted by
    /// Alice; Mallory concurrently asserts the *same value*; because the
    /// retraction never observed Mallory's claim, the value stays visible
    /// after their merge. Only once Jordan — who has seen both — retracts
    /// is it gone everywhere.
    #[dialog_common::test]
    async fn it_keeps_a_concurrent_assertion_the_retraction_never_observed() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let count_labels = |branch: crate::Branch| {
            let operator = &operator;
            async move {
                let rows: Vec<_> = branch
                    .claims()
                    .select(ArtifactSelector::new().the("task/label".parse().unwrap()))
                    .perform(operator)
                    .await
                    .unwrap()
                    .collect::<Vec<_>>()
                    .await;
                rows.len()
            }
        };
        let label = |value: &str| {
            Instruction::Assert(Artifact {
                the: "task/label".parse().unwrap(),
                of: "task:7".parse().unwrap(),
                is: Value::String(value.to_string()),
                cause: None,
            })
        };

        // Bob labels the task; everyone syncs it.
        let bob = repo.branch("bob").open().perform(&operator).await?;
        bob.commit(stream::iter(vec![label("urgent")]))
            .perform(&operator)
            .await?;
        for name in ["alice", "mallory", "jordan"] {
            let b = repo.branch(name).open().perform(&operator).await?;
            b.set_upstream(&bob).perform(&operator).await?;
            b.pull().perform(&operator).await?;
        }
        let alice = repo.branch("alice").load().perform(&operator).await?;
        let mallory = repo.branch("mallory").load().perform(&operator).await?;
        let jordan = repo.branch("jordan").load().perform(&operator).await?;

        // Concurrently: Alice retracts (observing only Bob's claim);
        // Mallory re-asserts the same value under her own claim.
        alice
            .commit(stream::iter(vec![Instruction::Retract(Artifact {
                the: "task/label".parse()?,
                of: "task:7".parse()?,
                is: Value::String("urgent".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        mallory
            .commit(stream::iter(vec![label("urgent")]))
            .perform(&operator)
            .await?;

        // Jordan pulls Alice's deletion, then Mallory's assertion.
        jordan.pull().from(&alice).perform(&operator).await?;
        assert_eq!(
            count_labels(jordan.clone()).await,
            0,
            "Alice's retraction lands"
        );
        jordan.pull().from(&mallory).perform(&operator).await?;
        assert_eq!(
            count_labels(jordan.clone()).await,
            1,
            "Mallory's claim was never observed by the retraction, so the label survives"
        );

        // Jordan, having now seen both, retracts — and it clears everywhere.
        jordan
            .commit(stream::iter(vec![Instruction::Retract(Artifact {
                the: "task/label".parse()?,
                of: "task:7".parse()?,
                is: Value::String("urgent".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        mallory.pull().from(&jordan).perform(&operator).await?;
        assert_eq!(
            count_labels(mallory.clone()).await,
            0,
            "Jordan observed Mallory's claim, so his retraction covers it"
        );

        Ok(())
    }

    /// Deletion is not forever: a re-assertion brings a fact back, and
    /// the resurrection survives an empty-base pull from a peer still
    /// holding the pre-deletion copy — the stale copy is rejected, the
    /// fresh claim stands. See the observed-remove semantics in
    /// `notes/version-control.md`.
    #[dialog_common::test]
    async fn it_resurrects_a_deleted_fact_and_the_resurrection_survives() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let titles = |branch: crate::Branch| {
            let operator = &operator;
            async move {
                let rows: Vec<_> = branch
                    .claims()
                    .select(ArtifactSelector::new().the("post/title".parse().unwrap()))
                    .perform(operator)
                    .await
                    .unwrap()
                    .collect::<Vec<_>>()
                    .await;
                rows.len()
            }
        };

        let main = repo.branch("main").open().perform(&operator).await?;
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Hej",
        )]))
        .perform(&operator)
        .await?;

        // Laptop and the (soon-stale) tablet both take the fact.
        let laptop = repo.branch("laptop").open().perform(&operator).await?;
        laptop.set_upstream(&main).perform(&operator).await?;
        laptop.pull().perform(&operator).await?;
        let tablet = repo.branch("tablet").open().perform(&operator).await?;
        tablet.set_upstream(&main).perform(&operator).await?;
        tablet.pull().perform(&operator).await?;

        // Laptop deletes, then brings it back — the tablet never learns
        // of either.
        laptop
            .commit(stream::iter(vec![Instruction::Retract(Artifact {
                the: "post/title".parse()?,
                of: "post:1".parse()?,
                is: Value::String("Hej".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        assert_eq!(titles(laptop.clone()).await, 0, "deleted");
        laptop
            .commit(stream::iter(vec![assert_one(
                "post/title",
                "post:1",
                "Hej",
            )]))
            .perform(&operator)
            .await?;
        assert_eq!(titles(laptop.clone()).await, 1, "resurrected");

        // Empty-base pull from the stale tablet, still holding the old
        // copy: the resurrection must stand.
        laptop.pull().from(&tablet).perform(&operator).await?;
        assert_eq!(
            titles(laptop.clone()).await,
            1,
            "a stale peer's copy must not un-resurrect the fact"
        );

        // And the tablet converges onto the resurrected fact.
        tablet.pull().from(&laptop).perform(&operator).await?;
        assert_eq!(
            titles(tablet.clone()).await,
            1,
            "the tablet converges on the resurrected fact"
        );

        Ok(())
    }

    /// A replaced value must not survive via a stale peer (R3 coverage in
    /// `notes/version-control.md`): the replace record's `supersedes`
    /// coverage (R3) retires the superseded claim on a replica that still
    /// holds it live — including across an empty-base pull, where the data
    /// differential carries no remove for the old value (the base never
    /// covered it). The superseded claim lives at *different* keys than the
    /// record's own value (keys embed the value hash), so coverage must scan
    /// the record's (entity, attribute) slot, not probe the record's keys.
    #[dialog_common::test]
    async fn it_retires_a_replaced_value_on_an_empty_base_pull() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // `feature` authors the original value.
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature
            .commit(stream::iter(vec![assert_one(
                "user/name",
                "user:1",
                "Alice",
            )]))
            .perform(&operator)
            .await?;

        // `main` adopts it, then replaces it. The replace record's
        // supersedes names the version of feature's claim.
        let main = repo.branch("main").open().perform(&operator).await?;
        main.pull().from(&feature).perform(&operator).await?;
        main.commit(stream::iter(vec![Instruction::Replace(Artifact {
            the: "user/name".parse()?,
            of: "user:1".parse()?,
            is: Value::String("Bob".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        // `feature` pulls main for the first time — an empty-base merge.
        // Its live copy of the old value can only be retired by the
        // incoming replace record's coverage.
        feature.pull().from(&main).perform(&operator).await?;

        let names: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            names.len(),
            1,
            "the superseded value must not survive next to its replacement: {names:?}"
        );
        assert_eq!(names[0].is, Value::String("Bob".into()));

        // Confluence: the reverse pull agrees and both replicas quiesce
        // onto the same tree.
        main.pull().from(&feature).perform(&operator).await?;
        assert_eq!(
            main.revision().map(|r| r.tree),
            feature.revision().map(|r| r.tree),
            "both replicas converge on the same tree"
        );

        Ok(())
    }

    /// The incrementally maintained context memo must agree exactly with
    /// the ancestry walk. Pull folds the delta's revision records into
    /// the local context instead of re-walking the DAG, and commit
    /// extends the memo by one version; if either drifted from
    /// `context_of`, the observed-remove screen would silently change
    /// behavior on later pulls (an under-watermark resurrects deletions,
    /// an over-watermark drops live claims).
    #[dialog_common::test]
    async fn it_maintains_the_context_memo_incrementally() -> Result<()> {
        use dialog_artifacts::history::context_of;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        for i in 0..3 {
            main.commit(stream::iter(vec![assert_one(
                "post/title",
                &format!("post:{i}"),
                "seed",
            )]))
            .perform(&operator)
            .await?;
        }

        // Adopt (fast-forward): the memo entry comes from the fold of
        // the delta's revision records.
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;

        let agree = |branch: crate::Branch| {
            let operator = &operator;
            async move {
                let head = branch.revision().expect("branch has a head");
                let memo = branch
                    .contexts()
                    .cached(&head.version())
                    .await
                    .expect("the memo is primed");
                let history = branch.history(operator);
                let walked = context_of(&head.version(), &history).await?;
                anyhow::Ok((memo, walked))
            }
        };

        let (memo, walked) = agree(feature.clone()).await?;
        assert_eq!(memo, walked, "adopt: memo must equal the walk");

        // Commit extends the memo by one version.
        feature
            .commit(stream::iter(vec![assert_one(
                "post/title",
                "post:9",
                "ours",
            )]))
            .perform(&operator)
            .await?;
        let (memo, walked) = agree(feature.clone()).await?;
        assert_eq!(memo, walked, "commit: memo must equal the walk");

        // A real merge folds the upstream's novel revisions plus the
        // minted merge itself.
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:10",
            "theirs",
        )]))
        .perform(&operator)
        .await?;
        feature.pull().perform(&operator).await?;
        let (memo, walked) = agree(feature.clone()).await?;
        assert_eq!(memo, walked, "merge: memo must equal the walk");

        Ok(())
    }

    /// Every published head carries its causal context under the head
    /// signature: it must equal the ancestry walk exactly, and tampering
    /// with it must fail verification like tampering with any other
    /// field.
    #[dialog_common::test]
    async fn it_publishes_the_watermark_with_the_head() -> Result<()> {
        use dialog_artifacts::history::{Edition, Origin, Version, context_of};

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        for i in 0..3 {
            main.commit(stream::iter(vec![assert_one(
                "post/title",
                &format!("post:{i}"),
                "seed",
            )]))
            .perform(&operator)
            .await?;
        }

        let head = main.revision().expect("main has a head");
        let published = head
            .context
            .clone()
            .expect("a freshly minted head publishes its context");
        let history = main.history(&operator);
        let walked = context_of(&head.version(), &history).await?;
        assert_eq!(
            published, walked,
            "the published watermark must equal the ancestry walk"
        );

        head.verify().expect("the untouched head verifies");

        // Inflating the watermark (claiming observation of a revision
        // that is not in the ancestry) must break the signature.
        let mut tampered = head.clone();
        let mut context = published.clone();
        context.record(Version::new(Origin::from([9u8; 32]), Edition::new(9)));
        tampered.context = Some(context);
        assert!(
            tampered.verify().is_err(),
            "a tampered watermark must fail head verification"
        );

        // Stripping it entirely must break the signature too.
        let mut stripped = head.clone();
        stripped.context = None;
        assert!(
            stripped.verify().is_err(),
            "a stripped watermark must fail head verification"
        );

        Ok(())
    }

    /// Adopting an upstream that has seen everything we have, when we
    /// have no local novelty, must not read the upstream's tree at all:
    /// the head (with its published watermark) is adopted by root and
    /// blocks hydrate lazily on demand. This is the guard against pull
    /// cost scaling with upstream churn in regions the replica never
    /// touches.
    #[dialog_common::test]
    async fn it_adopts_an_upstream_head_without_reading_its_novelty() -> Result<()> {
        use crate::RepositoryExt as _;
        use crate::helpers::Counting;
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let env = Counting::new(operator);
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&env)
            .await?;

        // The upstream accumulates plenty of novelty in a namespace the
        // replica never asks about.
        let main = repo.branch("main").open().perform(&env).await?;
        for i in 0..50 {
            main.commit(stream::iter(vec![assert_one(
                "user/name",
                &format!("user:{i}"),
                "resident",
            )]))
            .perform(&env)
            .await?;
        }

        // First pull: empty base, no local novelty, upstream knows
        // everything we know (we know nothing). Adopt by root.
        let feature = repo.branch("feature").open().perform(&env).await?;
        feature.set_upstream(&main).perform(&env).await?;
        env.reset();
        feature.pull().perform(&env).await?.expect("head adopted");
        assert_eq!(
            env.block_reads(),
            0,
            "adoption must not read the upstream tree: {:?}",
            env.snapshot()
        );
        assert_eq!(
            feature.revision().map(|r| r.tree),
            main.revision().map(|r| r.tree),
            "the upstream head is adopted verbatim"
        );

        // Steady state: upstream moves, we still have no novelty of our
        // own. Every subsequent pull is another zero-read adoption.
        main.commit(stream::iter(vec![assert_one(
            "user/name",
            "user:99",
            "new",
        )]))
        .perform(&env)
        .await?;
        env.reset();
        feature.pull().perform(&env).await?.expect("head adopted");
        assert_eq!(
            env.block_reads(),
            0,
            "a fast-forward pull must not read the upstream tree: {:?}",
            env.snapshot()
        );

        // The adopted data is really there: reads hydrate lazily.
        let rows: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&env)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(rows.len(), 51, "adopted facts are readable on demand");

        Ok(())
    }

    /// Pulling from an upstream whose watermark is included in ours is
    /// a no-op detected from the two heads alone: no tree walk, no
    /// block reads, the local head stands, and only the sync base
    /// advances.
    #[dialog_common::test]
    async fn it_skips_a_pull_from_an_upstream_that_has_seen_everything() -> Result<()> {
        use crate::RepositoryExt as _;
        use crate::helpers::Counting;

        let (operator, profile) = test_operator_with_profile().await;
        let env = Counting::new(operator);
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&env)
            .await?;

        let main = repo.branch("main").open().perform(&env).await?;
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Hej",
        )]))
        .perform(&env)
        .await?;

        // `feature` adopts main's head, then commits novelty of its own,
        // so feature has seen everything main has (and more).
        let feature = repo.branch("feature").open().perform(&env).await?;
        feature.set_upstream(&main).perform(&env).await?;
        feature.pull().perform(&env).await?;
        feature
            .commit(stream::iter(vec![assert_one(
                "post/title",
                "post:2",
                "Nej",
            )]))
            .perform(&env)
            .await?;
        let head = feature.revision().expect("feature has a head");

        // Feature pulls main again: main's watermark is included in
        // feature's, so there is nothing to gain. Zero reads, head
        // stands.
        env.reset();
        feature.pull().perform(&env).await?;
        assert_eq!(
            env.block_reads(),
            0,
            "a known-subsumed upstream must be skipped without reads: {:?}",
            env.snapshot()
        );
        assert_eq!(
            feature.revision().as_ref(),
            Some(&head),
            "the local head stands"
        );

        Ok(())
    }

    /// Wholesale adoption must be refused when we have observed
    /// something the upstream has not, even with no local commits: our
    /// extra knowledge can include a deletion of a fact the upstream
    /// still holds live, and adopting its tree would resurrect it. The
    /// watermark-inclusion gate forces the screened merge, where R1
    /// rejects the stale fact.
    #[dialog_common::test]
    async fn it_refuses_adoption_when_local_knowledge_exceeds_the_upstreams() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // `author` asserts the fact; `moderator` adopts it and retracts
        // it. `author` then commits unrelated novelty the moderator has
        // never seen, so author and moderator genuinely diverge.
        let author = repo.branch("author").open().perform(&operator).await?;
        author
            .commit(stream::iter(vec![assert_one(
                "post/title",
                "post:1",
                "Spam",
            )]))
            .perform(&operator)
            .await?;

        let moderator = repo.branch("moderator").open().perform(&operator).await?;
        moderator.set_upstream(&author).perform(&operator).await?;
        moderator.pull().perform(&operator).await?;
        moderator
            .commit(stream::iter(vec![Instruction::Retract(Artifact {
                the: "post/title".parse()?,
                of: "post:1".parse()?,
                is: Value::String("Spam".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        author
            .commit(stream::iter(vec![assert_one(
                "post/title",
                "post:2",
                "Legit",
            )]))
            .perform(&operator)
            .await?;

        // The replica learns of the deletion first (adopting the
        // moderator's head), then pulls the author, who still holds the
        // deleted fact live and has novelty of his own. The gate must
        // refuse adoption (we know the deletion, the author does not)
        // and the screened merge must keep the fact dead while taking
        // the novelty.
        let replica = repo.branch("replica").open().perform(&operator).await?;
        replica.set_upstream(&moderator).perform(&operator).await?;
        replica.pull().perform(&operator).await?;
        replica.pull().from(&author).perform(&operator).await?;

        let titles: Vec<_> = replica
            .claims()
            .select(ArtifactSelector::new().the("post/title".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let values: Vec<_> = titles.iter().map(|t| t.is.clone()).collect();
        assert!(
            values.contains(&Value::String("Legit".into())),
            "the author's genuine novelty lands: {values:?}"
        );
        assert!(
            !values.contains(&Value::String("Spam".into())),
            "the deleted fact must not be resurrected by adoption: {values:?}"
        );

        Ok(())
    }

    /// A replica with local novelty pulling an upstream that has seen
    /// everything else replays its own (small) delta onto the adopted
    /// upstream tree: reads scale with the replica's novelty, not with
    /// the upstream's churn.
    #[dialog_common::test]
    async fn it_replays_local_novelty_onto_an_upstream_without_reading_its_churn() -> Result<()> {
        use crate::RepositoryExt as _;
        use crate::helpers::Counting;
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let env = Counting::new(operator);
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&env)
            .await?;

        let main = repo.branch("main").open().perform(&env).await?;
        main.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:0",
            "seed",
        )]))
        .perform(&env)
        .await?;

        let feature = repo.branch("feature").open().perform(&env).await?;
        feature.set_upstream(&main).perform(&env).await?;
        feature.pull().perform(&env).await?;

        // The replica commits one fact of its own; the upstream churns
        // through two hundred commits in a namespace the replica never
        // touches.
        feature
            .commit(stream::iter(vec![assert_one(
                "post/title",
                "post:1",
                "ours",
            )]))
            .perform(&env)
            .await?;
        for i in 0..200 {
            main.commit(stream::iter(vec![assert_one(
                "user/name",
                &format!("user:{i}"),
                "resident",
            )]))
            .perform(&env)
            .await?;
        }

        env.reset();
        feature.pull().perform(&env).await?.expect("merged");
        let reads = env.block_reads();
        assert!(
            reads <= 30,
            "replaying one local commit must not read the upstream's churn \
             (got {reads} block reads): {:?}",
            env.snapshot()
        );

        // Both sides' content is present.
        let titles: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("post/title".parse()?))
            .perform(&env)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(titles.len(), 2, "seed and local novelty both present");
        let churn: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&env)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(churn.len(), 200, "the upstream churn is all adopted");

        Ok(())
    }

    /// A deletion of a fact that entered the replica after its sync
    /// base nets to nothing in the replica's data diff; only its
    /// covering record can retire the upstream's live copy during the
    /// replay. The record must ride the replayed history and screen the
    /// upstream tree.
    #[dialog_common::test]
    async fn it_carries_a_covering_record_when_replaying_onto_a_stale_holder() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        // The shared base: replica `f` syncs upstream `m` before the
        // contested fact exists anywhere.
        let m = repo.branch("m").open().perform(&operator).await?;
        m.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:0",
            "seed",
        )]))
        .perform(&operator)
        .await?;
        let f = repo.branch("f").open().perform(&operator).await?;
        f.set_upstream(&m).perform(&operator).await?;
        f.pull().perform(&operator).await?;

        // `a` authors the fact; both `m` and `f` adopt it laterally, so
        // it postdates f's sync base with m.
        let a = repo.branch("a").open().perform(&operator).await?;
        a.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Spam",
        )]))
        .perform(&operator)
        .await?;
        m.pull().from(&a).perform(&operator).await?;
        f.pull().from(&a).perform(&operator).await?;

        // f retracts it: net zero in f's data diff against its base
        // with m (the fact was never in that base), so only f's retract
        // record carries the deletion into the replay.
        f.commit(stream::iter(vec![Instruction::Retract(Artifact {
            the: "post/title".parse()?,
            of: "post:1".parse()?,
            is: Value::String("Spam".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

        f.pull().perform(&operator).await?.expect("merged");

        let values: Vec<_> = f
            .claims()
            .select(ArtifactSelector::new().the("post/title".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|t| t.is)
            .collect();
        assert!(
            values.contains(&Value::String("seed".into())),
            "the base fact survives: {values:?}"
        );
        assert!(
            !values.contains(&Value::String("Spam".into())),
            "the replayed covering record must retire the upstream's live copy: {values:?}"
        );

        Ok(())
    }

    /// When the replica's delta adds a claim the upstream has observed,
    /// the replay's swapped R1 drops it against the upstream's
    /// watermark: if the upstream still holds it the add was a no-op,
    /// and if the upstream's log covered it (as here), applying it
    /// would resurrect the deletion. The replica's own fresh claims are
    /// above the watermark and land as news.
    #[dialog_common::test]
    async fn it_drops_observed_adds_when_replaying_onto_a_covering_upstream() -> Result<()> {
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let m = repo.branch("m").open().perform(&operator).await?;
        m.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:0",
            "seed",
        )]))
        .perform(&operator)
        .await?;
        let f = repo.branch("f").open().perform(&operator).await?;
        f.set_upstream(&m).perform(&operator).await?;
        f.pull().perform(&operator).await?;

        // `a` authors the fact. The upstream adopts it AND covers it;
        // the replica adopts it and keeps it live, plus commits novelty
        // of its own.
        let a = repo.branch("a").open().perform(&operator).await?;
        a.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "Spam",
        )]))
        .perform(&operator)
        .await?;
        m.pull().from(&a).perform(&operator).await?;
        m.commit(stream::iter(vec![Instruction::Retract(Artifact {
            the: "post/title".parse()?,
            of: "post:1".parse()?,
            is: Value::String("Spam".to_string()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;
        f.pull().from(&a).perform(&operator).await?;
        f.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:2",
            "ours",
        )]))
        .perform(&operator)
        .await?;

        // f's delta adds the authored fact; m has observed it and
        // covered it, so the replay's swapped R1 must drop the add
        // rather than resurrect m's deletion.
        f.pull().perform(&operator).await?.expect("merged");

        let values: Vec<_> = f
            .claims()
            .select(ArtifactSelector::new().the("post/title".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|t| t.is)
            .collect();
        assert!(
            !values.contains(&Value::String("Spam".into())),
            "the upstream's deletion must not be resurrected: {values:?}"
        );
        assert!(
            values.contains(&Value::String("ours".into())),
            "the replica's own novelty survives the fallback: {values:?}"
        );

        Ok(())
    }

    /// A first-contact pull (no sync base) picks the merge direction by
    /// comparing the two watermarks' divergence masses: a small replica
    /// contacting a churning upstream replays its own few entries onto
    /// the adopted upstream tree instead of walking the upstream's
    /// churn. Reads track the smaller side.
    #[dialog_common::test]
    async fn it_first_contacts_a_churning_upstream_from_the_small_side() -> Result<()> {
        use crate::RepositoryExt as _;
        use crate::helpers::Counting;
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let env = Counting::new(operator);
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&env)
            .await?;

        // A churning upstream the replica has never synced with.
        let main = repo.branch("main").open().perform(&env).await?;
        for i in 0..200 {
            main.commit(stream::iter(vec![assert_one(
                "user/name",
                &format!("user:{i}"),
                "resident",
            )]))
            .perform(&env)
            .await?;
        }

        // The replica holds two facts of its own, nothing shared.
        let feature = repo.branch("feature").open().perform(&env).await?;
        for i in 0..2 {
            feature
                .commit(stream::iter(vec![assert_one(
                    "post/title",
                    &format!("post:{i}"),
                    "ours",
                )]))
                .perform(&env)
                .await?;
        }

        env.reset();
        feature
            .pull()
            .from(&main)
            .perform(&env)
            .await?
            .expect("merged");
        let reads = env.block_reads();
        assert!(
            reads <= 30,
            "a first-contact pull from the small side must not read the upstream's churn (got {reads}): {:?}",
            env.snapshot()
        );

        // Both sides' content is present in the merged state.
        let ours: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("post/title".parse()?))
            .perform(&env)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(ours.len(), 2, "our facts survive");
        let theirs: Vec<_> = feature
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&env)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(theirs.len(), 200, "the upstream churn is all adopted");

        Ok(())
    }

    /// Randomized three-replica convergence: three branches make
    /// deterministic pseudo-random writes (assert, replace, retract over
    /// small entity and value pools) interleaved with pseudo-random
    /// pairwise pulls, across every merge path the gates can pick
    /// (adopt, skip, replay, screened). Afterwards, bounded rounds of
    /// all-pairs pulls must land all three replicas on byte-identical
    /// trees. This is the convergence invariant stated in
    /// `notes/version-control.md`: same log, same cache, any exchange
    /// order.
    #[dialog_common::test]
    async fn it_converges_under_randomized_triangle_sync() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let a = repo.branch("a").open().perform(&operator).await?;
        let b = repo.branch("b").open().perform(&operator).await?;
        let c = repo.branch("c").open().perform(&operator).await?;
        let branches = [&a, &b, &c];

        // A small deterministic generator (an LCG): reproducible runs,
        // no wall-clock or OS randomness.
        let mut state: u64 = 0x5DEECE66D;
        let mut next = move |bound: u64| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % bound
        };

        let entity = |i: u64| format!("thing:{i}");
        let value = |i: u64| Value::String(format!("value {i}"));

        for _round in 0..12 {
            // Every branch performs one pseudo-random write.
            for branch in branches {
                let of: dialog_artifacts::Entity = entity(next(4)).parse()?;
                let the: dialog_artifacts::Attribute = "bench/field".parse()?;
                let is = value(next(3));
                let artifact = Artifact {
                    the,
                    of,
                    is,
                    cause: None,
                };
                let instruction = match next(3) {
                    0 => Instruction::Assert(artifact),
                    1 => Instruction::Replace(artifact),
                    _ => Instruction::Retract(artifact),
                };
                // A retract of an absent fact is a no-op commit; both
                // outcomes are fine for the property.
                let _ = branch
                    .commit(stream::iter(vec![instruction]))
                    .perform(&operator)
                    .await;
            }
            // One pseudo-random pairwise pull.
            let from = next(3) as usize;
            let into = (from + 1 + next(2) as usize) % 3;
            branches[into]
                .pull()
                .from(branches[from])
                .perform(&operator)
                .await?;
        }

        // Bounded all-pairs rounds must reach a fixed point where all
        // three roots agree.
        let mut converged = false;
        for _ in 0..6 {
            for into in 0..3 {
                for from in 0..3 {
                    if into != from {
                        branches[into]
                            .pull()
                            .from(branches[from])
                            .perform(&operator)
                            .await?;
                    }
                }
            }
            let roots: Vec<_> = branches
                .iter()
                .map(|branch| branch.revision().map(|r| r.tree))
                .collect();
            if roots[0] == roots[1] && roots[1] == roots[2] {
                converged = true;
                break;
            }
        }
        let roots: Vec<_> = branches
            .iter()
            .map(|branch| branch.revision().map(|r| r.tree))
            .collect();
        assert!(
            converged,
            "three replicas must converge within bounded all-pairs rounds: {roots:?}"
        );

        Ok(())
    }

    /// The graft merge: a replica that adopted a bulky upstream AND
    /// carries its own novelty pulls a small tracked upstream. Neither
    /// replay direction serves this (either walks the bulk); the graft
    /// partitions by divergence spans, adopts the bulk by subtree hash,
    /// and does entry work only where the change sets meet. Reads must
    /// track the small delta plus seams.
    #[dialog_common::test]
    async fn it_grafts_a_tracked_merge_without_walking_adopted_bulk() -> Result<()> {
        use crate::RepositoryExt as _;
        use crate::helpers::Counting;
        use dialog_artifacts::ArtifactSelector;
        use futures_util::StreamExt as _;

        let (operator, profile) = test_operator_with_profile().await;
        let env = Counting::new(operator);
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&env)
            .await?;

        let seed = repo.branch("seed").open().perform(&env).await?;
        seed.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:0",
            "seed",
        )]))
        .perform(&env)
        .await?;

        // Bob diverges from the seed while small; we sync him (tracked).
        let bob = repo.branch("bob").open().perform(&env).await?;
        bob.set_upstream(&seed).perform(&env).await?;
        bob.pull().perform(&env).await?;

        let us = repo.branch("us").open().perform(&env).await?;
        us.set_upstream(&seed).perform(&env).await?;
        us.pull().perform(&env).await?;
        us.pull().from(&bob).perform(&env).await?;

        // Alice's bulk lands on us (adopt-or-merge; either way we now
        // carry two hundred commits Bob has never seen), plus one commit
        // of our own so we are not a pure adoption.
        let alice = repo.branch("alice").open().perform(&env).await?;
        alice.set_upstream(&seed).perform(&env).await?;
        alice.pull().perform(&env).await?;
        for i in 0..200 {
            alice
                .commit(stream::iter(vec![assert_one(
                    "user/name",
                    &format!("user:{i}"),
                    "resident",
                )]))
                .perform(&env)
                .await?;
        }
        us.pull().from(&alice).perform(&env).await?;
        us.commit(stream::iter(vec![assert_one(
            "post/title",
            "post:1",
            "ours",
        )]))
        .perform(&env)
        .await?;

        // Bob moves by three commits; we pull him. The old replay walked
        // our whole divergence (the adopted bulk); the graft must not.
        for i in 0..3 {
            bob.commit(stream::iter(vec![assert_one(
                "city/name",
                &format!("city:{i}"),
                "bobton",
            )]))
            .perform(&env)
            .await?;
        }
        env.reset();
        us.pull().from(&bob).perform(&env).await?.expect("merged");
        let reads = env.block_reads();
        assert!(
            reads <= 80,
            "a graft merge must not walk the adopted bulk (got {reads} reads): {:?}",
            env.snapshot()
        );

        // Everything is present afterwards.
        let count = |the: &str| {
            let the: dialog_artifacts::Attribute = the.parse().unwrap();
            let us = us.clone();
            let env = &env;
            async move {
                anyhow::Ok(
                    us.claims()
                        .select(ArtifactSelector::new().the(the))
                        .perform(env)
                        .await?
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?
                        .len(),
                )
            }
        };
        assert_eq!(count("user/name").await?, 200, "the adopted bulk survives");
        assert_eq!(count("city/name").await?, 3, "bob's novelty lands");
        assert_eq!(count("post/title").await?, 2, "seed and our own fact stand");

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
