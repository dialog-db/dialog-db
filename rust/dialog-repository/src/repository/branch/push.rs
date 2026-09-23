use dialog_effects::MethodExt as _;
use dialog_effects::archive::prelude::{CatalogExt as _, GetBlockExt as _};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, ReadBlobExt as _, WriteBlobExt as _};
use std::collections::HashSet;

use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{
    Datum, Key as ArtifactKey, ShipmentRef, State, shipment_ref, shipment_refs,
};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::ArchiveExt as _;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::{BlobError, BlobReader, Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::{
    ArchivedNodeBody, ContentAddressedStorage as TreeStorage, MissingBlocks, MissingPolicy,
    NoveltyOp, PersistentNode, TreeDifference, into_owned,
};
use dialog_storage::StorageBackend as _;
use futures_util::{StreamExt as _, TryStreamExt as _, stream};

use super::resolve::resolve;
use crate::registry::RegistryEnv;
use crate::{
    Branch, ConnectedReplica, Index, LocalIndex, PublishError, PushError, RemoteArchiveIndex,
    RemoteSite, RepositoryMemoryExt, Revision, Upstream, UpstreamBranch,
};
use futures_util::future::join_all;

/// Command struct for pushing local changes to an upstream branch.
///
/// Borrows `&Branch` (non-consuming). Reads the branch's upstream to
/// dispatch to local or remote push logic.
pub struct Push<'a> {
    branch: &'a Branch,
    to: Option<Upstream>,
    confirm_upstream: bool,
}

impl<'a> Push<'a> {
    fn new(branch: &'a Branch) -> Self {
        Self {
            branch,
            to: None,
            confirm_upstream: true,
        }
    }

    /// Push without first confirming where upstream stands.
    ///
    /// A push reads the upstream head before its fast-forward check, so
    /// that a push doomed by another writer is refused before the upload
    /// rather than after it. That read costs a round trip, and a caller
    /// that *just* read the same cell — a sync sweep that pulls and then
    /// pushes — is paying for an answer it already has.
    ///
    /// Only the caller can weigh that, which is why this is not decided
    /// here from a freshness rule of our own: how recently *we* observed
    /// upstream says nothing about whether *someone else* has written
    /// since, so the saving is real only when we are the likely sole
    /// writer. Read [`Cell::age`](crate::Age) and decide.
    ///
    /// Skipping is safe, never merely cheap: the head write is a
    /// conditional request carrying the version we hold, so a remote
    /// that moved rejects it regardless. What is given up is *early*
    /// detection. The costs of being wrong:
    ///
    /// - The novelty upload ships before the rejection. Those blocks are
    ///   content-addressed, so the target absorbs them idempotently and
    ///   nothing is corrupted, but the bandwidth is spent.
    /// - The refusal arrives as
    ///   [`PublishError::VersionMismatch`](crate::PublishError) rather
    ///   than [`PushError::NonFastForward`]. A caller that reports
    ///   conflicts must recognize both.
    pub fn assuming_upstream(mut self) -> Self {
        self.confirm_upstream = false;
        self
    }

    /// Push to the given branch alone, instead of every upstream.
    ///
    /// Accepts either a `&Branch` or a `&ConnectedBranch` — the same inputs as
    /// [`Branch::set_upstream`]. The tree last synced with that branch
    /// drives the fast-forward check and the novelty upload, or the empty
    /// base if it never was (only a target with no revision of its own
    /// accepts such a push). A successful push records how far it got,
    /// but does not make the target an upstream: that is
    /// [`Branch::push_to`]'s to record.
    pub fn to(mut self, source: impl Into<UpstreamBranch>) -> Self {
        self.to = Some(Upstream::from(source.into()));
        self
    }
}

impl Branch {
    /// Create a command to push local changes to every branch this one
    /// pushes to.
    ///
    /// Chain [`Push::to`] to push to one branch alone instead.
    pub fn push(&self) -> Push<'_> {
        Push::new(self)
    }
}

impl Push<'_> {
    /// Execute the push operation.
    ///
    /// Push is fast-forward only:
    ///
    /// - `Ok(Some(revision))` — pushed; upstream now at `revision`.
    /// - `Ok(None)` — nothing to push (branch has no local revision).
    /// - `Err(PushError::NonFastForward)` — upstream has moved since
    ///   the last sync; pull to integrate before pushing again.
    ///
    /// For remote upstream, bytes land on the remote in reference order
    /// — blob bytes and spilled values, then by-reference frontier
    /// subtrees, then held novelty children-before-parents, then the
    /// revision — so EVERY prefix of a push (an aborted one included)
    /// leaves the remote closure-complete: a block's presence implies
    /// the presence of everything it references. That closure property
    /// is what makes another pusher's existence probes trustworthy, and
    /// it is why a published head never references bytes the remote is
    /// missing.
    ///
    /// The novelty walk reads the local archive only and treats a block
    /// held by reference as the boundary of local knowledge (see
    /// [`MissingBlocks`]): a base adopted by root from the target — the
    /// everyday scenario-3 pull — diffs cleanly without hydration, and
    /// only blocks this replica minted are shipped. A head carrying
    /// subtrees adopted from *other* tracked remotes is adjudicated at
    /// that boundary: content the target provably has (attribution when
    /// it is the sole tracked remote, one existence probe per subtree
    /// root otherwise) moves nothing; content it provably lacks is
    /// fetched from the remotes that hold it and streamed through to the
    /// target without being persisted here. The push fails loudly only
    /// for content reachable from no store at all.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, PushError>
    where
        Env: RegistryEnv
            + Provider<BlobRead>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Put>>
            + Provider<Fork<RemoteSite, Publish>>
            + Provider<Fork<RemoteSite, BlobImport>>
            + Provider<Fork<RemoteSite, BlobRead>>,
    {
        let branch = self.branch;
        let confirm = self.confirm_upstream;
        resolve(branch, env).await?;

        // Push to the given target -- the tracked entry for it, or, for
        // one not tracked yet, a fresh entry whose empty sync base only
        // fast-forwards onto an empty target -- or else to every branch
        // this one pushes to, at once: the pushes are independent.
        let Some(target) = self.to else {
            let upstreams: Vec<Upstream> = branch.pushes().iter().cloned().collect();
            if upstreams.is_empty() {
                return Err(PushError::BranchHasNoUpstream {
                    branch: branch.name().to_string(),
                });
            }
            // One upstream that cannot be pushed to does not keep the push
            // from the others: it lands where it can and reports the rest.
            let total = upstreams.len();
            let results = join_all(upstreams.into_iter().map(|upstream| async move {
                let target = upstream.target();
                (
                    target,
                    Box::pin(push_upstream(branch, upstream, confirm, env)).await,
                )
            }))
            .await;
            let mut pushed = None;
            let mut unreached = Vec::new();
            for (target, result) in results {
                match result {
                    Ok(revision) => pushed = pushed.or(revision),
                    Err(error) => unreached.push((target, error)),
                }
            }
            return match unreached.len() {
                0 => Ok(pushed),
                failed if failed == total => Err(unreached.remove(0).1),
                _ => Err(PushError::Partial {
                    pushed: pushed.map(Box::new),
                    unreached,
                }),
            };
        };
        if let Upstream::Local { branch: name, .. } = &target
            && name == branch.name()
        {
            return Err(PushError::UpstreamIsItself {
                branch: branch.name().to_string(),
            });
        }
        let upstream = branch
            .upstreams()
            .find(&target)
            .cloned()
            .unwrap_or_else(|| {
                let tree = branch.tracked().tree(&target.target());
                target.with_tree(tree)
            });
        Box::pin(push_upstream(branch, upstream, confirm, env)).await
    }
}

/// Push `branch` to one upstream, fast-forward only.
async fn push_upstream<Env>(
    branch: &Branch,
    upstream_state: Upstream,
    confirm_upstream: bool,
    env: &Env,
) -> Result<Option<Revision>, PushError>
where
    Env: RegistryEnv
        + Provider<BlobRead>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Put>>
        + Provider<Fork<RemoteSite, Publish>>
        + Provider<Fork<RemoteSite, BlobImport>>
        + Provider<Fork<RemoteSite, BlobRead>>,
{
    {
        let revision = match branch.revision() {
            Some(revision) => revision,
            None => return Ok(None),
        };
        let base = upstream_state.tree().clone();

        // Nothing new to push: the local head already equals the recorded
        // upstream sync point. Without this guard every sync tick re-publishes
        // the revision pointer to the remote (an ongoing `branch/*/revision`
        // PUT) and re-fetches + diffs the upstream for an empty novelty set,
        // even when no commit has landed since the last push. Short-circuit so
        // an idle branch does no push I/O.
        if revision.tree == base {
            return Ok(Some(revision));
        }

        match &upstream_state {
            Upstream::Local {
                branch: upstream_name,
                ..
            } => {
                let target = branch
                    .subject()
                    .branch(upstream_name.clone())
                    .open()
                    .perform(env)
                    .await?;

                let current = target.revision().map(|r| r.tree).unwrap_or_default();
                if current != base {
                    return Err(PushError::NonFastForward {
                        branch: branch.name().to_string(),
                        expected: base,
                        actual: current,
                    });
                }

                target.reset(revision.clone()).perform(env).await?;
            }
            Upstream::Unreachable { target, reason, .. } => {
                return Err(PushError::Unreachable {
                    upstream: target.to_string(),
                    reason: reason.clone(),
                });
            }
            Upstream::Remote {
                remote,
                branch: upstream_branch_name,
                ..
            } => {
                let upstream = remote
                    .branch(upstream_branch_name.clone())
                    .open()
                    .perform(env)
                    .await?;

                // Refresh the cache from the remote so our divergence
                // check sees the latest upstream tree, not whatever was
                // in our last snapshot. The caller may already hold a
                // fresh answer and say so; see [`Push::assuming_upstream`]
                // for what that gives up.
                if confirm_upstream {
                    upstream.fetch().perform(env).await?;
                }

                // The trust boundary, same as pull's: this head was minted
                // elsewhere, and every gate below (the fast-forward check,
                // the novelty diff, the upload) acts on it. A forged or
                // tampered head is rejected before a single byte moves.
                if let Some(fetched) = upstream.revision() {
                    fetched
                        .verify()
                        .map_err(dialog_artifacts::DialogArtifactsError::from)?;
                }

                let current = upstream.revision().map(|r| r.tree).unwrap_or_default();
                if current != base {
                    return Err(PushError::NonFastForward {
                        branch: branch.name().to_string(),
                        expected: base,
                        actual: current,
                    });
                }

                // Upload tree nodes present in our current tree but not
                // in the base, so the remote can hydrate the new tree
                // before we publish the revision pointing at it.
                //
                // The walk reads the local archive only, and a replica
                // legitimately holds whole subtrees by reference (a
                // fast-forward pull adopts the upstream head by root, zero
                // reads), so absence is information, not a fault
                // ([`MissingBlocks::Boundary`], both sides):
                //
                // - The BASE is the recorded sync point with this very
                //   upstream — a tree the target itself served or accepted —
                //   so an absent base-side block is definitionally present
                //   on the target. Losing its subtraction only over-uploads
                //   held nodes, which idempotent content-addressed puts
                //   absorb. Sound at any remote count.
                // - A block absent under OUR head is not our novelty (a
                //   replica stores what it mints). Whether the TARGET has
                //   it is adjudicated below, per unresolved subtree root:
                //   free attribution when the target is the only tracked
                //   remote, one existence probe otherwise, and a
                //   fetch-forward transfer — streamed through, never
                //   persisted locally — only for content the target
                //   provably lacks.
                let missing = MissingPolicy {
                    source: MissingBlocks::Boundary,
                    target: MissingBlocks::Boundary,
                };

                let index = branch.archive().index();
                let store = LocalIndex::new(env, index.clone());
                let base_tree = Index::from_hash(NodeHash::from(*base.hash()));
                let current_tree = Index::from_hash(NodeHash::from(*revision.tree.hash()));
                let tree_store = TreeStorage::new(TreeStorageBridge(store));
                let difference = TreeDifference::compute_with(
                    &base_tree,
                    &current_tree,
                    &tree_store,
                    &tree_store,
                    missing,
                )
                .await?;

                let remote_archive = remote.archive();
                let remote_index = remote_archive.index();

                // Everything below lands on the remote in REFERENCE ORDER —
                // a block's referents (blob bytes, spilled value blocks,
                // child nodes, forwarded subtrees) are durable before any
                // block that names them: blobs and spills first, then the
                // by-reference frontier subtrees, then the held novelty
                // children-before-parents, then the revision. The ordering
                // is a protocol invariant, not a nicety: the existence
                // probes prune a whole subtree on one positive answer, which
                // is sound only if a block's presence on a protocol-written
                // store implies the presence of everything it references —
                // and that holds only if every prefix of every push
                // (aborted ones included) leaves the store closure-complete.
                //
                // Attribution first, since the shipment of by-reference
                // content depends on it. When the target is the only remote
                // reachable from the tracked upstream set, everything held
                // by reference came from it — attribution, zero requests.
                // Otherwise one probe per item settles it, and only content
                // the target provably lacks is transferred, fetched from the
                // remotes that have it and streamed through without being
                // persisted.
                //
                // Reachable means TRANSITIVE: a local upstream shares this
                // archive, but its head can hold content by reference from
                // remotes only IT tracks — attribution that stopped at the
                // local entry would credit that content to this branch's own
                // remote and silently skip forwarding it.
                let tracked = tracked_remotes(branch, env).await?;
                let sole_remote = tracked.iter().all(|other| other.same(remote));
                let sources = if sole_remote {
                    Vec::new()
                } else {
                    source_remotes(&tracked, remote)
                };

                // Ship the blocks the tree nodes reference but the node
                // upload does not carry: blob bytes and spilled value
                // blocks, surfaced by ONE entry-level drain of the SAME
                // differential the node upload walks (`shipment_refs`), so
                // the changed paths are read once per push instead of once
                // per concern. These land FIRST: a node must never be
                // durable on the remote before the bytes its entries name,
                // or an aborted push leaves probe-trustable residue that a
                // later pusher prunes against — publishing a head whose
                // blobs the remote does not hold.
                // Shipments are independent of one another, so they cross
                // together: SHIPMENT_CONCURRENCY at a time, the whole set
                // awaited before anything that references them lands. On
                // the sign-in path this loop awaited each shipment in turn
                // and measured as the single largest cost of the push (one
                // round trip per spilled value, one after another).
                let blob_store = LocalIndex::new(env, index.clone());
                let shipments = shipment_refs(&difference)
                    .map(|shipment| {
                        ship(
                            shipment,
                            branch,
                            remote,
                            &remote_index,
                            &blob_store,
                            &sources,
                            sole_remote,
                            env,
                        )
                    })
                    .buffer_unordered(SHIPMENT_CONCURRENCY)
                    .try_collect::<()>();
                // Boxed like the node upload below: the stream carries the
                // differential and produces a large future.
                Box::pin(shipments).await?;

                // Adjudicate the by-reference frontier: subtree roots the
                // novelty walk could not enter. These land BEFORE the held
                // novelty — held nodes reference frontier roots as children,
                // and a parent durable before its subtree would be
                // probe-trustable residue on an aborted push.
                if !sole_remote {
                    // A virgin target (no fetched revision) holds nothing:
                    // every probe would miss, so skip them all and forward
                    // outright. One shared visited set across roots — a
                    // block reachable from two frontier links crosses once.
                    let target_may_have = upstream.revision().is_some();
                    let mut visited: HashSet<NodeHash> = HashSet::new();
                    for link in difference.unresolved_target() {
                        forward_subtree(
                            link.node.clone(),
                            branch,
                            remote,
                            &sources,
                            target_may_have,
                            &mut visited,
                            env,
                        )
                        .await?;
                    }
                }

                // Upload the held novelty children-before-parents: waves of
                // nodes whose in-set children are already durable, concurrent
                // within a wave, a barrier between waves. Children NOT in the
                // novelty set are either shared with the base (already on the
                // target) or by-reference frontier roots (settled above), so
                // by the time any node lands here its full closure is durable.
                let mut pending: Vec<_> = {
                    let novelty = difference.novel_nodes();
                    futures_util::pin_mut!(novelty);
                    let mut nodes = Vec::new();
                    while let Some(node) = novelty.next().await {
                        nodes.push(node?);
                    }
                    nodes
                };
                let mut durable: HashSet<NodeHash> = HashSet::new();
                let in_set: HashSet<NodeHash> =
                    pending.iter().map(|node| node.hash().clone()).collect();
                while !pending.is_empty() {
                    let (wave, rest): (Vec<_>, Vec<_>) = pending.into_iter().partition(|node| {
                        node_children(node).is_ok_and(|children| {
                            children
                                .iter()
                                .all(|child| !in_set.contains(child) || durable.contains(child))
                        })
                    });
                    if wave.is_empty() {
                        // A cycle cannot exist in a hash tree, so an empty
                        // wave means a node failed to decode (or the
                        // invariant broke some other way). Either way,
                        // continuing would publish a head the target cannot
                        // serve — fail the push instead.
                        for node in &rest {
                            node_children(node)?;
                        }
                        return Err(dialog_search_tree::DialogSearchTreeError::Node(
                            "novelty upload made no progress: children-before-parents \
                             ordering found no uploadable node"
                                .into(),
                        )
                        .into());
                    }
                    for node in &wave {
                        durable.insert(node.hash().clone());
                    }
                    let upload = remote_index
                        .upload(stream::iter(wave.into_iter().map(Ok)))
                        .perform(env);
                    // Boxed because the upload future carries the full
                    // stream type and produces large futures.
                    Box::pin(upload).await?;
                    pending = rest;
                }

                upstream.publish(revision.clone()).perform(env).await?;
            }
        }

        // Advance this upstream's recorded sync point to the just-pushed
        // tree. A target pushed explicitly for the first time gets tracked
        // here (appended, not made the default).
        //
        // Same tracking-cell protocol as the pull commit: this write races
        // other syncs of the cell (a pull, a push to another upstream, a
        // set_upstream through another handle), and a plain publish from a
        // stale snapshot would either fail the whole push AFTER the target
        // already advanced — leaving the base behind so every retried push
        // reads as non-fast-forward until a pull — or silently drop a
        // concurrent writer's entry. On a version mismatch, re-read: if our
        // entry is untouched, fold our advance into the current state and
        // publish once more; if our own entry moved, a concurrent sync of
        // this same upstream already recorded a consistent pair, so yield
        // rather than regress it.
        let target = upstream_state.target();
        let marker = branch.tracking().checkpoint();
        let mut tracking = branch.tracked();
        let advanced = upstream_state.clone().with_tree(revision.tree.clone());
        tracking.record(&advanced);
        // The record is written until it lands. A mismatch means another
        // write to the cell came first: each is some sync recording its
        // own upstream, so there are only ever as many as syncs in flight,
        // and folding ours into the current state again eventually lands.
        // Giving up would leave this upstream's base behind its head, and
        // every later push there would be refused as not a fast-forward.
        // Only a concurrent sync of this same upstream ends the loop early:
        // it already recorded a consistent pair, which ours must not undo.
        let mut publish = marker.publish(tracking, env).await;
        while let Err(PublishError::VersionMismatch { .. }) = publish {
            branch.tracking().resolve().perform(env).await?;
            let marker = branch.tracking().checkpoint();
            let mut tracking = branch.tracked();
            let ours_untouched = tracking.get(&target).is_none_or(|tree| *tree == base);
            if !ours_untouched {
                return Ok(Some(revision));
            }
            tracking.record(&advanced);
            publish = marker.publish(tracking, env).await;
        }
        publish?;

        Ok(Some(revision))
    }
}

/// The direct children of a tree node, by hash. A segment has none.
fn node_children(
    node: &PersistentNode<ArtifactKey, State<Datum>>,
) -> Result<Vec<NodeHash>, PushError> {
    match node.body() {
        ArchivedNodeBody::Index(index) => {
            let links = index.links()?;
            Ok(links.into_iter().map(|link| link.node).collect())
        }
        ArchivedNodeBody::Segment(_) => Ok(Vec::new()),
    }
}

/// Every remote reachable from the branch's upstreams, resolved
/// transitively through its local upstreams.
///
/// A local upstream lives in the same archive, so its blocks are "held"
/// exactly as this branch's are -- but its head can carry content by
/// reference whose provenance is a remote only IT tracks. Provenance is
/// what push attribution reasons over, so the walk follows every local
/// upstream into that branch's own upstreams (cycle-safe via a visited
/// set) and returns the union. Attribution is sound only against this
/// transitive set; the branch's own entries alone under-count where
/// by-reference content can have come from.
async fn tracked_remotes<Env>(
    branch: &Branch,
    env: &Env,
) -> Result<Vec<ConnectedReplica>, PushError>
where
    Env: Provider<Resolve> + ConditionalSync + 'static,
{
    fn gather(
        upstreams: &crate::Upstreams,
        remotes: &mut Vec<ConnectedReplica>,
        visited: &mut HashSet<String>,
        locals: &mut Vec<String>,
    ) {
        for entry in upstreams.iter() {
            match entry {
                Upstream::Remote { remote, .. } => {
                    if !remotes.iter().any(|known| known.same(remote)) {
                        remotes.push(remote.clone());
                    }
                }
                Upstream::Local { branch: name, .. } => {
                    if visited.insert(name.clone()) {
                        locals.push(name.clone());
                    }
                }
                Upstream::Unreachable { .. } => {}
            }
        }
    }

    let mut remotes: Vec<ConnectedReplica> = Vec::new();
    let mut visited: HashSet<String> = HashSet::from([branch.name().to_string()]);
    let mut locals: Vec<String> = Vec::new();
    // Where content came from is every branch a branch synced with, not
    // only those it tracks: a one-off pull adopts content as surely as a
    // tracked one.
    let host = branch.subject();
    gather(&branch.upstreams(), &mut remotes, &mut visited, &mut locals);
    gather(
        &branch.tracked().synced_with(&host),
        &mut remotes,
        &mut visited,
        &mut locals,
    );
    while let Some(name) = locals.pop() {
        let local = branch.subject().branch(name).load().perform(env).await?;
        gather(&local.upstreams(), &mut remotes, &mut visited, &mut locals);
        gather(
            &local.tracked().synced_with(&host),
            &mut remotes,
            &mut visited,
            &mut locals,
        );
    }
    Ok(remotes)
}

/// Every reachable remote other than the push target: where content the
/// target lacks can be fetched from. The forwarder tries them in order
/// and fails loudly only when content is available nowhere.
fn source_remotes(
    tracked: &[ConnectedReplica],
    target: &ConnectedReplica,
) -> Vec<ConnectedReplica> {
    tracked
        .iter()
        .filter(|remote| !remote.same(target))
        .cloned()
        .collect()
}

/// How many shipments (blob bytes, spilled value blocks) a push has in
/// flight at once. The same width as the node upload that follows them
/// (`UPLOAD_CONCURRENCY` in `remote/archive.rs`): the two phases are the
/// same traffic, content-addressed puts to one remote.
const SHIPMENT_CONCURRENCY: usize = 16;

/// Ship one thing a node references, ahead of the node: a blob's bytes
/// through the remote import sink, or a spilled value block through a
/// block put. Content this replica holds only by reference is not its
/// to ship: with the target the sole remote it has the bytes by
/// attribution, otherwise `ensure_*` adjudicates with one probe.
#[allow(clippy::too_many_arguments)]
async fn ship<Env>(
    shipment: Result<ShipmentRef, dialog_artifacts::DialogArtifactsError>,
    branch: &Branch,
    remote: &ConnectedReplica,
    remote_index: &RemoteArchiveIndex<'_>,
    blob_store: &LocalIndex<'_, Env>,
    sources: &[ConnectedReplica],
    sole_remote: bool,
    env: &Env,
) -> Result<(), PushError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<BlobRead>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<crate::Hydrate>
        + Provider<Fork<RemoteSite, Put>>
        + Provider<Fork<RemoteSite, BlobImport>>
        + Provider<Fork<RemoteSite, BlobRead>>
        + ConditionalSync
        + 'static,
{
    match shipment? {
        // Removals ship nothing; the remote keeps its bytes.
        ShipmentRef::BlobRemoved(_) => Ok(()),
        // The size rides on the ref (from the index record the
        // differential already read), so shipping needs no point read of
        // the current tree — such a read would descend by-reference
        // regions the novelty walk is careful never to require.
        ShipmentRef::BlobAdded { hash, size } => {
            let digest = dialog_common::Blake3Hash::from(hash);
            // Local bytes -> remote import sink. Mirrors the remote `Read`
            // fork in `branch/blob.rs` and `RemotePut`'s `Put` fork in
            // `remote/archive.rs`, substituting the blob `Import` effect
            // (single-part on the current providers).
            let source = branch
                .archive()
                .blob()
                .read(digest.clone())
                .perform(env)
                .await;
            let source = match source {
                Ok(source) => source,
                // Bytes this replica never held: the record rode into the
                // head by reference. Sole remote -> the target stores them
                // by attribution; otherwise adjudicate — probe the target,
                // forward from a source remote only on a miss.
                Err(BlobError::NotFound(_)) => {
                    if !sole_remote {
                        ensure_blob_on_target(digest, size, branch, remote, sources, env).await?;
                    }
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            };
            // An attempt is the whole transfer, since an import can fail
            // at any point up to its finish. The source opened above
            // serves the first; one that follows a failed attempt reads
            // the bytes again.
            let mut opened = Some(source);
            remote
                .reach(|address| {
                    let digest = digest.clone();
                    let opened = opened.take();
                    async move {
                        let mut source = match opened {
                            Some(source) => source,
                            None => {
                                branch
                                    .archive()
                                    .blob()
                                    .read(digest.clone())
                                    .perform(env)
                                    .await?
                            }
                        };
                        let mut sink = address
                            .subject
                            .clone()
                            .writer()
                            .archive()
                            .blob()
                            .import(digest, size)
                            .fork(address.site())
                            .perform(env)
                            .await?;
                        while let Some(chunk) = source.next().await? {
                            sink.write_all(&chunk).await?;
                        }
                        sink.finish().await?;
                        Ok::<_, BlobError>(())
                    }
                })
                .await?;
            Ok(())
        }
        // A value larger than the inline threshold lives as a
        // content-addressed block (addressed by its 32-byte value
        // reference) in the same store as the tree nodes. Local bytes ->
        // remote block put, mirroring the novel node upload.
        ShipmentRef::SpilledValue(reference) => {
            let bytes = match blob_store.get(&reference).await? {
                Some(bytes) => bytes,
                // Held by reference: not this replica's to ship. Sole
                // remote -> the target has it by attribution; otherwise
                // adjudicate.
                None => {
                    if !sole_remote {
                        ensure_block_on_target(
                            NodeHash::from(reference),
                            branch,
                            remote,
                            sources,
                            env,
                        )
                        .await?;
                    }
                    return Ok(());
                }
            };
            remote_index.put(Buffer::from(bytes)).perform(env).await?;
            Ok(())
        }
    }
}

/// One request answering "does `remote` hold this block": a forked
/// catalog get. The bytes of a hit are discarded — the answer is the
/// point — and a dumb store offers nothing cheaper than a get.
async fn remote_has_block<Env>(
    hash: &NodeHash,
    remote: &ConnectedReplica,
    env: &Env,
) -> Result<bool, PushError>
where
    Env: Provider<Fork<RemoteSite, Get>> + ConditionalSync + 'static,
{
    let found: Option<Vec<u8>> = remote
        .reach(|address| async move {
            address
                .subject
                .clone()
                .reader()
                .archive()
                .catalog("index")
                .get(hash.clone())
                .fork(&address.address)
                .perform(env)
                .await
        })
        .await
        .map_err(dialog_storage::DialogStorageError::from)
        .map_err(dialog_search_tree::DialogSearchTreeError::from)?;
    Ok(found.is_some())
}

/// A block's bytes from `remote`, if it holds them.
async fn remote_block<Env>(
    hash: &NodeHash,
    remote: &ConnectedReplica,
    env: &Env,
) -> Result<Option<Vec<u8>>, PushError>
where
    Env: Provider<Fork<RemoteSite, Get>> + ConditionalSync + 'static,
{
    remote
        .reach(|address| async move {
            address
                .subject
                .clone()
                .reader()
                .archive()
                .catalog("index")
                .get(hash.clone())
                .fork(&address.address)
                .perform(env)
                .await
        })
        .await
        .map_err(dialog_storage::DialogStorageError::from)
        .map_err(dialog_search_tree::DialogSearchTreeError::from)
        .map_err(PushError::from)
}

/// A block's bytes from wherever this replica can reach them: the local
/// archive first (free), then each source remote in order.
async fn block_from_anywhere<Env>(
    hash: &NodeHash,
    branch: &Branch,
    sources: &[ConnectedReplica],
    env: &Env,
) -> Result<Option<Vec<u8>>, PushError>
where
    Env:
        Provider<Get> + Provider<Put> + Provider<Fork<RemoteSite, Get>> + ConditionalSync + 'static,
{
    let local = LocalIndex::new(env, branch.archive().index());
    if let Some(bytes) = local
        .get(hash.as_bytes())
        .await
        .map_err(dialog_search_tree::DialogSearchTreeError::from)?
    {
        return Ok(Some(bytes));
    }
    for source in sources {
        if let Some(bytes) = remote_block(hash, source, env).await? {
            return Ok(Some(bytes));
        }
    }
    Ok(None)
}

/// Make sure the target holds a content-addressed block (a tree node's
/// sibling store also holds spilled values): probe once, and forward the
/// bytes from wherever they are reachable only on a miss. Never persists
/// the bytes locally — the pusher is a bridge here, not a replica.
async fn ensure_block_on_target<Env>(
    hash: NodeHash,
    branch: &Branch,
    target: &ConnectedReplica,
    sources: &[ConnectedReplica],
    env: &Env,
) -> Result<(), PushError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<crate::Hydrate>
        + Provider<Fork<RemoteSite, Put>>
        + ConditionalSync
        + 'static,
{
    if remote_has_block(&hash, target, env).await? {
        return Ok(());
    }
    let Some(bytes) = block_from_anywhere(&hash, branch, sources, env).await? else {
        return Err(dialog_search_tree::DialogSearchTreeError::Node(format!(
            "block {hash} is referenced by the head but reachable from no store: \
             not local, not on the push target, not on any tracked remote"
        ))
        .into());
    };
    target
        .archive()
        .index()
        .put(Buffer::from(bytes))
        .perform(env)
        .await?;
    Ok(())
}

/// Make sure the target holds a blob's bytes: probe once (a forked read,
/// dropped unconsumed on a hit), and stream them from wherever they are
/// reachable only on a miss.
async fn ensure_blob_on_target<Env>(
    digest: dialog_common::Blake3Hash,
    size: u64,
    branch: &Branch,
    target: &ConnectedReplica,
    sources: &[ConnectedReplica],
    env: &Env,
) -> Result<(), PushError>
where
    Env: Provider<BlobRead>
        + Provider<Fork<RemoteSite, BlobRead>>
        + Provider<Fork<RemoteSite, BlobImport>>
        + Provider<Resolve>
        + ConditionalSync
        + 'static,
{
    let probe = target
        .reach(|address| {
            let digest = digest.clone();
            async move {
                address
                    .subject
                    .clone()
                    .reader()
                    .archive()
                    .blob()
                    .read(digest)
                    .fork(address.site())
                    .perform(env)
                    .await
            }
        })
        .await;
    match probe {
        // Present; the unconsumed reader is dropped. (A ranged 1-byte
        // read would bound the probe's bandwidth too — refinement noted
        // in the version-control notes.)
        Ok(_) => return Ok(()),
        Err(BlobError::NotFound(_)) => {}
        Err(error) => return Err(error.into()),
    }

    // An attempt is the whole transfer, since an import can fail at any
    // point up to its finish. The source found here serves the first;
    // one that follows a failed attempt is found again.
    let mut opened = Some(blob_source(&digest, branch, sources, env).await?);
    target
        .reach(|address| {
            let digest = digest.clone();
            let opened = opened.take();
            async move {
                let mut source = match opened {
                    Some(source) => source,
                    None => blob_source(&digest, branch, sources, env).await?,
                };
                let mut sink = address
                    .subject
                    .clone()
                    .writer()
                    .archive()
                    .blob()
                    .import(digest, size)
                    .fork(address.site())
                    .perform(env)
                    .await?;
                while let Some(chunk) = source.next().await? {
                    sink.write_all(&chunk).await?;
                }
                sink.finish().await?;
                Ok::<_, BlobError>(())
            }
        })
        .await?;
    Ok(())
}

/// A reader of blob `digest` from wherever this replica can reach it:
/// the local archive first (free), then each source remote.
async fn blob_source<Env>(
    digest: &dialog_common::Blake3Hash,
    branch: &Branch,
    sources: &[ConnectedReplica],
    env: &Env,
) -> Result<BlobReader, BlobError>
where
    Env: Provider<BlobRead> + Provider<Fork<RemoteSite, BlobRead>> + ConditionalSync,
{
    match branch
        .archive()
        .blob()
        .read(digest.clone())
        .perform(env)
        .await
    {
        Err(BlobError::NotFound(_)) => {}
        found => return found,
    }
    for origin in sources {
        let found = origin
            .reach(|address| async move {
                address
                    .subject
                    .clone()
                    .reader()
                    .archive()
                    .blob()
                    .read(digest.clone())
                    .fork(address.site())
                    .perform(env)
                    .await
            })
            .await;
        match found {
            Err(BlobError::NotFound(_)) => continue,
            found => return found,
        }
    }
    Err(BlobError::NotFound(format!(
        "blob {digest:?} is referenced by the head but reachable from no store: \
         not local, not on the push target, not on any tracked remote"
    )))
}

/// Transfer a by-reference subtree to the target, minimally: probe each
/// node once (a positive prunes its whole subtree — sound because
/// uploads are children-before-parents, so presence implies subtree
/// presence), fetch a missing node from wherever it is reachable, ship
/// the blobs and spilled values its entries name, and upload the node
/// itself only after its children — all streamed through without ever
/// persisting a byte locally.
async fn forward_subtree<Env>(
    root: NodeHash,
    branch: &Branch,
    target: &ConnectedReplica,
    sources: &[ConnectedReplica],
    target_may_have: bool,
    visited: &mut HashSet<NodeHash>,
    env: &Env,
) -> Result<(), PushError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<BlobRead>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<crate::Hydrate>
        + Provider<Fork<RemoteSite, Put>>
        + Provider<Fork<RemoteSite, BlobRead>>
        + Provider<Fork<RemoteSite, BlobImport>>
        + ConditionalSync
        + 'static,
{
    enum Frame {
        Enter(NodeHash),
        Emit(Vec<u8>),
    }
    let mut stack = vec![Frame::Enter(root)];
    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter(hash) => {
                if !visited.insert(hash.clone()) {
                    continue;
                }
                if target_may_have && remote_has_block(&hash, target, env).await? {
                    continue;
                }
                let Some(bytes) = block_from_anywhere(&hash, branch, sources, env).await? else {
                    return Err(dialog_search_tree::DialogSearchTreeError::Node(format!(
                        "node {hash} is referenced by the head but reachable from no store: \
                         not local, not on the push target, not on any tracked remote"
                    ))
                    .into());
                };
                let node = PersistentNode::<ArtifactKey, State<Datum>>::try_from(Buffer::from(
                    bytes.clone(),
                ))?;

                // The entries this node carries (stored in a segment,
                // buffered in an index) may name blob bytes and spilled
                // value blocks the target also lacks; ship those before
                // the node lands, mirroring the top-level shipment loop.
                let mut entries: Vec<(ArtifactKey, State<Datum>)> = Vec::new();
                match node.body() {
                    ArchivedNodeBody::Segment(segment) => {
                        segment.for_each_entry::<ArtifactKey, _>(|key, value| {
                            entries.push((ArtifactKey::from(key.to_vec()), into_owned(value)?));
                            Ok(())
                        })?;
                    }
                    ArchivedNodeBody::Index(index) => {
                        for entry in index.all_novelty::<ArtifactKey>()? {
                            if let NoveltyOp::Assert(value) = entry.op {
                                entries.push((ArtifactKey::from(entry.key), value));
                            }
                        }
                    }
                }
                let mut references = Vec::new();
                for (key, value) in entries {
                    if let Some(reference) = shipment_ref(&key, &value, false)? {
                        references.push(reference);
                    }
                }
                stream::iter(references)
                    .map(|reference| async move {
                        match reference {
                            ShipmentRef::BlobAdded { hash, size } => {
                                let digest = dialog_common::Blake3Hash::from(hash);
                                ensure_blob_on_target(digest, size, branch, target, sources, env)
                                    .await
                            }
                            ShipmentRef::SpilledValue(reference) => {
                                ensure_block_on_target(
                                    NodeHash::from(reference),
                                    branch,
                                    target,
                                    sources,
                                    env,
                                )
                                .await
                            }
                            ShipmentRef::BlobRemoved(_) => Ok(()),
                        }
                    })
                    .buffer_unordered(SHIPMENT_CONCURRENCY)
                    .try_collect::<()>()
                    .await?;

                // The node's own upload waits for its children: Emit sits
                // beneath the child frames on the stack, so it pops only
                // after every child subtree is settled — the invariant the
                // probes rely on.
                stack.push(Frame::Emit(bytes));
                for child in node_children(&node)? {
                    stack.push(Frame::Enter(child));
                }
            }
            Frame::Emit(bytes) => {
                target
                    .archive()
                    .index()
                    .put(Buffer::from(bytes))
                    .perform(env)
                    .await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::PushError;
    use crate::helpers::test_repo;
    use anyhow::Result;
    use dialog_peer::helpers::test_session_with_peer;

    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::{StreamExt as _, stream};

    #[dialog_common::test]
    async fn it_pushes_to_local_upstream() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: Value::String("Alice".to_string()),
            cause: None,
        };
        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&operator)
            .await?;

        let feature_revision = feature.revision().expect("feature should have a revision");

        let result = feature.push().perform(&operator).await?;
        assert!(result.is_some());

        let main_reloaded = repo.branch("main").load().perform(&operator).await?;
        let main_rev = main_reloaded
            .revision()
            .expect("main should have a revision after push");
        assert_eq!(main_rev.tree, feature_revision.tree);

        Ok(())
    }

    /// Pushing a spilling value ships its block to the local upstream, a
    /// spilled value shared by many facts ships once, and a re-push with
    /// nothing new is a no-op (no re-upload).
    #[dialog_common::test]
    async fn it_pushes_spilled_value_blocks_once() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let big = "z".repeat(inline_n + 1);
        let value = Value::String(big);

        // Two facts share the same large value -> one spilled block.
        feature
            .commit(stream::iter(vec![
                Instruction::Assert(Artifact {
                    the: "doc/body".parse()?,
                    of: "doc:a".parse()?,
                    is: value.clone(),
                    cause: None,
                }),
                Instruction::Assert(Artifact {
                    the: "doc/body".parse()?,
                    of: "doc:b".parse()?,
                    is: value.clone(),
                    cause: None,
                }),
            ]))
            .perform(&operator)
            .await?;

        let first = feature.push().perform(&operator).await?;
        assert!(first.is_some(), "the first push lands the commit");

        // The main branch (the upstream) can now read both facts back,
        // reconstructing the shared spilled value from the shipped block.
        let main_reloaded = repo.branch("main").load().perform(&operator).await?;
        let results: Vec<_> = main_reloaded
            .claims()
            .select(dialog_artifacts::ArtifactSelector::new().the("doc/body".parse()?))
            .to_owned()
            .perform(&operator)
            .await?
            .filter_map(|r| async { r.ok() })
            .collect()
            .await;
        assert_eq!(
            results.len(),
            2,
            "both facts hydrate from the shipped block"
        );
        assert!(
            results.iter().all(|r| r.is == value),
            "the shared spilled value reconstructs for both facts"
        );

        // A re-push with nothing new is a no-op.
        let second = feature.push().perform(&operator).await?;
        assert_eq!(
            second.map(|r| r.tree),
            first.map(|r| r.tree),
            "a re-push with nothing new returns the same revision"
        );

        Ok(())
    }

    /// A second push with no intervening commit is a no-op: the local head
    /// already equals the recorded upstream sync point, so it returns the
    /// current revision without re-publishing. Guards the ongoing-`revision`-PUT
    /// regression where an idle sync tick re-pushed on every drain.
    #[dialog_common::test]
    async fn it_is_a_noop_when_nothing_new_to_push() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:123".parse()?,
                is: Value::String("Alice".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let revision = feature.revision().expect("feature should have a revision");

        // First push lands the commit.
        let first = feature.push().perform(&operator).await?;
        assert_eq!(
            first.map(|r| r.tree),
            Some(revision.tree.clone()),
            "first push lands the local head"
        );

        // Second push, with no new commit, is a no-op that still reports the
        // current revision.
        let second = feature.push().perform(&operator).await?;
        assert_eq!(
            second.map(|r| r.tree),
            Some(revision.tree),
            "second push with nothing new returns the current revision as a no-op"
        );

        Ok(())
    }

    /// A push whose tracking-cell snapshot went stale — another handle of
    /// the same branch reconfigured upstreams after this handle opened —
    /// must still succeed and fold its advance into the current cell
    /// state rather than failing after the target already advanced (which
    /// left the recorded base behind, so every retried push read as
    /// non-fast-forward until a pull) or clobbering the other handle's
    /// entry. The push-side analogue of
    /// `it_folds_tracking_updates_racing_from_another_handle`.
    #[dialog_common::test]
    async fn it_folds_tracking_updates_when_pushing_from_a_stale_handle() -> Result<()> {
        use crate::Upstream;
        use crate::helpers::test_repo;
        use dialog_peer::helpers::test_session_with_peer;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let backup = repo.branch("backup").open().perform(&operator).await?;

        // Handle A of "feature" opens (snapshotting the upstream cell),
        // then handle B reconfigures tracking, advancing the cell version
        // past A's snapshot.
        let feature_a = repo.branch("feature").open().perform(&operator).await?;
        feature_a.set_upstream(&main).perform(&operator).await?;
        feature_a
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: Value::String("Alice".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let feature_b = repo.branch("feature").open().perform(&operator).await?;
        feature_b.set_upstream(&backup).perform(&operator).await?;
        feature_b.push().to(&backup).perform(&operator).await?;

        // A's push to main runs against its stale tracking snapshot: it
        // must land, and the cell must end up carrying BOTH entries —
        // A's advanced base for main and B's entry for backup.
        let pushed = feature_a.push().to(&main).perform(&operator).await?;
        assert!(pushed.is_some(), "the stale-handle push lands");

        let fresh = repo.branch("feature").open().perform(&operator).await?;
        let upstreams = fresh.upstreams();
        let revision = feature_a.revision().expect("feature has a head");
        assert!(
            upstreams.iter().any(|entry| matches!(
                entry,
                Upstream::Local { branch, tree } if branch == "main" && *tree == revision.tree
            )),
            "A's tracking advance for main lands despite the stale snapshot"
        );
        assert!(
            upstreams
                .iter()
                .any(|entry| matches!(entry, Upstream::Local { branch, .. } if branch == "backup")),
            "B's entry for backup survives A's fold"
        );

        // And the retried bare push is a clean no-op, not NonFastForward.
        let again = feature_a.push().to(&main).perform(&operator).await?;
        assert_eq!(
            again.map(|r| r.tree),
            Some(revision.tree),
            "a re-push after the fold is a no-op"
        );

        Ok(())
    }

    /// A branch can push to an upstream other than its default: the target
    /// advances, starts being tracked with its own sync base, and the
    /// default stays put.
    #[dialog_common::test]
    async fn it_pushes_to_a_non_default_upstream_and_tracks_it() -> Result<()> {
        use crate::Upstream;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let backup = repo.branch("backup").open().perform(&operator).await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:123".parse()?,
                is: Value::String("Alice".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let revision = feature.revision().expect("feature has a revision");

        // Bare push targets the default upstream (main)...
        feature.push().perform(&operator).await?;
        let main = repo.branch("main").load().perform(&operator).await?;
        assert_eq!(main.revision().map(|r| r.tree), Some(revision.tree.clone()));

        // ... and an explicit push targets another branch entirely.
        let pushed = feature.push().to(&backup).perform(&operator).await?;
        assert!(pushed.is_some());
        let backup = repo.branch("backup").load().perform(&operator).await?;
        assert_eq!(
            backup.revision().map(|r| r.tree),
            Some(revision.tree.clone())
        );

        // A one-off push records how far it synced with backup without
        // making backup an upstream: main is still the only branch a bare
        // push goes to.
        let upstreams = feature.pushes();
        assert_eq!(upstreams.iter().count(), 1);
        assert!(matches!(
            upstreams.iter().next(),
            Some(Upstream::Local { branch, .. }) if branch == "main"
        ));
        assert!(feature.tracked().synced.iter().any(|entry| matches!(
            entry,
            crate::Synced { target: crate::Target::Local(branch), tree, .. }
                if branch == "backup" && *tree == revision.tree
        )));

        // Pushing to the branch itself is refused.
        let selfish = feature.push().to(&feature).perform(&operator).await;
        assert!(matches!(selfish, Err(PushError::UpstreamIsItself { .. })));

        Ok(())
    }

    /// Pushing to an untracked target that already has its own history is
    /// refused as non-fast-forward: with no recorded sync base, only an
    /// empty target can be fast-forwarded onto. Pull it first.
    #[dialog_common::test]
    async fn it_refuses_pushing_to_an_untracked_nonempty_target() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let occupied = repo.branch("occupied").open().perform(&operator).await?;
        occupied
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:theirs".parse()?,
                is: Value::String("Existing".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:ours".parse()?,
                is: Value::String("New".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let result = feature.push().to(&occupied).perform(&operator).await;
        assert!(
            matches!(result, Err(PushError::NonFastForward { .. })),
            "an untracked, nonempty target must not be overwritten: {result:?}"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_non_fast_forward_on_local_upstream_diverged() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let _hash = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let _hash = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: Value::String("feature@example.com".to_string()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let result = feature.push().perform(&operator).await;
        assert!(
            matches!(result, Err(PushError::NonFastForward { .. })),
            "Push should fail with NonFastForward when diverged, got: {result:?}"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_no_upstream_by_default() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("feature").open().perform(&operator).await?;

        assert!(branch.upstreams().is_empty());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_pushing_branch_without_upstream() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("feature").open().perform(&operator).await?;

        let result = branch.push().perform(&operator).await;
        assert!(
            matches!(result, Err(PushError::BranchHasNoUpstream { .. })),
            "Push should fail with BranchHasNoUpstream, got: {result:?}"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_none_when_pushing_empty_branch() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let result = feature.push().perform(&operator).await?;
        assert!(result.is_none(), "Push with no revision should return None");

        Ok(())
    }

    /// A bare push goes to every branch the branch pushes to, and each
    /// records how far it got.
    #[dialog_common::test]
    async fn it_pushes_to_every_upstream() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        let mut targets = Vec::new();
        for name in ["main", "backup"] {
            let target = repo.branch(name).open().perform(&operator).await?;
            feature.push_to(&target).perform(&operator).await?;
            targets.push(name);
        }
        feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: Value::String("Alice".into()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;
        let head = feature.revision().expect("committed");

        feature.push().perform(&operator).await?;

        for name in targets {
            let target = repo.branch(name).load().perform(&operator).await?;
            assert_eq!(
                target.revision(),
                Some(head.clone()),
                "{name} received the push"
            );
            assert_eq!(
                feature.tracked().get(&crate::Target::Local(name.into())),
                Some(&head.tree),
                "the push to {name} recorded how far it got"
            );
        }
        Ok(())
    }

    /// Pushing to many upstreams at once, every push records how far it
    /// got, however contended the record is: a push whose record was
    /// dropped would find its target moved past the recorded base next
    /// time, and be refused as not a fast-forward for good.
    #[dialog_common::test]
    async fn it_records_every_push_of_a_concurrent_push() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        let names = ["one", "two", "three", "four", "five"];
        for name in names {
            let target = repo.branch(name).open().perform(&operator).await?;
            feature.push_to(&target).perform(&operator).await?;
        }
        for value in ["Alice", "Bob"] {
            feature
                .commit(stream::iter(vec![Instruction::Assert(Artifact {
                    the: "user/name".parse()?,
                    of: format!("user:{value}").parse()?,
                    is: Value::String(value.into()),
                    cause: None,
                })]))
                .perform(&operator)
                .await?;
            feature.push().perform(&operator).await?;
            let head = feature.revision().expect("committed");
            for name in names {
                assert_eq!(
                    feature.tracked().get(&crate::Target::Local(name.into())),
                    Some(&head.tree),
                    "the push of {value} to {name} recorded how far it got"
                );
            }
        }
        Ok(())
    }
}
