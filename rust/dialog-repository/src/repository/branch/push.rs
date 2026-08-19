use std::collections::HashSet;

use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{
    Datum, Key as ArtifactKey, ShipmentRef, State, shipment_ref, shipment_refs,
};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{Buffer, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveExt as _, ArchiveSubjectExt as _, CatalogExt as _};
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
use dialog_effects::blob::{BlobError, Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::{
    ArchivedNodeBody, ContentAddressedStorage as TreeStorage, MissingBlocks, MissingPolicy,
    NoveltyOp, PersistentNode, TreeDifference, into_owned,
};
use dialog_storage::StorageBackend as _;
use futures_util::{StreamExt as _, stream};

use crate::{
    Branch, Index, LocalIndex, PublishError, PushError, RemoteRepository, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt, Revision, Upstream, UpstreamBranch,
};

/// Command struct for pushing local changes to an upstream branch.
///
/// Borrows `&Branch` (non-consuming). Reads the branch's upstream to
/// dispatch to local or remote push logic.
pub struct Push<'a> {
    branch: &'a Branch,
    to: Option<Upstream>,
}

impl<'a> Push<'a> {
    fn new(branch: &'a Branch) -> Self {
        Self { branch, to: None }
    }

    /// Push to the given branch instead of the default upstream.
    ///
    /// Accepts either a `&Branch` or a `&RemoteBranch` — the same inputs as
    /// [`Branch::set_upstream`]. If the target is already tracked, its
    /// recorded sync base drives the fast-forward check and the novelty
    /// upload; otherwise the empty base does (only a target with no
    /// revision of its own accepts such a push), and a successful push
    /// starts tracking the target — without changing the default upstream.
    pub fn to(mut self, source: impl Into<UpstreamBranch>) -> Self {
        self.to = Some(Upstream::from(source.into()));
        self
    }
}

impl Branch {
    /// Create a command to push local changes to the upstream branch.
    ///
    /// Targets the default upstream; chain [`Push::to`] to push to another
    /// tracked (or brand-new) upstream instead.
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
    /// For remote upstream, novel tree blocks are uploaded before the
    /// revision is published — children before parents, so an aborted
    /// push leaves the remote prefix-closed — and a published head never
    /// references bytes the remote is missing.
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
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<BlobRead>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Put>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<Fork<RemoteSite, Publish>>
            + Provider<Fork<RemoteSite, BlobImport>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;

        // Select the upstream entry to push to: the default when no
        // explicit target was given, otherwise the tracked entry for that
        // target — or, for a target not tracked yet, a fresh entry whose
        // empty sync base only fast-forwards onto an empty target.
        let upstreams = branch.upstreams();
        let upstream_state = match self.to {
            None => upstreams.default_upstream().cloned().ok_or_else(|| {
                PushError::BranchHasNoUpstream {
                    branch: branch.name().to_string(),
                }
            })?,
            Some(target) => {
                if let Upstream::Local { branch: name, .. } = &target
                    && name == branch.name()
                {
                    return Err(PushError::UpstreamIsItself {
                        branch: branch.name().to_string(),
                    });
                }
                upstreams.find(&target).cloned().unwrap_or(target)
            }
        };

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
            Upstream::Remote {
                remote: remote_name,
                branch: upstream_branch_name,
                ..
            } => {
                let remote = branch
                    .subject()
                    .remote(remote_name.clone())
                    .load()
                    .perform(env)
                    .await?;

                let upstream = remote
                    .branch(upstream_branch_name.clone())
                    .open()
                    .perform(env)
                    .await?;

                // Refresh the cache from the remote so our divergence
                // check sees the latest upstream tree, not whatever
                // was in our last snapshot.
                upstream.fetch().perform(env).await?;

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

                // Upload the held novelty children-before-parents: waves of
                // nodes whose in-set children are already durable, concurrent
                // within a wave, a barrier between waves. The ordering is a
                // protocol invariant, not a nicety — the existence probes
                // below prune a whole subtree on one positive answer, which
                // is sound only if a node's presence on a remote implies its
                // children's presence; leaves-first upload keeps every
                // aborted push prefix-closed.
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
                        // A cycle cannot exist in a hash tree; only a decode
                        // failure in `node_children` lands here. Surface it.
                        for node in &rest {
                            node_children(node)?;
                        }
                        break;
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

                // Adjudicate the by-reference frontier: subtree roots the
                // novelty walk could not enter. When the target is the only
                // tracked remote, every one of them came from it —
                // attribution, zero requests. Otherwise, one probe per root
                // settles a whole subtree, and only content the target
                // provably lacks is transferred, fetched from the remotes
                // that have it and streamed through without being persisted.
                let sole_remote = branch.upstreams().iter().all(|entry| match entry {
                    Upstream::Remote { remote, .. } => remote == remote_name,
                    Upstream::Local { .. } => true,
                });
                let sources = if sole_remote {
                    Vec::new()
                } else {
                    source_remotes(branch, remote_name, env).await
                };
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
                            &remote,
                            &sources,
                            target_may_have,
                            &mut visited,
                            env,
                        )
                        .await?;
                    }
                }

                // Ship the blocks the tree nodes reference but the node upload
                // does not carry: blob bytes and spilled value blocks. Both
                // are surfaced by ONE entry-level drain of the SAME
                // differential the node upload just walked (`shipment_refs`),
                // so the changed paths are read once per push instead of once
                // per concern. Bytes must land on the remote before we publish
                // a revision that references them, so a failed upload here
                // aborts the push with the revision still unpublished.
                let blob_store = LocalIndex::new(env, index.clone());
                let address = remote.address();
                let mut refs = std::pin::pin!(shipment_refs(&difference));
                while let Some(shipment) = refs.next().await {
                    match shipment? {
                        // Removals ship nothing; the remote keeps its bytes.
                        ShipmentRef::BlobRemoved(_) => {}
                        // The size rides on the ref (from the index record the
                        // differential already read), so shipping needs no
                        // point read of the current tree — such a read would
                        // descend by-reference regions the novelty walk is
                        // careful never to require.
                        ShipmentRef::BlobAdded { hash, size } => {
                            let digest = dialog_common::Blake3Hash::from(hash);
                            // Local bytes -> remote import sink. Mirrors the
                            // remote `Read` fork in `branch/blob.rs` and
                            // `RemotePut`'s `Put` fork in `remote/archive.rs`,
                            // substituting the blob `Import` effect
                            // (single-part on the current providers).
                            let source = branch
                                .archive()
                                .blob()
                                .read(digest.clone())
                                .perform(env)
                                .await;
                            let mut source = match source {
                                Ok(source) => source,
                                // Bytes this replica never held: the record
                                // rode into the head by reference. Sole
                                // remote → the target stores them by
                                // attribution; otherwise adjudicate — probe
                                // the target, forward from a source remote
                                // only on a miss.
                                Err(BlobError::NotFound(_)) => {
                                    if !sole_remote {
                                        ensure_blob_on_target(
                                            digest, size, branch, &remote, &sources, env,
                                        )
                                        .await?;
                                    }
                                    continue;
                                }
                                Err(error) => return Err(error.into()),
                            };
                            let mut sink = address
                                .subject
                                .clone()
                                .archive()
                                .blob()
                                .import(digest.clone(), size)
                                .fork(address.site())
                                .perform(env)
                                .await?;
                            while let Some(chunk) = source.next().await? {
                                sink.write_all(&chunk).await?;
                            }
                            sink.finish().await?;
                        }
                        // A value larger than the inline threshold lives as a
                        // content-addressed block (addressed by its 32-byte
                        // value reference) in the same store as the tree
                        // nodes. Local bytes -> remote block put, mirroring
                        // the novel node upload.
                        ShipmentRef::SpilledValue(reference) => {
                            let bytes = match blob_store.get(&reference).await? {
                                Some(bytes) => bytes,
                                // Held by reference: not this replica's to
                                // ship. Sole remote → the target has it by
                                // attribution; otherwise adjudicate.
                                None => {
                                    if !sole_remote {
                                        ensure_block_on_target(
                                            NodeHash::from(reference),
                                            branch,
                                            &remote,
                                            &sources,
                                            env,
                                        )
                                        .await?;
                                    }
                                    continue;
                                }
                            };
                            remote_index.put(Buffer::from(bytes)).perform(env).await?;
                        }
                    }
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
        let advanced = upstream_state.with_tree(revision.tree.clone());
        let marker = branch.upstream.checkpoint();
        let mut upstreams = branch.upstreams();
        upstreams.upsert(advanced.clone());
        let publish = marker.publish(upstreams, env).await;
        if let Err(PublishError::VersionMismatch { .. }) = publish {
            branch.upstream.resolve().perform(env).await?;
            let marker = branch.upstream.checkpoint();
            let mut upstreams = branch.upstreams();
            let ours_untouched = match upstreams.find(&advanced) {
                None => true,
                Some(entry) => *entry.tree() == base,
            };
            if ours_untouched {
                upstreams.upsert(advanced);
                match marker.publish(upstreams, env).await {
                    // The cell is contended; give up on the marker
                    // advance — the push itself landed, the next sync
                    // is just heavier.
                    Err(PublishError::VersionMismatch { .. }) => {}
                    other => other?,
                }
            }
        } else {
            publish?;
        }

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

/// Load every tracked remote other than the push target, best-effort:
/// a remote that fails to load is simply not a source. The forwarder
/// tries sources in order and fails loudly only when content is
/// available nowhere.
async fn source_remotes<Env>(branch: &Branch, target: &str, env: &Env) -> Vec<RemoteRepository>
where
    Env: Provider<Resolve> + ConditionalSync + 'static,
{
    let mut names: Vec<String> = Vec::new();
    for entry in branch.upstreams().iter() {
        if let Upstream::Remote { remote, .. } = entry
            && remote != target
            && !names.contains(remote)
        {
            names.push(remote.clone());
        }
    }
    let mut sources = Vec::with_capacity(names.len());
    for name in names {
        if let Ok(remote) = branch.subject().remote(name).load().perform(env).await {
            sources.push(remote);
        }
    }
    sources
}

/// One request answering "does `remote` hold this block": a forked
/// catalog get. The bytes of a hit are discarded — the answer is the
/// point — and a dumb store offers nothing cheaper than a get.
async fn remote_has_block<Env>(
    hash: &NodeHash,
    remote: &RemoteRepository,
    env: &Env,
) -> Result<bool, PushError>
where
    Env: Provider<Fork<RemoteSite, Get>> + ConditionalSync + 'static,
{
    let address = remote.address();
    let found: Option<Vec<u8>> = address
        .subject
        .clone()
        .archive()
        .catalog("index")
        .get(hash.clone())
        .fork(&address.address)
        .perform(env)
        .await
        .map_err(dialog_storage::DialogStorageError::from)
        .map_err(dialog_search_tree::DialogSearchTreeError::from)?;
    Ok(found.is_some())
}

/// A block's bytes from `remote`, if it holds them.
async fn remote_block<Env>(
    hash: &NodeHash,
    remote: &RemoteRepository,
    env: &Env,
) -> Result<Option<Vec<u8>>, PushError>
where
    Env: Provider<Fork<RemoteSite, Get>> + ConditionalSync + 'static,
{
    let address = remote.address();
    address
        .subject
        .clone()
        .archive()
        .catalog("index")
        .get(hash.clone())
        .fork(&address.address)
        .perform(env)
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
    sources: &[RemoteRepository],
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
    target: &RemoteRepository,
    sources: &[RemoteRepository],
    env: &Env,
) -> Result<(), PushError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Fork<RemoteSite, Get>>
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
    target: &RemoteRepository,
    sources: &[RemoteRepository],
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
    let address = target.address();
    let probe = address
        .subject
        .clone()
        .archive()
        .blob()
        .read(digest.clone())
        .fork(address.site())
        .perform(env)
        .await;
    match probe {
        // Present; the unconsumed reader is dropped. (A ranged 1-byte
        // read would bound the probe's bandwidth too — refinement noted
        // in the version-control notes.)
        Ok(_) => return Ok(()),
        Err(BlobError::NotFound(_)) => {}
        Err(error) => return Err(error.into()),
    }

    // Local bytes first (free), then each source remote.
    let mut source = match branch
        .archive()
        .blob()
        .read(digest.clone())
        .perform(env)
        .await
    {
        Ok(reader) => Some(reader),
        Err(BlobError::NotFound(_)) => None,
        Err(error) => return Err(error.into()),
    };
    if source.is_none() {
        for origin in sources {
            let origin_address = origin.address();
            match origin_address
                .subject
                .clone()
                .archive()
                .blob()
                .read(digest.clone())
                .fork(origin_address.site())
                .perform(env)
                .await
            {
                Ok(reader) => {
                    source = Some(reader);
                    break;
                }
                Err(BlobError::NotFound(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
    let Some(mut source) = source else {
        return Err(BlobError::NotFound(format!(
            "blob {digest:?} is referenced by the head but reachable from no store: \
             not local, not on the push target, not on any tracked remote"
        ))
        .into());
    };

    let mut sink = address
        .subject
        .clone()
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
    Ok(())
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
    target: &RemoteRepository,
    sources: &[RemoteRepository],
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
                for (key, value) in entries {
                    match shipment_ref(&key, &value, false)? {
                        Some(ShipmentRef::BlobAdded { hash, size }) => {
                            let digest = dialog_common::Blake3Hash::from(hash);
                            ensure_blob_on_target(digest, size, branch, target, sources, env)
                                .await?;
                        }
                        Some(ShipmentRef::SpilledValue(reference)) => {
                            ensure_block_on_target(
                                NodeHash::from(reference),
                                branch,
                                target,
                                sources,
                                env,
                            )
                            .await?;
                        }
                        _ => {}
                    }
                }

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
    use dialog_operator::helpers::test_operator_with_profile;

    use dialog_artifacts::{Artifact, Instruction, Value};
    use futures_util::{StreamExt as _, stream};

    #[dialog_common::test]
    async fn it_pushes_to_local_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        use dialog_operator::helpers::test_operator_with_profile;

        let (operator, profile) = test_operator_with_profile().await;
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

        let (operator, profile) = test_operator_with_profile().await;
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

        // Backup is now tracked with its own sync base; main stays default.
        let upstreams = feature.upstreams();
        assert_eq!(upstreams.iter().count(), 2);
        assert!(matches!(
            upstreams.default_upstream(),
            Some(Upstream::Local { branch, .. }) if branch == "main"
        ));
        assert!(upstreams.iter().any(|entry| matches!(
            entry,
            Upstream::Local { branch, tree } if branch == "backup" && *tree == revision.tree
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("feature").open().perform(&operator).await?;

        assert!(branch.upstream().is_none());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_pushing_branch_without_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;

        let result = feature.push().perform(&operator).await?;
        assert!(result.is_none(), "Push with no revision should return None");

        Ok(())
    }
}
