//! Views of a repository at one revision: readable, transactable, and
//! the content transfer built on them.
//!
//! A [`Snapshot`] holds its own head, so what it names moves only when
//! it commits -- unlike a [`Branch`](crate::Branch), whose head lives in
//! a memory cell every handle to the branch shares. It reads exactly
//! like a branch ([`claims`](Snapshot::claims), [`select`](Snapshot::select),
//! [`query`](Snapshot::query), [`history`](Snapshot::history), blob
//! reads) and it transacts exactly like one
//! ([`transaction`](Snapshot::transaction), [`commit`](Snapshot::commit)
//! return the branch's own command types). Clone to keep the view you
//! have before advancing.
//!
//! Nothing here publishes or moves a branch head. A snapshot's commits
//! are minted on a line of their own (see [`Snapshot`]); making one
//! visible under a name stays a separate act, as does content transfer:
//! [`Snapshot::export`] reads content out, [`Repository::import`] writes
//! content in.
//!
//! # Two channels, not one
//!
//! A snapshot's content divides into two kinds that cannot share a stream,
//! because they land in different providers and verify at different times:
//!
//! - **Blocks** (tree nodes and spilled values) are whole values written
//!   through `archive::Put`. Each carries the digest it must hash to,
//!   checked before it is stored.
//! - **Blobs** are streamed through `blob::Import`, opened with a declared
//!   digest and size and verified at `finish` -- a blob arrives in pieces,
//!   so it cannot be checked on arrival the way a block can.
//!
//! Both channels declare a digest and both are verified against it. What
//! differs is only *when* the check can happen.
//!
//! # Export is best-effort at the leaves; `download` is the guarantee
//!
//! [`SnapshotExport`] reads what the local store holds and reports referenced
//! content it could not find, rather than failing -- a snapshot of a
//! partially fetched branch is a legitimate thing to want.
//!
//! That tolerance stops at the tree itself. A missing spilled value or
//! blob is a leaf: it is skipped, recorded, and the rest of the export is
//! still coherent. A missing *node* is not, because the walk cannot pass
//! it and everything beneath it becomes unreachable -- there is no way to
//! enumerate what was lost, so a gap in the tree aborts.
//!
//! [`SnapshotExport::download`] resolves both by hydrating read-misses from an
//! upstream as the walk proceeds, which is what makes the result complete.

use std::collections::HashSet;

use async_stream::try_stream;
use dialog_artifacts::history::{
    CausalityCache, ContextCache, RevisionRecord, TreeHistory, Version,
};
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{
    ArtifactSelector, BlobIndexExt as _, Datum, DialogArtifactsError, Entity, Key, ShipmentRef,
    State, Statement, shipment_ref,
};
use dialog_capability::{Capability, Did, Fork, Provider, Subject};
use dialog_common::{Blake3Hash as NodeHash, Buffer, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveSubjectExt as _, CatalogExt as _};
use dialog_effects::archive::{Archive, Catalog, Get, Put};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
use dialog_effects::blob::{BlobError, BlobReader, Import as BlobImport, Read as BlobRead};
use dialog_effects::memory;
use dialog_query::query::Application;
use dialog_search_tree::{
    ArchivedNodeBody, ContentAddressedStorage as TreeStorage, NoveltyOp, Traversable as _, Visit,
    into_owned,
};
use futures_util::{Stream, StreamExt as _, stream};
use parking_lot::RwLock;

use dialog_varsig::Principal;

use crate::repository::source::{Caches, SourceRef};
use crate::{
    BlobArchive, Branch, Index, NetworkedIndex, Overlay, PublishError, RemoteRepository,
    RemoteSite, Repository, RepositoryArchiveExt as _, Revision, Select, SelectQuery,
    SnapshotError, TreeReference,
};

#[cfg(test)]
mod read_tests;

#[cfg(test)]
mod transaction_tests;

/// How many spill or blob fetches an export keeps in flight at once.
///
/// Concurrency, not parallelism: one task drives them, so this holds on
/// wasm too. Sized to overlap remote round-trips under a downloading
/// reach without swamping a local store or a remote's connection limits.
const FETCH_CONCURRENCY: usize = 16;

/// A block of a snapshot: content plus the digest it must hash to.
///
/// The digest is carried rather than recomputed on arrival because a
/// snapshot may cross a trust boundary -- a source that fetched from a
/// remote, or a snapshot read from a file. Bytes that hash to something
/// else would not fail loudly if simply stored (they land at a different
/// address, unreferenced, and surface much later as a missing node), so
/// the mismatch is caught where the untrusted bytes enter.
#[derive(Debug, Clone)]
pub struct Block {
    /// The address this block's content must hash to.
    pub digest: NodeHash,
    /// The block's bytes.
    pub content: Buffer,
}

impl Block {
    /// Build a block from content, deriving its address.
    pub fn new(content: Buffer) -> Self {
        let digest = content.blake3_hash().clone();
        Self { digest, content }
    }

    /// Whether the content actually hashes to the declared digest.
    pub fn is_intact(&self) -> bool {
        self.digest.matches(self.content.as_ref())
    }
}

/// A view of a repository at one revision.
///
/// Holds the repository's subject rather than the repository itself:
/// everything a read touches — the archive catalog, the blob channel —
/// derives from the subject, which lets a [`Branch`] mint a snapshot of
/// its own repository too (see [`Branch::snapshot`]).
///
/// # Pinned, and advanced only through itself
///
/// The head is held by the snapshot, not by a memory cell shared with
/// other handles: nothing but a commit through *this* handle can move
/// it. It reads and transacts with the same API a branch has —
/// [`transaction`](Self::transaction) and [`commit`](Self::commit)
/// return the branch's [`Transaction`](crate::Transaction) and
/// [`Commit`](crate::Commit), and `perform` returns the minted
/// [`Revision`] and advances the snapshot to it. A [`Clone`] copies the
/// head, so it is how you keep the view you have:
///
/// ```no_run
/// # use dialog_repository::Snapshot;
/// # async fn example<Env>(snapshot: Snapshot, env: &Env, facts: dialog_artifacts::Changes)
/// # -> anyhow::Result<()>
/// # where Env: dialog_capability::Provider<dialog_effects::memory::Resolve> {
/// # let _ = (env, facts);
/// let before = snapshot.clone();
/// // snapshot.transaction().integrate(facts).commit().perform(env).await?;
/// // `before` still reads the base revision; `snapshot` reads the new one.
/// # Ok(())
/// # }
/// ```
///
/// # Its own line
///
/// A revision's [`Version`] is `(origin, edition)`, and an origin must be
/// a single sequential actor: two revisions minted under one origin at
/// the same edition would be an equivocation. A snapshot is not the
/// branch it was taken from — the branch may advance from the same base
/// concurrently — so its commits are minted on a *lineage* of their own.
/// The lineage is allocated on the snapshot's first commit and kept for
/// the ones after, which is what makes a chain of transactions one
/// origin with increasing editions. A clone starts a line of its own for
/// the same reason: two clones transacting from the same base must not
/// collide.
///
/// The base branch is still reachable: the minted record's parent is
/// the base revision, so ancestry, causality, and
/// [`log`](Self::log) walk straight through.
///
/// # Induction lags like a branch's
///
/// Commit-time induction runs against a watermark — the revision rules
/// were last evaluated through — and stimulates them with what has
/// changed between it and the head. A snapshot keeps its own in memory,
/// opened at the revision it was taken on, where a branch keeps its in a
/// cell. A raw [`commit`](Self::commit) advances the head without
/// inducing, exactly as it does on a branch, and the next
/// [`transaction`](Self::transaction) catches up over that lag.
///
/// # Reads are local
///
/// A snapshot tracks no upstream. A read that misses a block the local
/// store does not hold fails rather than fetching; materialize first
/// with [`SnapshotExport::download`] (or
/// [`Branch::download`](crate::Branch::download)) when the revision was
/// pulled by reference.
#[derive(Debug)]
pub struct Snapshot {
    subject: Subject,
    head: RwLock<Head>,
    /// The tree commit-time induction has already run over — the
    /// in-memory counterpart of a branch's induction cell, which is all
    /// the watermark is ever read for (the lag is a diff between two
    /// tree roots). Starts at the tree the snapshot opened on (induction
    /// is fire-forward: a handle does not re-derive over state it
    /// inherited), and every inducing transaction moves it to the head
    /// it minted. A raw [`Snapshot::commit`] leaves it behind, which is
    /// exactly the lag the next transaction catches up over.
    induced: RwLock<TreeReference>,
    caches: Caches,
    overlay: Overlay,
}

/// What a snapshot's commits move: the revision, and the line they are
/// minted on once one has been allocated.
#[derive(Debug, Clone)]
struct Head {
    revision: Revision,
    /// `None` until the first commit through this handle.
    lineage: Option<Entity>,
}

impl<C: Principal> Repository<C> {
    /// A view at `revision`.
    ///
    /// The revision is taken by value: whatever advances afterwards, this
    /// view keeps naming the same state. How the revision was obtained is
    /// the caller's business -- a snapshot does not require the branch it
    /// was minted on to be present. Starts with cold caches; prefer
    /// [`Branch::snapshot`] when a branch handle at the revision is at
    /// hand, so its warm caches carry over.
    pub fn snapshot(&self, revision: Revision) -> Snapshot {
        Snapshot::new(self.subject(), revision)
    }
}

impl Branch {
    /// A snapshot of this branch's current revision, or `None` before
    /// its first commit.
    ///
    /// The snapshot shares this branch's content- and version-addressed
    /// caches, so blocks, plans, and records the branch has already read
    /// are warm for the snapshot, and what the snapshot reads or mints
    /// warms the branch in turn. It gets its own rule cache and live
    /// spine: those hold one head-tagged slot each, and two lines whose
    /// heads diverge would only evict each other's (see
    /// [`Branch::caches`]). The head is copied, not shared — the branch
    /// advancing afterwards leaves the snapshot where it was, and the
    /// snapshot's commits never touch the branch.
    ///
    /// Induction starts here too: the snapshot's watermark opens at this
    /// revision, so its first transaction fires forward rather than
    /// re-deriving over the branch's existing state.
    pub fn snapshot(&self) -> Option<Snapshot> {
        let revision = self.revision()?;
        Some(Snapshot {
            subject: self.subject(),
            induced: RwLock::new(revision.tree.clone()),
            head: RwLock::new(Head {
                revision,
                lineage: None,
            }),
            caches: self.caches(),
            overlay: Overlay::default(),
        })
    }
}

impl Snapshot {
    /// A view of `subject`'s repository at `revision`, with cold caches.
    pub fn new(subject: Subject, revision: Revision) -> Self {
        Snapshot {
            subject,
            induced: RwLock::new(revision.tree.clone()),
            head: RwLock::new(Head {
                revision,
                lineage: None,
            }),
            caches: Caches::new(),
            overlay: Overlay::default(),
        }
    }

    /// The revision this snapshot names.
    pub fn revision(&self) -> Revision {
        self.head.read().revision.clone()
    }

    /// The subject (repository) this snapshot is a view of.
    pub fn subject(&self) -> Subject {
        self.subject.clone()
    }

    /// The DID of the repository this snapshot is a view of.
    pub fn of(&self) -> &Did {
        self.subject.did()
    }

    /// Archive capability for this snapshot's subject.
    pub fn archive(&self) -> Capability<Archive> {
        self.subject().archive()
    }

    /// The archive catalog this snapshot's blocks live in.
    pub(crate) fn index(&self) -> Capability<Catalog> {
        self.subject.clone().archive().index()
    }

    /// The revision a commit builds on and the line it mints on, read
    /// together so they name the same head.
    pub(crate) fn head(&self) -> (Revision, Option<Entity>) {
        let head = self.head.read();
        (head.revision.clone(), head.lineage.clone())
    }

    /// Move the head from `base` to `next`, recording the line `next`
    /// was minted on.
    ///
    /// The in-memory counterpart of a branch's CAS publish: a commit
    /// builds on the head it read, and if another commit through this
    /// same handle advanced it in the meantime, adopting `next` would
    /// silently drop that one — so the advance is refused with the same
    /// [`VersionMismatch`](PublishError::VersionMismatch) a stale branch
    /// write gets, carrying the tree roots as the versions.
    pub(crate) fn advance(
        &self,
        base: &Revision,
        next: Revision,
        lineage: Entity,
    ) -> Result<(), PublishError> {
        let mut head = self.head.write();
        if head.revision != *base {
            return Err(PublishError::VersionMismatch {
                expected: Some(memory::Version::from(base.tree.hash())),
                actual: Some(memory::Version::from(head.revision.tree.hash())),
            });
        }
        head.revision = next;
        head.lineage = Some(lineage);
        Ok(())
    }

    /// The tree commit-time induction has already run over. The
    /// in-memory counterpart of the branch induction cell: the facts
    /// between it and the head are the lag an inducing transaction
    /// stimulates its rules with.
    pub(crate) fn induced(&self) -> TreeReference {
        self.induced.read().clone()
    }

    /// Record that induction has run through `revision`.
    pub(crate) fn record_induction(&self, revision: &Revision) {
        *self.induced.write() = revision.tree.clone();
    }

    /// The shared caches this snapshot reads and commits through.
    pub(crate) fn caches(&self) -> &Caches {
        &self.caches
    }

    /// The snapshot's artifact index.
    ///
    /// Use `.select(selector).perform(&env)` to query artifacts.
    pub fn claims(&self) -> SnapshotClaims<'_> {
        SnapshotClaims { snapshot: self }
    }

    /// Query with an application. Shortcut for
    /// `snapshot.query().select(query)`.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        SelectQuery::new(self, query)
    }

    /// Open a query over this snapshot: a
    /// [`QueryLayer`](crate::QueryLayer) rooted at it, to
    /// [`with`](crate::QueryLayer::with) statements into,
    /// [`join`](crate::QueryLayer::join) branches or other snapshots
    /// onto, and [`select`](crate::QueryLayer::select) from. Schema
    /// metadata is auto-injected at perform time, as for a branch.
    pub fn query(&self) -> crate::QueryLayer<'_> {
        crate::QueryLayer::from(self)
    }

    /// Open a query over this snapshot with `statement` folded into the
    /// overlay in one step. Shorthand for `self.query().with(stmt)`.
    pub fn with<S: Statement>(&self, statement: S) -> crate::QueryLayer<'_> {
        self.query().with(statement)
    }

    /// The snapshot's transient session overlay: assert or retract
    /// ephemeral facts that every read of this snapshot observes but no
    /// commit persists. A [`Clone`] shares it, like branch clones do.
    /// See [`Overlay`].
    pub fn overlay(&self) -> &Overlay {
        &self.overlay
    }

    /// This snapshot's blob store, the target for [`Blob`](crate::Blob)
    /// reads. Blob writes advance the line through the branch's memory
    /// cell, which a snapshot does not have — see [`BlobArchive`].
    pub fn blobs(&self) -> BlobArchive<'_> {
        BlobArchive::from(self)
    }

    /// The recorded claim lineage at this snapshot's revision. See
    /// [`Branch::history`].
    pub fn history<'a, Env>(&self, env: &'a Env) -> TreeHistory<NetworkedIndex<'a, Env>>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Fork<RemoteSite, Get>>
            + ConditionalSync
            + 'static,
    {
        SourceRef::from(self).history(env)
    }

    /// The snapshot's committed history, newest first — at most `limit`
    /// entries of `(version, record)`. See [`Branch::log`].
    pub async fn log<Env>(
        &self,
        env: &Env,
        limit: usize,
    ) -> Result<Vec<(Version, RevisionRecord)>, DialogArtifactsError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Fork<RemoteSite, Get>>
            + ConditionalSync
            + 'static,
    {
        SourceRef::from(self).log(env, limit).await
    }

    /// A shared handle to this snapshot's causal-verdict memo. See
    /// [`Branch::causality`].
    pub fn causality(&self) -> CausalityCache {
        self.caches.causality.clone()
    }

    /// A shared handle to this snapshot's causal-context memo. See
    /// [`Branch::contexts`].
    pub fn contexts(&self) -> ContextCache {
        self.caches.contexts.clone()
    }
}

// A clone is the same view on its own line: it copies the head and the
// induction watermark (so the original advancing leaves it where it is,
// and vice versa, and neither re-derives over what the other already
// induced), shares the caches and the session overlay (both safe to
// share, like a branch clone's), and starts without a lineage — see the
// type docs for why two handles that may both transact from one base
// must mint under different origins.
impl Clone for Snapshot {
    fn clone(&self) -> Self {
        Snapshot {
            subject: self.subject.clone(),
            induced: RwLock::new(self.induced()),
            head: RwLock::new(Head {
                revision: self.revision(),
                lineage: None,
            }),
            caches: self.caches.clone(),
            overlay: self.overlay.clone(),
        }
    }
}

/// The snapshot's artifact index. Created by [`Snapshot::claims`].
pub struct SnapshotClaims<'a> {
    snapshot: &'a Snapshot,
}

impl<'a> SnapshotClaims<'a> {
    /// Select artifacts matching the selector.
    pub fn select(self, selector: ArtifactSelector<Constrained>) -> Select<'a> {
        Select::from_source(SourceRef::from(self.snapshot), selector)
    }
}

/// One piece of a snapshot's content.
///
/// Blocks and blobs are separate variants rather than one uniform item
/// because they land in different providers and verify at different times.
/// A blob also cannot be a value: it may be far larger than memory, so it
/// travels as the reader it will be streamed from rather than as bytes.
pub enum Item {
    /// A whole block: its content, and the address that content must hash
    /// to. Verified on arrival.
    Block(Block),
    /// A blob: the address and length its content must have, and the
    /// reader its chunks come from. Verified once the last chunk lands.
    Blob {
        /// The address this blob's content must hash to.
        digest: NodeHash,
        /// The blob's total length in bytes.
        size: u64,
        /// The blob's content, in order.
        chunks: BlobReader,
    },
}

impl Item {
    /// The address this item's content must hash to.
    pub fn digest(&self) -> &NodeHash {
        match self {
            Item::Block(block) => &block.digest,
            Item::Blob { digest, .. } => digest,
        }
    }
}

/// How far an export reaches for content the local store does not hold.
///
/// Set through [`SnapshotExport::sparse`] and
/// [`SnapshotExport::download`]. Exclusive rather than combinable --
/// hydrating from an upstream and tolerating gaps are opposite answers to
/// the same question -- so the later call wins rather than accumulating.
pub enum Reach {
    /// Everything the revision references must be present locally.
    ///
    /// A missing block or blob fails the export naming what is absent.
    /// The default, because a snapshot that silently omits content is
    /// unusable in a way that only surfaces much later, at read time.
    Complete,

    /// Whatever is reachable from the root in local storage.
    ///
    /// Gaps are skipped and the walk continues
    /// past them where it can -- a subtree under an absent node is
    /// unreachable, so what lies beneath it cannot be enumerated. Corrupt
    /// content is still fatal: a block whose stored bytes do not match
    /// their address is a different failure from one that is simply not
    /// here.
    Sparse,

    /// Hydrate read-misses from an upstream as the walk proceeds.
    ///
    /// Fetched content is cached locally on the way through, so the
    /// export is complete at the cost of pulling whatever is absent over
    /// the network.
    Download(RemoteRepository),
}

/// Reads a snapshot's content out of a store.
///
/// Created by [`Snapshot::export`]. Yields a stream of [`Item`]s rather
/// than pushing into a consumer, so what happens to the content is the
/// caller's decision: hand it to [`Repository::import`], write it to a
/// file, count it, filter it.
pub struct SnapshotExport {
    snapshot: Snapshot,
    reach: Reach,
}

impl Snapshot {
    /// Prepare to read this snapshot's content.
    pub fn export(self) -> SnapshotExport {
        SnapshotExport {
            snapshot: self,
            reach: Reach::Complete,
        }
    }
}

impl SnapshotExport {
    /// Export whatever is reachable locally instead of requiring
    /// everything.
    ///
    /// Gaps are skipped rather than failing the export. Content that is
    /// present but corrupt still fails: bytes that do not match the address
    /// they were stored under are a different problem from bytes that are
    /// simply not here.
    pub fn sparse(mut self) -> Self {
        self.reach = Reach::Sparse;
        self
    }

    /// Hydrate read-misses from `upstream` as the walk proceeds.
    ///
    /// Fetched content is cached locally on the way through, so the export
    /// is complete at the cost of pulling whatever is absent over the
    /// network.
    pub fn download(mut self, upstream: RemoteRepository) -> Self {
        self.reach = Reach::Download(upstream);
        self
    }

    /// How far this export will reach.
    pub fn reach(&self) -> &Reach {
        &self.reach
    }

    /// Stream the snapshot's content.
    ///
    /// One walk of the tree. Nodes are yielded as they are visited, and
    /// because a leaf arrives already decoded, the content its entries
    /// reference is discovered in the same visit rather than in a second
    /// pass over the same rows -- those referenced blocks and blobs follow
    /// the nodes.
    pub fn perform<Env>(self, env: &Env) -> impl Stream<Item = Result<Item, SnapshotError>> + '_
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<BlobRead>
            + Provider<BlobImport>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + ConditionalSync
            + 'static,
    {
        let catalog = self.snapshot.index();
        let subject = self.snapshot.subject.clone();
        let upstream = match &self.reach {
            Reach::Download(remote) => Some(remote.clone()),
            Reach::Complete | Reach::Sparse => None,
        };
        // The index consumes one handle for block read-misses; blob bytes
        // travel their own channel, so the blob loop needs its own.
        let hydrate = upstream.clone();
        let sparse = matches!(self.reach, Reach::Sparse);
        let root = NodeHash::from(*self.snapshot.revision().tree.hash());

        try_stream! {
            // With an upstream a read-miss falls through to the remote and
            // is cached; without one the index is exactly what this store
            // holds.
            let index = NetworkedIndex::new(env, catalog, upstream);
            let storage = TreeStorage::new(TreeStorageBridge(index.clone()));
            let tree = Index::from_hash(root);

            let mut spills: HashSet<[u8; 32]> = HashSet::new();
            let mut blobs: Vec<NodeHash> = Vec::new();
            // A key may surface twice — its stored leaf entry plus a
            // buffered op riding an ancestor index node — naming the same
            // content; ship each blob once.
            let mut blob_seen: HashSet<NodeHash> = HashSet::new();

            let visits = tree.traverse_available(&storage);
            futures_util::pin_mut!(visits);
            while let Some(visit) = visits.next().await {
                let node = match visit? {
                    Visit::Present(node) => node,
                    Visit::Absent(hash) => {
                        if sparse {
                            continue;
                        }
                        Err(SnapshotError::MissingBlock { digest: hash })?;
                        continue;
                    }
                };

                // A node is already decoded here, so the entries it holds
                // are lifted in this visit — and every entry a node
                // physically holds is in the revision's closure. For a
                // leaf that includes entries a buffered op upstream has
                // superseded: reads screen those, but their bytes (and
                // the spill blocks and blobs they reference) are still
                // stored and still served to history reads. For an index
                // node it is the buffered asserts riding its novelty
                // buffers, whose keys and values reference content
                // exactly like stored entries (a buffered retract
                // references nothing of its own). The closure only
                // collects: classification returns artifact errors,
                // which do not belong in a tree-walk callback.
                let mut entries: Vec<(Key, State<Datum>)> = Vec::new();
                match node.body() {
                    ArchivedNodeBody::Segment(segment) => {
                        segment.for_each_entry::<Key, _>(|key, value| {
                            entries.push((Key::from(key.to_vec()), into_owned(value)?));
                            Ok(())
                        })?;
                    }
                    ArchivedNodeBody::Index(index) => {
                        for entry in index.all_novelty::<Key>()? {
                            if let NoveltyOp::Assert(value) = entry.op {
                                entries.push((Key::from(entry.key), value));
                            }
                        }
                    }
                }
                for (key, value) in entries {
                    match shipment_ref(&key, &value, false)? {
                        Some(ShipmentRef::SpilledValue(reference)) => {
                            spills.insert(reference);
                        }
                        Some(ShipmentRef::BlobAdded { hash, .. }) => {
                            let hash = NodeHash::from(hash);
                            if blob_seen.insert(hash.clone()) {
                                blobs.push(hash);
                            }
                        }
                        _ => {}
                    }
                }

                yield Item::Block(Block::new(node.buffer().clone()));
            }

            // Spilled value blocks, discovered above. Their reads are
            // independent, so they run concurrently (bounded) and yield
            // as they complete — against a downloading reach this
            // overlaps the remote round-trips instead of paying them one
            // after another.
            let mut spill_reads = stream::iter(spills.into_iter().map(
                |reference| {
                    let storage = &storage;
                    async move {
                        let digest = NodeHash::from(reference);
                        let bytes = storage.retrieve(&digest).await;
                        (digest, bytes)
                    }
                },
            ))
            .buffer_unordered(FETCH_CONCURRENCY);
            while let Some((digest, bytes)) = spill_reads.next().await {
                match bytes? {
                    Some(bytes) => {
                        yield Item::Block(Block { digest, content: Buffer::from(bytes) });
                    }
                    None if sparse => {}
                    None => Err(SnapshotError::MissingBlock { digest })?,
                }
            }
            drop(spill_reads);

            // Blob bytes, discovered above. The size comes from the tree's
            // own blob index rather than the traversal: `import` is opened
            // with it, and reading it costs no byte fetch. Each blob's
            // whole fetch (and, under a downloading reach, its local
            // import) is one future; they run concurrently (bounded) and
            // yield as they complete. `None` from a future means the blob
            // is unavailable — no index record, or no bytes anywhere the
            // reach extends — which sparse tolerates and complete refuses.
            let mut blob_reads = stream::iter(blobs.into_iter().map(|digest| {
                let tree = &tree;
                let index = &index;
                let hydrate = &hydrate;
                let subject = subject.clone();
                async move {
                    let Some(record) = tree.get_blob(index, digest.as_bytes()).await? else {
                        return Ok((digest, None));
                    };
                    let reader = subject
                        .clone()
                        .archive()
                        .blob()
                        .read(digest.clone())
                        .perform(env)
                        .await;
                    let reader = match (reader, hydrate) {
                        // A local miss the reach says to resolve. The index
                        // cannot serve this one -- blocks fall through to the
                        // remote via `Get`, blob bytes travel their own
                        // channel -- so fetch the whole blob through a local
                        // digest-verified import first (a lying remote
                        // surfaces as `DigestMismatch` at `finish`, and the
                        // bytes are cached like every other download), then
                        // serve the read from the now-local copy.
                        (Err(BlobError::NotFound(_)), Some(remote)) => {
                            let address = remote.address();
                            let mut source = address
                                .subject
                                .clone()
                                .archive()
                                .blob()
                                .read(digest.clone())
                                .fork(address.site())
                                .perform(env)
                                .await?;
                            let mut sink = subject
                                .clone()
                                .archive()
                                .blob()
                                .import(digest.clone(), record.size)
                                .perform(env)
                                .await?;
                            while let Some(chunk) = source.next().await? {
                                sink.write_all(&chunk).await?;
                            }
                            sink.finish().await?;
                            subject
                                .clone()
                                .archive()
                                .blob()
                                .read(digest.clone())
                                .perform(env)
                                .await
                        }
                        (reader, _) => reader,
                    };
                    match reader {
                        Ok(chunks) => Ok((digest, Some((record.size, chunks)))),
                        Err(BlobError::NotFound(_)) => Ok((digest, None)),
                        Err(error) => Err(SnapshotError::from(error)),
                    }
                }
            }))
            .buffer_unordered(FETCH_CONCURRENCY);
            while let Some(fetched) = blob_reads.next().await {
                let (digest, available) = fetched?;
                match available {
                    Some((size, chunks)) => {
                        yield Item::Blob { digest, size, chunks };
                    }
                    None if sparse => {}
                    None => Err(SnapshotError::MissingBlob { digest })?,
                }
            }
        }
    }
}

/// Writes snapshot content into a repository's storage.
///
/// The counterpart to [`SnapshotExport`]: it verifies each item against
/// the address it declares and stores it, and does nothing else. It does
/// not publish a head, mint a revision, or make anything visible -- a
/// destination holds the content afterwards but still resolves nothing by
/// name until someone publishes the revision, which travels separately.
///
/// Takes no revision for the same reason: blocks and blobs are
/// content-addressed, so nothing about storing them depends on which
/// revision referenced them.
pub struct SnapshotImport<Items> {
    subject: Subject,
    items: Items,
}

impl<C: Principal> Repository<C> {
    /// Prepare to write snapshot content into this repository's storage.
    pub fn import<Items>(&self, items: Items) -> SnapshotImport<Items>
    where
        Items: Stream<Item = Result<Item, SnapshotError>>,
    {
        SnapshotImport {
            subject: self.subject(),
            items,
        }
    }
}

impl<Items> SnapshotImport<Items>
where
    Items: Stream<Item = Result<Item, SnapshotError>>,
{
    /// Store every item, verifying each against the address it declares.
    ///
    /// Returns how many of each kind landed.
    pub async fn perform<Env>(self, env: &Env) -> Result<Imported, SnapshotError>
    where
        Env: Provider<Put> + Provider<BlobImport> + ConditionalSync + 'static,
    {
        let subject = self.subject;
        let mut imported = Imported::default();
        let items = self.items;
        futures_util::pin_mut!(items);

        while let Some(item) = items.next().await {
            match item? {
                Item::Block(block) => {
                    // Verify before storing. Content-addressed bytes that
                    // hash to something else do not fail loudly when
                    // stored -- they land at a different address where
                    // nothing references them, and surface much later as a
                    // missing node, far from the cause.
                    if !block.is_intact() {
                        return Err(SnapshotError::BlockDigestMismatch {
                            expected: block.digest.clone(),
                            actual: block.content.blake3_hash().clone(),
                        });
                    }
                    subject
                        .clone()
                        .archive()
                        .index()
                        .put(block.content)
                        .perform(env)
                        .await?;
                    imported.blocks += 1;
                }
                Item::Blob {
                    digest,
                    size,
                    mut chunks,
                } => {
                    let mut writer = subject
                        .clone()
                        .archive()
                        .blob()
                        .import(digest.clone(), size)
                        .perform(env)
                        .await?;
                    while let Some(chunk) = chunks.next().await? {
                        writer.write_all(&chunk).await?;
                    }
                    // A blob arrives in pieces, so its address can only be
                    // checked once the last one lands. This is what
                    // surfaces a lying source.
                    let written = writer.finish().await?;
                    if written != digest {
                        return Err(SnapshotError::BlobDigestMismatch {
                            expected: digest,
                            actual: written,
                        });
                    }
                    imported.blobs += 1;
                }
            }
        }

        Ok(imported)
    }
}

/// What an import stored.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Imported {
    /// Blocks written.
    pub blocks: u64,
    /// Blobs written.
    pub blobs: u64,
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_artifacts::{Artifact, Instruction, Value};
    use dialog_credentials::Credential;
    use dialog_effects::blob::BlobSource;
    use dialog_search_tree::PersistentNode;
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use futures_util::stream;

    use super::*;
    use dialog_capability::Subject;
    use dialog_effects::storage::{LocationExt as _, Storage as StorageFx};

    use crate::Blob;
    use crate::helpers::test_repo;
    use dialog_operator::DeriveOperator as _;
    use dialog_operator::helpers::{generate_data, test_operator_with_profile, unique_name};

    /// A blob source over bytes held in memory.
    struct Bytes(Option<Vec<u8>>);

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl BlobSource for Bytes {
        async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
            Ok(self.0.take())
        }
    }

    /// Drain an export into memory, so a test can assert on what it
    /// produced. Blobs are read out here because a `BlobReader` cannot
    /// outlive the stream that yielded it.
    async fn drain(
        items: impl Stream<Item = Result<Item, SnapshotError>>,
    ) -> Result<(Vec<Block>, Vec<(NodeHash, Vec<u8>)>)> {
        let mut blocks = Vec::new();
        let mut blobs = Vec::new();
        futures_util::pin_mut!(items);
        while let Some(item) = items.next().await {
            match item? {
                Item::Block(block) => blocks.push(block),
                Item::Blob {
                    digest, mut chunks, ..
                } => {
                    let mut bytes = Vec::new();
                    while let Some(chunk) = chunks.next().await? {
                        bytes.extend_from_slice(&chunk);
                    }
                    blobs.push((digest, bytes));
                }
            }
        }
        Ok((blocks, blobs))
    }

    struct Stage {
        env: dialog_operator::Operator<VolatileSpace>,
        profile: dialog_operator::Profile,
        repository: crate::Repository,
        revision: Revision,
        blob_bytes: Vec<u8>,
    }

    /// A second environment with the same repository mounted, so imported
    /// content has somewhere to land.
    async fn destination_for(stage: &Stage) -> Result<dialog_operator::Operator<VolatileSpace>> {
        let destination = Storage::<VolatileSpace>::volatile();
        StorageFx::profile(unique_name("snapshot-profile"))
            .create(Credential::Signer(stage.profile.signer().clone()))
            .perform(&destination)
            .await?;
        StorageFx::profile(unique_name("snapshot-repository"))
            .create(stage.repository.credential().clone())
            .perform(&destination)
            .await?;
        Ok(stage
            .profile
            .derive(b"snapshot-destination")
            .allow(Subject::any())
            .network(dialog_network::Network::default())
            .build(destination)
            .await?)
    }

    /// A branch holding enough to exercise every channel: ordinary facts,
    /// a value too large to inline (so it spills to its own block), and a
    /// blob.
    async fn stage() -> Result<Stage> {
        let (env, profile) = test_operator_with_profile().await;
        let repository = test_repo(&env, &profile).await;
        let branch = repository.branch("main").open().perform(&env).await?;

        let mut facts: Vec<Instruction> = generate_data(20)?
            .into_iter()
            .map(Instruction::Assert)
            .collect();
        let large = Value::String(
            "spilled".repeat(dialog_search_tree::Manifest::default().inline_n as usize + 1),
        );
        facts.push(Instruction::Assert(Artifact {
            the: "document/body".parse()?,
            of: "document:large".parse()?,
            is: large,
            cause: None,
        }));
        branch.commit(stream::iter(facts)).perform(&env).await?;

        let blob_bytes = b"snapshot blob".repeat(512);
        Blob::import(stream::iter(vec![Ok(blob_bytes.clone())]))
            .write(branch.blobs())
            .perform(&env)
            .await?;

        let revision = branch.revision().expect("staged branch has a revision");
        Ok(Stage {
            env,
            profile,
            repository,
            revision,
            blob_bytes,
        })
    }

    #[dialog_common::test]
    async fn it_exports_every_channel_of_a_revision() -> Result<()> {
        let stage = stage().await?;

        let (blocks, blobs) = drain(
            stage
                .repository
                .snapshot(stage.revision.clone())
                .export()
                .perform(&stage.env),
        )
        .await?;

        assert!(!blocks.is_empty(), "the block channel carried the tree");
        assert_eq!(blobs.len(), 1, "the blob channel carried the blob");
        assert_eq!(
            blobs[0].1, stage.blob_bytes,
            "the blob arrived byte for byte"
        );
        assert!(
            blocks.iter().all(Block::is_intact),
            "every exported block hashes to the address it declares"
        );
        Ok(())
    }

    // Round trip: content exported from one store and imported into
    // another leaves it there -- but the revision stays invisible, because
    // import deliberately publishes nothing.
    #[dialog_common::test]
    async fn it_round_trips_a_revision_into_another_store() -> Result<()> {
        let stage = stage().await?;
        let destination = destination_for(&stage).await?;
        let elsewhere = crate::Repository::from(stage.repository.credential().clone());

        let items = stage
            .repository
            .snapshot(stage.revision.clone())
            .export()
            .perform(&stage.env);
        let imported = elsewhere.import(items).perform(&destination).await?;

        assert!(imported.blocks > 0, "blocks landed");
        assert_eq!(imported.blobs, 1, "the blob landed");

        let branch = elsewhere
            .branch("main")
            .open()
            .perform(&destination)
            .await?;
        assert!(
            branch.revision().is_none(),
            "importing content publishes nothing"
        );
        Ok(())
    }

    // A block whose bytes do not hash to the address it declares must be
    // refused. Storing it would not fail loudly -- content-addressed bytes
    // land wherever they hash to, so a corrupt block simply becomes an
    // unreferenced one, and the revision that needed it reports a missing
    // node much later, far from the cause.
    #[dialog_common::test]
    async fn it_refuses_a_block_that_does_not_match_its_address() -> Result<()> {
        let stage = stage().await?;
        let (blocks, _) = drain(
            stage
                .repository
                .snapshot(stage.revision.clone())
                .export()
                .perform(&stage.env),
        )
        .await?;

        let destination = destination_for(&stage).await?;
        let elsewhere = crate::Repository::from(stage.repository.credential().clone());

        // Keep the address, corrupt the bytes.
        let honest = blocks.first().expect("export produced blocks").clone();
        let tampered = Item::Block(Block {
            digest: honest.digest.clone(),
            content: Buffer::from(b"not the bytes this address names".to_vec()),
        });

        let result = elsewhere
            .import(stream::iter(vec![Ok(tampered)]))
            .perform(&destination)
            .await;
        match result {
            Err(SnapshotError::BlockDigestMismatch { expected, actual }) => {
                assert_eq!(expected, honest.digest);
                assert_ne!(actual, honest.digest);
            }
            other => panic!("expected a digest mismatch, got {other:?}"),
        }

        // And nothing was written under the address it claimed.
        let stored = elsewhere
            .subject()
            .archive()
            .index()
            .get(honest.digest.clone())
            .perform(&destination)
            .await?;
        assert!(stored.is_none(), "a refused block must not be stored");
        Ok(())
    }

    // The blob equivalent, and the reason blobs are a separate variant: a
    // blob arrives in pieces, so its address cannot be checked on arrival.
    // The writer is opened with the declared digest and the mismatch
    // surfaces at `finish` -- which is what catches a lying source.
    //
    // Every blob provider verifies there (volatile, filesystem and S3 all
    // raise `BlobError::DigestMismatch`), so in practice the refusal comes
    // from the provider rather than from the import's own check. The check
    // is kept regardless: a future backend that trusted its input would
    // otherwise silently store bytes under an address they do not hash to.
    // What this pins is the contract -- corrupt bytes are refused, and the
    // error names both addresses -- not which layer noticed.
    #[dialog_common::test]
    async fn it_refuses_a_blob_that_does_not_match_its_address() -> Result<()> {
        let stage = stage().await?;
        let (_, blobs) = drain(
            stage
                .repository
                .snapshot(stage.revision.clone())
                .export()
                .perform(&stage.env),
        )
        .await?;

        let destination = destination_for(&stage).await?;
        let elsewhere = crate::Repository::from(stage.repository.credential().clone());

        let (digest, honest) = blobs.first().expect("export produced a blob");
        // Claim the real address, hand over different bytes of the same
        // length so the declared size still holds.
        let mut lying = honest.clone();
        lying[0] ^= 0xFF;
        let item = Item::Blob {
            digest: digest.clone(),
            size: honest.len() as u64,
            chunks: Box::new(Bytes(Some(lying))),
        };

        let result = elsewhere
            .import(stream::iter(vec![Ok(item)]))
            .perform(&destination)
            .await;
        match result {
            Err(SnapshotError::BlobDigestMismatch { expected, actual }) => {
                assert_eq!(&expected, digest, "the refusal names the declared address");
                assert_ne!(&actual, digest, "and what the bytes actually hash to");
            }
            // The provider renders addresses without the `blake3#` prefix
            // that `Display` carries, so compare on the encoded body.
            Err(SnapshotError::Blob(BlobError::DigestMismatch { expected, actual })) => {
                assert!(
                    digest.to_string().ends_with(&expected),
                    "the refusal names the declared address, got {expected}"
                );
                assert!(
                    !digest.to_string().ends_with(&actual),
                    "and what the bytes actually hash to, got {actual}"
                );
            }
            other => panic!("expected a blob digest mismatch, got {other:?}"),
        }
        Ok(())
    }

    // A replica holding the tree but not what its leaves point at.
    //
    // The absent-root case below stops the walk immediately, so it never
    // reaches the leaf-content paths. This is the shape a real partial
    // replica takes -- the tree is small and arrives first, blobs are
    // large and lag -- and it is the only way to exercise the spill and
    // blob arms of either reach.
    //
    // Built by importing just the tree nodes: a spilled value is a raw
    // encoded value rather than an rkyv node, so parsing separates the two
    // without depending on the order the export yields them in.
    async fn tree_only_destination(
        stage: &Stage,
    ) -> Result<(dialog_operator::Operator<VolatileSpace>, crate::Repository)> {
        let (blocks, _) = drain(
            stage
                .repository
                .snapshot(stage.revision.clone())
                .export()
                .perform(&stage.env),
        )
        .await?;

        let nodes: Vec<Block> = blocks
            .into_iter()
            .filter(|block| {
                PersistentNode::<Key, State<Datum>>::try_from(block.content.clone()).is_ok()
            })
            .collect();
        assert!(
            !nodes.is_empty(),
            "the staged revision has tree nodes to seed with"
        );

        let destination = destination_for(stage).await?;
        let elsewhere = crate::Repository::from(stage.repository.credential().clone());
        let imported = elsewhere
            .import(stream::iter(nodes.into_iter().map(Item::Block).map(Ok)))
            .perform(&destination)
            .await?;
        assert_eq!(imported.blobs, 0, "the blob was deliberately withheld");

        Ok((destination, elsewhere))
    }

    // With the tree present, the walk runs to completion and reaches the
    // leaf content -- so the default must fail on a spilled value or blob
    // it cannot read, not merely on an absent root.
    #[dialog_common::test]
    async fn it_refuses_leaf_content_it_cannot_read() -> Result<()> {
        let stage = stage().await?;
        let (destination, elsewhere) = tree_only_destination(&stage).await?;

        let refused = drain(
            elsewhere
                .snapshot(stage.revision.clone())
                .export()
                .perform(&destination),
        )
        .await;

        match refused {
            Err(error) => match error.downcast_ref::<SnapshotError>() {
                Some(SnapshotError::MissingBlob { .. } | SnapshotError::MissingBlock { .. }) => {}
                _ => panic!("expected a missing-content refusal, got {error:?}"),
            },
            Ok((blocks, blobs)) => panic!(
                "a complete export must not omit leaf content it cannot read, \
                 got {} blocks and {} blobs",
                blocks.len(),
                blobs.len()
            ),
        }
        Ok(())
    }

    // And the same store under `sparse` is the case the reach exists for:
    // the tree is walked and yielded, the unreadable leaf content is
    // skipped rather than fatal.
    #[dialog_common::test]
    async fn it_skips_leaf_content_it_cannot_read_when_sparse() -> Result<()> {
        let stage = stage().await?;
        let (destination, elsewhere) = tree_only_destination(&stage).await?;

        let (blocks, blobs) = drain(
            elsewhere
                .snapshot(stage.revision.clone())
                .export()
                .sparse()
                .perform(&destination),
        )
        .await?;

        assert!(
            !blocks.is_empty(),
            "sparse still yields the tree it can reach"
        );
        assert!(
            blobs.is_empty(),
            "the blob it cannot read is skipped, not fabricated"
        );
        Ok(())
    }

    // A revision whose content is elsewhere: the default refuses to produce
    // a snapshot that silently omits things, and `sparse` is how a caller
    // says it wants whatever is here.
    #[dialog_common::test]
    async fn it_separates_a_partial_export_from_a_failed_one() -> Result<()> {
        let stage = stage().await?;
        // An environment that has the repository mounted but none of its
        // content -- exactly a replica that has not fetched yet.
        let empty = destination_for(&stage).await?;
        let elsewhere = crate::Repository::from(stage.repository.credential().clone());

        let refused = drain(
            elsewhere
                .snapshot(stage.revision.clone())
                .export()
                .perform(&empty),
        )
        .await;
        assert!(
            refused.is_err(),
            "a complete export must not quietly omit what it cannot read"
        );

        let (blocks, blobs) = drain(
            elsewhere
                .snapshot(stage.revision.clone())
                .export()
                .sparse()
                .perform(&empty),
        )
        .await?;
        assert!(
            blocks.is_empty() && blobs.is_empty(),
            "an unreachable root is the frontier: nothing below it can be reached"
        );
        Ok(())
    }
}
