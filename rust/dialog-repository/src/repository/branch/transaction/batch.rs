//! Staged transaction chains: commit without publishing, publish once.
//!
//! A commit on a branch transaction does NOT move the branch head.
//! It mints the revision — on the branch's own origin, at the successor
//! edition, byte-identical to what a published commit would mint — and
//! hands back a [`TransactionBatch`] holding the staged chain. Further
//! transactions extend the chain; [`TransactionBatch::publish`] moves
//! the branch head to the chain tip with one CAS, making every staged
//! commit visible atomically, at exactly the versions minted.
//!
//! This is what lets a writer record a fact naming its own commit
//! without prediction or a second published commit: commit once, read
//! the minted [`Version`] off the batch, assert it in the next commit
//! on the same chain, publish. Either both commits become visible or
//! neither does.
//!
//! A batch that loses the publish CAS (the head moved: a pull, another
//! writer) is stale wholesale — its editions are taken. Recovery is to
//! re-run the transaction against the fresh head, minting fresh
//! versions; nothing staged is ever rewritten.

use super::induce::{induce, succession_in_own};
use super::succession;
use super::{
    Line, Transaction, TransactionCommit, carry_footprint, touches_rule_facts, touches_rules,
};
use crate::Ephemeral;
use crate::placement::{
    Pointer, Recorded, control_layer, follows_selection, memory_layer, selected, session_layer,
    take_dispatched,
};
use crate::repository::branch::asset::store_assets;
use crate::repository::branch::commit::{Amended, Mint, Minted, Outcome};
use crate::repository::branch::session::{Composite, shared_metadata};
use crate::repository::branch::transaction::{DurableLayer, StackContext};
use crate::repository::source::{Caches, Live, View};
use crate::Channel;
use crate::stack::durable::{Durables, Pinned};
use crate::{
    Branch, Cell, Checkpoint, CommitError, PublishError, QueryLayer, RemoteSite, Revision,
    SelectQuery, Snapshot, SnapshotClaims, Staged, TransactionQuery, origin_of,
};
use dialog_capability::history::Origin;
use dialog_capability::{Capability, Did, Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::history::{CausalityCache, Context, ContextCache, RevisionRecord, Version};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::tree::WriteScope;
use dialog_corpus::{Change, Changes, Entity, Fact, Selector, Statement, Update as _};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, Operator, OperatorExt as _};
use dialog_effects::blob::Import as BlobImport;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::blob::Size as BlobSize;
use dialog_effects::memory::{Current, Publish, Resolve, Version as MemoryVersion};
use dialog_query::query::Application;
use dialog_search_tree::Cache;
use dialog_search_tree::nodes;
use std::iter::once;
use std::mem::take;
use std::slice::from_ref;
use std::sync::Arc;

/// A staged chain of commits on a branch: minted, persisted, invisible.
///
/// Produced by committing a branch [`Transaction`] (or another link on
/// the same batch). Every staged commit is a real revision on the
/// branch's OWN origin — the successor edition of the head the chain
/// builds on — so [`version`](Self::version) reports exactly what
/// [`publish`](Self::publish) makes visible, synchronously and without
/// prediction. Until publish, the branch head does not move: staged
/// blocks persist to the shared archive (unreachable garbage if the
/// batch is dropped), but no head cell, watermark, or shared memo
/// changes.
///
/// Reads through [`claims`](Self::claims) / [`select`](Self::select) /
/// [`query`](Self::query) serve the staged state.
///
/// Publish CAS's the branch head from the head the chain was staged
/// from to the chain tip. If the head moved in between, publish fails
/// with [`PublishError::VersionMismatch`] and the whole batch is stale:
/// its editions now belong to whatever advanced the head. Recovery is
/// to re-run the transaction on the fresh head — versions come out
/// fresh, which is why a fact citing a staged version must ride the
/// same batch as the commit it cites.
#[must_use = "staged commits are invisible until .publish().perform(&env) moves the branch head"]
pub struct TransactionBatch {
    /// The staged view: reads serve the chain tip, and commits mint on
    /// the branch's own line (see `Snapshot::staged`). Carries the
    /// version-keyed memos (records, contexts, causality) batch-locally:
    /// a dropped batch's versions can be re-minted with different
    /// content by the retry, so nothing keyed by them may leak into the
    /// branch's shared memos before the publish CAS settles who owns
    /// them. Content-keyed caches (nodes, spills, rules, plans, spine)
    /// are shared with the branch.
    snapshot: Snapshot,
    /// The branch head cell as checkpointed when staging began; publish
    /// CAS's the chain tip against it.
    head: Checkpoint<Revision>,
    /// The same cell, for the empty-chain staleness re-check.
    cell: Cell<Revision>,
    /// The cell version staging began at, compared on an empty-chain
    /// publish.
    base_version: Option<MemoryVersion>,
    /// The branch's induction watermark cell, advanced at publish (every
    /// staged commit induced, so the published head is fully induced).
    induction: Cell<Revision>,
    /// The staged links in chain order, for seeding the branch's
    /// verified-record memo once the head has actually advanced.
    chain: Vec<(Version, RevisionRecord)>,
    /// The chain tip's causal context, for the branch's context memo.
    context: Option<Arc<Context>>,
    /// The branch's shared memos, seeded at publish.
    records: Cache<Version, RevisionRecord>,
    contexts: ContextCache,
    /// The transients rule heads emitted while inducing the staged
    /// links, across every round of every link. See
    /// [`induced`](Self::induced).
    induced: Changes,
    /// The writes of this chain placed outside the primary, tick by
    /// tick in the order the ticks ran, each tick's by the layer each is
    /// placed on, as a stack routes them: what its memory layers take
    /// once the chain publishes, one instant per tick that wrote a layer.
    /// Empty for a chain committed on the branch alone.
    shares: Vec<Vec<(Entity, Changes)>>,
    /// The stack's memory layers, by name, that take the shares.
    memories: Vec<(Entity, Ephemeral)>,
    /// The branch's session overlay, and the facts the staged links
    /// retract that only it holds: retracted from it once the publish
    /// lands, so what a transaction read is what its publish leaves.
    overlay: Ephemeral,
    overlay_retractions: Vec<Fact>,
    /// The primary's branch entity when the chain was committed through
    /// a stack: its publish then records, in the stack's own layer, the
    /// revision of every layer that moved (see [`record_revisions`]).
    revisions: Option<Entity>,
    /// The trees this chain minted for the stack's durable layers, by
    /// layer: the revision each layer's writes left it at, which the
    /// chain's primary commit points at. Nothing publishes them: the
    /// primary's publish is the layers' too.
    durables: Vec<(Entity, Revision)>,
    /// The stack's durable layers and where the chain read them from:
    /// once the publish has won, the layers stand where the chain left
    /// them. `None` for a chain committed on the branch alone.
    pinned: Option<(Arc<Durables>, Pinned)>,
    /// The commands the chain's transactions dispatched themselves, the
    /// writes into `control:` among them, and their writes into the
    /// other channels, named for each: with [`induced`](Self::induced),
    /// what the publish lands on the stack's channels.
    dispatched: Changes,
    /// The channels of the stack the chain was committed through, the
    /// loopback first, where the publish lands what was written into
    /// them. `None` for a chain committed on the branch alone, which
    /// lands nothing.
    channels: Option<Vec<Channel>>,
}

/// The facts `changes` retracts that `overlay` holds. A transaction's
/// read hides such a fact, and the commit's retraction of it lands
/// nothing in the tree, which never held it: the publish retracts it
/// from the overlay, so the stack's row does not come back.
fn overlay_retractions(changes: &Staged, overlay: &Ephemeral) -> Vec<Fact> {
    let mut retracted = Vec::new();
    for (the, of, change) in changes.log() {
        let Change::Retract(value) = change else {
            continue;
        };
        let selector = Selector::new()
            .the(the.clone())
            .of(of.clone())
            .is(value.clone());
        if !overlay.scan(&selector).is_empty() {
            retracted.push(Fact {
                the: the.clone(),
                of: of.clone(),
                is: value.clone(),
                cause: None,
            });
        }
    }
    retracted
}

/// Retract `facts` from the session overlay, once the publish that
/// retracted them landed.
fn retract_from_overlay(overlay: &Ephemeral, facts: Vec<Fact>) -> Result<(), CommitError> {
    if facts.is_empty() {
        return Ok(());
    }
    let mut changes = Changes::new();
    for fact in facts {
        changes.dissociate(fact.the, fact.of, fact.is);
    }
    overlay.apply(changes)?;
    Ok(())
}

impl TransactionBatch {
    /// The [`Version`] of the chain tip — for a batch with staged
    /// commits, the version the latest one minted, which is exactly the
    /// version [`publish`](Self::publish) makes visible. A batch whose
    /// commits were all no-ops reports the version of the head it was
    /// staged from.
    ///
    /// This is authoritative, not predicted: the commit that minted it
    /// has already happened. A writer that wants a fact naming its own
    /// commit stages the commit, reads this, and asserts it in the next
    /// link of the same batch.
    pub fn version(&self) -> Version {
        self.snapshot.revision().version()
    }

    /// The chain tip: the [`Revision`] publish will move the branch
    /// head to.
    pub fn revision(&self) -> Revision {
        self.snapshot.revision()
    }

    /// The caches the staged view reads and commits through.
    #[cfg(test)]
    pub(crate) fn caches(&self) -> &Caches {
        self.snapshot.caches()
    }

    /// The transient facts inductive rules concluded while this chain
    /// was staged: every transient head emitted in any induction round
    /// of any staged link. The transients the transactions dispatched
    /// themselves are not included; the caller already holds those.
    ///
    /// Induction reads these as the next round's stimulus and then drops
    /// them, so nothing is ever committed for them. A host that runs its
    /// own handlers for a command (outside the rule system) reads them
    /// here, before [`publish`](Self::publish) consumes the batch, and
    /// runs them after the publish lands. Without that, a rule whose
    /// head is such a command commits cleanly and nothing happens.
    pub fn induced(&self) -> &Changes {
        &self.induced
    }

    /// The writes placed outside the primary that this chain's commits
    /// made, the ones their rules derived included, tick by tick: one
    /// entry per tick that wrote a layer, in the order the ticks ran,
    /// each holding that tick's writes by the layer each is placed on.
    /// A link's own writes are its first tick, and each induction round
    /// after it is one more. Once the chain publishes, each layer takes
    /// each tick's writes as an instant of its own, in this order.
    pub fn shares(&self) -> &[Vec<(Entity, Changes)>] {
        &self.shares
    }

    /// Start the next transaction on this chain. Its commit extends the
    /// batch by one more staged revision.
    pub fn transaction(self) -> Transaction<TransactionBatch> {
        Transaction::on(self)
    }

    /// Assert a claim into the next transaction on this chain.
    /// Shorthand for `.transaction().assert(claim)`.
    pub fn assert<C: Statement>(self, claim: C) -> Transaction<TransactionBatch> {
        self.transaction().assert(claim)
    }

    /// Retract a claim in the next transaction on this chain.
    /// Shorthand for `.transaction().retract(claim)`.
    pub fn retract<C: Statement>(self, claim: C) -> Transaction<TransactionBatch> {
        self.transaction().retract(claim)
    }

    /// Dispatch a transient into the next transaction on this chain.
    /// Shorthand for `.transaction().dispatch(claim)`.
    pub fn dispatch<C: Statement>(self, claim: C) -> Transaction<TransactionBatch> {
        self.transaction().dispatch(claim)
    }

    /// The staged fact index, serving the chain tip.
    pub fn claims(&self) -> SnapshotClaims<'_> {
        self.snapshot.claims()
    }

    /// Query the staged state with an application.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        self.snapshot.select(query)
    }

    /// Open a query over the staged state.
    pub fn query(&self) -> QueryLayer<'_> {
        self.snapshot.query()
    }

    /// Publish the staged chain: one CAS moving the branch head to the
    /// chain tip. See [`BatchPublish::perform`].
    pub fn publish(self) -> BatchPublish {
        BatchPublish { batch: self }
    }
}

/// Command publishing a [`TransactionBatch`] — created by
/// [`TransactionBatch::publish`], executed with `.perform(&env)`.
pub struct BatchPublish {
    batch: TransactionBatch,
}

impl BatchPublish {
    /// Move the branch head to the staged chain tip, CAS'd against the
    /// head the chain was staged from. On success every staged commit
    /// becomes visible atomically — at exactly the versions minted — the
    /// branch's shared memos are seeded with the staged records, and the
    /// induction watermark advances to the tip. On a lost CAS nothing is
    /// adopted and the batch is consumed: re-run the transaction against
    /// the fresh head.
    ///
    /// A batch whose commits were all no-ops publishes nothing, but does
    /// not silently succeed from a stale view either: the head is
    /// re-read and compared, so "nothing to do" judged against a
    /// superseded head fails with the same
    /// [`VersionMismatch`](PublishError::VersionMismatch) any other
    /// stale write gets.
    pub async fn perform<Env>(self, env: &Env) -> Result<Revision, CommitError>
    where
        Env: Provider<Publish> + Provider<Resolve> + ConditionalSync,
    {
        let TransactionBatch {
            snapshot,
            head,
            cell,
            base_version,
            induction,
            chain,
            context,
            records,
            contexts,
            induced,
            shares,
            memories,
            overlay,
            overlay_retractions,
            revisions,
            durables,
            pinned,
            dispatched,
            channels,
        } = self.batch;
        let tip = snapshot.revision();
        // Through a stack, what the chain wrote into its channels, the
        // commands its transactions dispatched and the ones its rules
        // dispatched in any tick among them, lands on each channel's
        // outside end once the publish has won, as one tick of its log:
        // a refused publish lands none, so a dispatch re-run from a moved
        // head re-runs no effect.
        // The stack's own layer then records the revision of every layer
        // that moved, the channels' among them, so the recording names
        // where each stands once the dispatch is done.
        let land = |channels: Option<Vec<Channel>>| -> Result<(), CommitError> {
            match channels {
                Some(channels) => {
                    let mut written = dispatched;
                    written.merge(induced);
                    land_in(&channels, written);
                    if let Some(branch) = &revisions {
                        record_revisions(branch, &memories, &channels, &overlay, &tip)?;
                    }
                    Ok(())
                }
                None => Ok(()),
            }
        };
        // The durable layers have no head: the trees the chain minted
        // for them are named by the primary's commit, so once the
        // primary's publish has won they stand there, and a lost one
        // leaves them pointed where they were, the minted trees an orphan
        // block set nothing names.
        let settle = |tip: &Revision| {
            if let Some((state, mut pinned)) = pinned {
                for (layer, revision) in durables {
                    if let Some(at) = state.layers().iter().position(|held| *held == layer) {
                        pinned.revisions[at] = Some(revision);
                    }
                }
                pinned.at = Some(tip.tree.clone());
                state.set(pinned);
            }
        };

        if chain.is_empty() {
            // Nothing staged. A same-value publish would bump the cell
            // version and fail concurrent writers spuriously, so re-read
            // and compare instead — mirroring the no-op handling in
            // `Commit::perform`. The induction watermark still advances
            // below: a no-op transaction was still an inducing instant
            // (this is what lets `Branch::induce` adopt a head).
            cell.resolve().perform(env).await?;
            let actual = cell.edition().map(|edition| edition.version);
            if actual != base_version {
                return Err(PublishError::VersionMismatch {
                    expected: base_version,
                    current: Current::Unknown,
                }
                .into());
            }
            advance_watermark(&induction, &tip, Some(tip.version()), env).await?;
            settle(&tip);
            retract_from_overlay(&overlay, overlay_retractions)?;
            apply_shares(&memories, shares)?;
            land(channels)?;
            return Ok(tip);
        }

        // The head the chain built on, by the first parent of its first
        // link: where the watermark stands when no other writer moved it.
        let base = chain
            .first()
            .and_then(|(_, record)| record.parents.first().cloned());
        head.publish(tip.clone(), env).await?;
        settle(&tip);
        retract_from_overlay(&overlay, overlay_retractions)?;
        // The memory layers take their shares only now that the primary's
        // commit is published: a refused publish shows in no layer. Each
        // tick lands as its own instant, in the order the ticks ran.
        apply_shares(&memories, shares)?;

        // Only now that the head has actually advanced do the staged
        // records and context enter the branch's shared memos: they are
        // keyed by version, and until the CAS settled, a competing
        // writer could have owned these editions.
        for (version, record) in chain {
            records.insert(version, record);
        }
        if let Some(context) = context {
            contexts.insert(tip.version(), context);
        }

        // Advance the induction watermark: every staged commit ran
        // commit-time induction, so the published head is fully induced.
        advance_watermark(&induction, &tip, base, env).await?;

        land(channels)?;
        Ok(tip)
    }
}

/// Land `written` on the outside ends of `channels`, the loopback first:
/// what it writes bound for no layer in the loopback's, what it writes
/// into each other channel in that one's. Every channel is landed in,
/// so every reader waiting is woken, whether or not anything landed for
/// it.
fn land_in(channels: &[Channel], mut written: Changes) {
    let mut shares = written.take_shares();
    for (at, channel) in channels.iter().enumerate() {
        if at == 0 {
            channel.land(&written);
            continue;
        }
        match shares.iter().position(|(layer, _)| layer == channel.entity()) {
            Some(index) => channel.land(&shares.swap_remove(index).1),
            None => channel.land(&Changes::new()),
        }
    }
}

/// Advance the induction watermark `induction` to `tip`, the head this
/// writer has just landed, built on the head whose version is `base`.
///
/// The watermark is local to a replica, but one replica can have more
/// than one writer: two processes over one store, say. In the window
/// after the head moved and before this write, another writer can land
/// a commit on `tip` and advance the watermark itself. A lost CAS here
/// must not fail a commit whose head has already landed. So the cell is
/// read again, and moved to `tip` only while it still stands at `base`,
/// or at no revision. Anywhere else, another writer moved it on, or left
/// it behind, which the next inducing instant catches up over.
async fn advance_watermark<Env>(
    induction: &Cell<Revision>,
    tip: &Revision,
    base: Option<Version>,
    env: &Env,
) -> Result<(), CommitError>
where
    Env: Provider<Publish> + Provider<Resolve> + ConditionalSync,
{
    if induction.content().as_ref() == Some(tip) {
        return Ok(());
    }
    match induction.publish(tip.clone()).perform(env).await {
        Err(PublishError::VersionMismatch { .. }) => {}
        other => return other.map_err(CommitError::from),
    }
    induction.resolve().perform(env).await?;
    if induction.content().as_ref() == Some(tip) {
        return Ok(());
    }
    let standing = induction.content().map(|revision| revision.version());
    if standing.is_some() && standing != base {
        return Ok(());
    }
    match induction.publish(tip.clone()).perform(env).await {
        Err(PublishError::VersionMismatch { .. }) => Ok(()),
        other => other.map_err(CommitError::from),
    }
}

/// Command committing a branch transaction (or a chain link) and
/// publishing in one step — created by [`TransactionCommit::publish`],
/// executed with `.perform(&env)`. The one-shot form: equivalent to
/// staging the batch and immediately publishing it.
pub struct TransactionPublish<L: Line> {
    commit: TransactionCommit<L>,
}

impl<'a> TransactionCommit<&'a Branch> {
    /// Commit and publish in one step: stage the batch, then move the
    /// branch head to it. The one-shot write path.
    pub fn publish(self) -> TransactionPublish<&'a Branch> {
        TransactionPublish { commit: self }
    }
}

impl TransactionCommit<TransactionBatch> {
    /// Fold this link into the chain's tip instead of staging a new
    /// commit after it: the tip keeps its version, and its tree grows to
    /// include this link's changes. Committing a chain as one commit or as
    /// a commit followed by amends records the same history, and after
    /// canonicalizing the same tree.
    ///
    /// Only a staged revision is amended, one this batch minted and nobody
    /// has seen: a published revision never changes under a version
    /// others may hold. A batch with nothing staged commits as usual.
    pub fn amend(mut self) -> Self {
        self.amend = true;
        self
    }

    /// Commit this link and publish the whole staged chain in one step.
    pub fn publish(self) -> TransactionPublish<TransactionBatch> {
        TransactionPublish { commit: self }
    }
}

impl TransactionPublish<&Branch> {
    /// Run induction, mint the commit, and publish it — returning the
    /// newly-published [`Revision`] (or the unchanged head when the
    /// settled batch is a no-op).
    pub async fn perform<Env>(self, env: &Env) -> Result<Revision, CommitError>
    where
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let batch = Box::pin(self.commit.perform(env)).await?;
        batch.publish().perform(env).await
    }
}

impl TransactionPublish<TransactionBatch> {
    /// Commit the link, then publish the whole staged chain — returning
    /// the newly-published [`Revision`].
    pub async fn perform<Env>(self, env: &Env) -> Result<Revision, CommitError>
    where
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let batch = Box::pin(self.commit.perform(env)).await?;
        batch.publish().perform(env).await
    }
}

impl Transaction<TransactionBatch> {
    /// Run queries against this transaction's "as-if committed" view of
    /// the staged chain. See [`Transaction::<&Branch>::query`].
    pub fn query(&self) -> TransactionQuery<'_> {
        // The batch owns its snapshot, so nothing else moves it: pinning
        // it per query reads the chain tip.
        TransactionQuery::new(Live::Snapshot(&self.line.snapshot).pin(), self.layers())
    }
}

impl TransactionCommit<&Branch> {
    /// Run induction, then STAGE the commit: mint it on the branch's
    /// own origin — the successor edition of the head, exactly what a
    /// published commit would mint — without moving the branch head.
    /// The returned [`TransactionBatch`] chains further commits and
    /// publishes the lot with one CAS.
    ///
    /// Staging needs no publish capability; only
    /// [`TransactionBatch::publish`] does.
    pub async fn perform<Env>(self, env: &Env) -> Result<TransactionBatch, CommitError>
    where
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let branch = self.line;

        // The head the transaction pinned when it opened: what its
        // queries read, what the mint builds on, and the publish CAS
        // target. If anything advanced the cell since, publish fails
        // rather than silently adopting a chain built on a superseded
        // head.
        let base_version = self.pinned.as_ref().map(|edition| edition.version.clone());
        let head = branch.revision.checkpoint_at(base_version.clone());
        let cell = branch.revision.clone();
        let base = self.pinned.map(|edition| edition.content);

        // A head that moved since the transaction opened makes the chain
        // stale before it is staged: its publish would lose the CAS, so
        // refuse here, before settling, inducing and minting a link
        // nobody can publish. The CAS at publish stays the guarantee;
        // this is only the early word.
        if !branch.revision.is_at(base_version.as_ref()) {
            return Err(PublishError::VersionMismatch {
                expected: base_version,
                current: Current::Unknown,
            }
            .into());
        }

        // Through a stack with durable layers, the layers were read where
        // the primary pointed them when the transaction opened. A primary
        // the transaction pinned elsewhere than where they were read
        // would have its commit point the layers at trees built on stale
        // ones: they are read again, and the transaction refused as if
        // its head had moved, so the caller runs it again on fresh ones.
        if let Some(stack) = &self.stack
            && stack.pinned.at != base.as_ref().map(|head| head.tree.clone())
            && !stack.state.layers().is_empty()
        {
            stack.state.refresh(base.as_ref(), env).await?;
            return Err(PublishError::VersionMismatch {
                expected: base_version,
                current: Current::Unknown,
            }
            .into());
        }

        // The line staged commits mint on: the branch's own identity,
        // derived once here (memoized) so the batch's snapshot can carry
        // it for the links that follow.
        let authority = Identify.perform(env).await?;
        let (line, _) = branch.commit_identity(authority.profile(), &authority.did());
        let overlay = branch.overlay().clone();
        let retractions = overlay_retractions(&self.changes, &overlay);

        // The layers a write named for them lands in once the commit
        // publishes: the stack's, and the branch's own session overlay
        // as `session:`.
        let revisions = self.stack.as_ref().map(|stack| stack.branch.clone());
        let mut memories = self
            .stack
            .as_ref()
            .map(|stack| stack.memories.clone())
            .unwrap_or_default();
        memories.push((session_layer(), overlay.clone()));
        let channels = self.stack.as_ref().map(|stack| stack.channels.clone());
        let pinned = self
            .stack
            .as_ref()
            .map(|stack| (stack.state.clone(), stack.pinned.clone()));
        let (outcome, induced, shares, durables, dispatched) = Box::pin(mint_link(
            View::at(Live::Branch(branch), base.as_ref()),
            |profile, issuer| branch.commit_identity(profile, issuer),
            self.changes,
            self.transients,
            self.allow_empty,
            self.canonicalize,
            None,
            self.stack,
            Vec::new(),
            env,
        ))
        .await?;

        // Version-keyed memos are batch-local (see the field docs on
        // `TransactionBatch::snapshot`); content-keyed caches stay
        // shared with the branch. The batch-local ones are sized by the
        // budget the branch's caches were built with.
        let entries = branch.caches().limits.entry_capacity();
        let caches = Caches {
            causality: CausalityCache::with_capacity(entries),
            contexts: ContextCache::with_capacity(entries),
            records: Cache::with_capacity(entries),
            ..branch.caches()
        };

        Ok(match outcome {
            Outcome::Unchanged(tip) => TransactionBatch {
                snapshot: Snapshot::staged(branch.subject(), tip, caches, line),
                head,
                cell,
                base_version,
                induction: branch.induction_cell().clone(),
                chain: Vec::new(),
                context: None,
                records: branch.records(),
                contexts: branch.contexts(),
                induced,
                shares,
                memories,
                overlay,
                overlay_retractions: retractions,
                revisions,
                durables,
                pinned,
                dispatched,
                channels,
            },
            Outcome::Minted(minted) => {
                let Minted {
                    revision,
                    record,
                    context,
                } = *minted;
                let snapshot = Snapshot::staged(branch.subject(), revision.clone(), caches, line);
                let version = revision.version();
                snapshot.caches().records.insert(version, record.clone());
                if let Some(context) = &context {
                    snapshot.caches().contexts.insert(version, context.clone());
                }
                TransactionBatch {
                    snapshot,
                    head,
                    cell,
                    base_version,
                    induction: branch.induction_cell().clone(),
                    chain: vec![(version, record)],
                    context,
                    records: branch.records(),
                    contexts: branch.contexts(),
                    induced,
                    shares,
                    memories,
                    overlay,
                    overlay_retractions: retractions,
                    revisions,
                    durables,
                    pinned,
                    dispatched,
                    channels,
                }
            }
        })
    }
}

impl TransactionCommit<TransactionBatch> {
    /// Run induction, then extend the staged chain by one more commit,
    /// minted on the same line at the next edition. See
    /// [`TransactionCommit::<&Branch>::perform`].
    pub async fn perform<Env>(self, env: &Env) -> Result<TransactionBatch, CommitError>
    where
        Env: Provider<BlobSize>
            + Provider<BlobImport>
            + Provider<Get>
            + Provider<BlobRead>
            + Provider<Put>
            + Provider<Import>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<dialog_corpus::Preload>
            + Provider<dialog_corpus::Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static,
    {
        let mut batch = self.line;
        let (base, lineage) = batch.snapshot.head();
        let line = lineage.expect("a transaction batch's line is seeded at construction");

        // Amend only a staged revision: the chain's tip, which nobody has
        // seen. A batch with nothing staged sits on the published head,
        // which is never amended; its amend is an ordinary commit.
        let amend = match (self.amend, batch.chain.last()) {
            (true, Some((_, record))) => Some(Amended {
                record: record.clone(),
                context: batch.context.clone(),
            }),
            _ => None,
        };
        let amending = amend.is_some();
        let retractions = overlay_retractions(&self.changes, &batch.overlay);
        if batch.revisions.is_none() {
            batch.revisions = self.stack.as_ref().map(|stack| stack.branch.clone());
        }
        if batch.channels.is_none() {
            batch.channels = self.stack.as_ref().map(|stack| stack.channels.clone());
        }
        if batch.pinned.is_none() {
            batch.pinned = self
                .stack
                .as_ref()
                .map(|stack| (stack.state.clone(), stack.pinned.clone()));
        }
        let staged = take(&mut batch.durables);

        let (outcome, induced, shares, durables, dispatched) = Box::pin(mint_link(
            View::at(Live::Snapshot(&batch.snapshot), Some(&base)),
            |_, issuer| (line.clone(), origin_of(&line, issuer)),
            self.changes,
            self.transients,
            self.allow_empty,
            self.canonicalize,
            amend,
            self.stack,
            staged,
            env,
        ))
        .await?;
        batch.durables = durables;
        induced.assert(&mut batch.induced);
        batch.dispatched.merge(dispatched);
        batch.shares.extend(shares);
        batch.overlay_retractions.extend(retractions);

        if let Outcome::Minted(minted) = outcome {
            let Minted {
                revision,
                record,
                context,
            } = *minted;
            batch.snapshot.advance(&base, revision.clone(), line)?;
            let version = revision.version();
            batch
                .snapshot
                .caches()
                .records
                .insert(version, record.clone());
            if let Some(context) = &context {
                batch
                    .snapshot
                    .caches()
                    .contexts
                    .insert(version, context.clone());
            }
            if amending {
                // The amended tip keeps its version and record; the chain
                // holds it once, as it would a single commit.
                batch.chain.pop();
            }
            batch.chain.push((version, record));
            batch.context = context;
        }
        Ok(batch)
    }
}

/// Fold `more` shares into `shares`, layer by layer.
fn merge_shares(shares: &mut Vec<(Entity, Changes)>, more: Vec<(Entity, Changes)>) {
    for (layer, changes) in more {
        match shares.iter_mut().find(|(held, _)| *held == layer) {
            Some((_, held)) => held.merge(changes),
            None => shares.push((layer, changes)),
        }
    }
}

/// Land each tick's share of each layer in it, tick by tick in the order
/// the ticks ran: a layer mints one instant per tick that changed it, so
/// a reader of its instants sees every state the rules saw. A share of a
/// layer the stack does not hold was refused at commit, so none is
/// left over here.
fn apply_shares(
    memories: &[(Entity, Ephemeral)],
    ticks: Vec<Vec<(Entity, Changes)>>,
) -> Result<(), CommitError> {
    for shares in ticks {
        for (layer, changes) in shares {
            if let Some((_, memory)) = memories.iter().find(|(held, _)| *held == layer) {
                memory.apply(changes)?;
            }
        }
    }
    Ok(())
}

/// Record in the stack's own layer, `overlay`, the revision of every
/// layer of the stack that moved, once `tip` has published and the
/// dispatch's writes have landed on `channels`: one row under `last`
/// for each layer whose revision is not the one `overlay` holds,
/// replacing the row it held, as one instant; a publish after which no
/// layer stands anywhere new writes nothing and mints no instant. The
/// primary's row is `branch`'s `dialog.branch/revision`; every memory
/// layer's and every channel's, its `dialog.layer/revision`.
///
/// The stack's own row names the revision `overlay` stood at before
/// the records that followed its last change: once this dispatch's
/// writes into it have landed, the revision they left it at, and for a
/// dispatch that wrote nothing into it, the one it already stood at.
/// The recording is written as a record ([`Ephemeral::record`]), which
/// does not move that revision, so recording is never itself a move of
/// the stack's own layer, and a dispatch that wrote nothing into
/// `session:` and moved no other layer records nothing.
fn record_revisions(
    branch: &Entity,
    memories: &[(Entity, Ephemeral)],
    channels: &[Channel],
    overlay: &Ephemeral,
    tip: &Revision,
) -> Result<(), CommitError> {
    let recorded = Recorded {
        branch,
        primary: Some(tip),
        memories,
        channels,
        own: overlay,
    };
    overlay.record(recorded.rows())?;
    Ok(())
}

/// Route `shares`, the writes named for a layer, to where each lands:
/// one named for `memory:` is the primary's and comes back as the second
/// value; one named for a layer among `held` goes to it; one named for
/// any other layer is refused.
fn route_shares(
    shares: Vec<(Entity, Changes)>,
    held: &[Entity],
) -> Result<(Vec<(Entity, Changes)>, Changes), CommitError> {
    let mut routed = Vec::new();
    let mut primary = Changes::new();
    let memory = memory_layer();
    for (layer, share) in shares {
        if layer == memory {
            primary.merge(share);
        } else if held.contains(&layer) {
            routed.push((layer, share));
        } else {
            let attribute = share
                .iter()
                .next()
                .map(|(_, the, _)| the.to_string())
                .unwrap_or_default();
            return Err(CommitError::UnboundLayer {
                attribute,
                layer: layer.to_string(),
            });
        }
    }
    Ok((routed, primary))
}

/// NaN is not a value: a commit that would write one is refused before
/// anything lands, as deserialization and the rule compiler refuse it.
fn refuse_nan<'a>(changes: impl Iterator<Item = &'a Change>) -> Result<(), CommitError> {
    for change in changes {
        if let Change::Assert(dialog_corpus::Value::Float(float), _) = change
            && float.is_nan()
        {
            return Err(dialog_corpus::DialogCorpusError::InvalidValue(
                "NaN is not a value".into(),
            )
            .into());
        }
    }
    Ok(())
}

/// Mint one staged link on `view`, the line at the revision the link
/// builds on: run commit-time induction over the batch, then [`Mint`]
/// the settled changes on the line `line` names, carrying the trigger
/// footprint forward. Nothing is published or adopted — the caller owns
/// what happens to the [`Outcome`], to the transients induction emitted,
/// and to the shares, returned alongside it.
///
/// Through a `stack`, settlement and induction read the stack's
/// composite, and the writes placed outside the primary, the ones the
/// rules derived included, are taken out before anything is settled or
/// minted and returned as the shares, tick by tick: the link's own
/// writes first, then each induction round's that wrote a layer. The
/// tree never holds a fact placed on a memory layer.
#[allow(clippy::too_many_arguments)]
async fn mint_link<Env>(
    view: View<'_>,
    line: impl FnOnce(&Did, &Did) -> (Entity, Origin),
    changes: Staged,
    transients: Changes,
    allow_empty: bool,
    canonicalize: bool,
    amend: Option<Amended>,
    stack: Option<StackContext>,
    staged: Vec<(Entity, Revision)>,
    env: &Env,
) -> Result<Link, CommitError>
where
    Env: Provider<BlobSize>
        + Provider<BlobImport>
        + Provider<Get>
        + Provider<BlobRead>
        + Provider<Put>
        + Provider<Import>
        + Provider<nodes::Write<CorpusNode>>
        + ConditionalSync
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<ArchiveLoad>
        + Provider<nodes::Write<CorpusNode>>
        + ConditionalSync
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + ConditionalSync
        + Provider<BlobLoad>
        + Provider<dialog_corpus::Preload>
        + Provider<dialog_corpus::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
        + 'static,
{
    refuse_nan(changes.log().iter().map(|(_, _, change)| change))?;
    let operator = Identify.perform(env).await?;
    let composite = match &stack {
        Some(stack) => stack.composite.clone(),
        None => Composite::of(view.to_source()),
    };
    // The layers a write named for them (`in:`) can land in: the
    // stack's, and the line's own session overlay as `session:`. A
    // write named for any other is refused before anything is settled
    // or minted; one named for `memory:` is the primary's.
    let held: Vec<Entity> = stack
        .iter()
        .flat_map(|stack| {
            stack
                .memories
                .iter()
                .map(|(layer, _)| layer.clone())
                .chain(stack.durables.iter().map(|durable| durable.layer.clone()))
        })
        .chain(once(session_layer()))
        .collect();
    let mut changes = changes;
    let mut transients = transients;
    let mut shares = changes.take_shares();
    // Through a stack, the layer its own layer selects takes the
    // writes named for no layer, but the machinery under `dialog.`,
    // before anything is settled: the primary never settles a cell that
    // will not land in it. The selected layer is routed as any named
    // one, so one the stack does not hold refuses the commit.
    let selection = stack
        .as_ref()
        .and_then(|_| selected(view.to_source().overlay()))
        .filter(|layer| *layer != memory_layer());
    if let Some(layer) = &selection {
        let moved = changes.take_cells(follows_selection);
        merge_shares(&mut shares, moved.into_layer(layer.clone()).take_shares());
    }
    // A write into a channel is a dispatch: the first tick's stimulus,
    // read as a row of the channel, landing in no layer. One into the
    // loopback, `control:`, is bound for no layer, as a dispatched
    // command is.
    let control = control_layer();
    let channels: Vec<Entity> = stack
        .iter()
        .flat_map(|stack| stack.channels.iter().map(|channel| channel.entity().clone()))
        .collect();
    let shares: Vec<(Entity, Changes)> = shares
        .into_iter()
        .filter_map(|(layer, share)| {
            if layer == control {
                transients.merge(share);
                None
            } else if channels.contains(&layer) {
                transients.merge(share.into_layer(layer));
                None
            } else {
                Some((layer, share))
            }
        })
        .collect();
    let (shares, primary) = route_shares(shares, &held)?;
    changes.apply(primary);
    // The link's commands and its writes into channels, before the rows
    // the channels' outside ends sent and the stack's revision rows join
    // its stimulus: what its publish lands on the stack's channels.
    let mut dispatched = transients.clone();
    let looped = take_dispatched(&mut dispatched);
    dispatched.merge(looped);
    // Through a stack, the revision of every layer that moved since the
    // stack last recorded it is the dispatch's stimulus, read as rows of
    // `session:`: a rule premised on a layer's revision fires when the
    // layer moved. A recording does not move the stack's own layer, so
    // its row is stimulus only when something else wrote into it since.
    if let Some(stack) = &stack {
        let source = view.to_source();
        let recorded = Recorded {
            branch: &stack.branch,
            primary: view.revision(),
            memories: &stack.memories,
            channels: &stack.channels,
            own: source.overlay(),
        };
        transients.merge(recorded.stimulus().into_layer(session_layer()));
        transients.merge(stack.received.clone());
    }
    let mut changes = Box::pin(succession::settle(
        composite.clone(),
        shared_metadata(from_ref(&view), &operator),
        &changes,
        env,
    ))
    .await?;
    // The link's own writes into layers are its first tick: the rounds
    // read each layer as that tick leaves it, and they are stimulus of
    // the first round, as the link's writes into the primary are.
    for (layer, share) in &shares {
        if *layer == session_layer() {
            let source = view.to_source();
            changes.merge(succession_in_own(source.overlay(), share).into_layer(layer.clone()));
        }
        changes.merge(share.clone().into_layer(layer.clone()));
    }
    let (induced, rounds) = induce(
        &view,
        &composite,
        &mut changes,
        transients,
        selection.as_ref(),
        env,
    )
    .await?;
    // The link's own writes into layers are its first tick; what the
    // rules derived into a layer follows, one tick per round that wrote
    // one, routed the same way. Induction left every round's writes in
    // `changes` too, merged, for the rounds to read: they land tick by
    // tick instead, so the merged copy is dropped.
    changes.take_shares();
    let mut ticks = Vec::with_capacity(1 + rounds.len());
    if !shares.is_empty() {
        ticks.push(shares);
    }
    for round in rounds {
        let (derived, primary) = route_shares(round, &held)?;
        changes.merge(primary);
        if !derived.is_empty() {
            ticks.push(derived);
        }
    }
    let durables = match &stack {
        Some(stack) => {
            Box::pin(mint_durables(
                &stack.durables,
                &mut ticks,
                &mut changes,
                staged,
                &operator,
                env,
            ))
            .await?
        }
        None => staged,
    };
    let touches = touches_rules(&changes);
    let touches_rule_facts = touches_rule_facts(&changes);
    let previous = view.revision().cloned();
    let machinery = store_assets(&view, changes.take_assets(), env).await?;
    let rule_cache = view.rule_cache();
    let outcome = Mint {
        view,
        changes: changes.into_stream(),
        entries: Vec::new(),
        machinery,
        scope: WriteScope::Application,
        allow_empty,
        canonicalize,
        amend,
        rebuild: None,
    }
    .perform(env, line)
    .await?;
    if let Outcome::Minted(minted) = &outcome {
        if !touches {
            carry_footprint(&rule_cache, previous.as_ref(), &minted.revision);
        }
        if !touches_rule_facts {
            rule_cache.carry_rules_root(
                previous.as_ref().map(|revision| *revision.tree.hash()),
                Some(*minted.revision.tree.hash()),
            );
        }
    }
    Ok((outcome, induced, ticks, durables, dispatched))
}

/// What [`mint_link`] mints: the link's outcome on the primary, the
/// transients its rules emitted, its writes into memory layers tick by
/// tick, the trees it minted for durable layers, by layer, and the
/// commands it dispatched itself.
type Link = (
    Outcome,
    Changes,
    Vec<Vec<(Entity, Changes)>>,
    Vec<(Entity, Revision)>,
    Changes,
);

/// Take the writes into `durables` out of `ticks` and mint each
/// layer's, every tick of them together, as the layer's next tree: on
/// the tree an earlier link of the chain minted for it (`staged`), or on
/// the one the primary pointed it at when the dispatch began. The writes
/// are settled against the layer as a commit on it would settle them.
/// Each layer a write moved gets its pointer facts in `changes`, the
/// primary's writes, naming the tree it was minted at. Answers the minted
/// revisions, by layer, `staged` among them. Nothing is published: the
/// primary's commit is what names them.
async fn mint_durables<Env>(
    durables: &[DurableLayer],
    ticks: &mut Vec<Vec<(Entity, Changes)>>,
    changes: &mut Changes,
    mut staged: Vec<(Entity, Revision)>,
    operator: &Capability<Operator>,
    env: &Env,
) -> Result<Vec<(Entity, Revision)>, CommitError>
where
    Env: Provider<BlobSize>
        + Provider<BlobImport>
        + Provider<Get>
        + Provider<BlobRead>
        + Provider<Put>
        + Provider<Import>
        + Provider<nodes::Write<CorpusNode>>
        + ConditionalSync
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<ArchiveLoad>
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + Provider<BlobLoad>
        + Provider<dialog_corpus::Preload>
        + Provider<dialog_corpus::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
        + 'static,
{
    if durables.is_empty() {
        return Ok(staged);
    }
    let mut writes: Vec<(Entity, Changes)> = Vec::new();
    for tick in ticks.iter_mut() {
        let (into, rest): (Vec<_>, Vec<_>) = take(tick)
            .into_iter()
            .partition(|(layer, _)| durables.iter().any(|durable| durable.layer == *layer));
        *tick = rest;
        merge_shares(&mut writes, into);
    }
    ticks.retain(|tick| !tick.is_empty());
    for (layer, share) in writes {
        let Some(durable) = durables.iter().find(|durable| durable.layer == layer) else {
            continue;
        };
        let at = staged.iter().position(|(held, _)| *held == layer);
        let base = match at {
            Some(at) => Some(staged.remove(at).1),
            None => durable.revision.clone(),
        };
        let view = durable.view_at(base.as_ref());
        let settled = Box::pin(succession::settle(
            Composite::of(view.to_source()),
            shared_metadata(from_ref(&view), operator),
            &Staged::from(share),
            env,
        ))
        .await?;
        drop(view);
        let revision = match Box::pin(durable.mint(base.as_ref(), settled, env)).await? {
            Some(minted) => {
                Pointer {
                    layer: &layer,
                    revision: &minted,
                }
                .assert(changes);
                minted
            }
            None => match base {
                Some(base) => base,
                None => continue,
            },
        };
        staged.push((layer, revision));
    }
    Ok(staged)
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use crate::{Branch, CommitError, PublishError};
    use anyhow::Result;
    use dialog_corpus::history::Edition;
    use dialog_peer::Peer;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::query::Output;
    use dialog_query::{Concept, Entity, Query, Term};
    use dialog_storage::provider::storage::VolatileSpace;

    pub mod note {
        #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Body(pub String);
    }

    #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Note {
        pub this: Entity,
        pub body: note::Body,
    }

    fn note(body: &str) -> Result<Note> {
        Ok(Note {
            this: Entity::new()?,
            body: note::Body(body.into()),
        })
    }

    async fn bodies(
        branch: &Branch,
        operator: &Peer<VolatileSpace, dialog_peer::Session>,
    ) -> Result<Vec<String>> {
        let mut bodies: Vec<String> = branch
            .query()
            .select(Query::<Note> {
                this: Term::var("this"),
                body: Term::var("body"),
            })
            .perform(operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|note| note.body.0)
            .collect();
        bodies.sort();
        Ok(bodies)
    }

    /// A staged chain is invisible on the branch until publish, then
    /// every link lands atomically at exactly the versions minted.
    #[dialog_common::test]
    async fn it_stages_privately_and_publishes_atomically() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let seed = branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let batch = branch
            .transaction()
            .assert(note("first")?)
            .commit()
            .perform(&operator)
            .await?;

        // The staged commit minted on the branch's own line, at the
        // head's successor edition — yet the branch head did not move.
        assert_eq!(
            batch.version().origin,
            seed.version().origin,
            "staged commits mint on the branch's own origin"
        );
        assert_eq!(batch.version().edition, seed.version().edition.successor());
        assert_eq!(branch.revision(), Some(seed.clone()));
        assert_eq!(bodies(&branch, &operator).await?, vec!["seed"]);

        // Chain a second link; the batch reads its own staged state.
        let batch = batch
            .assert(note("second")?)
            .commit()
            .perform(&operator)
            .await?;
        assert_eq!(
            batch.version().edition,
            seed.version().edition.successor().successor()
        );

        let tip = batch.revision();
        let head = batch.publish().perform(&operator).await?;
        assert_eq!(head, tip, "publish moves the head to the chain tip");
        assert_eq!(branch.revision(), Some(head));
        assert_eq!(
            bodies(&branch, &operator).await?,
            vec!["first", "second", "seed"]
        );

        Ok(())
    }

    /// The tonk flow: commit claims, read the minted version off the
    /// batch, assert a fact naming it in the next link, publish both
    /// atomically. The recorded version is exactly the one the branch
    /// history reports for the commit carrying the claims.
    #[dialog_common::test]
    async fn it_captures_the_version_a_commit_minted() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let batch = branch
            .transaction()
            .assert(note("claim")?)
            .commit()
            .perform(&operator)
            .await?;
        let version = batch.version();

        batch
            .assert(note(&format!("committed-at:{version}"))?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // The capture fact is on the branch, naming the version the
        // history actually holds for the claim commit.
        let recorded = bodies(&branch, &operator).await?;
        assert!(
            recorded.contains(&format!("committed-at:{version}")),
            "the capture fact names the minted version: {recorded:?}"
        );
        let log = branch.log(&operator, 4).await?;
        assert!(
            log.iter().any(|(recorded, _)| *recorded == version),
            "the branch history holds the captured version"
        );

        Ok(())
    }

    /// A batch whose base head moved loses the publish CAS loudly; a
    /// re-run against the fresh head reconciles.
    #[dialog_common::test]
    async fn it_fails_publish_after_the_head_moved_then_recovers() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let batch = branch
            .transaction()
            .assert(note("staged")?)
            .commit()
            .perform(&operator)
            .await?;

        // Another writer publishes while the batch is staged.
        branch
            .transaction()
            .assert(note("interloper")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let raced = batch.publish().perform(&operator).await;
        assert!(
            matches!(
                raced,
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
            ),
            "a batch staged on a superseded head must lose the CAS; got {raced:?}"
        );

        // Recovery: re-run against the fresh head.
        branch
            .transaction()
            .assert(note("staged")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            bodies(&branch, &operator).await?,
            vec!["interloper", "seed", "staged"]
        );

        Ok(())
    }

    /// A batch publishes while another writer has left the watermark behind
    /// its base: the head lands, and the watermark is not dragged forward
    /// from where the other writer left it, since it stands neither at the
    /// base nor at no revision. A watermark left behind is caught up at the
    /// next inducing instant.
    #[dialog_common::test]
    async fn it_publishes_a_batch_past_a_watermark_left_behind() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let genesis = branch
            .transaction()
            .assert(note("genesis")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let batch = branch
            .transaction()
            .assert(note("staged")?)
            .commit()
            .perform(&operator)
            .await?;
        let tip = batch.revision();

        // Another writer moves the watermark back to `genesis`.
        let other = repo.branch("main").induction();
        other.resolve().perform(&operator).await?;
        other.publish(genesis.clone()).perform(&operator).await?;

        let head = batch.publish().perform(&operator).await?;
        assert_eq!(head, tip);
        assert_eq!(branch.revision(), Some(tip));

        let watermark = repo.branch("main").induction();
        watermark.resolve().perform(&operator).await?;
        assert_eq!(watermark.content(), Some(genesis));
        Ok(())
    }

    /// Two writers over one replica: the first lands `tip` on `seed`, and
    /// before it advances the watermark, the second lands `tip2` on `tip`
    /// and advances the watermark to `tip2`. The first writer's advance
    /// loses its compare-and-set against a cell it last saw at `seed`; it
    /// does not fail, and leaves the watermark at `tip2`, past its own
    /// head.
    #[dialog_common::test]
    async fn it_leaves_a_watermark_another_writer_moved_past_the_tip() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let seed = branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        // The first writer's view of the watermark, read before the moves.
        let stale = repo.branch("main").induction();
        stale.resolve().perform(&operator).await?;
        assert_eq!(stale.content(), Some(seed.clone()));

        let tip = branch
            .transaction()
            .assert(note("tip")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        // The second writer, through a handle of its own, lands `tip2` on
        // `tip` and advances the watermark to it.
        let other = repo.branch("main").open().perform(&operator).await?;
        let tip2 = other
            .transaction()
            .assert(note("tip2")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let standing = repo.branch("main").induction();
        standing.resolve().perform(&operator).await?;
        assert_eq!(
            standing.content(),
            Some(tip2.clone()),
            "the other writer moved it on"
        );

        super::advance_watermark(&stale, &tip, Some(seed.version()), &operator).await?;

        let watermark = repo.branch("main").induction();
        watermark.resolve().perform(&operator).await?;
        assert_eq!(watermark.content(), Some(tip2));
        Ok(())
    }

    /// A writer whose view of the watermark is behind its head's base loses
    /// its first compare-and-set to another writer that advanced the
    /// watermark to that base. Read again, the cell stands at the base, so
    /// the writer publishes again and lands the watermark at its tip.
    #[dialog_common::test]
    async fn it_advances_a_watermark_another_writer_left_at_the_base() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let seed = branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        // The writer's view of the watermark, read while it stood at `seed`.
        let stale = repo.branch("main").induction();
        stale.resolve().perform(&operator).await?;
        assert_eq!(stale.content(), Some(seed));

        // Another writer lands `base` and advances the watermark to it.
        let base = branch
            .transaction()
            .assert(note("base")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        // The writer's own head, built on `base`.
        let tip = branch
            .transaction()
            .assert(note("tip")?)
            .commit()
            .perform(&operator)
            .await?
            .revision();

        let standing = repo.branch("main").induction();
        standing.resolve().perform(&operator).await?;
        assert_eq!(
            standing.content(),
            Some(base.clone()),
            "the other writer left it at the base"
        );

        super::advance_watermark(&stale, &tip, Some(base.version()), &operator).await?;

        let watermark = repo.branch("main").induction();
        watermark.resolve().perform(&operator).await?;
        assert_eq!(watermark.content(), Some(tip));
        Ok(())
    }

    /// A virgin branch stages a genesis chain: nothing exists on the
    /// branch until the batch publishes its first-ever head.
    #[dialog_common::test]
    async fn it_stages_a_genesis_chain() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        assert_eq!(branch.revision(), None);

        let batch = branch
            .transaction()
            .assert(note("genesis")?)
            .commit()
            .perform(&operator)
            .await?;
        assert_eq!(batch.version().edition, Edition::GENESIS);
        assert_eq!(
            branch.revision(),
            None,
            "staging establishes no head on a virgin branch"
        );

        let head = batch.publish().perform(&operator).await?;
        assert_eq!(branch.revision(), Some(head));
        assert_eq!(bodies(&branch, &operator).await?, vec!["genesis"]);

        Ok(())
    }

    /// An all-no-op batch publishes nothing — but does not report
    /// success from a stale view either.
    #[dialog_common::test]
    async fn it_treats_an_empty_batch_as_a_noop_only_at_the_current_head() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let seed = branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // No changes staged: the batch sits at the head it was staged
        // from and publish is a no-op returning it.
        let batch = branch.transaction().commit().perform(&operator).await?;
        assert_eq!(batch.version(), seed.version());
        assert_eq!(batch.publish().perform(&operator).await?, seed);
        assert_eq!(branch.revision(), Some(seed.clone()));

        // The same no-op judged against a superseded head fails loudly.
        let stale = branch.transaction().commit().perform(&operator).await?;
        branch
            .transaction()
            .assert(note("interloper")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let raced = stale.publish().perform(&operator).await;
        assert!(
            matches!(
                raced,
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
            ),
            "an empty batch from a stale view must not silently succeed; got {raced:?}"
        );

        Ok(())
    }

    /// An amend folds into the staged tip: the tip keeps its version, and
    /// publishing makes both links visible at that one version.
    #[dialog_common::test]
    async fn it_amends_the_staged_tip_in_place() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let seed = branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let batch = branch
            .transaction()
            .assert(note("first")?)
            .commit()
            .perform(&operator)
            .await?;
        let staged = batch.version();

        let batch = batch
            .assert(note("second")?)
            .commit()
            .amend()
            .perform(&operator)
            .await?;
        assert_eq!(batch.version(), staged, "the amend keeps the tip's version");
        assert_eq!(batch.version().edition, seed.version().edition.successor());
        assert_eq!(branch.revision(), Some(seed), "nothing is published yet");

        let head = batch.publish().perform(&operator).await?;
        assert_eq!(head.version(), staged, "one version is published");
        assert_eq!(
            bodies(&branch, &operator).await?,
            vec!["first", "second", "seed"]
        );
        Ok(())
    }

    /// Committing and then amending records what one commit of the same
    /// changes records: canonicalized, the two trees are the same tree.
    #[dialog_common::test]
    async fn it_amends_to_the_tree_one_commit_would_have() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let (first, second) = (note("first")?, note("second")?);
        let whole = branch
            .transaction()
            .assert(first.clone())
            .assert(second.clone())
            .commit()
            .canonicalize()
            .perform(&operator)
            .await?;
        let amended = branch
            .transaction()
            .assert(first)
            .commit()
            .perform(&operator)
            .await?
            .assert(second)
            .commit()
            .amend()
            .canonicalize()
            .perform(&operator)
            .await?;

        assert_eq!(whole.version(), amended.version());
        assert_eq!(
            whole.revision().tree,
            amended.revision().tree,
            "one commit and a commit plus an amend make the same tree"
        );
        Ok(())
    }

    /// History recorded by an amend folds into what the amended commit
    /// already recorded at the same history key, exactly as two writes of
    /// one commit fold.
    ///
    /// Replacing a published value records a claim citing the version that
    /// published it; retracting the replacement in an amend writes the same
    /// history key and must keep that citation, as retracting it in the
    /// same commit does, rather than overwrite it with a record citing
    /// nothing.
    #[dialog_common::test]
    async fn it_folds_history_across_amends() -> Result<()> {
        use dialog_corpus::{Changes, Relation, Update as _, Value};

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let the: Relation = "note/body".parse()?;
        let of = dialog_corpus::Entity::new()?;
        let replace = |value: &str| {
            let mut changes = Changes::new();
            changes.associate(
                the.clone(),
                of.clone(),
                Value::String(value.into()),
                dialog_corpus::Pick::Last,
            );
            changes
        };
        let retract = |value: &str| {
            let mut changes = Changes::new();
            changes.dissociate(the.clone(), of.clone(), Value::String(value.into()));
            changes
        };

        branch
            .transaction()
            .integrate(replace("a"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let mut both = replace("b");
        both.dissociate(the.clone(), of.clone(), Value::String("b".into()));
        let whole = branch
            .transaction()
            .integrate(both)
            .commit()
            .canonicalize()
            .perform(&operator)
            .await?;
        let amended = branch
            .transaction()
            .integrate(replace("b"))
            .commit()
            .perform(&operator)
            .await?
            .transaction()
            .integrate(retract("b"))
            .commit()
            .amend()
            .canonicalize()
            .perform(&operator)
            .await?;

        assert_eq!(whole.version(), amended.version());
        assert_eq!(
            whole.revision().tree,
            amended.revision().tree,
            "history folds across amends as it folds within a commit"
        );
        Ok(())
    }

    /// A chain canonicalized only at its last link reaches the canonical
    /// tree even when that link changes nothing: the canonicalize rides the
    /// amend, which replaces the tip's tree in place, so it is never dropped
    /// with the no-op.
    #[dialog_common::test]
    async fn it_canonicalizes_an_amend_that_changes_nothing() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // The same two links on the same head: the chains mint one
        // version, and differ only in when they canonicalized. The second
        // link retracts a fact the chain never held, so it changes nothing.
        let (seed, absent) = (note("seed")?, note("absent")?);
        let every_link = branch
            .transaction()
            .assert(seed.clone())
            .commit()
            .canonicalize()
            .perform(&operator)
            .await?
            .retract(absent.clone())
            .commit()
            .amend()
            .canonicalize()
            .perform(&operator)
            .await?;
        let last_link = branch
            .transaction()
            .assert(seed)
            .commit()
            .perform(&operator)
            .await?
            .retract(absent)
            .commit()
            .amend()
            .canonicalize()
            .perform(&operator)
            .await?;

        assert_eq!(every_link.version(), last_link.version());
        assert_eq!(
            every_link.revision().tree,
            last_link.revision().tree,
            "a chain canonicalized at its last link reaches the canonical tree \
             whether or not that link changed anything"
        );
        Ok(())
    }

    /// An amend that changes nothing leaves the line's live spine held, keyed
    /// by the tip's root, so the next link appends to it instead of opening
    /// the tip's root frame from bytes again.
    #[dialog_common::test]
    async fn it_keeps_the_spine_across_an_amend_that_changes_nothing() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let staged = branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .perform(&operator)
            .await?;
        let root = dialog_common::Blake3Hash::from(*staged.revision().tree.hash());
        assert_eq!(branch.spine().root(), Some(root.clone()));

        let amended = staged
            .retract(note("absent")?)
            .commit()
            .amend()
            .perform(&operator)
            .await?;
        assert_eq!(
            dialog_common::Blake3Hash::from(*amended.revision().tree.hash()),
            root,
            "an amend that changes nothing keeps the tip"
        );
        assert_eq!(
            branch.spine().root(),
            Some(root),
            "the spine is held for the next link"
        );
        Ok(())
    }

    /// A batch with nothing staged sits on the published head, which is
    /// never amended: its amend commits a new revision instead.
    #[dialog_common::test]
    async fn it_commits_an_amend_with_nothing_staged() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let seed = branch
            .transaction()
            .assert(note("seed")?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let empty = branch.transaction().commit().perform(&operator).await?;
        assert_eq!(empty.revision(), seed, "an empty commit stages nothing");

        let batch = empty
            .assert(note("next")?)
            .commit()
            .amend()
            .perform(&operator)
            .await?;
        assert_eq!(
            batch.version().edition,
            seed.version().edition.successor(),
            "the published head is not amended; the amend commits"
        );
        Ok(())
    }
}
