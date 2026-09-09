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

use super::induce::induce;
use super::{Transaction, TransactionCommit, carry_footprint, touches_rules, transaction_view};
use crate::placement::{Partitioned, Placements};
use crate::repository::branch::commit::{Mint, Minted, Outcome};
use crate::repository::branch::session::Composite;
use crate::repository::source::{Caches, SourceRef};
use crate::{
    Branch, Cell, Checkpoint, CommitError, PublishError, QueryLayer, RemoteSite, Revision,
    SelectQuery, Snapshot, SnapshotClaims, TransactionQuery, origin_of,
};
use dialog_artifacts::history::{CausalityCache, Context, ContextCache, RevisionRecord, Version};
use dialog_artifacts::tree::WriteScope;
use dialog_artifacts::{Changes, Entity, Statement};
use dialog_capability::history::Origin;
use dialog_capability::{Did, Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, OperatorExt as _};
use dialog_effects::memory::{Publish, Resolve, Version as MemoryVersion};
use dialog_query::query::Application;
use dialog_search_tree::Cache;

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
    context: Option<Context>,
    /// The branch's shared memos, seeded at publish.
    records: Cache<Version, RevisionRecord>,
    contexts: ContextCache,
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

    /// The staged artifact index, serving the chain tip.
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
        } = self.batch;
        let tip = snapshot.revision();

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
                    actual,
                }
                .into());
            }
            if induction.content().as_ref() != Some(&tip) {
                induction.publish(tip.clone()).perform(env).await?;
            }
            return Ok(tip);
        }

        head.publish(tip.clone(), env).await?;

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
        if induction.content().as_ref() != Some(&tip) {
            induction.publish(tip.clone()).perform(env).await?;
        }

        Ok(tip)
    }
}

/// Command committing a branch transaction (or a chain link) and
/// publishing in one step — created by [`TransactionCommit::publish`],
/// executed with `.perform(&env)`. The one-shot form: equivalent to
/// staging the batch and immediately publishing it.
pub struct TransactionPublish<Line> {
    commit: TransactionCommit<Line>,
}

impl<'a> TransactionCommit<&'a Branch> {
    /// Commit and publish in one step: stage the batch, then move the
    /// branch head to it. The one-shot write path.
    pub fn publish(self) -> TransactionPublish<&'a Branch> {
        TransactionPublish { commit: self }
    }
}

impl TransactionCommit<TransactionBatch> {
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
        let batch = Box::pin(self.commit.perform(env)).await?;
        batch.publish().perform(env).await
    }
}

impl TransactionPublish<TransactionBatch> {
    /// Commit the link, then publish the whole staged chain — returning
    /// the newly-published [`Revision`].
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
        let batch = Box::pin(self.commit.perform(env)).await?;
        batch.publish().perform(env).await
    }
}

impl Transaction<TransactionBatch> {
    /// Run queries against this transaction's "as-if committed" view of
    /// the staged chain. See [`Transaction::<&Branch>::query`].
    pub fn query(&self) -> TransactionQuery<'_> {
        TransactionQuery::new(
            SourceRef::Snapshot(&self.line.snapshot),
            &transaction_view(&self.changes, &self.transients),
        )
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
        let branch = self.line;

        // The publish CAS target, captured before the mint reads the
        // head it builds on: if anything advances the cell from here,
        // publish fails rather than silently adopting a chain built on
        // a superseded head.
        let head = branch.revision.checkpoint();
        let cell = branch.revision.clone();
        let base_version = branch.revision.edition().map(|edition| edition.version);
        let base = branch.revision();

        // The line staged commits mint on: the branch's own identity,
        // derived once here (memoized) so the batch's snapshot can carry
        // it for the links that follow.
        let authority = Identify.perform(env).await?;
        let (line, _) = branch.commit_identity(authority.profile(), &authority.did());

        let outcome = Box::pin(mint_link(
            SourceRef::Branch(branch),
            base,
            |profile, issuer| branch.commit_identity(profile, issuer),
            self.changes,
            self.transients,
            self.allow_empty,
            self.canonicalize,
            env,
        ))
        .await?;

        // Version-keyed memos are batch-local (see the field docs on
        // `TransactionBatch::snapshot`); content-keyed caches stay
        // shared with the branch.
        let caches = Caches {
            causality: CausalityCache::new(),
            contexts: ContextCache::new(),
            records: Cache::new(),
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
                snapshot.caches().contexts.insert(version, context.clone());
                TransactionBatch {
                    snapshot,
                    head,
                    cell,
                    base_version,
                    induction: branch.induction_cell().clone(),
                    chain: vec![(version, record)],
                    context: Some(context),
                    records: branch.records(),
                    contexts: branch.contexts(),
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
        let mut batch = self.line;
        let (base, lineage) = batch.snapshot.head();
        let line = lineage.expect("a transaction batch's line is seeded at construction");

        let outcome = Box::pin(mint_link(
            SourceRef::Snapshot(&batch.snapshot),
            Some(base.clone()),
            |_, issuer| (line.clone(), origin_of(&line, issuer)),
            self.changes,
            self.transients,
            self.allow_empty,
            self.canonicalize,
            env,
        ))
        .await?;

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
            batch
                .snapshot
                .caches()
                .contexts
                .insert(version, context.clone());
            batch.chain.push((version, record));
            batch.context = Some(context);
        }
        Ok(batch)
    }
}

/// Mint one staged link: run commit-time induction over the batch, then
/// [`Mint`] the settled changes on the line `line` names, carrying the
/// trigger footprint forward. Nothing is published or adopted — the
/// caller owns what happens to the [`Outcome`].
#[allow(clippy::too_many_arguments)]
async fn mint_link<Env>(
    source: SourceRef<'_>,
    base: Option<Revision>,
    line: impl FnOnce(&Did, &Did) -> (Entity, Origin),
    changes: Changes,
    transients: Changes,
    allow_empty: bool,
    canonicalize: bool,
    env: &Env,
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
    let view = Composite::of(source.to_source());
    let changes = settle(source, &view, changes, transients, env).await?;
    mint(source, base, line, changes, allow_empty, canonicalize, env).await
}

/// Settle a transaction's changes against `view`: run commit-time
/// induction, then route by attribute placement. Session-bound
/// instructions land in `source`'s ephemeral store now; the tree-bound
/// remainder is returned for minting.
pub(crate) async fn settle<Env>(
    source: SourceRef<'_>,
    view: &Composite,
    mut changes: Changes,
    transients: Changes,
    env: &Env,
) -> Result<Changes, CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    induce(source, view, &mut changes, transients, env).await?;
    let placements = Placements::resolve(source, &changes, env).await?;
    let Partitioned {
        tree: changes,
        session,
    } = placements.partition(changes, source.bindings())?;
    source.overlay().apply(session);
    Ok(changes)
}

/// Mint one link from already-settled, tree-bound changes on `base`,
/// carrying the trigger footprint forward when the changes touch no
/// rule structure.
async fn mint<Env>(
    source: SourceRef<'_>,
    base: Option<Revision>,
    line: impl FnOnce(&Did, &Did) -> (Entity, Origin),
    changes: Changes,
    allow_empty: bool,
    canonicalize: bool,
    env: &Env,
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
    let touches = touches_rules(&changes);
    let previous = base.clone();
    let outcome = Mint {
        source,
        base,
        changes: changes.into_stream(),
        entries: Vec::new(),
        scope: WriteScope::Application,
        allow_empty,
        canonicalize,
    }
    .perform(env, line)
    .await?;
    if let Outcome::Minted(minted) = &outcome
        && !touches
    {
        carry_footprint(&source.rule_cache(), previous.as_ref(), &minted.revision);
    }
    Ok(outcome)
}

impl TransactionBatch {
    /// Stage a batch on `branch` from an explicit base rather than the
    /// handle's head: the first link is minted from already-settled
    /// changes on `base`, and publish will CAS against `base_version`.
    /// What a [`Stack`](crate::Stack) opens for a line at the head it
    /// captured, so a handle that moved since the capture cannot make
    /// the publish overwrite the move.
    pub(crate) async fn stage<Env>(
        branch: &Branch,
        base: Option<Revision>,
        base_version: Option<MemoryVersion>,
        changes: Changes,
        env: &Env,
    ) -> Result<TransactionBatch, CommitError>
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
        let head = branch.revision_cell().checkpoint_at(base_version.clone());
        let cell = branch.revision_cell().clone();

        let authority = Identify.perform(env).await?;
        let (line, _) = branch.commit_identity(authority.profile(), &authority.did());

        let pinned = base.clone();
        let outcome = Box::pin(mint(
            SourceRef::Pinned(branch, pinned.as_ref()),
            base,
            |profile, issuer| branch.commit_identity(profile, issuer),
            changes,
            false,
            false,
            env,
        ))
        .await?;

        let caches = Caches {
            causality: CausalityCache::new(),
            contexts: ContextCache::new(),
            records: Cache::new(),
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
                snapshot.caches().contexts.insert(version, context.clone());
                TransactionBatch {
                    snapshot,
                    head,
                    cell,
                    base_version,
                    induction: branch.induction_cell().clone(),
                    chain: vec![(version, record)],
                    context: Some(context),
                    records: branch.records(),
                    contexts: branch.contexts(),
                }
            }
        })
    }

    /// Extend the chain by one link minted from already-settled
    /// changes: no induction, no routing.
    pub(crate) async fn mint<Env>(
        &mut self,
        changes: Changes,
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
        let (base, lineage) = self.snapshot.head();
        let line = lineage.expect("a transaction batch's line is seeded at construction");
        let outcome = Box::pin(mint(
            SourceRef::Snapshot(&self.snapshot),
            Some(base.clone()),
            |_, issuer| (line.clone(), origin_of(&line, issuer)),
            changes,
            false,
            false,
            env,
        ))
        .await?;
        if let Outcome::Minted(minted) = outcome {
            let Minted {
                revision,
                record,
                context,
            } = *minted;
            self.snapshot.advance(&base, revision.clone(), line)?;
            let version = revision.version();
            self.snapshot
                .caches()
                .records
                .insert(version, record.clone());
            self.snapshot
                .caches()
                .contexts
                .insert(version, context.clone());
            self.chain.push((version, record));
            self.context = Some(context);
        }
        Ok(self.snapshot.revision())
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use crate::{Branch, CommitError, PublishError};
    use anyhow::Result;
    use dialog_artifacts::history::Edition;
    use dialog_operator::Operator;
    use dialog_operator::helpers::test_operator_with_profile;
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

    async fn bodies(branch: &Branch, operator: &Operator<VolatileSpace>) -> Result<Vec<String>> {
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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

    /// A virgin branch stages a genesis chain: nothing exists on the
    /// branch until the batch publishes its first-ever head.
    #[dialog_common::test]
    async fn it_stages_a_genesis_chain() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
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
        let (operator, profile) = test_operator_with_profile().await;
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
}
