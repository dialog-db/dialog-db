//! A transaction through a stack that the embedder builds itself: the
//! commit an [`Invocation`](super::invoke::Invocation) runs, without the
//! re-run from a moved head, reporting what its publish did as an
//! invocation does.
//!
//! [`Stack::transaction`] opens a [`StackTransaction`]. It reads and
//! writes as a branch [`Transaction`] does, through every layer of the
//! stack, and commits the same way: [`commit`](StackTransaction::commit)
//! stages a [`StackBatch`], which chains further links and publishes the
//! lot with one compare-and-swap, and
//! [`commit().publish()`](StackCommit::publish) does both in one step.
//! What differs is what its publish answers: an [`InvocationReport`],
//! naming the head the publish left the primary at, the ticks its
//! induction ran and the commands it landed on
//! [`control:`](crate::CONTROL), whose [`effect`](InvocationReport::effect)
//! runs the stack's providers over them. A branch transaction's publish
//! answers the [`Revision`] alone.

use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_corpus::history::Version;
use dialog_corpus::{Changes, Entity, Statement};
use dialog_effects::memory::{Publish, Resolve};
use dialog_query::query::Application;

use super::effect::is_lifecycle;
use super::invoke::InvocationReport;
use crate::{
    Branch, CommitError, Line, QueryLayer, Revision, SelectQuery, SnapshotClaims, Stack, StackEnv,
    Transaction, TransactionBatch, TransactionCommit, TransactionQuery,
};

/// A transaction through a [`Stack`], opened by [`Stack::transaction`],
/// or by [`StackBatch::transaction`] for the next link of a staged
/// chain.
///
/// It reads every layer of the stack ([`query`](Self::query)) and
/// accumulates writes and commands as a branch [`Transaction`] does: a
/// write lands in the layer its descriptor names, and a command, one
/// [`dispatch`](Self::dispatch)ed or written `.at(control_layer())`,
/// is a row of [`control:`](crate::CONTROL). Its commit runs induction
/// over the stack, and its publish, once it has won its
/// compare-and-swap, lands the commands on `control:` and reports them
/// in an [`InvocationReport`]. A publish that loses it fails with
/// [`PublishError::VersionMismatch`](crate::PublishError::VersionMismatch),
/// lands nothing and is not re-run: the caller resolved against the head
/// it chose, and opens a new transaction on the fresh one.
#[must_use = "a transaction writes nothing until it is committed and published"]
pub struct StackTransaction<L: Line> {
    transaction: Transaction<L>,
    stack: Stack,
}

/// Command committing a [`StackTransaction`]: `.perform(&env)` stages a
/// [`StackBatch`], and [`publish`](Self::publish) publishes it in the
/// same step. The stack-bound [`TransactionCommit`].
pub struct StackCommit<L: Line> {
    commit: TransactionCommit<L>,
    stack: Stack,
}

/// Command committing a [`StackTransaction`] and publishing it in one
/// step, created by [`StackCommit::publish`]. Answers an
/// [`InvocationReport`].
pub struct StackPublish<L: Line> {
    commit: StackCommit<L>,
}

/// A staged chain of commits through a [`Stack`]: a [`TransactionBatch`]
/// whose publish reports like an invocation. Produced by committing a
/// [`StackTransaction`]; [`transaction`](Self::transaction) opens the
/// next link and [`publish`](Self::publish) publishes the chain.
#[must_use = "staged commits are invisible until .publish().perform(&env) moves the branch head"]
pub struct StackBatch {
    batch: TransactionBatch,
    stack: Stack,
}

/// Command publishing a [`StackBatch`], created by
/// [`StackBatch::publish`]. Answers an [`InvocationReport`].
pub struct StackBatchPublish {
    batch: StackBatch,
}

impl Stack {
    /// Start a transaction through the stack. Its reads see every
    /// layer; its commit routes each write to the layer its descriptor
    /// names (`in:`), the primary with none named, and the memory layers
    /// take their shares once the primary's commit publishes.
    ///
    /// Its publish reports like an invocation: once it has won, the
    /// commands it dispatched (with [`dispatch`](StackTransaction::dispatch),
    /// or written `.at(control_layer())`) and the ones its rules
    /// dispatched land in the stack's [`CONTROL`](crate::CONTROL) for one
    /// tick, as [`Invocation::perform`](super::invoke::Invocation::perform)
    /// lands them, wake every [`Arrivals`](super::Arrivals) waiting, and
    /// are named by the [`InvocationReport`] the publish answers, whose
    /// [`effect`](InvocationReport::effect) runs the stack's providers
    /// over them. That holds for the one-step
    /// `.commit().publish().perform(&env)` and for a staged chain,
    /// `.commit().perform(&env)` then the batch's
    /// [`publish`](StackBatch::publish), whose report names the commands
    /// of every link. Unlike an invocation it is not re-run when the head
    /// moved under it: its publish fails with
    /// [`PublishError::VersionMismatch`](crate::PublishError::VersionMismatch)
    /// and lands nothing, and the caller runs it again.
    ///
    /// ```no_run
    /// # use dialog_repository::{Stack, StackEnv, control_layer};
    /// # use dialog_corpus::Statement as _;
    /// # async fn example(
    /// #     stack: Stack,
    /// #     env: impl StackEnv + Clone + dialog_common::ConditionalSend,
    /// # ) -> anyhow::Result<()> {
    /// let doc: dialog_corpus::Entity = "doc:1".parse()?;
    /// let report = stack
    ///     .transaction()
    ///     .assert(dialog_query::the!("doc/title").of(doc.clone()).is("Notes".to_string()))
    ///     .assert(dialog_query::the!("doc/index").of(doc).is(true).at(control_layer()))
    ///     .commit()
    ///     .publish()
    ///     .perform(&env)
    ///     .await?;
    /// let revision = report.revision();
    /// // The commands it landed, run through the stack's providers.
    /// report.effect().perform(&env).await?;
    /// # let _ = revision;
    /// # Ok(())
    /// # }
    /// ```
    pub fn transaction(&self) -> StackTransaction<&Branch> {
        StackTransaction {
            transaction: self.receiving(Changes::new()),
            stack: self.clone(),
        }
    }
}

impl<L: Line> StackTransaction<L> {
    /// Assert a claim into this transaction, into the layer it names
    /// (with [`Statement::at`]), the primary with none named.
    pub fn assert<C: Statement>(mut self, claim: C) -> Self {
        self.transaction = self.transaction.assert(claim);
        self
    }

    /// Retract a claim from this transaction.
    pub fn retract<C: Statement>(mut self, claim: C) -> Self {
        self.transaction = self.transaction.retract(claim);
        self
    }

    /// Dispatch a claim as a command: a write into
    /// [`control:`](crate::CONTROL). See [`Transaction::dispatch`].
    pub fn dispatch<C: Statement>(mut self, claim: C) -> Self {
        self.transaction = self.transaction.dispatch(claim);
        self
    }

    /// Integrate an external [`Changes`] batch into this transaction.
    /// See [`Transaction::integrate`].
    pub fn integrate(mut self, changes: Changes) -> Self {
        self.transaction = self.transaction.integrate(changes);
        self
    }

    /// Finalize the transaction into a commit command. See
    /// [`Transaction::commit`].
    pub fn commit(self) -> StackCommit<L> {
        StackCommit {
            commit: self.transaction.commit(),
            stack: self.stack,
        }
    }
}

impl StackTransaction<&Branch> {
    /// Run queries against this transaction's "as-if committed" view of
    /// the stack, at the head the transaction opened at. See
    /// [`Transaction::<&Branch>::query`].
    pub fn query(&self) -> TransactionQuery<'_> {
        self.transaction.query()
    }
}

impl StackTransaction<TransactionBatch> {
    /// Run queries against this transaction's "as-if committed" view of
    /// the staged chain. See [`Transaction::<TransactionBatch>::query`].
    pub fn query(&self) -> TransactionQuery<'_> {
        self.transaction.query()
    }
}

impl<L: Line> StackCommit<L> {
    /// Mint a revision even when the settled change batch leaves the
    /// indexes untouched. See [`TransactionCommit::allow_empty`].
    pub fn allow_empty(mut self) -> Self {
        self.commit = self.commit.allow_empty();
        self
    }

    /// Flush write buffers to the leaves before publishing. See
    /// [`TransactionCommit::canonicalize`].
    pub fn canonicalize(mut self) -> Self {
        self.commit = self.commit.canonicalize();
        self
    }

    /// Commit and publish in one step, answering an
    /// [`InvocationReport`].
    pub fn publish(self) -> StackPublish<L> {
        StackPublish { commit: self }
    }
}

impl StackCommit<TransactionBatch> {
    /// Fold this link into the chain's tip instead of staging a new
    /// commit after it. See [`TransactionCommit::<TransactionBatch>::amend`].
    pub fn amend(mut self) -> Self {
        self.commit = self.commit.amend();
        self
    }
}

impl StackCommit<&Branch> {
    /// Run induction over the stack, then STAGE the commit without
    /// moving the primary's head. See
    /// [`TransactionCommit::<&Branch>::perform`].
    pub async fn perform<Env: StackEnv>(self, env: &Env) -> Result<StackBatch, CommitError> {
        let batch = Box::pin(self.commit.perform(env)).await?;
        Ok(StackBatch {
            batch,
            stack: self.stack,
        })
    }
}

impl StackCommit<TransactionBatch> {
    /// Run induction, then extend the staged chain by one more commit.
    /// See [`TransactionCommit::<TransactionBatch>::perform`].
    pub async fn perform<Env: StackEnv>(self, env: &Env) -> Result<StackBatch, CommitError> {
        let batch = Box::pin(self.commit.perform(env)).await?;
        Ok(StackBatch {
            batch,
            stack: self.stack,
        })
    }
}

impl StackPublish<&Branch> {
    /// Run induction, mint the commit and publish it, answering the
    /// [`InvocationReport`] of the publish. See
    /// [`StackBatchPublish::perform`].
    pub async fn perform<Env: StackEnv>(self, env: &Env) -> Result<InvocationReport, CommitError> {
        let batch = Box::pin(self.commit.perform(env)).await?;
        batch.publish().perform(env).await
    }
}

impl StackPublish<TransactionBatch> {
    /// Commit the link, then publish the whole staged chain, answering
    /// the [`InvocationReport`] of the publish. See
    /// [`StackBatchPublish::perform`].
    pub async fn perform<Env: StackEnv>(self, env: &Env) -> Result<InvocationReport, CommitError> {
        let batch = Box::pin(self.commit.perform(env)).await?;
        batch.publish().perform(env).await
    }
}

impl StackBatch {
    /// The [`Version`] of the chain tip. See [`TransactionBatch::version`].
    pub fn version(&self) -> Version {
        self.batch.version()
    }

    /// The chain tip: the [`Revision`] publish will move the primary's
    /// head to.
    pub fn revision(&self) -> Revision {
        self.batch.revision()
    }

    /// The transient facts inductive rules concluded while this chain
    /// was staged. See [`TransactionBatch::induced`].
    pub fn induced(&self) -> &Changes {
        self.batch.induced()
    }

    /// The writes placed outside the primary that this chain's commits
    /// made, tick by tick. See [`TransactionBatch::shares`].
    pub fn shares(&self) -> &[Vec<(Entity, Changes)>] {
        self.batch.shares()
    }

    /// How many ticks the chain ran, every link's induction rounds
    /// summed: what the report of its publish counts.
    #[cfg(test)]
    pub(crate) fn ticks(&self) -> u32 {
        self.batch.ticks()
    }

    /// Start the next transaction on this chain. Its commit extends the
    /// batch by one more staged revision.
    pub fn transaction(self) -> StackTransaction<TransactionBatch> {
        StackTransaction {
            transaction: self.batch.transaction(),
            stack: self.stack,
        }
    }

    /// Assert a claim into the next transaction on this chain.
    /// Shorthand for `.transaction().assert(claim)`.
    pub fn assert<C: Statement>(self, claim: C) -> StackTransaction<TransactionBatch> {
        self.transaction().assert(claim)
    }

    /// Retract a claim in the next transaction on this chain.
    /// Shorthand for `.transaction().retract(claim)`.
    pub fn retract<C: Statement>(self, claim: C) -> StackTransaction<TransactionBatch> {
        self.transaction().retract(claim)
    }

    /// Dispatch a command into the next transaction on this chain.
    /// Shorthand for `.transaction().dispatch(claim)`.
    pub fn dispatch<C: Statement>(self, claim: C) -> StackTransaction<TransactionBatch> {
        self.transaction().dispatch(claim)
    }

    /// The staged fact index, serving the chain tip.
    pub fn claims(&self) -> SnapshotClaims<'_> {
        self.batch.claims()
    }

    /// Query the staged state with an application.
    pub fn select<Q: Application>(&self, query: Q) -> SelectQuery<'_, Q> {
        self.batch.select(query)
    }

    /// Open a query over the staged state.
    pub fn query(&self) -> QueryLayer<'_> {
        self.batch.query()
    }

    /// Publish the staged chain: one compare-and-swap moving the
    /// primary's head to the chain tip. See
    /// [`StackBatchPublish::perform`].
    pub fn publish(self) -> StackBatchPublish {
        StackBatchPublish { batch: self }
    }
}

impl StackBatchPublish {
    /// Publish the staged chain as [`BatchPublish::perform`](crate::BatchPublish::perform)
    /// does, and report it as an invocation: the head the publish left
    /// the primary at, the ticks the chain's induction ran, every link's
    /// summed, and the commands it landed on [`control:`](crate::CONTROL),
    /// those its transactions dispatched and those its rules dispatched,
    /// every link's. Run the stack's providers over them with
    /// [`InvocationReport::effect`].
    ///
    /// A publish that loses its compare-and-swap fails with
    /// [`PublishError::VersionMismatch`](crate::PublishError::VersionMismatch),
    /// lands nothing and reports nothing.
    pub async fn perform<Env>(self, env: &Env) -> Result<InvocationReport, CommitError>
    where
        Env: Provider<Publish> + Provider<Resolve> + ConditionalSync,
    {
        let StackBatch { batch, stack } = self.batch;
        InvocationReport::publish(stack, batch, env).await
    }
}

impl InvocationReport {
    /// Publish `batch`, a chain committed through `stack`, and report
    /// what the publish did: the head it left the primary at, the ticks
    /// the chain ran, and the commands and the tick's own rows it landed
    /// on [`control:`](crate::CONTROL), their lifecycle rows left out. A
    /// lost compare-and-swap reports nothing.
    pub(crate) async fn publish<Env>(
        stack: Stack,
        batch: TransactionBatch,
        env: &Env,
    ) -> Result<Self, CommitError>
    where
        Env: Provider<Publish> + Provider<Resolve> + ConditionalSync,
    {
        let ticks = batch.ticks();
        let landed = batch
            .looped()
            .into_iter()
            .filter(|fact| !is_lifecycle(&fact.the))
            .collect();
        let revision = batch.publish().perform(env).await?;
        Ok(Self::published(stack, revision, ticks, landed))
    }
}
