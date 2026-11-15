//! Driving a stack: an invocation runs one **dispatch**, the unit of
//! publication, re-run from the new head when it loses its
//! compare-and-swap. A dispatch takes what the outside ends of the
//! stack's channels sent as its first tick's stimulus, with the commands
//! the invocation sends, and once it has published, what it wrote into a
//! channel, its commands among them, lands on that channel's outside end
//! for one tick. Its [`InvocationReport`] names those commands, and
//! [`InvocationReport::effect`] is the next, separate step: running the
//! stack's providers over them (see [`effect`](super::effect)).
//!
//! An invocation is built by [`Stack::invoke`], for a command,
//! [`Stack::transact`], for a body that reads the stack and stages
//! writes against what it read, or [`Stack::assert`], for writes alone.

use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::future::Future;
use std::mem::take;
use std::pin::Pin;
use std::sync::Arc;

use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_corpus::{Changes, Fact, Statement};
use parking_lot::Mutex;

use super::effect::{Effect, is_lifecycle};
use crate::placement::take_dispatched;
use crate::{Channel, CommitError, PublishError, QueryLayer, Revision, Stack, StackEnv};

/// How many times a dispatch is re-run from a moved head before the
/// mismatch is reported. A run is a function of the head and the
/// stimulus, so each re-run is as sound as the first; the cap guards
/// against a head that moves faster than this writer can follow.
pub const MAX_RERUNS: u32 = 8;

/// The future one run of a [`Stack::transact`] body returns.
#[cfg(not(target_arch = "wasm32"))]
type Attempted = Pin<Box<dyn Future<Output = Result<(), CommitError>> + Send>>;
/// The future one run of a [`Stack::transact`] body returns.
#[cfg(target_arch = "wasm32")]
type Attempted = Pin<Box<dyn Future<Output = Result<(), CommitError>>>>;

/// A [`Stack::transact`] body, run once per attempt.
#[cfg(not(target_arch = "wasm32"))]
type Body = Arc<dyn Fn(Attempt) -> Attempted + Send + Sync>;
/// A [`Stack::transact`] body, run once per attempt.
#[cfg(target_arch = "wasm32")]
type Body = Arc<dyn Fn(Attempt) -> Attempted>;

/// Command driving one dispatch through a [`Stack`]. Built by
/// [`Stack::invoke`], [`Stack::transact`] or [`Stack::assert`]; run with
/// [`perform`](Self::perform).
///
/// A dispatch is one unit of publication. The commands and the writes
/// beside them are the stimulus of a transaction through the stack: its
/// commit runs induction to a fixed point over the primary and every
/// layer, routes each write to the layer its attribute is placed on,
/// and publishes the primary's share with one compare-and-swap against
/// the head the dispatch began at. A dispatch that writes only to memory
/// layers, or only invokes, publishes nothing. When the head has moved,
/// the whole dispatch is re-run from the new head with the original
/// stimulus, up to [`MAX_RERUNS`] times, and a [`Stack::transact`] body
/// is run again first, reading the stack at the new head.
///
/// A command is a row of [`control:`](crate::CONTROL), the built-in
/// [`Channel`]: one [`invoke`](Self::invoke)d, sent into the loopback's
/// outside end as the dispatch starts, one written
/// `.at(control_layer())`, and one a rule's head lands there in any
/// tick. Each is the stimulus of the tick after the one that wrote it
/// (the first tick, for one sent), marked there by its
/// `dialog.command/invoked` row (see [`effect`](super::effect)), and
/// lands nowhere. Once the dispatch has published, and only then, its
/// commands land on the loopback's outside end as one tick: what the
/// [`InvocationReport`] names, what [`InvocationReport::effect`] runs
/// the stack's providers over, and what [`Stack::arrivals`] reads, so a
/// lost compare-and-swap re-runs no effect.
///
/// The dispatch also takes what the outside end of every other channel
/// sent ([`Channel::send`]) before it started: its first tick reads each
/// such row as a row of its channel, and no later tick does. What it
/// writes into such a channel lands on that channel's outside end once
/// it has published, as a command lands on the loopback's.
#[derive(Clone)]
pub struct Invocation {
    stack: Stack,
    changes: Changes,
    sent: Changes,
    body: Option<Body>,
}

/// One run of a [`Stack::transact`] body: what it reads the stack
/// through and stages its writes and commands on. The body is handed a
/// fresh one per attempt, so what an attempt that lost its head staged
/// is never published. Cheap to clone: clones stage into the same
/// attempt.
#[derive(Clone)]
pub struct Attempt {
    stack: Stack,
    number: u32,
    staged: Arc<Mutex<Staged>>,
}

/// What an [`Attempt`] staged.
#[derive(Default)]
struct Staged {
    changes: Changes,
    sent: Changes,
}

/// What one [`Invocation`] did: the head it left the primary at, the
/// ticks its dispatch ran and the commands it landed on the outside end
/// of [`control:`](crate::CONTROL). Its [`effect`](Self::effect) runs
/// the stack's providers over those commands, as a separate step.
#[derive(Clone)]
pub struct InvocationReport {
    stack: Stack,
    revision: Revision,
    ticks: u32,
    commands: Vec<Fact>,
}

impl Stack {
    /// Invoke `command` through the stack: send it into the loopback,
    /// [`control:`](crate::CONTROL), and run a dispatch, which takes it
    /// with whatever else the stack's channels were sent. See
    /// [`Invocation`].
    pub fn invoke<C: Statement>(&self, command: C) -> Invocation {
        self.invocation().invoke(command)
    }

    /// Write `facts` through the stack with no command and nothing read
    /// first: an import, a seed, a test fixture. Shorthand for a
    /// [`transact`](Self::transact) whose body asserts `facts` alone.
    /// See [`Invocation`].
    pub fn assert<S: Statement>(&self, facts: S) -> Invocation {
        self.invocation().assert(facts)
    }

    /// Transact through the stack: run `body` against an [`Attempt`],
    /// which reads the stack at the head the attempt begins at
    /// ([`Attempt::query`]) and stages the writes and commands the body
    /// decides on from what it read ([`Attempt::assert`],
    /// [`Attempt::retract`], [`Attempt::invoke`]), then publish them as
    /// one dispatch. When the publish loses its compare-and-swap, `body`
    /// is run again on a fresh attempt, reading the stack at the new
    /// head, and what the lost attempt staged is dropped: a write
    /// computed from a read is published only against the head it read.
    /// A body that fails fails the invocation, and nothing is written.
    ///
    /// ```no_run
    /// # use dialog_repository::{Stack, StackEnv};
    /// # use dialog_query::{AttributeQuery, Term};
    /// # use dialog_query::attribute::Relation;
    /// # use dialog_query::query::Output as _;
    /// # use dialog_corpus::{Entity, Value};
    /// # async fn example<Env: StackEnv + Clone + dialog_common::ConditionalSend>(stack: Stack, env: Env) -> anyhow::Result<()> {
    /// let counter: Entity = "counter:1".parse()?;
    /// let reader = env.clone();
    /// let report = stack
    ///     .transact(move |tx| {
    ///         let (env, counter) = (reader.clone(), counter.clone());
    ///         async move {
    ///             let value = AttributeQuery::from(
    ///                 Term::<Relation>::from(Relation::from(
    ///                     "counter/value".parse::<dialog_corpus::Relation>().expect("an attribute"),
    ///                 ))
    ///                 .of(Term::<Entity>::from(counter.clone()))
    ///                 .is(Term::<u128>::var("value")),
    ///             );
    ///             let rows = tx.query().select(value).perform(&env).try_vec().await?;
    ///             let n = match rows.first().map(|row| &row.is) {
    ///                 Some(Value::UnsignedInt(n)) => *n,
    ///                 _ => 0,
    ///             };
    ///             tx.assert(dialog_query::the!("counter/value").of(counter).is(n + 1));
    ///             Ok(())
    ///         }
    ///     })
    ///     .perform(&env)
    ///     .await?;
    /// // The commands it landed, run through the stack's providers.
    /// report.effect().perform(&env).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn transact<F, Fut>(&self, body: F) -> Invocation
    where
        F: Fn(Attempt) -> Fut + ConditionalSend + ConditionalSync + 'static,
        Fut: Future<Output = Result<(), CommitError>> + ConditionalSend + 'static,
    {
        let mut invocation = self.invocation();
        invocation.body = Some(Arc::new(move |attempt| -> Attempted {
            Box::pin(body(attempt))
        }));
        invocation
    }

    /// An invocation with nothing in it yet.
    pub(crate) fn invocation(&self) -> Invocation {
        Invocation {
            stack: self.clone(),
            changes: Changes::new(),
            sent: Changes::new(),
            body: None,
        }
    }

    /// Read again the head of the primary, and where it points every
    /// durable layer: what a dispatch re-run after a lost
    /// compare-and-swap builds on.
    pub(crate) async fn refresh<Env: StackEnv>(&self, env: &Env) -> Result<(), CommitError> {
        self.primary().refresh(env).await?;
        self.repoint(env).await
    }
}

impl Attempt {
    /// Which run of the body this is: 1 for the first, one more for each
    /// re-run from a moved head.
    pub fn number(&self) -> u32 {
        self.number
    }

    /// The stack this attempt runs through.
    pub fn stack(&self) -> &Stack {
        &self.stack
    }

    /// A query over the stack, as [`Stack::query`] reads it: the primary
    /// at the head this attempt begins at, with every layer, each
    /// durable one where that head points it. What the attempt has
    /// staged is not read.
    pub fn query(&self) -> QueryLayer<'_> {
        self.stack.query()
    }

    /// Assert `claim` in this attempt, into the layer it names (with
    /// [`Statement::at`]), the primary or the selected layer with none
    /// named. Chainable.
    pub fn assert<C: Statement>(&self, claim: C) -> &Self {
        claim.assert(&mut self.staged.lock().changes);
        self
    }

    /// Retract `claim` in this attempt. Chainable.
    pub fn retract<C: Statement>(&self, claim: C) -> &Self {
        claim.retract(&mut self.staged.lock().changes);
        self
    }

    /// Invoke `command` beside this attempt's writes: sent into the
    /// loopback, [`control:`](crate::CONTROL), as [`Stack::invoke`]
    /// sends its own. Chainable.
    pub fn invoke<C: Statement>(&self, command: C) -> &Self {
        command.assert(&mut self.staged.lock().sent);
        self
    }

    /// What the attempt staged: its writes and the commands it sent.
    fn take(&self) -> (Changes, Changes) {
        let mut staged = self.staged.lock();
        (take(&mut staged.changes), take(&mut staged.sent))
    }
}

impl Invocation {
    /// Assert `claim` beside the command.
    pub fn assert<C: Statement>(mut self, claim: C) -> Self {
        claim.assert(&mut self.changes);
        self
    }

    /// Retract `claim` beside the command.
    pub fn retract<C: Statement>(mut self, claim: C) -> Self {
        claim.retract(&mut self.changes);
        self
    }

    /// Invoke `command` too, in the same tick: sent into the loopback,
    /// [`control:`](crate::CONTROL), as [`Stack::invoke`] sends its own.
    pub fn invoke<C: Statement>(mut self, command: C) -> Self {
        command.assert(&mut self.sent);
        self
    }

    /// Run the dispatch: take what the stack's channels were sent, this
    /// invocation's commands with what was sent into the loopback, run
    /// the [`transact`](Stack::transact) body if there is one, then
    /// commit and publish, re-running from the new head (the body first)
    /// when the publish loses its compare-and-swap, and land what it
    /// wrote into each channel on its outside end for one tick. Returns
    /// the [`InvocationReport`]: the head the dispatch left the primary
    /// at (the revision it published, or the unchanged head when nothing
    /// durable was written), the ticks it ran and the commands it
    /// landed. No provider runs here: that is
    /// [`InvocationReport::effect`].
    ///
    /// A dispatch that fails puts back what the channels' outside ends
    /// sent, for the next dispatch to take; its own commands are not
    /// put back.
    pub async fn perform<Env: StackEnv>(self, env: &Env) -> Result<InvocationReport, CommitError> {
        let Invocation {
            stack,
            changes,
            sent,
            body,
        } = self;
        let channels = stack.channels();
        let taken: Vec<(Channel, Changes)> = channels
            .iter()
            .map(|channel| (channel.clone(), channel.take(Changes::new())))
            .collect();
        let mut transients = Changes::new();
        let mut received = Changes::new();
        for (at, (channel, rows)) in taken.iter().enumerate() {
            if at == 0 {
                transients.merge(rows.clone());
            } else {
                received.merge(rows.clone().into_layer(channel.entity().clone()));
            }
        }
        transients.merge(sent);
        let outcome = Box::pin(publish(
            &stack,
            body.as_ref(),
            &changes,
            &transients,
            &received,
            env,
        ))
        .await;
        if outcome.is_err() {
            for (channel, rows) in taken {
                if !rows.is_empty() {
                    channel.restore(rows);
                }
            }
        }
        outcome
    }
}

impl InvocationReport {
    /// A report of no dispatch: the primary left at `revision`, no tick
    /// run and nothing landed.
    pub(crate) fn unmoved(stack: Stack, revision: Revision) -> Self {
        Self {
            stack,
            revision,
            ticks: 0,
            commands: Vec::new(),
        }
    }

    /// The head the dispatch left the primary at: the revision it
    /// published, or the unchanged head when nothing durable was written.
    pub fn revision(&self) -> &Revision {
        &self.revision
    }

    /// How many ticks the dispatch ran, in the attempt that published:
    /// the rounds of induction its commit ran to a fixed point. A
    /// dispatch whose writes and commands fired nothing ran one; one with
    /// nothing to read ran none.
    pub fn ticks(&self) -> u32 {
        self.ticks
    }

    /// The rows the dispatch landed on the outside end of
    /// [`control:`](crate::CONTROL), grouped by command entity: the
    /// commands it sent and the ones its rules dispatched, what the
    /// stack's providers read. A command's lifecycle rows
    /// (`dialog.command/*`, see [`effect`](super::effect)) are no command
    /// and are left out.
    pub fn commands(&self) -> &[Fact] {
        &self.commands
    }

    /// The next step: the stack's providers over the commands this
    /// dispatch landed. Nothing runs until it is performed
    /// ([`Effect::perform`]), so an embedder can return before it and
    /// perform it later.
    pub fn effect(&self) -> Effect {
        Effect::new(
            self.stack.clone(),
            self.revision.clone(),
            self.commands.clone(),
        )
    }
}

impl Debug for InvocationReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("InvocationReport")
            .field("revision", &self.revision)
            .field("ticks", &self.ticks)
            .field("commands", &self.commands)
            .finish()
    }
}

/// One dispatch of `changes`, `transients` and `received`, the rows the
/// channels other than the loopback were sent, through `stack`, with
/// `body` run first on each attempt: commit and publish, re-run from the
/// new head when the publish loses its compare-and-swap, the primary's,
/// which is the durable layers' too. Reports the head it left the
/// primary at, the ticks the winning attempt ran and the commands it
/// landed. The publish that wins lands the dispatch's commands in the
/// stack's `control:`, as every publish of a transaction through the
/// stack does.
async fn publish<Env: StackEnv>(
    stack: &Stack,
    body: Option<&Body>,
    changes: &Changes,
    transients: &Changes,
    received: &Changes,
    env: &Env,
) -> Result<InvocationReport, CommitError> {
    let mut reruns = 0;
    // The durable layers are read where the primary points them now: a
    // commit through another handle may have moved it since the stack
    // last read them.
    stack.repoint(env).await?;
    loop {
        // The transaction pins the head before the body reads: a body
        // reads that head or a later one, and either way the publish
        // compares against the pinned one, so a write computed from a
        // read never lands on a head the read did not see.
        let transaction = stack.receiving(received.clone());
        let mut changes = changes.clone();
        let mut transients = transients.clone();
        if let Some(body) = body {
            let attempt = Attempt {
                stack: stack.clone(),
                number: reruns + 1,
                staged: Arc::default(),
            };
            body(attempt.clone()).await?;
            let (written, sent) = attempt.take();
            changes.merge(written);
            transients.merge(sent);
        }
        transients.merge(take_dispatched(&mut changes));
        let transaction = transaction.assert(changes).dispatch(transients);
        let outcome = match Box::pin(transaction.commit().perform(env)).await {
            Ok(batch) => {
                let ticks = batch.ticks();
                let commands: Vec<Fact> = batch
                    .looped()
                    .into_iter()
                    .filter(|fact| !is_lifecycle(&fact.the))
                    .collect();
                batch
                    .publish()
                    .perform(env)
                    .await
                    .map(|revision| InvocationReport {
                        stack: stack.clone(),
                        revision,
                        ticks,
                        commands,
                    })
            }
            Err(error) => Err(error),
        };
        match outcome {
            Ok(report) => return Ok(report),
            Err(CommitError::Publish(lost @ PublishError::VersionMismatch { .. })) => {
                if reruns == MAX_RERUNS {
                    return Err(lost.into());
                }
                reruns += 1;
                stack.refresh(env).await?;
            }
            Err(error) => return Err(error),
        }
    }
}
