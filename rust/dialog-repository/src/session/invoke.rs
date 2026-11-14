//! Driving a session: a dispatch is one unit of publication, re-run from
//! the new head when it loses its compare-and-swap, and answered by the
//! session's handlers once it has published.

use std::collections::VecDeque;
use std::sync::Arc;

use dialog_corpus::{Changes, Statement};

use crate::{CommitError, PublishError, Revision, Session, SessionEnv};

/// How many times a dispatch is re-run from a moved head before the
/// mismatch is reported. A run is a function of the head and the
/// stimulus, so each re-run is as sound as the first; the cap guards
/// against a head that moves faster than this writer can follow.
pub const MAX_RERUNS: u32 = 8;

/// How many dispatches one invocation may chain through its handlers'
/// answers before the chain is reported as diverging: a handler that
/// answers every answer with another command would otherwise never end.
pub const MAX_ANSWERS: u32 = 16;

/// What answers the commands a dispatch leaves unanswered by its rules:
/// something that performs an effect, built into dialog or registered
/// by the embedder (see [`Session::handle`]).
///
/// A handler sees every command of a dispatch, the one invoked and the
/// ones its rules dispatched, after the dispatch has published, so a
/// lost compare-and-swap re-runs no effect. It answers with commands,
/// which the session dispatches as the stimulus of the next tick, where
/// a rule captures what it wants of the answer. A command it does not
/// take it leaves out of its answer. An answer names the command it
/// answers by that command's entity.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Handler: dialog_common::ConditionalSend + dialog_common::ConditionalSync {
    /// The answers to those of `commands` this handler takes.
    async fn answer(&self, commands: &Changes) -> Result<Changes, HandlerError>;
}

/// A handler could not answer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct HandlerError(pub String);

/// Command driving one dispatch through a [`Session`]. Built by
/// [`Session::invoke`] or [`Session::transact`]; run with
/// [`perform`](Self::perform).
///
/// A dispatch is one unit of publication. The command and the writes
/// beside it are the stimulus of a transaction through the session: its
/// commit runs induction to a fixed point over the primary and every
/// layer, routes each write to the layer its attribute is placed on,
/// and publishes the primary's share with one compare-and-swap against
/// the head the dispatch began at. A dispatch that writes only to memory
/// layers, or only dispatches, publishes nothing. When the head has
/// moved, the whole dispatch is re-run from the new head with the
/// original stimulus, up to [`MAX_RERUNS`] times.
///
/// Once a dispatch has published, the session's handlers answer its
/// commands (see [`Handler`]), and their answers are dispatched the same
/// way, until no handler answers or [`MAX_ANSWERS`] dispatches have
/// chained.
#[derive(Clone)]
pub struct Invocation {
    session: Session,
    changes: Changes,
    transients: Changes,
}

impl Session {
    /// Dispatch `command` through the session. See [`Invocation`].
    pub fn invoke<C: Statement>(&self, command: C) -> Invocation {
        self.transact(Changes::new()).dispatch(command)
    }

    /// Write `facts` through the session with no command: an import, a
    /// seed, a test fixture. See [`Invocation`].
    pub fn transact<S: Statement>(&self, facts: S) -> Invocation {
        Invocation {
            session: self.clone(),
            changes: Changes::new(),
            transients: Changes::new(),
        }
        .assert(facts)
    }

    /// Answer `commands` with every handler the session holds, in the
    /// order they were registered.
    async fn answer(&self, commands: &Changes) -> Result<Changes, CommitError> {
        let mut answers = Changes::new();
        for handler in self.handlers() {
            let answer = handler
                .answer(commands)
                .await
                .map_err(|HandlerError(reason)| CommitError::Handler(reason))?;
            answer.assert(&mut answers);
        }
        Ok(answers)
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

    /// Dispatch `command` too, in the same tick.
    pub fn dispatch<C: Statement>(mut self, command: C) -> Self {
        command.assert(&mut self.transients);
        self
    }

    /// Run the dispatch: commit and publish, re-running from the new
    /// head when the publish loses its compare-and-swap; then have the
    /// session's handlers answer its commands and dispatch their answers
    /// the same way, until none answers. Returns the head the invocation
    /// left the primary at: the revision its last dispatch published, or
    /// the unchanged head when nothing durable was written.
    pub async fn perform<Env: SessionEnv>(self, env: &Env) -> Result<Revision, CommitError> {
        let Invocation {
            session,
            mut changes,
            mut transients,
        } = self;
        let mut answered: u32 = 0;
        loop {
            let (revision, commands) = dispatch(&session, &changes, &transients, env).await?;
            let answers = session.answer(&commands).await?;
            if answers.is_empty() {
                return Ok(revision);
            }
            answered += 1;
            if answered > MAX_ANSWERS {
                return Err(CommitError::AnswerDivergence(MAX_ANSWERS));
            }
            changes = Changes::new();
            transients = answers;
        }
    }
}

/// One dispatch of `changes` and `transients` through `session`: commit
/// and publish, re-run from the new head when the publish loses its
/// compare-and-swap. Answers the head it left the primary at and every
/// command of the dispatch: the ones dispatched and the ones its rules
/// dispatched in any tick.
async fn dispatch<Env: SessionEnv>(
    session: &Session,
    changes: &Changes,
    transients: &Changes,
    env: &Env,
) -> Result<(Revision, Changes), CommitError> {
    let mut reruns = 0;
    loop {
        let transaction = session
            .transaction()
            .assert(changes.clone())
            .dispatch(transients.clone());
        let batch = match Box::pin(transaction.commit().perform(env)).await {
            Ok(batch) => batch,
            Err(CommitError::Publish(lost @ PublishError::VersionMismatch { .. })) => {
                if reruns == MAX_RERUNS {
                    return Err(lost.into());
                }
                reruns += 1;
                session.primary().refresh(env).await?;
                continue;
            }
            Err(error) => return Err(error),
        };
        let mut commands = transients.clone();
        batch.induced().clone().assert(&mut commands);
        match batch.publish().perform(env).await {
            Ok(revision) => {
                session.record(revision.clone(), commands.clone());
                return Ok((revision, commands));
            }
            Err(CommitError::Publish(lost @ PublishError::VersionMismatch { .. })) => {
                if reruns == MAX_RERUNS {
                    return Err(lost.into());
                }
                reruns += 1;
                session.primary().refresh(env).await?;
            }
            Err(error) => return Err(error),
        }
    }
}

/// How many dispatches the session's command feed keeps. A reader that
/// falls further behind is told it lost dispatches and must start over.
pub const FEED_CAPACITY: usize = 1024;

/// One dispatch as the session's command feed records it: every command
/// of the dispatch, and the head it left the primary at.
#[derive(Debug, Clone, PartialEq)]
pub struct Dispatched {
    /// The dispatch's place in the feed, from 1; each dispatch of the
    /// session takes the next.
    pub sequence: u64,
    /// The head the dispatch left the primary at.
    pub revision: Revision,
    /// The commands of the dispatch: the ones invoked and the ones its
    /// rules dispatched, in any tick. Each is keyed by its command's
    /// entity, which an answer names.
    pub commands: Changes,
}

/// A reader of the session's command feed: what a consumer outside the
/// session's process reads to answer its commands, where an in-process
/// consumer registers a [`Handler`]. Built by [`Session::commands`];
/// each [`poll`](Self::poll) yields the dispatches since the last.
///
/// The feed is the session's control channel: every command a dispatch
/// leaves, after the dispatch has published, in order, once. A consumer
/// answers by invoking the answer through the session
/// ([`Session::invoke`]), naming the command it answers by its entity.
pub struct Commands {
    session: Session,
    /// The sequence of the last dispatch read; 0 before the first poll.
    sequence: u64,
}

/// The feed moved past a reader: more than [`FEED_CAPACITY`] dispatches
/// happened since it last polled, so some are gone. The reader starts
/// over from the session's state, as a subscription recomputes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the command feed moved past sequence {behind}; it now starts at {oldest}")]
pub struct Lost {
    /// The sequence the reader had read up to.
    pub behind: u64,
    /// The oldest sequence the feed still holds.
    pub oldest: u64,
}

impl Commands {
    /// The dispatches since the last poll, oldest first; empty when none
    /// happened. The reader then stands at the newest.
    pub fn poll(&mut self) -> Result<Vec<Dispatched>, Lost> {
        let dispatches = self.session.dispatched_since(self.sequence)?;
        if let Some(last) = dispatches.last() {
            self.sequence = last.sequence;
        }
        Ok(dispatches)
    }

    /// Stand at the newest dispatch: what a reader does after [`Lost`],
    /// once it has read the session's state afresh.
    pub fn catch_up(&mut self) {
        self.sequence = self.session.inner.feed.read().sequence;
    }
}

/// The session's command feed: a bounded log of its dispatches.
#[derive(Default)]
pub(crate) struct Feed {
    /// The sequence of the newest dispatch; 0 before any.
    sequence: u64,
    dispatches: VecDeque<Dispatched>,
}

impl Session {
    /// Read the session's command feed from its newest dispatch on: what
    /// a consumer answers the session's commands from, when it runs
    /// outside the session's process or wants to answer in its own time
    /// (see [`Commands`]). A consumer in the session's process that wants
    /// to answer before the invocation returns registers a [`Handler`]
    /// instead.
    pub fn commands(&self) -> Commands {
        Commands {
            session: self.clone(),
            sequence: self.inner.feed.read().sequence,
        }
    }

    /// Record a dispatch in the feed, with every command it left.
    fn record(&self, revision: Revision, commands: Changes) {
        if commands.is_empty() {
            return;
        }
        let mut feed = self.inner.feed.write();
        feed.sequence += 1;
        if feed.dispatches.len() == FEED_CAPACITY {
            feed.dispatches.pop_front();
        }
        let sequence = feed.sequence;
        feed.dispatches.push_back(Dispatched {
            sequence,
            revision,
            commands,
        });
    }

    /// The dispatches after `sequence`, oldest first, or [`Lost`] when
    /// the feed no longer holds the one right after it.
    fn dispatched_since(&self, sequence: u64) -> Result<Vec<Dispatched>, Lost> {
        let feed = self.inner.feed.read();
        if sequence >= feed.sequence {
            return Ok(Vec::new());
        }
        match feed.dispatches.front() {
            Some(oldest) if oldest.sequence > sequence + 1 => Err(Lost {
                behind: sequence,
                oldest: oldest.sequence,
            }),
            _ => Ok(feed
                .dispatches
                .iter()
                .filter(|dispatched| dispatched.sequence > sequence)
                .cloned()
                .collect()),
        }
    }
}

/// A handler closing over a function: for an embedder whose answer needs
/// no state of its own, and for tests.
pub struct Answering<F>(pub F);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<F> Handler for Answering<F>
where
    F: Fn(&Changes) -> Result<Changes, HandlerError>
        + dialog_common::ConditionalSend
        + dialog_common::ConditionalSync,
{
    async fn answer(&self, commands: &Changes) -> Result<Changes, HandlerError> {
        (self.0)(commands)
    }
}

impl Session {
    /// Hold `handler` in this session: from now on every dispatch's
    /// commands are offered to it, after the ones registered before.
    /// Chainable.
    pub fn handle(&self, handler: Arc<dyn Handler>) -> &Self {
        self.inner.handlers.write().push(handler);
        self
    }

    /// The handlers this session holds, in registration order.
    pub(crate) fn handlers(&self) -> Vec<Arc<dyn Handler>> {
        self.inner.handlers.read().clone()
    }
}
