//! Driving a session: a dispatch is one unit of publication, re-run from
//! the new head when it loses its compare-and-swap. Once it has
//! published, its commands land in [`control:`](crate::CONTROL) for
//! one tick, where the session's providers read them (see
//! [`Arrivals`](super::Arrivals)).

use dialog_corpus::{Changes, Statement};

use crate::placement::take_dispatched;
use crate::{CommitError, PublishError, Revision, Session, SessionEnv};

/// How many times a dispatch is re-run from a moved head before the
/// mismatch is reported. A run is a function of the head and the
/// stimulus, so each re-run is as sound as the first; the cap guards
/// against a head that moves faster than this writer can follow.
pub const MAX_RERUNS: u32 = 8;

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
/// A command is a write into [`control:`](crate::CONTROL): one
/// [`dispatch`](Self::dispatch)ed, one written `.at(control_layer())`,
/// and one a rule's head lands there in any tick. Each is the stimulus
/// of the tick after the one that wrote it and lands nowhere. Once the
/// dispatch has published, and only then, its commands land in the
/// session's `control:` layer as one instant and are gone in the next:
/// what the session's providers read (see
/// [`Session::arrivals`](crate::Session::arrivals)), so a lost
/// compare-and-swap re-runs no effect. A provider answers by invoking,
/// and its answer is a dispatch of its own, with its own
/// compare-and-swap.
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

    /// Read again the head of the primary and of every durable layer:
    /// what a dispatch re-run after a lost compare-and-swap builds on.
    pub(crate) async fn refresh<Env: SessionEnv>(&self, env: &Env) -> Result<(), CommitError> {
        self.primary().refresh(env).await?;
        for (_, branch) in self.durables() {
            branch.refresh(env).await?;
        }
        Ok(())
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

    /// Dispatch `command` too, in the same tick: a write into
    /// [`control:`](crate::CONTROL), as `.at(control_layer())` is.
    pub fn dispatch<C: Statement>(mut self, command: C) -> Self {
        command.assert(&mut self.transients);
        self
    }

    /// Run the dispatch: commit and publish, re-running from the new
    /// head when the publish loses its compare-and-swap, then land its
    /// commands in `control:` for one tick. Returns the head the dispatch
    /// left the primary at: the revision it published, or the unchanged
    /// head when nothing durable was written.
    pub async fn perform<Env: SessionEnv>(self, env: &Env) -> Result<Revision, CommitError> {
        let Invocation {
            session,
            mut changes,
            mut transients,
        } = self;
        transients.merge(take_dispatched(&mut changes));
        dispatch(&session, &changes, &transients, env).await
    }
}

/// One dispatch of `changes` and `transients` through `session`: commit
/// and publish, re-run from the new heads when the publish loses its
/// compare-and-swap, on the primary or on any durable layer. Answers the
/// head it left the primary at. The publish that wins lands the
/// dispatch's commands in the session's `control:`, as every publish of
/// a transaction through the session does.
async fn dispatch<Env: SessionEnv>(
    session: &Session,
    changes: &Changes,
    transients: &Changes,
    env: &Env,
) -> Result<Revision, CommitError> {
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
                session.refresh(env).await?;
                continue;
            }
            Err(error) => return Err(error),
        };
        match batch.publish().perform(env).await {
            Ok(revision) => return Ok(revision),
            Err(CommitError::Publish(lost @ PublishError::VersionMismatch { .. })) => {
                if reruns == MAX_RERUNS {
                    return Err(lost.into());
                }
                reruns += 1;
                session.refresh(env).await?;
            }
            Err(error) => return Err(error),
        }
    }
}
