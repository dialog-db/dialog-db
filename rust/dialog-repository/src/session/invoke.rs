//! Driving a session: a dispatch is one unit of publication, re-run from
//! the new head when it loses its compare-and-swap.

use dialog_corpus::{Changes, Statement};

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
    /// head when the publish loses its compare-and-swap. Returns the
    /// head the dispatch left the primary at: the revision it published,
    /// or the unchanged head when it wrote nothing durable.
    pub async fn perform<Env: SessionEnv>(self, env: &Env) -> Result<Revision, CommitError> {
        let mut reruns = 0;
        loop {
            let transaction = self
                .session
                .transaction()
                .assert(self.changes.clone())
                .dispatch(self.transients.clone());
            match Box::pin(transaction.commit().publish().perform(env)).await {
                Err(CommitError::Publish(lost @ PublishError::VersionMismatch { .. })) => {
                    if reruns == MAX_RERUNS {
                        return Err(lost.into());
                    }
                    reruns += 1;
                    self.session.primary().refresh(env).await?;
                }
                outcome => return outcome,
            }
        }
    }
}
