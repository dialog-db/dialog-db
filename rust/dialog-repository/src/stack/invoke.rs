//! Driving a stack: a dispatch is one unit of publication, re-run from
//! the new head when it loses its compare-and-swap. Once it has
//! published, its commands land in [`control:`](crate::CONTROL) for
//! one tick, where the stack's providers read them (see
//! [`Arrivals`](super::Arrivals)).

use dialog_corpus::{Changes, Statement};

use crate::placement::take_dispatched;
use crate::{CommitError, PublishError, Revision, Stack, StackEnv};

/// How many times a dispatch is re-run from a moved head before the
/// mismatch is reported. A run is a function of the head and the
/// stimulus, so each re-run is as sound as the first; the cap guards
/// against a head that moves faster than this writer can follow.
pub const MAX_RERUNS: u32 = 8;

/// Command driving one dispatch through a [`Stack`]. Built by
/// [`Stack::invoke`] or [`Stack::transact`]; run with
/// [`perform`](Self::perform).
///
/// A dispatch is one unit of publication. The command and the writes
/// beside it are the stimulus of a transaction through the stack: its
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
/// stack's `control:` layer as one instant and are gone in the next:
/// what the stack's providers read (see
/// [`Stack::arrivals`](crate::Stack::arrivals)), so a lost
/// compare-and-swap re-runs no effect. A provider answers by invoking,
/// and its answer is a dispatch of its own, with its own
/// compare-and-swap.
#[derive(Clone)]
pub struct Invocation {
    stack: Stack,
    changes: Changes,
    transients: Changes,
}

impl Stack {
    /// Dispatch `command` through the stack. See [`Invocation`].
    pub fn invoke<C: Statement>(&self, command: C) -> Invocation {
        self.transact(Changes::new()).dispatch(command)
    }

    /// Write `facts` through the stack with no command: an import, a
    /// seed, a test fixture. See [`Invocation`].
    pub fn transact<S: Statement>(&self, facts: S) -> Invocation {
        Invocation {
            stack: self.clone(),
            changes: Changes::new(),
            transients: Changes::new(),
        }
        .assert(facts)
    }

    /// Read again the head of the primary, and where it points every
    /// durable layer: what a dispatch re-run after a lost
    /// compare-and-swap builds on.
    pub(crate) async fn refresh<Env: StackEnv>(&self, env: &Env) -> Result<(), CommitError> {
        self.primary().refresh(env).await?;
        self.repoint(env).await
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
    pub async fn perform<Env: StackEnv>(self, env: &Env) -> Result<Revision, CommitError> {
        let Invocation {
            stack,
            mut changes,
            mut transients,
        } = self;
        transients.merge(take_dispatched(&mut changes));
        dispatch(&stack, &changes, &transients, env).await
    }
}

/// One dispatch of `changes` and `transients` through `stack`: commit
/// and publish, re-run from the new head when the publish loses its
/// compare-and-swap, the primary's, which is the durable layers' too.
/// Answers the head it left the primary at. The publish that wins lands the
/// dispatch's commands in the stack's `control:`, as every publish of
/// a transaction through the stack does.
async fn dispatch<Env: StackEnv>(
    stack: &Stack,
    changes: &Changes,
    transients: &Changes,
    env: &Env,
) -> Result<Revision, CommitError> {
    let mut reruns = 0;
    // The durable layers are read where the primary points them now: a
    // commit through another handle may have moved it since the stack
    // last read them.
    stack.repoint(env).await?;
    loop {
        let transaction = stack
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
                stack.refresh(env).await?;
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
                stack.refresh(env).await?;
            }
            Err(error) => return Err(error),
        }
    }
}
