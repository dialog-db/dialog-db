//! Driving a stack: a dispatch is one unit of publication, re-run from
//! the new head when it loses its compare-and-swap. It takes what the
//! outside ends of the stack's channels sent as its first tick's
//! stimulus, and once it has published, what it wrote into a channel,
//! its commands among them, lands on that channel's outside end for one
//! tick, where the stack's providers read them (see
//! [`Arrivals`](super::Arrivals)).

use dialog_corpus::{Changes, Statement};

use crate::placement::take_dispatched;
use crate::{Channel, CommitError, PublishError, Revision, Stack, StackEnv};

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
/// A command is a row of [`control:`](crate::CONTROL), the built-in
/// [`Channel`]: one [`invoke`](Stack::invoke)d or
/// [`dispatch`](Self::dispatch)ed, sent into the loopback's outside end
/// as the dispatch starts, one written `.at(control_layer())`, and one
/// a rule's head lands there in any tick. Each is the stimulus of the
/// tick after the one that wrote it (the first tick, for one sent) and
/// lands nowhere. Once the dispatch has published, and only then, its
/// commands land on the loopback's outside end as one tick: what the
/// stack's providers read (see
/// [`Stack::arrivals`](crate::Stack::arrivals)), so a lost
/// compare-and-swap re-runs no effect. A provider answers by invoking,
/// and its answer is a dispatch of its own, with its own
/// compare-and-swap.
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
}

impl Stack {
    /// Invoke `command` through the stack: send it into the loopback,
    /// [`control:`](crate::CONTROL), and run a dispatch, which takes it
    /// with whatever else the stack's channels were sent. See
    /// [`Invocation`].
    pub fn invoke<C: Statement>(&self, command: C) -> Invocation {
        self.transact(Changes::new()).dispatch(command)
    }

    /// Write `facts` through the stack with no command: an import, a
    /// seed, a test fixture. See [`Invocation`].
    pub fn transact<S: Statement>(&self, facts: S) -> Invocation {
        Invocation {
            stack: self.clone(),
            changes: Changes::new(),
            sent: Changes::new(),
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

    /// Dispatch `command` too, in the same tick: sent into the loopback,
    /// [`control:`](crate::CONTROL), as [`Stack::invoke`] sends its own.
    pub fn dispatch<C: Statement>(mut self, command: C) -> Self {
        command.assert(&mut self.sent);
        self
    }

    /// Run the dispatch: take what the stack's channels were sent, this
    /// invocation's commands with what was sent into the loopback, then
    /// commit and publish, re-running from the new head when the publish
    /// loses its compare-and-swap, and land what it wrote into each
    /// channel on its outside end for one tick. Returns the head the
    /// dispatch left the primary at: the revision it published, or the
    /// unchanged head when nothing durable was written.
    ///
    /// A dispatch that fails puts back what the channels' outside ends
    /// sent, for the next dispatch to take; its own commands are not
    /// put back.
    pub async fn perform<Env: StackEnv>(self, env: &Env) -> Result<Revision, CommitError> {
        let Invocation {
            stack,
            mut changes,
            sent,
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
        transients.merge(take_dispatched(&mut changes));
        let outcome = dispatch(&stack, &changes, &transients, &received, env).await;
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

/// One dispatch of `changes`, `transients` and `received`, the rows the
/// channels other than the loopback were sent, through `stack`: commit
/// and publish, re-run from the new head when the publish loses its
/// compare-and-swap, the primary's, which is the durable layers' too.
/// Answers the head it left the primary at. The publish that wins lands the
/// dispatch's commands in the stack's `control:`, as every publish of
/// a transaction through the stack does.
async fn dispatch<Env: StackEnv>(
    stack: &Stack,
    changes: &Changes,
    transients: &Changes,
    received: &Changes,
    env: &Env,
) -> Result<Revision, CommitError> {
    let mut reruns = 0;
    // The durable layers are read where the primary points them now: a
    // commit through another handle may have moved it since the stack
    // last read them.
    stack.repoint(env).await?;
    loop {
        let transaction = stack
            .receiving(received.clone())
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
