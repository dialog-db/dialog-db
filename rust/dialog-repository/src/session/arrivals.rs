//! What arrives in a layer: the rows newly asserted there, each once,
//! read through a query. How a provider observes the commands it
//! performs.
//!
//! A provider subscribes to the command concept it performs, in the
//! layer the commands land in: the loopback, [`control:`](crate::CONTROL),
//! where every published dispatch's commands stand for one tick, or a
//! channel, `control:<name>` ([`Store::Control`](crate::Store::Control)),
//! where they stay until the consumer retracts them. Either way the
//! provider wants the commands, not a view: a row's retraction, the
//! one-tick expiry of `control:` or the consumer's own on a channel, is
//! not news to it. So [`Arrivals`] reads the layer's instants
//! ([`Ephemeral::since`]), keeps what they assert, drops what they
//! retract, and yields the rows of its query those assertions make, each
//! once, oldest first.
//!
//! A provider answers by invoking ([`Session::invoke`]): a command naming
//! the one it answers by that command's entity, which a rule captures on
//! the layer it wants. The answer is a dispatch of its own, with its own
//! compare-and-swap. A command reaches `control:` only once its dispatch
//! has published, so a dispatch re-run from a moved head re-runs no
//! effect.
//!
//! ```no_run
//! # use dialog_repository::{Session, SessionEnv, control_layer};
//! # use dialog_query::{AttributeQuery, Term};
//! # use dialog_query::attribute::Relation;
//! # use dialog_corpus::Entity;
//! # async fn example(session: Session, env: impl SessionEnv) -> anyhow::Result<()> {
//! let fetch = AttributeQuery::from(
//!     Term::<Relation>::from(Relation::from("cmd.fetch/node".parse::<dialog_corpus::Relation>()?))
//!         .of(Term::<Entity>::var("command"))
//!         .is(Term::<Entity>::var("node")),
//! );
//! let mut fetches = session.arrivals(&control_layer(), fetch).expect("every session has control:");
//! loop {
//!     for command in fetches.next(&env).await? {
//!         let dialog_corpus::Value::Entity(node) = command.is else { continue };
//!         // Perform the effect, then answer by invoking.
//!         session
//!             .invoke(dialog_query::the!("cmd.fetched/node").of(command.of).is(node))
//!             .perform(&env)
//!             .await?;
//!     }
//! }
//! # }
//! ```

use std::mem::take;
use std::pin::pin;

use dialog_corpus::{Change, Changes, Entity, Fact, Pick, Relation, Update as _};
use dialog_query::EvaluationError;
use dialog_query::query::{Application, Output as _};

use crate::{Ephemeral, QueryLayer, Session, SessionEnv};

/// The rows of a query that the assertions arriving in one layer of a
/// session make, each once, oldest first: what a provider reads its
/// commands from. Built by [`Session::arrivals`]; read with
/// [`poll`](Self::poll), or [`next`](Self::next) to wait for them.
///
/// The query is evaluated over the arrived rows alone, read as rows of
/// the layer, so a row the layer held before it arrived is never yielded
/// twice, and a provider reads the session ([`Session::query`]) for
/// anything else it needs. The first read yields what the layer held
/// when the reader was built: for a channel, every row no consumer has
/// retracted; for `control:`, nothing, as it holds nothing between
/// dispatches.
///
/// The layer's instants are bounded ([`Ephemeral::since`]). A reader the
/// instants moved past reads the layer as it holds it now instead: a
/// channel loses nothing, since its rows stay until consumed, and may
/// yield a row again that the reader had not retracted; the commands
/// that stood in `control:` meanwhile are gone. Polling after every
/// dispatch, as a provider in the invoke loop does, never falls behind.
pub struct Arrivals<Q: Application> {
    session: Session,
    layer: Entity,
    memory: Ephemeral,
    query: Q,
    /// The sequence of the layer's last instant read.
    sequence: u64,
    /// The rows the layer held when the reader was built, yielded by the
    /// first read.
    held: Vec<Fact>,
}

impl Session {
    /// Read what arrives in `layer`, through `query`: the loopback,
    /// [`control_layer`](crate::control_layer), for the commands each
    /// published dispatch leaves, or a channel the session holds. `None`
    /// for a layer the session does not hold. See [`Arrivals`].
    pub fn arrivals<Q: Application>(&self, layer: &Entity, query: Q) -> Option<Arrivals<Q>> {
        let memory = self.layer(layer)?.clone();
        let sequence = memory.revision().sequence;
        let held = memory.export().iter().filter_map(asserted).collect();
        Some(Arrivals {
            session: self.clone(),
            layer: layer.clone(),
            memory,
            query,
            sequence,
            held,
        })
    }
}

/// The fact `change` asserts, if it asserts one.
fn asserted((of, the, change): (&Entity, &Relation, &Change)) -> Option<Fact> {
    match change {
        Change::Assert(is, _) => Some(Fact {
            the: the.clone(),
            of: of.clone(),
            is: is.clone(),
            cause: None,
        }),
        Change::Retract(_) => None,
    }
}

impl<Q: Application> Arrivals<Q> {
    /// The layer this reader reads.
    pub fn layer(&self) -> &Entity {
        &self.layer
    }

    /// The rows of the query the layer's assertions since the last read
    /// make, oldest first; empty when nothing the query reads arrived.
    /// The reader then stands at the layer's newest instant.
    pub async fn poll<'a, Env: SessionEnv>(
        &'a mut self,
        env: &'a Env,
    ) -> Result<Vec<Q::Conclusion>, EvaluationError> {
        let mut arrived = take(&mut self.held);
        match self.memory.since(self.sequence) {
            Some(instants) => {
                if let Some(last) = instants.last() {
                    self.sequence = last.sequence;
                }
                arrived.extend(instants.into_iter().flat_map(|instant| instant.asserted));
            }
            // The instants moved past this reader: what the layer holds
            // now stands for what it missed.
            None => {
                self.sequence = self.memory.revision().sequence;
                arrived.extend(self.memory.export().iter().filter_map(asserted));
            }
        }
        if arrived.is_empty() {
            return Ok(Vec::new());
        }
        // Every arrival is read, the ones of one cell too: a row under
        // `last` that a later one replaced arrived all the same.
        let mut batch = Changes::new();
        for fact in arrived {
            batch.associate(fact.the, fact.of, fact.is, Pick::All);
        }
        let batch = Ephemeral::holding(batch);
        QueryLayer::new()
            .with_memory(self.layer.clone(), &batch)
            .select(self.query.clone())
            .perform(env)
            .try_vec()
            .await
    }

    /// The next rows to arrive: what [`poll`](Self::poll) yields, waiting
    /// for the session's next dispatch to publish while it yields
    /// nothing. Only a dispatch through the session wakes it; a write
    /// into the layer's [`Ephemeral`] directly is read on the next poll.
    pub async fn next<Env: SessionEnv>(
        &mut self,
        env: &Env,
    ) -> Result<Vec<Q::Conclusion>, EvaluationError> {
        let session = self.session.clone();
        loop {
            // Listening before reading, so a dispatch landing between the
            // read and the wait is not slept through.
            let landed = session.inner.landed.notified();
            let mut landed = pin!(landed);
            landed.as_mut().enable();
            let rows = self.poll(env).await?;
            if !rows.is_empty() {
                return Ok(rows);
            }
            landed.await;
        }
    }
}
