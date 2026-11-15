//! What arrives on a channel's outside end: the rows each published
//! dispatch wrote into it, each once, read through a query. How a
//! provider observes the commands it performs.
//!
//! A provider subscribes to the command concept it performs, on the
//! channel the commands land in: the loopback, [`control:`](crate::CONTROL),
//! where every published dispatch's commands land, or another
//! [`Channel`] the stack holds. A channel's outside end
//! is a log of ticks, one per published dispatch that wrote into it, and
//! [`Arrivals`] reads it from its own position, yielding the rows of its
//! query those ticks assert, each once, oldest first. A row lives one
//! tick: its expiry is no news to the provider, and nothing is retracted.
//!
//! A provider answers by invoking ([`Stack::invoke`]): a command naming
//! the one it answers by that command's entity, which a rule captures on
//! the layer it wants. The answer is a dispatch of its own, with its own
//! compare-and-swap. A command reaches the outside end of `control:` only once its dispatch
//! has published, so a dispatch re-run from a moved head re-runs no
//! effect.
//!
//! ```no_run
//! # use dialog_repository::{Stack, StackEnv, control_layer};
//! # use dialog_query::{AttributeQuery, Term};
//! # use dialog_query::attribute::Relation;
//! # use dialog_corpus::Entity;
//! # async fn example(stack: Stack, env: impl StackEnv) -> anyhow::Result<()> {
//! let fetch = AttributeQuery::from(
//!     Term::<Relation>::from(Relation::from("cmd.fetch/node".parse::<dialog_corpus::Relation>()?))
//!         .of(Term::<Entity>::var("command"))
//!         .is(Term::<Entity>::var("node")),
//! );
//! let mut fetches = stack.arrivals(&control_layer(), fetch).expect("every stack has control:");
//! loop {
//!     for command in fetches.next(&env).await? {
//!         let dialog_corpus::Value::Entity(node) = command.is else { continue };
//!         // Perform the effect, then answer by invoking.
//!         stack
//!             .invoke(dialog_query::the!("cmd.fetched/node").of(command.of).is(node))
//!             .perform(&env)
//!             .await?;
//!     }
//! }
//! # }
//! ```

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use dialog_corpus::{Changes, Entity, Fact, Pick, Update as _};
use dialog_query::EvaluationError;
use dialog_query::query::{Application, Output as _};

use crate::{Channel, Ephemeral, OpenStackEnv, QueryLayer, Stack};

/// The rows of a query that the ticks landing on one channel of a stack
/// make, each once, oldest first: what a provider reads its commands
/// from. Built by [`Stack::arrivals`]; read with [`poll`](Self::poll), or
/// [`next`](Self::next) to wait for them.
///
/// A reader can also take the arrived facts themselves, with no query:
/// [`rows`](Self::rows), or [`next_rows`](Self::next_rows) to wait for
/// them, on a reader built by [`Stack::arrivals_rows`] (or any other).
/// Read rows when a command is told apart by the attributes its entity
/// carries rather than by one concept; read through a query to take one
/// concept's commands, bound as its terms.
///
/// The query is evaluated over the arrived rows alone, read as rows of
/// the channel, and a provider reads the stack ([`Stack::query`]) for
/// anything else it needs.
///
/// The reader holds a position in the channel's log, and the log keeps
/// every tick some reader has not passed, so a reader that was not
/// reading loses nothing: its next read yields every tick it missed,
/// oldest first. A reader built when the log holds ticks reads them
/// first: while no reader exists, the log keeps its latest tick alone.
/// Dropping the reader releases what it held.
pub struct Arrivals<Q = ()> {
    channel: Channel,
    query: Q,
    /// The sequence of the channel's last tick read.
    position: Arc<AtomicU64>,
}

impl Stack {
    /// Read what arrives on the outside end of the channel `layer`,
    /// through `query`: the loopback,
    /// [`control_layer`](crate::control_layer), for the commands each
    /// published dispatch leaves, or another channel the stack holds.
    /// `None` for a layer that is no channel of the stack. See
    /// [`Arrivals`].
    pub fn arrivals<Q: Application>(&self, layer: &Entity, query: Q) -> Option<Arrivals<Q>> {
        self.reader(layer, query)
    }

    /// Read what arrives on the channel `layer` row by row, with no
    /// query: a reader whose [`rows`](Arrivals::rows) and
    /// [`next_rows`](Arrivals::next_rows) yield every fact a tick landed
    /// there asserts. `None` for a layer that is no channel of the stack.
    /// See [`Arrivals`].
    pub fn arrivals_rows(&self, layer: &Entity) -> Option<Arrivals> {
        self.reader(layer, ())
    }

    fn reader<Q>(&self, layer: &Entity, query: Q) -> Option<Arrivals<Q>> {
        let channel = self.channel_of(layer)?;
        let position = channel.reader();
        Some(Arrivals {
            channel,
            query,
            position,
        })
    }
}

impl<Q> Arrivals<Q> {
    /// The layer this reader reads: its channel's name.
    pub fn layer(&self) -> &Entity {
        self.channel.entity()
    }

    /// Every fact the ticks landed since the last read assert, oldest
    /// first, with no query: what a provider reads when it decodes a
    /// command by the set of attributes its entity carries, rather than
    /// by one concept. Empty when nothing arrived. The reader then stands
    /// at the channel's latest tick, as after [`poll`](Self::poll): the
    /// two read the same arrivals, so a row one yields the other does
    /// not.
    ///
    /// Read rows to take everything that arrived, a whole command of
    /// whatever shape; read through a query ([`Stack::arrivals`] and
    /// [`poll`](Self::poll)) to take only the commands of one concept,
    /// bound as its terms. A query whose every term is a variable but a
    /// constant `in` naming the channel reads the same rows as claims.
    pub fn rows(&mut self) -> Vec<Fact> {
        self.channel.read(&self.position)
    }

    /// The next facts to arrive: what [`rows`](Self::rows) yields,
    /// waiting for the stack's next dispatch to publish while it yields
    /// nothing. Only a dispatch through the stack wakes it, as with
    /// [`next`](Self::next).
    pub async fn next_rows(&mut self) -> Vec<Fact> {
        let channel = self.channel.clone();
        loop {
            // Listening before reading, so a dispatch landing between the
            // read and the wait is not slept through.
            let landed = channel.landed().notified();
            let mut landed = pin!(landed);
            landed.as_mut().enable();
            let rows = self.rows();
            if !rows.is_empty() {
                return rows;
            }
            landed.await;
        }
    }
}

impl<Q: Application> Arrivals<Q> {
    /// The rows of the query the ticks landed since the last read make,
    /// oldest first; empty when nothing the query reads arrived. The
    /// reader then stands at the channel's latest tick.
    pub async fn poll<'a, Env: OpenStackEnv>(
        &'a mut self,
        env: &'a Env,
    ) -> Result<Vec<Q::Conclusion>, EvaluationError> {
        let arrived = self.rows();
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
            .with_memory(self.channel.entity().clone(), &batch)
            .select(self.query.clone())
            .perform(env)
            .try_vec()
            .await
    }

    /// The next rows to arrive: what [`poll`](Self::poll) yields, waiting
    /// for the stack's next dispatch to publish while it yields
    /// nothing. Only a dispatch through the stack wakes it.
    pub async fn next<Env: OpenStackEnv>(
        &mut self,
        env: &Env,
    ) -> Result<Vec<Q::Conclusion>, EvaluationError> {
        let channel = self.channel.clone();
        loop {
            // Listening before reading, so a dispatch landing between the
            // read and the wait is not slept through.
            let landed = channel.landed().notified();
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
