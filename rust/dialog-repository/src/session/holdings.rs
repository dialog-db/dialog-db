//! What this replica holds of a tree, learned and changed by command.
//!
//! A tree resolver (`tree/node`, `tree/span`, `tree/entry`,
//! `tree/block`) reads a node as if the whole tree were local: it
//! fetches a node the replica lacks, so whether the replica held it
//! cannot be seen through a query. That keeps a resolver's rows a
//! function of the handle it is given, and it leaves one thing a query
//! cannot say: whether asking for a node would fetch it.
//!
//! That is asked by command, before the query is issued. [`Holdings`] is
//! the provider dialog has for it: it reads the node commands a
//! session's dispatches leave in [`control:`](crate::CONTROL)
//! ([`Session::arrivals`]) and answers each by invoking.
//!
//! | command | answered with |
//! |---|---|
//! | `dialog.node.probe/node` of a command, the node to ask about | `dialog.node.probed/node` and `dialog.node.probed/local` of the same command |
//! | `dialog.node.fetch/node` of a command, the node to bring local | `dialog.node.fetched/node` and `dialog.node.fetched/local` of the same command |
//!
//! `local` is whether the replica holds the node: for a probe, as it
//! stood when asked, with nothing fetched; for a fetch, after trying, so
//! `false` means no place the replica knows of had the node.
//!
//! The provider asserts nothing. Its answer is a command, dispatched as
//! the next tick's stimulus, and a rule of the space decides whether to
//! keep it and where. A space that wants to show what it holds places an
//! attribute on a memory layer and captures the answer there: the fact
//! is then an ordinary one a query reads and a subscription follows, it
//! is never committed or synced, and it is gone with the session. A
//! space with no such rule gets nothing, which is how a command it has
//! no use for is ignored.
//!
//! ```no_run
//! # use dialog_repository::{Branch, Holdings, SessionEnv};
//! # async fn example(branch: Branch, env: impl SessionEnv) -> anyhow::Result<()> {
//! let session = branch.session().perform(&env).await?;
//! let mut holdings = Holdings::serve(&session);
//! let root: dialog_corpus::Entity = "node:8QsR69j39jU7acb4H9VfsSpR2SrHJ6f2PPKHoosnsAPr".parse()?;
//! session
//!     .invoke(dialog_query::the!("dialog.node.probe/node").of("probe:1".parse()?).is(root))
//!     .perform(&env)
//!     .await?;
//! // Answer what that dispatch asked.
//! holdings.poll(&env).await?;
//! # Ok(())
//! # }
//! ```

use dialog_common::Blake3Hash;
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Changes, Entity, Pick, Update as _, Value};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_query::attribute::Relation;
use dialog_query::{AttributeQuery, EvaluationError, Term};
use dialog_search_tree::nodes;

use crate::{Arrivals, CommitError, Session, SessionEnv, control_layer};

/// The command asking whether a node is held: its value is the node.
pub const PROBE: &str = "dialog.node.probe/node";
/// The node a [`PROBE`] asked about, on the answer.
pub const PROBED: &str = "dialog.node.probed/node";
/// Whether the replica held the node a [`PROBE`] asked about.
pub const PROBED_LOCAL: &str = "dialog.node.probed/local";
/// The command asking for a node to be brought local: its value is the
/// node.
pub const FETCH: &str = "dialog.node.fetch/node";
/// The node a [`FETCH`] asked for, on the answer.
pub const FETCHED: &str = "dialog.node.fetched/node";
/// Whether the replica holds the node a [`FETCH`] asked for, having
/// tried.
pub const FETCHED_LOCAL: &str = "dialog.node.fetched/local";

/// The provider could not answer.
#[derive(Debug, thiserror::Error)]
pub enum HoldingsError {
    /// Reading the commands that arrived failed.
    #[error("reading the node commands: {0}")]
    Arrivals(#[from] EvaluationError),
    /// Reading a node failed, as distinct from the node not being there.
    #[error("reading a node: {0}")]
    Read(String),
    /// Dispatching the answers failed.
    #[error("answering the node commands: {0}")]
    Answer(#[from] CommitError),
}

/// The provider of the node commands for one session: see the
/// [module docs](self).
///
/// What "held" and "fetched" mean is the environment's: a node read
/// asked for [locally](nodes::Read::locally) answers from what the
/// replica holds, and one not so asked hydrates from wherever the
/// subject's blocks live and keeps what arrives.
pub struct Holdings {
    session: Session,
    catalog: CatalogScope,
    probes: Arrivals<AttributeQuery>,
    fetches: Arrivals<AttributeQuery>,
}

/// The query for the commands of one attribute: every command and the
/// node it names.
fn commands(the: &str) -> AttributeQuery {
    let the: dialog_corpus::Relation = the.parse().expect("a node command's attribute");
    AttributeQuery::from(
        Term::<Relation>::from(Relation::from(the))
            .of(Term::<Entity>::var("command"))
            .is(Term::<Entity>::var("node")),
    )
}

impl Holdings {
    /// Provide the node commands for `session`, from here on: a command
    /// dispatched before this is not answered.
    pub fn serve(session: &Session) -> Self {
        let control = control_layer();
        let arrivals = |the| {
            session
                .arrivals(&control, commands(the))
                .expect("every session has control:")
        };
        Self {
            session: session.clone(),
            catalog: session.primary().archive().index(),
            probes: arrivals(PROBE),
            fetches: arrivals(FETCH),
        }
    }

    /// Answer the node commands that arrived since the last call, in one
    /// dispatch: every probe, then every fetch. Returns how many it
    /// answered. A command whose value is not a `node:` entity is not
    /// this provider's and is not answered.
    ///
    /// Call it after a dispatch that may have asked, or in a loop of
    /// one's own; nothing here waits.
    pub async fn poll<Env: SessionEnv>(&mut self, env: &Env) -> Result<usize, HoldingsError> {
        let mut asked = Vec::new();
        for (arrivals, fetching) in [(&mut self.probes, false), (&mut self.fetches, true)] {
            for command in arrivals.poll(env).await? {
                let Value::Entity(node) = command.is else {
                    continue;
                };
                let Some(hash) = node.node_hash() else {
                    continue;
                };
                asked.push((command.of, node, hash, fetching));
            }
        }
        if asked.is_empty() {
            return Ok(0);
        }

        let mut answers = Changes::new();
        for (command, node, hash, fetching) in &asked {
            let read =
                nodes::Read::<CorpusNode>::new(self.catalog.clone(), Blake3Hash::from(*hash));
            let read = if *fetching { read } else { read.locally() };
            let local = read
                .perform(env)
                .await
                .map_err(|error| HoldingsError::Read(error.to_string()))?
                .is_some();
            let (about, held) = if *fetching {
                (FETCHED, FETCHED_LOCAL)
            } else {
                (PROBED, PROBED_LOCAL)
            };
            answer(&mut answers, command, about, Value::Entity(node.clone()));
            answer(&mut answers, command, held, Value::Boolean(local));
        }
        self.session.invoke(answers).perform(env).await?;
        Ok(asked.len())
    }
}

/// One fact of an answer: `the` of the command it answers.
fn answer(answers: &mut Changes, command: &Entity, the: &str, is: Value) {
    answers.associate(
        the.parse().expect("a node command's attribute"),
        command.clone(),
        is,
        Pick::All,
    );
}
