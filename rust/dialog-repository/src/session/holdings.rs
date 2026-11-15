//! What this replica holds of a tree, learned and changed by command.
//!
//! A tree resolver (`tree/node`, `tree/span`, `tree/entry`,
//! `tree/block`) reads a node as if the whole tree were local: it
//! fetches a node the replica lacks, so whether the replica held it
//! cannot be seen through a query. That keeps a resolver's rows a
//! function of the handle it is given, and it leaves one thing a query
//! cannot say: whether asking for a node would fetch it.
//!
//! That is asked by command, before the query is issued. This module
//! answers such commands, in two forms:
//!
//! - [`answers`] takes the commands a dispatch left in
//!   [`control:`](crate::CONTROL) and returns their answers, for an
//!   embedder that reads `control:` itself and runs its providers at a
//!   moment of its choosing. It invokes what comes back.
//! - [`Holdings`] reads `control:` for one session
//!   ([`Session::arrivals_rows`]) and answers by invoking:
//!   [`poll`](Holdings::poll) after a dispatch, or
//!   [`next`](Holdings::next) in a loop of its own.
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
//! Nothing here asserts a fact. An answer is a command, dispatched as
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

use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Change, Changes, Entity, Fact, Pick, Relation, Update as _, Value};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_search_tree::nodes;

use crate::{Arrivals, Branch, CommitError, Session, SessionEnv, control_layer};

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

/// The node commands could not be answered.
#[derive(Debug, thiserror::Error)]
pub enum HoldingsError {
    /// Reading a node failed, as distinct from the node not being there.
    #[error("reading a node: {0}")]
    Read(String),
    /// Dispatching the answers failed.
    #[error("answering the node commands: {0}")]
    Answer(#[from] CommitError),
}

/// One node command: the command's entity, the node it names, and
/// whether it asks for the node to be fetched.
type Asked = (Entity, Entity, [u8; 32], bool);

/// The node command `the` of `command` with value `is` is, if it is
/// one. A command of another attribute, or whose value is not a `node:`
/// entity, is not.
fn asked(command: &Entity, the: &Relation, is: &Value) -> Option<Asked> {
    let fetching = match the.to_string().as_str() {
        PROBE => false,
        FETCH => true,
        _ => return None,
    };
    let Value::Entity(node) = is else {
        return None;
    };
    Some((command.clone(), node.clone(), node.node_hash()?, fetching))
}

/// The answers to `asked`, read through `env`: the two facts of each
/// command's answer, on the command's own entity.
async fn answer_all<Env>(
    catalog: &CatalogScope,
    asked: &[Asked],
    env: &Env,
) -> Result<Changes, HoldingsError>
where
    Env: Provider<nodes::Read<CorpusNode>> + ConditionalSync,
{
    let mut answers = Changes::new();
    for (command, node, hash, fetching) in asked {
        let read = nodes::Read::<CorpusNode>::new(catalog.clone(), Blake3Hash::from(*hash));
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
    Ok(answers)
}

/// The answers to the node commands among `commands`, for the tree of
/// `branch`, read through `env`. Empty when none of `commands` is a node
/// command.
///
/// For an embedder that reads what a dispatch left in `control:` itself
/// and runs its providers then: it invokes what this returns
/// (`session.invoke(answers)`), as it would any provider's answer.
///
/// What "held" and "fetched" mean is the environment's: a node read
/// asked for [locally](nodes::Read::locally) answers from what the
/// replica holds, and one not so asked hydrates from wherever the
/// subject's blocks live and keeps what arrives.
pub async fn answers<Env>(
    branch: &Branch,
    commands: &Changes,
    env: &Env,
) -> Result<Changes, HoldingsError>
where
    Env: Provider<nodes::Read<CorpusNode>> + ConditionalSync,
{
    let asked: Vec<Asked> = commands
        .iter()
        .filter_map(|(command, the, change)| match change {
            Change::Assert(is, _) => asked(command, the, is),
            Change::Retract(_) => None,
        })
        .collect();
    answer_all(&branch.archive().index(), &asked, env).await
}

/// The provider of the node commands for one session: see the
/// [module docs](self).
pub struct Holdings {
    session: Session,
    catalog: CatalogScope,
    commands: Arrivals,
}

impl Holdings {
    /// Provide the node commands for `session`, from here on: a command
    /// dispatched before this is not answered.
    pub fn serve(session: &Session) -> Self {
        Self {
            session: session.clone(),
            catalog: session.primary().archive().index(),
            commands: session
                .arrivals_rows(&control_layer())
                .expect("every session has control:"),
        }
    }

    /// Answer the node commands that arrived since the last call, in one
    /// dispatch. Returns how many it answered. Nothing here waits: call
    /// it after a dispatch that may have asked.
    pub async fn poll<Env: SessionEnv>(&mut self, env: &Env) -> Result<usize, HoldingsError> {
        let arrived = self.commands.rows();
        self.answer(arrived, env).await
    }

    /// Wait for the session's next dispatch to leave a node command, and
    /// answer what arrived. Returns how many it answered, at least one.
    /// A provider that runs on its own calls this in a loop.
    pub async fn next<Env: SessionEnv>(&mut self, env: &Env) -> Result<usize, HoldingsError> {
        loop {
            let arrived = self.commands.next_rows().await;
            let answered = self.answer(arrived, env).await?;
            if answered > 0 {
                return Ok(answered);
            }
        }
    }

    /// Answer the node commands among `arrived` in one dispatch.
    async fn answer<Env: SessionEnv>(
        &self,
        arrived: Vec<Fact>,
        env: &Env,
    ) -> Result<usize, HoldingsError> {
        let asked: Vec<Asked> = arrived
            .iter()
            .filter_map(|fact| asked(&fact.of, &fact.the, &fact.is))
            .collect();
        if asked.is_empty() {
            return Ok(0);
        }
        let answers = answer_all(&self.catalog, &asked, env).await?;
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
