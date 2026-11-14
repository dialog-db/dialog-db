//! What this replica holds of a tree, learned and changed by command.
//!
//! A tree resolver (`tree/node`, `tree/span`, `tree/entry`,
//! `tree/block`) reads a node as if the whole tree were local: it
//! fetches a node the replica lacks, so whether the replica held it
//! cannot be seen through a query. That keeps a resolver's rows a
//! function of the handle it is given, and it leaves one thing a query
//! cannot say: whether asking for a node would fetch it.
//!
//! That is asked by command, before the query is issued. [`Holdings`]
//! answers such commands ([`Holdings::answers`]), and is wired to a
//! session as a handler ([`Session::handle`](crate::Session::handle)).
//! How a session offers commands to what answers them is being redrawn
//! (a provider as a subscription over the command, in the layers work);
//! the wiring here follows it, and what is answered does not change:
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
//! The handler asserts nothing. Its answer is a command, dispatched as
//! the next tick's stimulus, and a rule of the space decides whether to
//! keep it and where. A space that wants to show what it holds places an
//! attribute on a memory layer and captures the answer there: the fact
//! is then an ordinary one a query reads and a subscription follows, it
//! is never committed or synced, and it is gone with the session. A
//! space with no such rule gets nothing, which is how a command it has
//! no use for is ignored.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use dialog_repository::{Branch, Holdings, SessionEnv};
//! # async fn example<Env>(branch: Branch, env: Env) -> anyhow::Result<()>
//! # where Env: SessionEnv + Clone {
//! let session = branch.session().perform(&env).await?;
//! session.handle(Arc::new(Holdings::of(&branch, env.clone())));
//! let root: dialog_corpus::Entity = "node:8QsR69j39jU7acb4H9VfsSpR2SrHJ6f2PPKHoosnsAPr".parse()?;
//! session
//!     .invoke(dialog_query::the!("dialog.node.probe/node").of("probe:1".parse()?).is(root))
//!     .perform(&env)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Change, Changes, Entity, Pick, Relation, Update as _, Value};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_search_tree::nodes;

use crate::{Branch, Handler, HandlerError};

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

/// The handler for the node commands: see the [module docs](self).
///
/// It reads through an environment of its own, the one it was given
/// when it was made, since a handler is offered commands and nothing
/// else. That environment decides what "held" and "fetched" mean: a
/// node read asked for [locally](nodes::Read::locally) answers from what
/// the replica holds, and one not so asked hydrates from wherever the
/// subject's blocks live and keeps what arrives.
pub struct Holdings<Env> {
    env: Env,
    catalog: CatalogScope,
}

impl<Env> Holdings<Env> {
    /// The handler for the nodes of `branch`'s tree, reading through
    /// `env`.
    pub fn of(branch: &Branch, env: Env) -> Self {
        Self {
            env,
            catalog: branch.archive().index(),
        }
    }
}

impl<Env> Holdings<Env>
where
    Env: Provider<nodes::Read<CorpusNode>> + ConditionalSend + ConditionalSync,
{
    /// Whether the replica holds `node`, fetching it first when `fetch`.
    async fn holds(&self, node: [u8; 32], fetch: bool) -> Result<bool, String> {
        let read = nodes::Read::<CorpusNode>::new(self.catalog.clone(), Blake3Hash::from(node));
        let read = if fetch { read } else { read.locally() };
        read.perform(&self.env)
            .await
            .map(|found| found.is_some())
            .map_err(|error| format!("reading a node: {error}"))
    }

    /// The answers to the node commands among `commands`: for each
    /// [`PROBE`] and [`FETCH`] whose value is a `node:` entity, the two
    /// facts of its answer, on the command's own entity. Any other
    /// command is left out.
    ///
    /// This is the whole of what the handler does, and it does not
    /// depend on how a session offers commands and takes answers: the
    /// [`Handler`] below is one way of wiring it, and a provider that
    /// reads commands some other way calls this and invokes what it
    /// returns.
    pub async fn answers(&self, commands: &Changes) -> Result<Changes, String> {
        let relation =
            |name: &str| -> Relation { name.parse().expect("a node command's attribute") };
        let (probe, fetch) = (relation(PROBE), relation(FETCH));

        // What is asked, read off the commands before anything is
        // awaited: a command whose value names no node is not this
        // handler's, and is left out of its answer.
        let mut asked = Vec::new();
        for (command, the, change) in commands.iter() {
            let fetching = if *the == fetch {
                true
            } else if *the == probe {
                false
            } else {
                continue;
            };
            let Change::Assert(Value::Entity(node), _) = change else {
                continue;
            };
            let Some(hash) = node.node_hash() else {
                continue;
            };
            asked.push((command.clone(), node.clone(), hash, fetching));
        }

        let mut answers = Changes::new();
        for (command, node, hash, fetching) in asked {
            let local = self.holds(hash, fetching).await?;
            let (about, held) = if fetching {
                (FETCHED, FETCHED_LOCAL)
            } else {
                (PROBED, PROBED_LOCAL)
            };
            answer(&mut answers, &command, about, Value::Entity(node));
            answer(&mut answers, &command, held, Value::Boolean(local));
        }
        Ok(answers)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> Handler for Holdings<Env>
where
    Env: Provider<nodes::Read<CorpusNode>> + ConditionalSend + ConditionalSync,
{
    async fn answer(&self, commands: &Changes) -> Result<Changes, HandlerError> {
        self.answers(commands).await.map_err(HandlerError)
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
