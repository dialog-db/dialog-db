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
//! has the two commands and the providers that answer them
//! ([`Stack::provide`](crate::Stack::provide)):
//!
//! | command | concept | answered with |
//! |---|---|---|
//! | `dialog.node.probe/node` of a command, the node to ask about | [`probe`] | `dialog.node.probed/node` and `dialog.node.probed/local` of the same command |
//! | `dialog.node.fetch/node` of a command, the node to bring local | [`fetch`] | `dialog.node.fetched/node` and `dialog.node.fetched/local` of the same command |
//!
//! `local` is whether the replica holds the node: for a probe, as it
//! stood when asked, with nothing fetched; for a fetch, after trying, so
//! `false` means no place the replica knows of had the node.
//!
//! A provider asserts nothing that is kept. Its answer is written into
//! [`control:`](crate::CONTROL), so it is a command in its turn, there
//! for one tick, and a rule of the space decides whether to keep it and
//! where. A space that wants to show what it holds places an attribute
//! on a layer of the session and captures the answer there: the fact is
//! then an ordinary one a query reads and a subscription follows, it is
//! never committed or synced, and it is gone with the session. A space
//! with no such rule gets nothing, which is how a command it has no use
//! for is ignored.
//!
//! A command whose value names no node, and a read that fails, are the
//! provider's failure: the command gets a `dialog.command/failed` row
//! with the reason, as any command's does.
//!
//! ```no_run
//! # use dialog_repository::{Branch, StackEnv, holdings};
//! # async fn example<Env>(branch: Branch, env: Env) -> anyhow::Result<()>
//! # where Env: StackEnv + Clone + dialog_common::ConditionalSend {
//! let stack = branch.stack().perform(&env).await?;
//! holdings::provide(&stack, env.clone())?;
//! let root: dialog_corpus::Entity = "node:8QsR69j39jU7acb4H9VfsSpR2SrHJ6f2PPKHoosnsAPr".parse()?;
//! let report = stack
//!     .invoke(dialog_query::the!("dialog.node.probe/node").of("probe:1".parse()?).is(root))
//!     .perform(&env)
//!     .await?;
//! // Answer what that dispatch asked, when the embedder chooses.
//! report.effect().perform(&env).await?;
//! # Ok(())
//! # }
//! ```

use dialog_capability::Provider as Performs;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Changes, Statement as _, Update as _, Value};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_query::attribute::{Cardinality, Relation};
use dialog_query::{AttributeDescriptor, ConceptDescriptor};
use dialog_search_tree::nodes;

use crate::{Command, CommitError, Failure, Provider, Stack, control_layer};

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

/// The concept of a command whose one field, `node`, is `the`.
fn command(the: Relation) -> ConceptDescriptor {
    ConceptDescriptor::try_from([(
        "node",
        AttributeDescriptor::new(the, "", Cardinality::One, None),
    )])
    .expect("a concept of one field")
}

/// The concept of the command asking whether a node is held ([`PROBE`]).
pub fn probe() -> ConceptDescriptor {
    command(dialog_query::the!("dialog.node.probe/node"))
}

/// The concept of the command asking for a node to be brought local
/// ([`FETCH`]).
pub fn fetch() -> ConceptDescriptor {
    command(dialog_query::the!("dialog.node.fetch/node"))
}

/// The answer to one node command: whether the replica holds the node it
/// names, fetching it first when `fetching`, as the two facts of the
/// answer, written into `control:` on the command's own entity.
async fn answer<Env>(
    command: Command,
    catalog: CatalogScope,
    env: Env,
    fetching: bool,
) -> Result<Changes, Failure>
where
    Env: Performs<nodes::Read<CorpusNode>> + ConditionalSend + ConditionalSync,
{
    let asked = if fetching { FETCH } else { PROBE };
    let node = command
        .rows()
        .iter()
        .find(|row| row.the.to_string() == asked)
        .map(|row| &row.is);
    let Some(Value::Entity(node)) = node else {
        return Err(Failure::new(format!("{asked} names no entity: {node:?}")));
    };
    let Some(hash) = node.node_hash() else {
        return Err(Failure::new(format!("{node} is not a node")));
    };

    let read = nodes::Read::<CorpusNode>::new(catalog, Blake3Hash::from(hash));
    let read = if fetching { read } else { read.locally() };
    let local = read
        .perform(&env)
        .await
        .map_err(|error| Failure::new(format!("reading {node}: {error}")))?
        .is_some();

    let (about, held) = if fetching {
        (FETCHED, FETCHED_LOCAL)
    } else {
        (PROBED, PROBED_LOCAL)
    };
    let mut facts = Changes::new();
    for (the, is) in [
        (about, Value::Entity(node.clone())),
        (held, Value::Boolean(local)),
    ] {
        facts.associate(
            the.parse().expect("a node command's attribute"),
            command.entity().clone(),
            is,
            Cardinality::Many,
        );
    }
    // Into `control:`, so the answer is a command a rule can capture and
    // nothing else keeps.
    let mut answer = Changes::new();
    facts.at(control_layer()).assert(&mut answer);
    Ok(answer)
}

/// The providers of the two node commands for the tree of `stack`'s
/// primary, reading through `env`: the probe's, then the fetch's.
///
/// What "held" and "fetched" mean is `env`'s: a node read asked for
/// [locally](nodes::Read::locally) answers from what the replica holds,
/// and one not so asked hydrates from wherever the subject's blocks live
/// and keeps what arrives. A provider is handed a command and nothing
/// to read through, which is why it keeps `env`.
pub fn providers<Env>(stack: &Stack, env: Env) -> [Provider; 2]
where
    Env: Performs<nodes::Read<CorpusNode>> + Clone + ConditionalSend + ConditionalSync + 'static,
{
    let provider = |concept: &ConceptDescriptor, fetching: bool| {
        let catalog = stack.primary().archive().index();
        let env = env.clone();
        Provider::new(concept, move |command| {
            answer(command, catalog.clone(), env.clone(), fetching)
        })
    };
    [provider(&probe(), false), provider(&fetch(), true)]
}

/// Register [`providers`] with `stack`.
pub fn provide<Env>(stack: &Stack, env: Env) -> Result<(), CommitError>
where
    Env: Performs<nodes::Read<CorpusNode>> + Clone + ConditionalSend + ConditionalSync + 'static,
{
    for provider in providers(stack, env) {
        stack.provide(provider)?;
    }
    Ok(())
}
