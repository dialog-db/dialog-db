//! Composites through a real tree, on the volatile and the filesystem
//! providers, and a syntax tree kept as one.

use anyhow::{Context, Result};
use dialog_corpus::helpers::composite_decodes;
use dialog_corpus::selector::Constrained;
use dialog_corpus::{Cardinality, Composite, Entity, Fact, Instruction, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use futures_util::{TryStreamExt as _, stream};

use crate::helpers::test_repo;
use crate::{Branch, Repository};

fn text(text: &str) -> Value {
    Value::String(text.into())
}

fn entity(uri: &str) -> Value {
    Value::Entity(uri.parse().expect("an entity"))
}

/// A list of `items`.
fn list(items: impl IntoIterator<Item = Value>) -> Value {
    Value::Composite(Composite::list(items).expect("a list"))
}

/// A map from text keys.
fn map<'a>(entries: impl IntoIterator<Item = (&'a str, Value)>) -> Value {
    Value::Composite(
        Composite::map(entries.into_iter().map(|(key, value)| (text(key), value))).expect("a map"),
    )
}

/// The composites written: a record, a map of text keys, a list with
/// repeats, a record nesting a map nesting a list, and a list past the
/// inline threshold, which spills to a block of its own.
fn composites() -> Result<Vec<Value>> {
    let record = Value::Composite(Composite::record([
        ("geo/lat".parse()?, Value::Float(37.77)),
        ("geo/lon".parse()?, Value::Float(-122.42)),
    ])?);
    let keyed = map([("type", entity("tuple:")), ("name", text("t"))]);
    let repeats = list([text("a"), text("b"), text("a")]);
    let nested = Value::Composite(Composite::record([
        ("doc/title".parse()?, text("notes")),
        (
            "doc/body".parse()?,
            map([("items", list([keyed.clone(), repeats.clone()]))]),
        ),
    ])?);
    let large = list((0..200).map(|n| text(&format!("item {n}"))));
    Ok(vec![record, keyed, repeats, nested, large])
}

async fn commit<S: PeerSpace>(
    branch: &Branch,
    facts: Vec<Fact>,
    session: &Peer<S, Session>,
) -> Result<()> {
    branch
        .commit(stream::iter(
            facts
                .into_iter()
                .map(|fact| Instruction::Assert(fact, Cardinality::Many)),
        ))
        .perform(session)
        .await?;
    Ok(())
}

async fn read<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    selector: Selector<Constrained>,
    session: &Peer<S, Session>,
) -> Result<Vec<Fact>> {
    let branch = repo.branch("main").load().perform(session).await?;
    Ok(branch
        .claims()
        .select(selector)
        .to_owned()
        .perform(session)
        .await?
        .try_collect::<Vec<Fact>>()
        .await?)
}

/// Writes every composite, then reads each back from a branch loaded
/// afresh, by its entity, by its attribute and by its value: equal, with
/// one identity, and a spilled one through its block.
async fn round_trip<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let values = composites()?;
    let facts: Vec<Fact> = values
        .iter()
        .map(|is| {
            Ok(Fact {
                the: "x/kept".parse()?,
                of: Entity::new()?,
                is: is.clone(),
                cause: None,
            })
        })
        .collect::<Result<_>>()?;
    commit(&branch, facts.clone(), session).await?;

    let all = read(&repo, Selector::new().the("x/kept".parse()?), session).await?;
    assert_eq!(all.len(), facts.len(), "every composite, by attribute");
    for fact in &facts {
        let by_entity = read(&repo, Selector::new().of(fact.of.clone()), session).await?;
        assert_eq!(by_entity.len(), 1);
        assert_eq!(by_entity[0].is, fact.is, "by its entity");
        assert_eq!(by_entity[0].is.reference(), fact.is.reference());
        let by_value = read(&repo, Selector::new().is(fact.is.clone()), session).await?;
        assert_eq!(
            by_value.iter().map(|f| &f.of).collect::<Vec<_>>(),
            vec![&fact.of],
            "by its value"
        );
    }
    Ok(())
}

/// Composites go into a tree on the volatile provider and come back.
#[dialog_common::test]
async fn it_round_trips_composites_through_a_volatile_tree() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    round_trip(&session, &peer).await
}

/// Composites go into a tree on the filesystem provider and come back.
///
/// Native only: the filesystem provider is the native one. In the browser
/// a tree is kept in IndexedDB and OPFS, which the web test run of
/// `it_round_trips_composites_through_a_volatile_tree` does not reach
/// either; the composite's bytes are the same on every provider, so what
/// this adds is the provider's own handling of the keys and blocks.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_round_trips_composites_through_a_filesystem_tree() -> Result<()> {
    use std::fs::read_dir;

    let root = tempfile::tempdir()?;
    use crate::helpers::filesystem_session_with_peer;

    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    round_trip(&session, &peer).await?;
    // Something was written under the directory, not elsewhere.
    assert!(read_dir(root.path())?.next().is_some());
    Ok(())
}

/// A node of the syntax tree as `syntax.rs` keeps it: a map that says its
/// `type`, given its fields in the order `order` puts them.
fn node(kind: &str, fields: Vec<(&str, Value)>, reverse: bool) -> Value {
    let mut fields = fields;
    fields.push(("type", entity(&format!("{kind}:"))));
    if reverse {
        fields.reverse();
    }
    map(fields)
}

/// `(todo ?t [1 2] state: :open tags: {"errand" _ "home" _} where: {:author ?who})`
/// as `Syntax::value` writes it, built from plain data: an application
/// with an unlabelled variable and tuple and three labelled parts, a set
/// (a map of its members, each to a blank) and a struct (a map with
/// entries). The notation spells a set `{: "errand" : "home"}`, which
/// `Syntax::value` refuses today for its repeated empty key.
fn syntax_tree(reverse: bool) -> Value {
    let variable = |name: &str| node("variable", vec![("name", text(name))], reverse);
    let constant = |value: Value| node("constant", vec![("value", value)], reverse);
    let reference = |name: &str| {
        node(
            "reference",
            vec![("name", Value::Symbol(name.parse().expect("a symbol")))],
            reverse,
        )
    };
    let entries = |pairs: Vec<(Value, Value)>| {
        let mut pairs: Vec<Value> = pairs
            .into_iter()
            .map(|(key, value)| list([key, value]))
            .collect();
        if reverse {
            pairs.reverse();
        }
        // A struct's entries are kept in the order of their keys' bytes.
        pairs.sort_by_key(|pair| match pair {
            Value::Composite(pair) => pair.item(0).map(|key| key.to_bytes()),
            _ => None,
        });
        node("map", vec![("entries", list(pairs))], reverse)
    };
    let tuple = node(
        "tuple",
        vec![(
            "items",
            list([
                constant(Value::UnsignedInt(1)),
                constant(Value::UnsignedInt(2)),
            ]),
        )],
        reverse,
    );
    let blank = || node("blank", vec![], reverse);
    let set = entries(vec![
        (constant(text("errand")), blank()),
        (constant(text("home")), blank()),
    ]);
    let author = entries(vec![(reference("/author"), variable("who"))]);
    let mut terms = vec![
        ("state", constant(entity("open:"))),
        ("tags", set),
        ("where", author),
    ];
    if reverse {
        terms.reverse();
    }
    let operand = node(
        "parameters",
        vec![
            ("items", list([variable("t"), tuple])),
            ("terms", map(terms)),
        ],
        reverse,
    );
    node(
        "application",
        vec![("operator", reference("/todo")), ("operand", operand)],
        reverse,
    )
}

fn step(value: Value, key: &str) -> Result<Value> {
    let Value::Composite(map) = value else {
        anyhow::bail!("{key}: not a composite");
    };
    map.first(&text(key)).with_context(|| format!("no {key}"))
}

fn index(value: Value, at: usize) -> Result<Value> {
    let Value::Composite(list) = value else {
        anyhow::bail!("{at}: not a composite");
    };
    list.item(at).with_context(|| format!("no item {at}"))
}

/// A syntax tree is kept as one composite in a branch: read back, a node
/// deep in it is reached by its path without decoding the nodes beside
/// it, and built in another order it is the same value with the same
/// merkle reference.
#[dialog_common::test]
async fn it_keeps_a_syntax_tree_and_reaches_a_node_in_place() -> Result<()> {
    let tree = syntax_tree(false);
    let again = syntax_tree(true);
    assert_eq!(tree, again);
    assert_eq!(tree.reference(), again.reference());
    assert_eq!(tree.to_bytes(), again.to_bytes());

    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    let of = Entity::new()?;
    commit(
        &branch,
        vec![Fact {
            the: "notation/form".parse()?,
            of: of.clone(),
            is: tree.clone(),
            cause: None,
        }],
        &session,
    )
    .await?;
    let read_back = read(&repo, Selector::new().of(of), &session).await?;
    let stored = read_back[0].is.clone();
    assert_eq!(stored, tree);
    assert_eq!(stored.reference(), tree.reference());

    // operand.terms.where.entries[0][1].name: the variable `?who`, five
    // composites down.
    let before = composite_decodes();
    let entries = step(
        step(step(step(stored, "operand")?, "terms")?, "where")?,
        "entries",
    )?;
    let who = step(index(index(entries, 0)?, 1)?, "name")?;
    let decoded = composite_decodes() - before;
    assert_eq!(who, text("who"));
    assert_eq!(decoded, 1, "only the name returned is decoded");
    Ok(())
}
