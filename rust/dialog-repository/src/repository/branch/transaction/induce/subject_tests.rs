//! What a rule fires for when a premise leaves its subject unnamed.
//!
//! `alice` has an event of kind `click` and `bob` one of kind `view`.
//! A rule reads an event's user and kind without naming the event. A
//! commit then writes one field of `bob`'s event, and the rule is to
//! fire for `bob`: `alice`'s event matches as it did before the commit
//! and nothing of it was written.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::helpers::test_repo;
use crate::rules::Transient;
use crate::{Branch, Repository};
use anyhow::Result;
use dialog_corpus::{Cardinality, Changes, Entity, Instruction, Relation, Selector, Update, Value};
use dialog_credentials::Credential;
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::InductiveRule;
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::StreamExt as _;
use serde_json::{Value as Json, json};

type Operator = Peer<VolatileSpace, Session>;

/// A branch of a fresh repository and the session that writes to it.
struct World {
    operator: Operator,
    // Held so the repository outlives the test's commits.
    _repository: Repository<Credential>,
    branch: Branch,
}

async fn world() -> Result<World> {
    let (operator, profile) = test_session_with_peer().await;
    let repository = test_repo(&operator, &profile).await;
    let branch = repository.branch("main").open().perform(&operator).await?;
    Ok(World {
        operator,
        _repository: repository,
        branch,
    })
}

fn var(name: &str) -> Json {
    json!({ "?": { "name": name } })
}

fn entity(name: &str) -> Entity {
    name.parse().expect("a test entity parses")
}

fn relation(the: &str) -> Relation {
    the.parse().expect("a test attribute parses")
}

fn text(value: &str) -> Value {
    Value::String(value.into())
}

fn write(changes: &mut Changes, the: &str, of: &str, is: Value) {
    changes.associate(relation(the), entity(of), is, Cardinality::One);
}

/// The premise reading an event's user into `this` and its kind into
/// `kind`: with the event named, in a variable nothing else reads, or
/// with its subject left unnamed.
fn event(named: bool) -> Json {
    let mut bound = json!({ "user": var("this"), "kind": var("kind") });
    if named {
        bound["this"] = var("event");
    }
    json!({
        "assert": {
            "with": {
                "user": { "the": "evt/user", "as": "entity:" },
                "kind": { "the": "evt/kind", "as": "text:" }
            }
        },
        "where": bound
    })
}

/// A head of one text field `kind` under `the`.
fn kind_head(the: &str) -> Json {
    json!({ "with": { "kind": { "the": the, "as": "text:" } } })
}

fn rule(head: Json, when: Vec<Json>, unless: Vec<Json>) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "assert!": head,
        "when": when,
        "unless": unless,
    }))?)
}

/// `alice` has an event of kind `click`, `bob` one of kind `view`.
fn events() -> Changes {
    let mut changes = Changes::new();
    write(
        &mut changes,
        "evt/user",
        "evt:1",
        Value::Entity(entity("user:alice")),
    );
    write(&mut changes, "evt/kind", "evt:1", text("click"));
    write(
        &mut changes,
        "evt/user",
        "evt:2",
        Value::Entity(entity("user:bob")),
    );
    write(&mut changes, "evt/kind", "evt:2", text("view"));
    changes
}

/// What `changes` asserts, a line a fact, sorted.
fn lines(changes: &Changes) -> Vec<String> {
    let mut lines: Vec<String> = changes
        .clone()
        .into_instructions()
        .into_iter()
        .filter_map(|instruction| match instruction {
            Instruction::Assert(fact, _) => Some(format!("{} {} {:?}", fact.the, fact.of, fact.is)),
            Instruction::Retract(_) => None,
        })
        .collect();
    lines.sort();
    lines
}

/// Commit `changes` and return the transients its rules emitted.
async fn commit(world: &World, changes: Changes) -> Result<Vec<String>> {
    let batch = world
        .branch
        .transaction()
        .assert(changes)
        .commit()
        .perform(&world.operator)
        .await?;
    let emitted = lines(batch.induced());
    batch.publish().perform(&world.operator).await?;
    world.branch.refresh(&world.operator).await?;
    Ok(emitted)
}

/// Commit `ground`, then install `rules` with `transient` marked
/// commands.
async fn install(
    world: &World,
    rules: Vec<InductiveRule>,
    transient: &[Entity],
    ground: Changes,
) -> Result<()> {
    commit(world, ground).await?;
    let mut transaction = world.branch.transaction();
    for concept in transient {
        transaction = transaction.assert(Transient(concept.clone()));
    }
    for rule in rules {
        transaction = transaction.assert(rule);
    }
    transaction
        .commit()
        .publish()
        .perform(&world.operator)
        .await?;
    world.branch.refresh(&world.operator).await?;
    Ok(())
}

/// The values `of` holds under `the`.
async fn values(world: &World, the: &str, of: &str) -> Result<Vec<Value>> {
    let selector = Selector::new().the(relation(the)).of(entity(of));
    let stream = world
        .branch
        .claims()
        .select(selector)
        .perform(&world.operator)
        .await?;
    let facts: Vec<_> = stream.collect().await;
    Ok(facts
        .into_iter()
        .map(|item| item.and_then(|view| view.to_owned()).map(|fact| fact.is))
        .collect::<Result<Vec<_>, _>>()?)
}

/// A rule that issues a `seen` command for the user of an event, with
/// its kind, and the concept to mark transient.
fn seen(named: bool) -> Result<(InductiveRule, Entity)> {
    let rule = rule(kind_head("cmd.seen/kind"), vec![event(named)], vec![])?;
    let concept = rule.conclusion().this();
    Ok((rule, concept))
}

/// `bob`'s event changes its kind to `click`, in a commit that writes
/// nothing of `alice`'s, whose event is a `click` already. Who the
/// rule is fired for, read from the commands it issues.
async fn fired_when_a_kind_changes(named: bool) -> Result<Vec<String>> {
    let world = world().await?;
    let (rule, concept) = seen(named)?;
    install(&world, vec![rule], &[concept], events()).await?;

    let mut changes = Changes::new();
    write(&mut changes, "evt/kind", "evt:2", text("click"));
    commit(&world, changes).await
}

fn alice_seen() -> String {
    format!("cmd.seen/kind user:alice {:?}", text("click"))
}

fn bob_seen() -> String {
    format!("cmd.seen/kind user:bob {:?}", text("click"))
}

/// A rule that leaves a premise's subject unnamed fires for the entity
/// a commit wrote. `bob`'s event becomes a `click`; `alice`'s event is
/// a `click` already and the commit writes nothing of it, so no command
/// is issued for `alice`. The same rule with the event named, in a
/// variable nothing else reads, means the same thing and fires for the
/// same user.
#[dialog_common::test]
async fn it_fires_for_the_entity_a_commit_wrote_and_no_other() -> Result<()> {
    assert_eq!(
        fired_when_a_kind_changes(true).await?,
        vec![bob_seen()],
        "with the event named, the rule fires for the event written"
    );
    assert_eq!(
        fired_when_a_kind_changes(false).await?,
        vec![bob_seen()],
        "with the event unnamed, it fires for the same event and not for \
         alice's, which holds the kind written and was not touched"
    );
    Ok(())
}

/// What `alice` and `bob` hold under `user/kind` after: a rule records
/// the kind of each user's event, `alice`'s record is then written over
/// by hand, and `bob`'s event changes its kind to the kind of
/// `alice`'s.
async fn recorded_after_a_write_by_hand() -> Result<(Vec<Value>, Vec<Value>)> {
    let world = world().await?;
    let record = rule(kind_head("user/kind"), vec![event(false)], vec![])?;
    install(&world, vec![record], &[], events()).await?;

    let mut by_hand = Changes::new();
    write(&mut by_hand, "user/kind", "user:alice", text("by hand"));
    commit(&world, by_hand).await?;

    let mut changes = Changes::new();
    write(&mut changes, "evt/kind", "evt:2", text("click"));
    commit(&world, changes).await?;
    Ok((
        values(&world, "user/kind", "user:alice").await?,
        values(&world, "user/kind", "user:bob").await?,
    ))
}

/// A firing for an untouched entity is not only work done twice: it
/// writes. A durable head under `last` writes its value again over
/// whatever was written since. A commit about `bob` leaves `alice`'s
/// record, written over by hand, as it is.
#[dialog_common::test]
async fn it_leaves_what_an_untouched_entity_holds() -> Result<()> {
    let (alice, bob) = recorded_after_a_write_by_hand().await?;
    assert_eq!(
        alice,
        vec![text("by hand")],
        "alice's record stands: the commit wrote nothing of her event"
    );
    assert_eq!(bob, vec![text("click")]);
    Ok(())
}

/// The counts of `alice` and `bob` after: a rule issues a `seen`
/// command for the user of an event, a second rule counts the commands
/// a user was sent, and `bob`'s event changes its kind to the kind of
/// `alice`'s.
async fn counted() -> Result<(Vec<Value>, Vec<Value>)> {
    let world = world().await?;
    let (issue, concept) = seen(false)?;
    let count = rule(
        json!({ "with": { "count": { "the": "user/count", "as": "natural:" } } }),
        vec![
            json!({
                "assert": kind_head("cmd.seen/kind"),
                "where": { "this": var("this"), "kind": var("kind") }
            }),
            json!({
                "assert": { "with": { "count": { "the": "user/count", "as": "natural:" } } },
                "where": { "this": var("this"), "count": var("prev") }
            }),
            json!({
                "assert": "math/sum",
                "where": { "of": var("prev"), "with": 1, "is": var("count") }
            }),
        ],
        vec![],
    )?;
    let mut ground = events();
    write(
        &mut ground,
        "user/count",
        "user:alice",
        Value::UnsignedInt(0),
    );
    write(&mut ground, "user/count", "user:bob", Value::UnsignedInt(0));
    install(&world, vec![issue, count], &[concept], ground).await?;

    let mut changes = Changes::new();
    write(&mut changes, "evt/kind", "evt:2", text("click"));
    commit(&world, changes).await?;
    Ok((
        values(&world, "user/count", "user:alice").await?,
        values(&world, "user/count", "user:bob").await?,
    ))
}

/// A command issued for an untouched entity is acted on like any
/// other. Installing the rules counts each user's event once. `bob`'s
/// click then counts `bob` again, and `alice` stays at one.
#[dialog_common::test]
async fn it_issues_no_command_for_an_untouched_entity() -> Result<()> {
    let (alice, bob) = counted().await?;
    assert_eq!(
        alice,
        vec![Value::UnsignedInt(1)],
        "alice is counted once: bob's click issues no command for her"
    );
    assert_eq!(bob, vec![Value::UnsignedInt(2)]);
    Ok(())
}

/// A commit that writes one field of an event and a commit that
/// writes two fire the rule alike: for the event written. Two rows of
/// one entity are seeded together and find that entity. Seeded one by
/// one, which is what the two together must equal and what this crate
/// checks them against in tests, the kind alone must not find `alice`'s
/// event as well.
#[dialog_common::test]
async fn it_fires_alike_for_one_field_written_and_for_two() -> Result<()> {
    let world = world().await?;
    let (rule, concept) = seen(false)?;
    install(&world, vec![rule], &[concept], events()).await?;

    let mut changes = Changes::new();
    write(
        &mut changes,
        "evt/user",
        "evt:3",
        Value::Entity(entity("user:carol")),
    );
    write(&mut changes, "evt/kind", "evt:3", text("click"));
    assert_eq!(
        commit(&world, changes).await?,
        vec![format!("cmd.seen/kind user:carol {:?}", text("click"))],
        "a whole event written fires for its user"
    );
    Ok(())
}

/// What is not seeded is not narrowed. A rule fired in full, here
/// because a fact its `unless` read is retracted, is evaluated over
/// everything, and fires for every user its body matches.
#[dialog_common::test]
async fn it_still_fires_in_full_for_every_match() -> Result<()> {
    let world = world().await?;
    let rule = rule(
        kind_head("cmd.seen/kind"),
        vec![event(false)],
        vec![json!({
            "assert": { "with": { "muted": { "the": "user/muted", "as": "boolean:" } } },
            "where": { "this": var("this"), "muted": true }
        })],
    )?;
    let concept = rule.conclusion().this();
    let mut ground = events();
    write(&mut ground, "user/muted", "user:bob", Value::Boolean(true));
    install(&world, vec![rule], &[concept], ground).await?;

    let mut unmute = Changes::new();
    unmute.dissociate(
        relation("user/muted"),
        entity("user:bob"),
        Value::Boolean(true),
    );
    assert_eq!(
        commit(&world, unmute).await?,
        vec![
            alice_seen(),
            format!("cmd.seen/kind user:bob {:?}", text("view")),
        ],
        "a retraction under `unless` fires the rule in full"
    );
    Ok(())
}
