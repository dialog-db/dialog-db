//! What a rule fires for when a premise leaves its subject unnamed.
//!
//! Each scenario is one list of commits run twice: seeded as now, with
//! the subject bound to the entity a row is about, and as before, with
//! the row's value bound alone (`unnamed::as_before`). The two
//! outcomes are asserted side by side, so the tests say what changed
//! and would fail if either way moved.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::unnamed;
use crate::helpers::test_repo;
use crate::rules::Transient;
use crate::{Branch, Repository};
use anyhow::Result;
use dialog_corpus::{Changes, Entity, Instruction, Pick, Relation, Selector, Update, Value};
use dialog_credentials::Credential;
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::{DeductiveRule, InductiveRule};
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

/// Seeding as it was before for as long as this lives, and as it is
/// now when it drops.
struct AsBefore;

impl AsBefore {
    fn new() -> Self {
        unnamed::as_before(true);
        AsBefore
    }
}

impl Drop for AsBefore {
    fn drop(&mut self) {
        unnamed::as_before(false);
    }
}

/// How a scenario is seeded.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Seeded {
    /// The subject bound to the entity a row is about.
    Now,
    /// The row's value bound alone, as before.
    Before,
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
    changes.associate(relation(the), entity(of), is, Pick::Last);
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
    write(&mut changes, "evt/user", "evt:1", Value::Entity(entity("user:alice")));
    write(&mut changes, "evt/kind", "evt:1", text("click"));
    write(&mut changes, "evt/user", "evt:2", Value::Entity(entity("user:bob")));
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
            Instruction::Assert(fact, _) => {
                Some(format!("{} {} {:?}", fact.the, fact.of, fact.is))
            }
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

/// Commit the events, then install `rules` with `transient` marked
/// commands and `deductive` beside them.
async fn install(
    world: &World,
    rules: Vec<InductiveRule>,
    deductive: Vec<DeductiveRule>,
    transient: &[Entity],
    ground: Changes,
) -> Result<()> {
    commit(world, ground).await?;
    let mut transaction = world.branch.transaction();
    for concept in transient {
        transaction = transaction.assert(Transient(concept.clone()));
    }
    for rule in deductive {
        transaction = transaction.assert(rule);
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
async fn fired_when_a_kind_changes(named: bool, seeded: Seeded) -> Result<Vec<String>> {
    let _before = (seeded == Seeded::Before).then(AsBefore::new);
    let world = world().await?;
    let (rule, concept) = seen(named)?;
    install(&world, vec![rule], vec![], &[concept], events()).await?;

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

/// A rule that leaves a premise's subject unnamed is fired for the
/// entity a commit wrote. Before, it was fired for every entity holding
/// the value written, `alice` among them, whose event the commit did
/// not touch. The same rule with the event named, in a variable nothing
/// else reads, was fired for `bob` alone before, as it is now: the two
/// spellings mean one thing and now fire for one thing.
#[dialog_common::test]
async fn it_fires_for_the_entity_a_commit_wrote_and_no_other() -> Result<()> {
    assert_eq!(
        fired_when_a_kind_changes(false, Seeded::Now).await?,
        vec![bob_seen()],
        "the unnamed subject is bound to the event written"
    );
    assert_eq!(
        fired_when_a_kind_changes(false, Seeded::Before).await?,
        vec![alice_seen(), bob_seen()],
        "before, the value alone was bound, and alice's event holds it too"
    );
    assert_eq!(
        fired_when_a_kind_changes(true, Seeded::Now).await?,
        vec![bob_seen()],
        "the named subject is bound now"
    );
    assert_eq!(
        fired_when_a_kind_changes(true, Seeded::Before).await?,
        vec![bob_seen()],
        "as it was before"
    );
    Ok(())
}

/// What `alice` and `bob` hold under `user/kind` after: a rule records
/// the kind of each user's event, `alice`'s record is then written over
/// by hand, and `bob`'s event changes its kind to the kind of
/// `alice`'s.
async fn recorded_after_a_write_by_hand(seeded: Seeded) -> Result<(Vec<Value>, Vec<Value>)> {
    let _before = (seeded == Seeded::Before).then(AsBefore::new);
    let world = world().await?;
    let record = rule(kind_head("user/kind"), vec![event(false)], vec![])?;
    install(&world, vec![record], vec![], &[], events()).await?;

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

/// The firing for an untouched entity is not only work done twice: it
/// writes. A durable head under `last` writes its value again over
/// whatever was written since. Before, `bob`'s click put `alice`'s
/// record back to `click` over the value written by hand. Now a commit
/// about `bob` leaves `alice`'s record alone.
#[dialog_common::test]
async fn it_leaves_what_an_untouched_entity_holds() -> Result<()> {
    let (alice, bob) = recorded_after_a_write_by_hand(Seeded::Now).await?;
    assert_eq!(alice, vec![text("by hand")], "alice's record stands");
    assert_eq!(bob, vec![text("click")]);

    let (alice, bob) = recorded_after_a_write_by_hand(Seeded::Before).await?;
    assert_eq!(
        alice,
        vec![text("click")],
        "before, the rule fired for alice again and wrote over it"
    );
    assert_eq!(bob, vec![text("click")]);
    Ok(())
}

/// The counts of `alice` and `bob` after: a rule issues a `seen`
/// command for the user of an event, a second rule counts the commands
/// a user was sent, and `bob`'s event changes its kind to the kind of
/// `alice`'s.
async fn counted(seeded: Seeded) -> Result<(Vec<Value>, Vec<Value>)> {
    let _before = (seeded == Seeded::Before).then(AsBefore::new);
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
    write(&mut ground, "user/count", "user:alice", Value::UnsignedInt(0));
    write(&mut ground, "user/count", "user:bob", Value::UnsignedInt(0));
    install(&world, vec![issue, count], vec![], &[concept], ground).await?;

    let mut changes = Changes::new();
    write(&mut changes, "evt/kind", "evt:2", text("click"));
    commit(&world, changes).await?;
    Ok((
        values(&world, "user/count", "user:alice").await?,
        values(&world, "user/count", "user:bob").await?,
    ))
}

/// A command issued for an untouched entity is acted on like any
/// other. Installing the rules counts each user's event once. Before,
/// `bob`'s click then counted `alice` a second time; now it counts
/// `bob` alone.
#[dialog_common::test]
async fn it_issues_no_command_for_an_untouched_entity() -> Result<()> {
    let (alice, bob) = counted(Seeded::Now).await?;
    assert_eq!(alice, vec![Value::UnsignedInt(1)], "alice is counted once");
    assert_eq!(bob, vec![Value::UnsignedInt(2)]);

    let (alice, bob) = counted(Seeded::Before).await?;
    assert_eq!(
        alice,
        vec![Value::UnsignedInt(2)],
        "before, bob's click counted alice again"
    );
    assert_eq!(bob, vec![Value::UnsignedInt(2)]);
    Ok(())
}

/// A commit that writes two fields of one event seeds them together,
/// and together they find the event alone, before as now. Seeded one by
/// one, which is what the two together must equal, the kind alone found
/// `alice`'s event too before: this crate's check of one against the
/// other failed on such a commit, so it is run as now only. A commit
/// of one field fired for `alice` and a commit of two did not.
#[dialog_common::test]
async fn it_fires_alike_for_one_field_written_and_for_two() -> Result<()> {
    let world = world().await?;
    let (rule, concept) = seen(false)?;
    install(&world, vec![rule], vec![], &[concept], events()).await?;

    let mut changes = Changes::new();
    write(&mut changes, "evt/user", "evt:3", Value::Entity(entity("user:carol")));
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
/// everything, and fires for every user its body matches, before as
/// now.
#[dialog_common::test]
async fn it_still_fires_in_full_for_every_match() -> Result<()> {
    for seeded in [Seeded::Now, Seeded::Before] {
        let _before = (seeded == Seeded::Before).then(AsBefore::new);
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
        install(&world, vec![rule], vec![], &[concept], ground).await?;

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
            "{seeded:?}: a retraction under `unless` fires the rule in full"
        );
    }
    Ok(())
}

/// An event that mirrors another has its kind: `evt/kind` derived from
/// `evt/kind`.
fn mirror() -> Result<DeductiveRule> {
    Ok(serde_json::from_value(json!({
        "deduce": kind_head("evt/kind"),
        "when": [
            {
                "assert": { "with": { "of": { "the": "mirror/of", "as": "entity:" } } },
                "where": { "this": var("this"), "of": var("origin") }
            },
            {
                "assert": kind_head("evt/kind"),
                "where": { "this": var("origin"), "kind": var("kind") }
            }
        ]
    }))?)
}

/// `carol`'s event mirrors `alice`'s, and `alice`'s event changes its
/// kind. Who a rule over events is fired for.
async fn fired_through_a_mirror(named: bool, seeded: Seeded) -> Result<Vec<String>> {
    let _before = (seeded == Seeded::Before).then(AsBefore::new);
    let world = world().await?;
    let (rule, concept) = seen(named)?;
    let mut ground = events();
    write(&mut ground, "evt/user", "evt:9", Value::Entity(entity("user:carol")));
    write(&mut ground, "mirror/of", "evt:9", Value::Entity(entity("evt:1")));
    install(&world, vec![rule], vec![mirror()?], &[concept], ground).await?;

    let mut changes = Changes::new();
    write(&mut changes, "evt/kind", "evt:1", text("scroll"));
    commit(&world, changes).await
}

/// A row seeds the entity it is about. A fact a deductive rule derives
/// from that row for another entity is not that row, and is found by
/// evaluating the rule in full, which induction does for a premise that
/// changed derivedly. An attribute derived from itself changes
/// derivedly whenever it is written, so a rule premised on it is fired
/// in full: for `carol`, whose mirrored event changed its kind with
/// `alice`'s, and for `bob`, whose event matches as it did.
///
/// Before, the attribute counted as written and not as derived, and the
/// rule was seeded. With its subject unnamed the seed bound the value
/// alone and happened on `carol`'s derived event, which holds the same
/// value. So the narrower seed needs the wider reach, or it would lose
/// `carol`.
#[dialog_common::test]
async fn it_fires_in_full_when_a_written_attribute_is_derived_from_itself() -> Result<()> {
    let seen = |user: &str, kind: &str| format!("cmd.seen/kind user:{user} {:?}", text(kind));
    let everyone = vec![
        seen("alice", "scroll"),
        seen("bob", "view"),
        seen("carol", "scroll"),
    ];
    assert_eq!(
        fired_through_a_mirror(false, Seeded::Now).await?,
        everyone,
        "the unnamed subject"
    );
    assert_eq!(
        fired_through_a_mirror(true, Seeded::Now).await?,
        everyone,
        "the named subject"
    );
    Ok(())
}
