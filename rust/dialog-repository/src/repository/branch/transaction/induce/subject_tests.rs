//! What a rule fires for when a premise leaves its subject unnamed.
//!
//! `alice` has an event of kind `click` and `bob` one of kind `view`.
//! A rule reads an event's user and kind without naming the event. A
//! commit then writes one field of `bob`'s event, and the rule is to
//! fire for `bob`: `alice`'s event matches as it did before the commit
//! and nothing of it was written.
//!
//! What a rule fires for when a deductive rule derives what it reads
//! from what a commit writes. A row seeds the fact it is, so a rule
//! premised on an attribute a deductive rule concludes from something
//! touched is fired in full, the attribute written or not. Each of
//! those tests checks the firings against full evaluations of the rule
//! before and after the commit (see [`Observed`]).

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
        transaction = transaction
            .assert(dialog_query::StorableRule::try_from(rule).expect("a storable rule"));
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

fn deductive(value: Json) -> Result<DeductiveRule> {
    Ok(serde_json::from_value(value)?)
}

/// A field reading `the` as text, holding many values when `many`.
fn text_field(the: &str, many: bool) -> Json {
    if many {
        json!({ "the": the, "as": "text:", "cardinality": "many" })
    } else {
        json!({ "the": the, "as": "text:" })
    }
}

/// A rule issuing a command under `head`, for the user of an event,
/// with each value of `read` the event holds; with the event named,
/// in a variable nothing else reads, or with its subject unnamed. The
/// command's field holds many values, so no firing hides another.
fn watch(head: &str, read: &str, many: bool, named: bool) -> Result<(InductiveRule, Entity)> {
    let mut bound = json!({ "user": var("this"), "kind": var("kind") });
    if named {
        bound["this"] = var("event");
    }
    let rule = rule(
        json!({ "with": { "kind": text_field(head, true) } }),
        vec![json!({
            "assert": {
                "with": {
                    "user": { "the": "evt/user", "as": "entity:" },
                    "kind": text_field(read, many)
                }
            },
            "where": bound
        })],
        vec![],
    )?;
    let concept = rule.conclusion().this();
    Ok((rule, concept))
}

/// What a rule over events fired for in a commit, and what full
/// evaluations of the same rule find before and after that commit,
/// each a sorted `user value` line a firing.
#[derive(Debug)]
struct Observed {
    fired: Vec<String>,
    before: Vec<String>,
    after: Vec<String>,
}

impl Observed {
    /// The firings a full evaluation finds after the commit and not
    /// before: the matches the commit made.
    fn made(&self) -> Vec<String> {
        self.after
            .iter()
            .filter(|line| !self.before.contains(line))
            .cloned()
            .collect()
    }

    /// The differential check: the rule fired for every match the
    /// commit made, and only for matches a full evaluation after it
    /// finds.
    fn check(&self) {
        for line in self.made() {
            assert!(
                self.fired.contains(&line),
                "the commit made the match {line} and the rule did not fire for it: {self:?}"
            );
        }
        for line in &self.fired {
            assert!(
                self.after.contains(line),
                "the rule fired for {line}, which a full evaluation does not find: {self:?}"
            );
        }
    }
}

/// The lines of `emitted` under `head`, without it.
fn under(emitted: &[String], head: &str) -> Vec<String> {
    let prefix = format!("{head} ");
    emitted
        .iter()
        .filter_map(|line| line.strip_prefix(&prefix).map(String::from))
        .collect()
}

/// One firing, as [`Observed`] lists it.
fn fired(user: &str, kind: &str) -> String {
    format!("user:{user} {:?}", text(kind))
}

/// A scenario: `ground` is committed, then `deductive` beside a rule
/// [`watch`]ing `read`, then `written`. With `install_with_write`, the
/// deductive rules are installed by the commit that writes `written`
/// instead.
struct Scenario {
    read: &'static str,
    many: bool,
    deductive: Vec<DeductiveRule>,
    ground: Changes,
    written: Changes,
    install_with_write: bool,
}

/// Runs `scenario` with the event named or not, and observes the
/// commit of what it writes.
async fn observe(scenario: Scenario, named: bool) -> Result<Observed> {
    let world = world().await?;
    commit(&world, scenario.ground).await?;

    let (rule, concept) = watch("cmd.seen/kind", scenario.read, scenario.many, named)?;
    let mut transaction = world
        .branch
        .transaction()
        .assert(Transient(concept))
        .assert(dialog_query::StorableRule::try_from(rule).expect("a storable rule"));
    if !scenario.install_with_write {
        for deduce in &scenario.deductive {
            transaction = transaction.assert(
                dialog_query::StorableRule::try_from(deduce.clone()).expect("a storable rule"),
            );
        }
    }
    transaction
        .commit()
        .publish()
        .perform(&world.operator)
        .await?;
    world.branch.refresh(&world.operator).await?;

    // A rule installed by a commit is evaluated in full over what the
    // commit leaves: one installed alone before the write, and one
    // installed by the write.
    let (before, concept) = watch("cmd.before/kind", scenario.read, scenario.many, named)?;
    let mut install = Changes::new();
    install.assert(Transient(concept));
    install.assert(dialog_query::StorableRule::try_from(before).expect("a storable rule"));
    let before = under(&commit(&world, install).await?, "cmd.before/kind");

    let (after, concept) = watch("cmd.after/kind", scenario.read, scenario.many, named)?;
    let mut changes = scenario.written;
    changes.assert(Transient(concept));
    changes.assert(dialog_query::StorableRule::try_from(after).expect("a storable rule"));
    if scenario.install_with_write {
        for deduce in scenario.deductive {
            changes.assert(dialog_query::StorableRule::try_from(deduce).expect("a storable rule"));
        }
    }
    let emitted = commit(&world, changes).await?;
    let observed = Observed {
        fired: under(&emitted, "cmd.seen/kind"),
        before,
        after: under(&emitted, "cmd.after/kind"),
    };
    observed.check();
    Ok(observed)
}

/// An event that mirrors another has its kind: `evt/kind` of one
/// event derived from `evt/kind` of another.
fn mirror() -> Result<DeductiveRule> {
    deductive(json!({
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
    }))
}

/// The events, and `carol`'s event mirroring `alice`'s.
fn mirrored() -> Changes {
    let mut ground = events();
    write(
        &mut ground,
        "evt/user",
        "evt:9",
        Value::Entity(entity("user:carol")),
    );
    write(
        &mut ground,
        "mirror/of",
        "evt:9",
        Value::Entity(entity("evt:1")),
    );
    ground
}

/// `alice`'s event changes its kind to `scroll`.
fn alice_scrolls() -> Changes {
    let mut written = Changes::new();
    write(&mut written, "evt/kind", "evt:1", text("scroll"));
    written
}

/// A row seeds the entity it is about, and a deductive rule derives
/// from it for another: `carol`'s event mirrors `alice`'s, so when
/// `alice`'s kind is written, `carol`'s changes too. The rule is fired
/// in full and finds `carol`. A full firing finds every match, so `bob`,
/// whose event matches as it did before, is fired for as well: the
/// cost of finding `carol`.
#[dialog_common::test]
async fn it_fires_for_an_entity_a_written_attribute_is_derived_for() -> Result<()> {
    for named in [false, true] {
        let observed = observe(
            Scenario {
                read: "evt/kind",
                many: false,
                deductive: vec![mirror()?],
                ground: mirrored(),
                written: alice_scrolls(),
                install_with_write: false,
            },
            named,
        )
        .await?;
        assert_eq!(
            observed.made(),
            vec![fired("alice", "scroll"), fired("carol", "scroll")],
            "named {named}: the write makes alice's match and carol's derived one"
        );
        assert_eq!(
            observed.fired,
            vec![
                fired("alice", "scroll"),
                fired("bob", "view"),
                fired("carol", "scroll"),
            ],
            "named {named}: fired in full"
        );
    }
    Ok(())
}

/// A deductive rule that derives an event's kind from that same
/// event's kind, and derives nothing new. The written attribute is
/// still concluded from something touched, so the rule over it is
/// fired in full: for `bob`, written, and for `alice`, matching as
/// before. Only a full firing is sound for every rule whose
/// conclusion is concluded from something touched, so this one, which
/// a seed would have served, is not told apart from the others.
#[dialog_common::test]
async fn it_fires_in_full_when_the_derivation_restates_the_entity_written() -> Result<()> {
    let restate = deductive(json!({
        "deduce": kind_head("evt/kind"),
        "when": [
            {
                "assert": kind_head("evt/kind"),
                "where": { "this": var("this"), "kind": var("kind") }
            },
            {
                "assert": { "with": { "user": { "the": "evt/user", "as": "entity:" } } },
                "where": { "this": var("this"), "user": var("user") }
            }
        ]
    }))?;
    let mut written = Changes::new();
    write(&mut written, "evt/kind", "evt:2", text("click"));
    let observed = observe(
        Scenario {
            read: "evt/kind",
            many: false,
            deductive: vec![restate],
            ground: events(),
            written,
            install_with_write: false,
        },
        false,
    )
    .await?;
    assert_eq!(observed.made(), vec![fired("bob", "click")]);
    assert_eq!(
        observed.fired,
        vec![fired("alice", "click"), fired("bob", "click")]
    );
    Ok(())
}

/// A deductive rule that derives at the entity it reads, at another
/// value: an event whose tags hold `click` is tagged with its alias as
/// well. Writing `click` to `bob`'s tags derives `tap` there too, which
/// a seed binding the value written does not find.
#[dialog_common::test]
async fn it_fires_for_a_value_derived_at_the_entity_written() -> Result<()> {
    let tags = || json!({ "with": { "kind": text_field("evt/tags", true) } });
    let alias = deductive(json!({
        "deduce": tags(),
        "when": [
            { "assert": tags(), "where": { "this": var("this"), "kind": "click" } },
            {
                "assert": { "with": { "alias": text_field("evt/alias", false) } },
                "where": { "this": var("this"), "alias": var("kind") }
            }
        ]
    }))?;
    let mut ground = events();
    write(&mut ground, "evt/alias", "evt:2", text("tap"));
    let mut written = Changes::new();
    written.associate(
        relation("evt/tags"),
        entity("evt:2"),
        text("click"),
        Cardinality::Many,
    );
    let observed = observe(
        Scenario {
            read: "evt/tags",
            many: true,
            deductive: vec![alias],
            ground,
            written,
            install_with_write: false,
        },
        false,
    )
    .await?;
    assert_eq!(
        observed.fired,
        vec![fired("bob", "click"), fired("bob", "tap")],
        "tap is derived at bob's event from the click written there"
    );
    assert_eq!(observed.made(), observed.fired);
    Ok(())
}

/// A deductive rule that derives an event's kind from its tag, in a
/// commit that writes `alice`'s kind and `bob`'s tag. The tag's row
/// seeds nothing of a rule that reads only kinds, so `bob`'s derived
/// kind is found only in full.
#[dialog_common::test]
async fn it_fires_for_an_attribute_derived_from_another_written_beside_it() -> Result<()> {
    let tagged = deductive(json!({
        "deduce": kind_head("evt/kind"),
        "when": [{
            "assert": { "with": { "tag": text_field("evt/tag", false) } },
            "where": { "this": var("this"), "tag": var("kind") }
        }]
    }))?;
    let mut written = alice_scrolls();
    write(&mut written, "evt/tag", "evt:2", text("drag"));
    let observed = observe(
        Scenario {
            read: "evt/kind",
            many: false,
            deductive: vec![tagged],
            ground: events(),
            written,
            install_with_write: false,
        },
        false,
    )
    .await?;
    assert_eq!(
        observed.fired,
        vec![fired("alice", "scroll"), fired("bob", "drag")]
    );
    assert_eq!(observed.made(), observed.fired);
    Ok(())
}

/// A chain: one rule derives `evt/shown` of an event from its own kind,
/// a second derives the kind of a mirroring event from the `evt/shown`
/// of the event it mirrors. Writing `alice`'s kind reaches `carol`'s
/// through both.
#[dialog_common::test]
async fn it_fires_for_an_entity_reached_through_a_chain_of_rules() -> Result<()> {
    let shown = deductive(json!({
        "deduce": { "with": { "shown": text_field("evt/shown", false) } },
        "when": [{
            "assert": kind_head("evt/kind"),
            "where": { "this": var("this"), "kind": var("shown") }
        }]
    }))?;
    let copied = deductive(json!({
        "deduce": kind_head("evt/kind"),
        "when": [
            {
                "assert": { "with": { "of": { "the": "mirror/of", "as": "entity:" } } },
                "where": { "this": var("this"), "of": var("origin") }
            },
            {
                "assert": { "with": { "shown": text_field("evt/shown", false) } },
                "where": { "this": var("origin"), "shown": var("kind") }
            }
        ]
    }))?;
    let observed = observe(
        Scenario {
            read: "evt/kind",
            many: false,
            deductive: vec![shown, copied],
            ground: mirrored(),
            written: alice_scrolls(),
            install_with_write: false,
        },
        false,
    )
    .await?;
    assert_eq!(
        observed.made(),
        vec![fired("alice", "scroll"), fired("carol", "scroll")]
    );
    assert_eq!(
        observed.fired,
        vec![
            fired("alice", "scroll"),
            fired("bob", "view"),
            fired("carol", "scroll"),
        ]
    );
    Ok(())
}

/// A deductive rule with several premises, one reading the event's own
/// tags and one the tags of the event it mirrors: a mirroring event
/// that is tagged takes the tags of its origin too. Tagging `alice`'s
/// event tags `carol`'s.
#[dialog_common::test]
async fn it_fires_through_a_rule_with_premises_on_several_entities() -> Result<()> {
    let tags = || json!({ "with": { "kind": text_field("evt/tags", true) } });
    let inherit = deductive(json!({
        "deduce": tags(),
        "when": [
            { "assert": tags(), "where": { "this": var("this"), "kind": var("own") } },
            {
                "assert": { "with": { "of": { "the": "mirror/of", "as": "entity:" } } },
                "where": { "this": var("this"), "of": var("origin") }
            },
            { "assert": tags(), "where": { "this": var("origin"), "kind": var("kind") } }
        ]
    }))?;
    let mut ground = mirrored();
    let tag = |changes: &mut Changes, of: &str, value: &str| {
        changes.associate(
            relation("evt/tags"),
            entity(of),
            text(value),
            Cardinality::Many,
        );
    };
    tag(&mut ground, "evt:1", "click");
    tag(&mut ground, "evt:9", "own");
    let mut written = Changes::new();
    tag(&mut written, "evt:1", "scroll");
    let observed = observe(
        Scenario {
            read: "evt/tags",
            many: true,
            deductive: vec![inherit],
            ground,
            written,
            install_with_write: false,
        },
        false,
    )
    .await?;
    assert_eq!(
        observed.made(),
        vec![fired("alice", "scroll"), fired("carol", "scroll")]
    );
    assert_eq!(
        observed.fired,
        vec![
            fired("alice", "click"),
            fired("alice", "scroll"),
            fired("carol", "click"),
            fired("carol", "own"),
            fired("carol", "scroll"),
        ]
    );
    Ok(())
}

/// The mirror installed by the commit that writes `alice`'s kind: what
/// it concludes is derived in that commit, and `carol`'s kind, which
/// it derives for the first time, is found.
#[dialog_common::test]
async fn it_fires_for_what_a_rule_installed_with_the_write_derives() -> Result<()> {
    let observed = observe(
        Scenario {
            read: "evt/kind",
            many: false,
            deductive: vec![mirror()?],
            ground: mirrored(),
            written: alice_scrolls(),
            install_with_write: true,
        },
        false,
    )
    .await?;
    assert_eq!(
        observed.made(),
        vec![fired("alice", "scroll"), fired("carol", "scroll")]
    );
    assert_eq!(
        observed.fired,
        vec![
            fired("alice", "scroll"),
            fired("bob", "view"),
            fired("carol", "scroll"),
        ]
    );
    Ok(())
}

/// A retraction touches its attribute as an assertion does: retracting
/// `alice`'s tag retracts `carol`'s inherited one, and the rule is
/// fired in full over what is left. The commit makes no match, and the
/// firings are those a full evaluation finds.
#[dialog_common::test]
async fn it_fires_in_full_when_a_derived_attribute_is_retracted() -> Result<()> {
    let tags = || json!({ "with": { "kind": text_field("evt/tags", true) } });
    let inherit = deductive(json!({
        "deduce": tags(),
        "when": [
            {
                "assert": { "with": { "of": { "the": "mirror/of", "as": "entity:" } } },
                "where": { "this": var("this"), "of": var("origin") }
            },
            { "assert": tags(), "where": { "this": var("origin"), "kind": var("kind") } }
        ]
    }))?;
    let mut ground = mirrored();
    ground.associate(
        relation("evt/tags"),
        entity("evt:1"),
        text("click"),
        Cardinality::Many,
    );
    ground.associate(
        relation("evt/tags"),
        entity("evt:2"),
        text("view"),
        Cardinality::Many,
    );
    let mut written = Changes::new();
    written.dissociate(relation("evt/tags"), entity("evt:1"), text("click"));
    let observed = observe(
        Scenario {
            read: "evt/tags",
            many: true,
            deductive: vec![inherit],
            ground,
            written,
            install_with_write: false,
        },
        false,
    )
    .await?;
    assert_eq!(observed.made(), Vec::<String>::new());
    assert_eq!(observed.fired, vec![fired("bob", "view")]);
    Ok(())
}

/// The wider reach is only where a derivation is: with the mirror
/// installed, a commit writing an event's user, which no deductive
/// rule reads or concludes, seeds the rule, and `alice`'s record,
/// written over by hand, stands.
#[dialog_common::test]
async fn it_leaves_an_untouched_entity_when_no_rule_derives_what_is_written() -> Result<()> {
    let world = world().await?;
    let record = rule(kind_head("user/kind"), vec![event(false)], vec![])?;
    let mut ground = mirrored();
    write(&mut ground, "evt/kind", "evt:3", text("drag"));
    commit(&world, ground).await?;
    world
        .branch
        .transaction()
        .assert(dialog_query::StorableRule::try_from(mirror()?).expect("a storable rule"))
        .assert(dialog_query::StorableRule::try_from(record).expect("a storable rule"))
        .commit()
        .publish()
        .perform(&world.operator)
        .await?;
    world.branch.refresh(&world.operator).await?;

    let mut by_hand = Changes::new();
    write(&mut by_hand, "user/kind", "user:alice", text("by hand"));
    commit(&world, by_hand).await?;

    let mut written = Changes::new();
    write(
        &mut written,
        "evt/user",
        "evt:3",
        Value::Entity(entity("user:dave")),
    );
    commit(&world, written).await?;
    assert_eq!(
        values(&world, "user/kind", "user:alice").await?,
        vec![text("by hand")]
    );
    assert_eq!(
        values(&world, "user/kind", "user:dave").await?,
        vec![text("drag")]
    );
    Ok(())
}
