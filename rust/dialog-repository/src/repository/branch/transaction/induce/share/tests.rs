//! Rules of a round that state one body: that it is evaluated once,
//! which rules share and which do not, and that every commit writes
//! what it writes with sharing turned off.
//!
//! Three things hold these tests to the optimization and not to the
//! engine under it:
//!
//! - Every rule fired from another rule's matches is also evaluated on
//!   its own, in every test of this crate, and the two must write the
//!   same (`oracle::shares` in `induce.rs`).
//! - The tests here run their commits a second time with sharing off
//!   ([`probe::off`]) and compare what was committed and reported.
//! - They count what was shared ([`probe::take`]), so none of them
//!   passes because nothing was.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::super::oracle;
use super::{order_decides, probe};
use crate::helpers::test_repo;
use crate::rules::{Induced, Transient};
use crate::{Branch, Repository};
use anyhow::Result;
use dialog_corpus::{Changes, Entity, Instruction, Pick, Relation, Selector, Update, Value};
use dialog_credentials::Credential;
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::InductiveRule;
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::StreamExt as _;
use serde_json::{Value as Json, json};

type Operator = Peer<VolatileSpace, Session>;

/// Every attribute a test here reads back.
const ATTRIBUTES: &[&str] = &[
    "item/list",
    "item/tag",
    "item/rank",
    "item/archived",
    "list/owner",
    "out.a/by",
    "out.a/list",
    "out.a/owner",
    "out.a/tag",
    "out.a/rank",
    "out.b/by",
    "out.b/list",
    "out.b/owner",
    "out.b/tag",
    "out.b/rank",
    "out.c/rank",
    "out.noted/by",
    "out.noted/who",
    "cmd.note/by",
];

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

/// Sharing off for as long as this lives, and back on when it drops,
/// whatever the test did in between.
struct Unshared;

impl Unshared {
    fn new() -> Self {
        probe::off(true);
        Unshared
    }
}

impl Drop for Unshared {
    fn drop(&mut self) {
        probe::off(false);
    }
}

fn var(name: &str) -> Json {
    json!({ "?": { "name": name } })
}

/// A concept field reading or writing `the` as `kind` under `pick`.
fn field(the: &str, kind: &str, pick: &str) -> Json {
    json!({ "the": the, "as": kind, "pick": pick })
}

/// A premise or head of the fields `fields`, each `(name, descriptor)`.
fn concept(fields: &[(&str, Json)]) -> Json {
    let with: serde_json::Map<String, Json> = fields
        .iter()
        .map(|(name, descriptor)| ((*name).to_string(), descriptor.clone()))
        .collect();
    json!({ "with": with })
}

/// The premise applying `concept` with `terms`, each `(field, variable)`.
fn premise(concept: Json, terms: &[(&str, &str)]) -> Json {
    let bound: serde_json::Map<String, Json> = terms
        .iter()
        .map(|(name, variable)| ((*name).to_string(), var(variable)))
        .collect();
    json!({ "assert": concept, "where": bound })
}

fn rule(polarity: &str, head: Json, when: Vec<Json>, unless: Vec<Json>) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        polarity: head,
        "when": when,
        "unless": unless,
    }))?)
}

fn entity(name: &str) -> Entity {
    name.parse().expect("a test entity parses")
}

fn relation(the: &str) -> Relation {
    the.parse().expect("a test attribute parses")
}

fn assert(changes: &mut Changes, the: &str, of: &str, is: Value, pick: Pick) {
    changes.associate(relation(the), entity(of), is, pick);
}

/// What `changes` holds, a line a change, sorted: cells in a fixed
/// order, and a cell's changes in the order they were recorded, which
/// is what a commit applies them in.
fn lines(changes: &Changes) -> Vec<String> {
    changes
        .clone()
        .into_instructions()
        .into_iter()
        .map(|instruction| match instruction {
            Instruction::Assert(fact, pick) => {
                format!("assert {} {} {:?} {:?}", fact.of, fact.the, fact.is, pick)
            }
            Instruction::Retract(fact) => {
                format!("retract {} {} {:?}", fact.of, fact.the, fact.is)
            }
        })
        .collect()
}

/// Commit `changes` with `commands` dispatched, and return every
/// instruction the commit stages and every transient its rules
/// emitted.
async fn commit(world: &World, changes: Changes, commands: Changes) -> Result<Vec<String>> {
    let batch = world
        .branch
        .transaction()
        .assert(changes)
        .dispatch(commands)
        .commit()
        .perform(&world.operator)
        .await?;
    let mut written = lines(batch.induced());
    written.push("--".into());
    batch.publish().perform(&world.operator).await?;
    world.branch.refresh(&world.operator).await?;
    Ok(written)
}

/// Every fact the branch holds under [`ATTRIBUTES`], sorted.
async fn facts(world: &World) -> Result<Vec<String>> {
    let mut all = Vec::new();
    for the in ATTRIBUTES {
        let selector = Selector::new().the(relation(the));
        let stream = world
            .branch
            .claims()
            .select(selector)
            .perform(&world.operator)
            .await?;
        let found: Vec<_> = stream.collect().await;
        for item in found {
            let fact = item.and_then(|view| view.to_owned())?;
            all.push(format!("{} {} {:?}", fact.the, fact.of, fact.is));
        }
    }
    all.sort();
    Ok(all)
}

/// The command premise: a `touch` of `this` by `by`.
fn touched(by: &str) -> Json {
    premise(
        concept(&[
            ("target", field("cmd.touch/target", "entity:", "last:")),
            ("by", field("cmd.touch/by", "entity:", "last:")),
        ]),
        &[("target", "this"), ("by", by)],
    )
}

/// The list `this` is on.
fn listed(list: &str) -> Json {
    premise(
        concept(&[("list", field("item/list", "entity:", "last:"))]),
        &[("this", "this"), ("list", list)],
    )
}

/// The owner of the list `list`.
fn owned(list: &str, owner: &str) -> Json {
    premise(
        concept(&[("owner", field("list/owner", "entity:", "last:"))]),
        &[("this", list), ("owner", owner)],
    )
}

/// Each tag of `this`.
fn tagged(tag: &str) -> Json {
    premise(
        concept(&[("tag", field("item/tag", "text:", "all:"))]),
        &[("this", "this"), ("tag", tag)],
    )
}

/// The rank of `this`, a number of any kind.
fn ranked(rank: &str) -> Json {
    premise(
        concept(&[("rank", field("item/rank", "number:", "last:"))]),
        &[("this", "this"), ("rank", rank)],
    )
}

/// A head of one field named for `variable`, writing it under `the`.
fn head(variable: &str, the: &str, kind: &str, pick: &str) -> Json {
    concept(&[(variable, field(the, kind, pick))])
}

/// Items on lists with owners, tags and ranks, for rules to read.
fn ground() -> Changes {
    let mut changes = Changes::new();
    for item in 0..4 {
        let of = format!("item:{item}");
        assert(
            &mut changes,
            "item/list",
            &of,
            Value::Entity(entity(&format!("list:{}", item % 2))),
            Pick::Last,
        );
        for tag in ["red", "green"].iter().take(1 + item % 2) {
            assert(
                &mut changes,
                "item/tag",
                &of,
                Value::String((*tag).into()),
                Pick::All,
            );
        }
    }
    assert(
        &mut changes,
        "item/rank",
        "item:0",
        Value::UnsignedInt(7),
        Pick::Last,
    );
    assert(
        &mut changes,
        "item/rank",
        "item:1",
        Value::SignedInt(-5),
        Pick::Last,
    );
    for list in 0..2 {
        assert(
            &mut changes,
            "list/owner",
            &format!("list:{list}"),
            Value::Entity(entity(&format!("user:{list}"))),
            Pick::Last,
        );
    }
    changes
}

/// Add a `touch` of `item` by `user` to `commands`, as command
/// `command`.
fn touch_into(commands: &mut Changes, command: usize, item: usize, user: usize) {
    let of = format!("cmd:{command}");
    assert(
        commands,
        "cmd.touch/target",
        &of,
        Value::Entity(entity(&format!("item:{item}"))),
        Pick::Last,
    );
    assert(
        commands,
        "cmd.touch/by",
        &of,
        Value::Entity(entity(&format!("user:{user}"))),
        Pick::Last,
    );
}

/// A `touch` of `item` by `user`, as command `command`.
fn touch(command: usize, item: usize, user: usize) -> Changes {
    let mut commands = Changes::new();
    touch_into(&mut commands, command, item, user);
    commands
}

/// Install `rules` over the ground facts, each of `transient` marked a
/// command.
async fn install(world: &World, rules: &[InductiveRule], transient: &[Entity]) -> Result<()> {
    commit(world, ground(), Changes::new()).await?;
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

/// What one way of running a list of commits left behind.
#[derive(Debug, PartialEq)]
struct Outcome {
    /// What each commit reported, in order.
    reported: Vec<String>,
    /// What the branch holds at the end.
    facts: Vec<String>,
    /// How many rules wrote from another rule's matches, and how many
    /// that could have were evaluated on their own for the order's sake.
    shared: (usize, usize),
    /// How many seeded evaluations ran.
    evaluations: usize,
}

/// Install `rules` and run `commits`, each the changes and the commands
/// of one commit, with sharing on or off.
async fn run(
    rules: &[InductiveRule],
    transient: &[Entity],
    commits: &[(Changes, Changes)],
    share: bool,
) -> Result<Outcome> {
    let _unshared = (!share).then(Unshared::new);
    let world = world().await?;
    install(&world, rules, transient).await?;
    probe::take();
    oracle::evaluations();
    let mut reported = Vec::new();
    for (changes, commands) in commits {
        reported.extend(commit(&world, changes.clone(), commands.clone()).await?);
    }
    Ok(Outcome {
        reported,
        facts: facts(&world).await?,
        shared: probe::take(),
        evaluations: oracle::evaluations(),
    })
}

/// Run `commits` both ways and require one outcome but for the counts,
/// which are returned: shared, then unshared.
async fn both(
    rules: &[InductiveRule],
    transient: &[Entity],
    commits: &[(Changes, Changes)],
) -> Result<(Outcome, Outcome)> {
    let shared = run(rules, transient, commits, true).await?;
    let unshared = run(rules, transient, commits, false).await?;
    assert_eq!(
        shared.reported, unshared.reported,
        "sharing a body must not change what a commit reports"
    );
    assert_eq!(
        shared.facts, unshared.facts,
        "sharing a body must not change what is committed"
    );
    assert_eq!(
        unshared.shared,
        (0, 0),
        "with sharing off no rule reads another's matches"
    );
    Ok((shared, unshared))
}

/// Two rules that state one body and conclude different things from it
/// are evaluated once: the second writes its head from the first's
/// matches. With sharing off the same commit evaluates the body twice
/// and writes the same.
#[dialog_common::test]
async fn it_evaluates_once_a_body_two_rules_state() -> Result<()> {
    let rules = [
        rule(
            "assert!",
            head("by", "out.a/by", "entity:", "last:"),
            vec![touched("by"), listed("list")],
            vec![],
        )?,
        rule(
            "assert!",
            head("list", "out.a/list", "entity:", "last:"),
            vec![touched("by"), listed("list")],
            vec![],
        )?,
    ];
    let commits = [(Changes::new(), touch(1, 2, 1))];
    let (shared, unshared) = both(&rules, &[], &commits).await?;

    assert!(
        shared.facts.contains(&format!(
            "out.a/by item:2 {:?}",
            Value::Entity(entity("user:1"))
        )),
        "the first head is written: {:?}",
        shared.facts
    );
    assert!(
        shared.facts.contains(&format!(
            "out.a/list item:2 {:?}",
            Value::Entity(entity("list:0"))
        )),
        "the second head is written: {:?}",
        shared.facts
    );
    assert_eq!(
        shared.shared,
        (1, 0),
        "one of the two rules reads the other's matches"
    );
    assert!(shared.evaluations > 0);
    assert_eq!(
        unshared.evaluations,
        2 * shared.evaluations,
        "the body is evaluated once, where it was evaluated once a rule"
    );
    Ok(())
}

/// A round's candidates are partitioned by body, not shared all or
/// none: three rules of one body, two of another and one alone are
/// three evaluations, where they were six.
#[dialog_common::test]
async fn it_partitions_a_round_by_body() -> Result<()> {
    let near = || vec![touched("by"), listed("list")];
    let far = || vec![touched("by"), listed("list"), owned("list", "owner")];
    let rules = [
        rule(
            "assert!",
            head("by", "out.a/by", "entity:", "last:"),
            near(),
            vec![],
        )?,
        rule(
            "assert!",
            head("list", "out.a/list", "entity:", "last:"),
            near(),
            vec![],
        )?,
        rule(
            "assert!",
            head("by", "out.b/by", "entity:", "all:"),
            near(),
            vec![],
        )?,
        rule(
            "assert!",
            head("owner", "out.a/owner", "entity:", "last:"),
            far(),
            vec![],
        )?,
        rule(
            "assert!",
            head("list", "out.b/list", "entity:", "last:"),
            far(),
            vec![],
        )?,
        rule(
            "assert!",
            head("tag", "out.a/tag", "text:", "all:"),
            vec![touched("by"), tagged("tag")],
            vec![],
        )?,
    ];
    let commits = [(Changes::new(), touch(1, 1, 0))];
    let (shared, unshared) = both(&rules, &[], &commits).await?;

    assert_eq!(
        shared.shared,
        (3, 0),
        "two rules read the first body's matches and one the second's"
    );
    assert_eq!(
        unshared.evaluations,
        2 * shared.evaluations,
        "six rules, each seeded alike, are three evaluations"
    );
    for the in [
        "out.a/by",
        "out.a/list",
        "out.b/by",
        "out.a/owner",
        "out.b/list",
        "out.a/tag",
    ] {
        assert!(
            shared.facts.iter().any(|fact| fact.starts_with(the)),
            "{the} is written: {:?}",
            shared.facts
        );
    }
    Ok(())
}

/// What makes two bodies one is their premises under a renaming of
/// variables, in any order: names and order do not keep two rules from
/// sharing, and a different join does.
#[dialog_common::test]
async fn it_shares_under_a_renaming_and_not_across_meanings() -> Result<()> {
    let named = rule(
        "assert!",
        head("by", "out.a/by", "entity:", "last:"),
        vec![touched("by"), listed("list"), owned("list", "owner")],
        vec![],
    )?;
    let renamed = rule(
        "assert!",
        head("boss", "out.a/owner", "entity:", "last:"),
        vec![owned("where", "boss"), listed("where"), touched("who")],
        vec![],
    )?;
    // The same three premises, but the owner read is of the one who
    // touched, not of the list.
    let other = rule(
        "assert!",
        head("owner", "out.b/owner", "entity:", "last:"),
        vec![touched("by"), listed("list"), owned("by", "owner")],
        vec![],
    )?;

    let body = |rule: &InductiveRule| Induced::new(rule.clone());
    let (named_body, renamed_body, other_body) = (body(&named), body(&renamed), body(&other));
    let same = |a: &Induced, b: &Induced| {
        a.body()
            .zip(b.body())
            .is_some_and(|(a, b)| a.bucket() == b.bucket() && a.same(b))
    };
    assert!(same(&named_body, &renamed_body), "names and order aside");
    assert!(!same(&named_body, &other_body), "another join");

    // `boss` in the renamed rule stands where `owner` does in the
    // named one, and a name the body does not bind stands nowhere.
    let reads = renamed_body
        .body()
        .expect("a body")
        .reads(named_body.body().expect("a body"));
    assert!(
        reads
            .variable("boss")
            .is_some_and(|owner| owner != "by" && owner != "this"),
        "`boss` reads a local of the named rule, not the head it concludes"
    );
    assert_eq!(reads.variable("this"), Some("this"));
    assert_eq!(reads.variable("nobody"), None);

    let commits = [(Changes::new(), touch(1, 3, 0))];
    let (shared, _) = both(&[named, renamed, other], &[], &commits).await?;
    assert_eq!(shared.shared, (1, 0), "two of the three share");
    assert!(
        shared.facts.contains(&format!(
            "out.a/owner item:3 {:?}",
            Value::Entity(entity("user:1"))
        )),
        "the renamed rule wrote the owner of the list: {:?}",
        shared.facts
    );
    Ok(())
}

/// A head's type reaches the body: a numeric head field narrows a
/// number the body left open, so the scan reads that type alone. Two
/// rules with the same premises and heads that narrow one variable
/// differently do not state one body, and are not shared: under the
/// natural head's typing the integer head would lose its negative
/// match.
#[dialog_common::test]
async fn it_does_not_share_bodies_their_heads_type_differently() -> Result<()> {
    let natural = rule(
        "assert!",
        head("rank", "out.a/rank", "natural:", "last:"),
        vec![touched("by"), ranked("rank")],
        vec![],
    )?;
    let integer = rule(
        "assert!",
        head("rank", "out.b/rank", "integer:", "last:"),
        vec![touched("by"), ranked("rank")],
        vec![],
    )?;
    // A second integer head types the body as the first does.
    let again = rule(
        "assert!",
        head("rank", "out.c/rank", "integer:", "last:"),
        vec![touched("by"), ranked("rank")],
        vec![],
    )?;

    let (a, b, c) = (
        Induced::new(natural.clone()),
        Induced::new(integer.clone()),
        Induced::new(again.clone()),
    );
    let (a, b, c) = (
        a.body().expect("a body"),
        b.body().expect("a body"),
        c.body().expect("a body"),
    );
    assert_eq!(
        a.premises(),
        b.premises(),
        "the premises are the same under their own names"
    );
    assert!(!a.same(b), "the types the heads gave them are not");
    assert!(b.same(c), "two heads of one type leave one body");

    // item:1 ranks -5 and item:0 ranks 7.
    let commits = [
        (Changes::new(), touch(1, 1, 0)),
        (Changes::new(), touch(2, 0, 0)),
    ];
    let (shared, _) = both(&[natural, integer, again], &[], &commits).await?;
    assert_eq!(
        shared.shared,
        (2, 0),
        "the two integer rules share, a commit each; the natural one never"
    );
    let negative = format!("{:?}", Value::SignedInt(-5));
    assert!(
        shared
            .facts
            .contains(&format!("out.b/rank item:1 {negative}"))
            && shared
                .facts
                .contains(&format!("out.c/rank item:1 {negative}")),
        "the integer heads read the negative rank: {:?}",
        shared.facts
    );
    assert!(
        !shared
            .facts
            .iter()
            .any(|fact| fact.starts_with("out.a/rank item:1")),
        "the natural head does not: {:?}",
        shared.facts
    );
    Ok(())
}

/// Matches come in the order of the rule they were found under, which
/// is not every sharing rule's own. Where the order decides what is
/// written, two tags to one cell under `last` or two members of one
/// set, the rule is evaluated on its own.
#[dialog_common::test]
async fn it_evaluates_a_rule_alone_when_the_order_of_matches_decides() -> Result<()> {
    let body = || vec![touched("by"), tagged("tag")];
    let rules = [
        rule(
            "assert!",
            head("tag", "out.a/tag", "text:", "last:"),
            body(),
            vec![],
        )?,
        rule(
            "assert!",
            head("tag", "out.b/tag", "text:", "all:"),
            body(),
            vec![],
        )?,
        rule(
            "assert!",
            head("by", "out.a/by", "entity:", "last:"),
            body(),
            vec![],
        )?,
    ];
    // item:1 has two tags, so each touch of it matches twice; item:0
    // has one.
    let commits = [
        (Changes::new(), touch(1, 1, 0)),
        (Changes::new(), touch(2, 0, 0)),
    ];
    let (shared, _) = both(&rules, &[], &commits).await?;
    // Of the three rules one is first. Touching item:1, the two tag
    // heads write two values to one cell and the `by` head writes one
    // value twice; touching item:0, every head writes one value.
    let (read, alone) = shared.shared;
    assert_eq!(read + alone, 4, "two rules a commit are not the first");
    assert!(
        alone >= 1,
        "a head that writes two tags to one cell is evaluated alone"
    );
    assert!(
        read >= 2,
        "where a cell is written once, the matches are read"
    );
    Ok(())
}

/// Two rules of one body may list its premises in two orders: a stored
/// rule's premises are sorted under names its head takes part in
/// choosing. Matches are found premise by premise, so the two rules
/// find the same matches in two orders, and where two matches write
/// one cell under `last` the two rules elect different values. A rule
/// that reads another's matches must still write what it writes alone.
///
/// Eight rules state one body, two premises alike but for their
/// variable, and differ in the attribute they write. The test requires
/// that both orders occur among them and that somewhere the two orders
/// do elect different values, so it cannot pass by every rule agreeing
/// with the first.
#[dialog_common::test]
async fn it_writes_in_its_own_order_a_body_two_rules_list_in_two_orders() -> Result<()> {
    let edge = |from: &str| {
        premise(
            concept(&[("to", field("edge/to", "entity:", "all:"))]),
            &[("this", from), ("to", "this")],
        )
    };
    let domains = [
        "out.a", "out.b", "out.c", "out.d", "out.e", "out.f", "out.g", "out.h",
    ];
    let rules: Vec<InductiveRule> = domains
        .iter()
        .map(|domain| {
            rule(
                "assert!",
                head("from", &format!("{domain}/from"), "entity:", "last:"),
                vec![edge("from"), edge("other")],
                vec![],
            )
        })
        .collect::<Result<_>>()?;

    // Which premise each rule lists first, as it is stored and decoded:
    // the one whose subject the head writes, or the other.
    let head_first: Vec<bool> = rules
        .iter()
        .map(|rule| {
            let stored = InductiveRule::decode(&rule.encode()).expect("a stored rule decodes");
            let written = stored
                .conclusion()
                .with()
                .iter()
                .map(|(name, _)| name.to_string())
                .next()
                .expect("a head has a field");
            let first = stored.analysis().premises[0].parameters();
            first.get("this").and_then(|term| term.name()) == Some(written.as_str())
        })
        .collect();
    assert!(
        head_first.contains(&true) && head_first.contains(&false),
        "the rules list the body in both orders: {head_first:?}"
    );

    // Each hub is reached from one node, and one commit adds an edge
    // to it from a second. The pairs are named so that whichever order
    // a scan gives two nodes in, some hub has the old one first. The
    // heads are commands, so what a rule elects for a hub is what the
    // commit reports under its attribute.
    const HUBS: usize = 6;
    let pair = |hub: usize| {
        if hub.is_multiple_of(2) {
            (format!("node:a{hub}"), format!("node:z{hub}"))
        } else {
            (format!("node:z{hub}"), format!("node:a{hub}"))
        }
    };
    let transient: Vec<Entity> = rules.iter().map(|rule| rule.conclusion().this()).collect();
    let world_of = |share: bool| {
        let rules = rules.clone();
        let transient = transient.clone();
        async move {
            let _unshared = (!share).then(Unshared::new);
            let world = world().await?;
            let link = |changes: &mut Changes, from: &str, hub: usize| {
                assert(
                    changes,
                    "edge/to",
                    from,
                    Value::Entity(entity(&format!("hub:{hub}"))),
                    Pick::All,
                );
            };
            let mut edges = Changes::new();
            for hub in 0..HUBS {
                link(&mut edges, &pair(hub).0, hub);
            }
            commit(&world, edges, Changes::new()).await?;
            install(&world, &rules, &transient).await?;
            probe::take();

            let mut changes = Changes::new();
            for hub in 0..HUBS {
                link(&mut changes, &pair(hub).1, hub);
            }
            let reported = commit(&world, changes, Changes::new()).await?;
            anyhow::Ok((reported, probe::take()))
        }
    };
    let (shared, counts) = world_of(true).await?;
    let (unshared, _) = world_of(false).await?;
    assert_eq!(shared, unshared, "every rule elects what it elects alone");

    // What each rule elected for each hub.
    let mut differ = false;
    for hub in 0..HUBS {
        let elected: Vec<&str> = domains
            .iter()
            .map(|domain| {
                let cell = format!("assert hub:{hub} {domain}/from ");
                let lines: Vec<&String> = shared
                    .iter()
                    .filter(|line| line.starts_with(&cell))
                    .collect();
                assert_eq!(
                    lines.len(),
                    1,
                    "one value is elected under `last`: {lines:?}"
                );
                &lines[0][cell.len()..]
            })
            .collect();
        differ |= elected.iter().any(|value| *value != elected[0]);
    }
    assert!(
        differ,
        "at some hub the two orders elect different values: {shared:?}"
    );
    let (read, alone) = counts;
    assert_eq!(read + alone, rules.len() - 1, "all eight state one body");
    assert_eq!(
        alone,
        rules.len() - 1,
        "each writes one cell two ways, so each is evaluated alone"
    );
    Ok(())
}

/// Which heads the order of writing decides for: only those that write
/// one cell differently.
#[dialog_common::test]
fn it_tells_when_the_order_of_heads_decides() {
    let cell = |the: &str, is: &str, pick: Pick| (relation(the), Value::String(is.into()), pick);
    let head = |of: &str, cells: Vec<(Relation, Value, Pick)>| (entity(of), cells);

    assert!(!order_decides(&[]));
    assert!(!order_decides(&[head(
        "item:1",
        vec![cell("out/a", "x", Pick::Last)]
    )]));
    assert!(
        !order_decides(&[
            head("item:1", vec![cell("out/a", "x", Pick::Last)]),
            head("item:2", vec![cell("out/a", "y", Pick::Last)]),
        ]),
        "two entities are two cells"
    );
    assert!(
        order_decides(&[
            head("item:1", vec![cell("out/a", "x", Pick::Last)]),
            head("item:1", vec![cell("out/a", "y", Pick::Last)]),
        ]),
        "two values to one cell under `last`: the later stands"
    );
    assert!(
        order_decides(&[
            head("item:1", vec![cell("out/a", "x", Pick::All)]),
            head("item:1", vec![cell("out/a", "y", Pick::All)]),
        ]),
        "two members of one set are kept in the order written"
    );
    assert!(
        !order_decides(&[
            head(
                "item:1",
                vec![
                    cell("out/a", "x", Pick::Last),
                    cell("out/b", "p", Pick::Last)
                ]
            ),
            head(
                "item:1",
                vec![
                    cell("out/a", "x", Pick::Last),
                    cell("out/c", "q", Pick::Last)
                ]
            ),
        ]),
        "two heads that write a cell the same leave it the same either way"
    );
    assert!(
        order_decides(&[
            head(
                "item:1",
                vec![
                    cell("out/a", "x", Pick::Last),
                    cell("out/a", "y", Pick::All)
                ]
            ),
            head(
                "item:1",
                vec![
                    cell("out/a", "y", Pick::All),
                    cell("out/a", "x", Pick::Last)
                ]
            ),
        ]),
        "one cell written twice by each head, in another order"
    );
}

/// A head that writes a collection shares like any other: the key it
/// writes under is a variable of the body, read through the renaming
/// as the value is.
#[dialog_common::test]
async fn it_shares_a_body_with_a_head_that_writes_a_collection() -> Result<()> {
    // Who touched an item, each under a position of their own.
    let members = json!({
        "with": {
            "member": {
                "the": { "domain": "out.members", "keyed": "sequence" },
                "cardinality": "many",
                "as": "entity:"
            }
        }
    });
    let body = |who: &str, list: &str, position: &str| {
        vec![
            touched(who),
            listed(list),
            json!({
                "assert": "dialog/position",
                "where": {
                    "member": var(who),
                    "after": "",
                    "before": "",
                    "is": var(position)
                }
            }),
        ]
    };
    let rules = [
        rule(
            "assert!",
            members,
            body("member", "list", "member/key"),
            vec![],
        )?,
        rule(
            "assert!",
            head("who", "out.a/by", "entity:", "all:"),
            body("who", "where", "at"),
            vec![],
        )?,
        rule(
            "assert!",
            head("on", "out.a/list", "entity:", "last:"),
            body("user", "on", "place"),
            vec![],
        )?,
    ];
    let commits = [
        (Changes::new(), touch(1, 1, 1)),
        (Changes::new(), touch(2, 1, 0)),
        (Changes::new(), touch(3, 0, 0)),
    ];

    let shared_world = world().await?;
    install(&shared_world, &rules, &[]).await?;
    probe::take();
    for (changes, commands) in &commits {
        commit(&shared_world, changes.clone(), commands.clone()).await?;
    }
    assert_eq!(
        probe::take(),
        (6, 0),
        "the three rules state one body, the collection head among them: two read the first's matches, a commit"
    );
    let shared = (
        members_of(&shared_world, "item:1").await?,
        members_of(&shared_world, "item:0").await?,
    );

    // What the collection head wrote is what it writes with sharing
    // off.
    let unshared = {
        let _unshared = Unshared::new();
        let world = world().await?;
        install(&world, &rules, &[]).await?;
        for (changes, commands) in &commits {
            commit(&world, changes.clone(), commands.clone()).await?;
        }
        (
            members_of(&world, "item:1").await?,
            members_of(&world, "item:0").await?,
        )
    };
    assert_eq!(shared, unshared);
    assert_eq!(
        shared.0.len(),
        2,
        "item:1 holds the two who touched it, each under a key: {:?}",
        shared.0
    );
    assert_eq!(shared.1.len(), 1, "and item:0 the one: {:?}", shared.1);
    Ok(())
}

/// What `item` holds in the `out.members` collection, each entry with
/// its key.
async fn members_of(world: &World, item: &str) -> Result<Vec<String>> {
    let selector = Selector::new().of(entity(item));
    let stream = world
        .branch
        .claims()
        .select(selector)
        .perform(&world.operator)
        .await?;
    let facts: Vec<_> = stream.collect().await;
    let mut found = Vec::new();
    for fact in facts {
        let fact = fact.and_then(|view| view.to_owned())?;
        if fact.the.to_string().starts_with("out.members/") {
            found.push(format!("{} {:?}", fact.the, fact.is));
        }
    }
    found.sort();
    Ok(found)
}

/// A small generator: the same sequence for the same seed on every
/// platform.
struct Dice(u64);

impl Dice {
    fn roll(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.roll() % bound as u64) as usize
    }
}

/// One of five bodies, its variables named for rule `at`, its premises
/// rotated by `turn`: the premises, the `unless`, and each variable a
/// head may write with the kind it holds.
#[allow(clippy::type_complexity)]
fn generated_body(
    which: usize,
    at: usize,
    turn: usize,
) -> (Vec<Json>, Vec<Json>, Vec<(String, &'static str)>) {
    let name = |base: &str| format!("{base}{at}");
    let (by, list, owner, tag, rank) = (
        name("by"),
        name("list"),
        name("owner"),
        name("tag"),
        name("rank"),
    );
    let (mut when, unless, writes) = match which {
        0 => (
            vec![touched(&by), listed(&list)],
            vec![],
            vec![(by, "entity:"), (list, "entity:")],
        ),
        1 => (
            vec![touched(&by), listed(&list), owned(&list, &owner)],
            vec![],
            vec![(by, "entity:"), (list, "entity:"), (owner, "entity:")],
        ),
        2 => (
            vec![touched(&by), tagged(&tag)],
            vec![],
            vec![(by, "entity:"), (tag, "text:")],
        ),
        3 => (
            vec![listed(&list)],
            vec![json!({
                "assert": concept(&[("archived", field("item/archived", "boolean:", "last:"))]),
                "where": { "this": var("this"), "archived": true }
            })],
            vec![(list, "entity:")],
        ),
        _ => (
            vec![touched(&by), ranked(&rank)],
            vec![],
            vec![(by, "entity:"), (rank, "number:")],
        ),
    };
    let turn = turn % when.len();
    when.rotate_left(turn);
    (when, unless, writes)
}

/// A rule set made from `dice`: two or three rules on each of one or
/// two bodies, and sometimes a pair of rules reading a command the
/// others issue. Returns the rules and the concepts to mark transient.
fn generated_rules(dice: &mut Dice) -> Result<(Vec<InductiveRule>, Vec<Entity>)> {
    let mut rules = Vec::new();
    let mut transient = Vec::new();
    let mut at = 0;
    let first = dice.below(5);
    let bodies = if dice.below(2) == 0 {
        vec![first]
    } else {
        vec![first, (first + 1 + dice.below(4)) % 5]
    };
    for which in bodies {
        for _ in 0..2 + dice.below(2) {
            at += 1;
            let (when, unless, writes) = generated_body(which, at, dice.below(3));
            let (variable, kind) = &writes[dice.below(writes.len())];
            let base: String = variable.chars().filter(|c| c.is_alphabetic()).collect();
            let kind = match *kind {
                "number:" => ["natural:", "integer:", "number:"][dice.below(3)],
                kind => kind,
            };
            let domain = ["out.a", "out.b"][dice.below(2)];
            let pick = ["last:", "all:"][dice.below(2)];
            let polarity = if dice.below(5) == 0 {
                "retract!"
            } else {
                "assert!"
            };
            rules.push(rule(
                polarity,
                head(variable, &format!("{domain}/{base}"), kind, pick),
                when,
                unless,
            )?);
        }
    }
    if dice.below(2) == 0 {
        // A rule issues a command from the first body or the second,
        // and two rules read it: they state one body in the round
        // after.
        at += 1;
        let (when, unless, writes) = generated_body(dice.below(2), at, 0);
        let (by, _) = &writes[0];
        let issue = rule(
            "assert!",
            head(by, "cmd.note/by", "entity:", "last:"),
            when,
            unless,
        )?;
        transient.push(issue.conclusion().this());
        rules.push(issue);
        for (reader, the) in [("noted", "out.noted/by"), ("who", "out.noted/who")] {
            rules.push(rule(
                "assert!",
                head(reader, the, "entity:", "all:"),
                vec![premise(
                    concept(&[("by", field("cmd.note/by", "entity:", "last:"))]),
                    &[("this", "this"), ("by", reader)],
                )],
                vec![],
            )?);
        }
    }
    Ok((rules, transient))
}

/// A list of commits made from `dice`: commands, moves, tags added and
/// taken away, ranks of every kind of number, owners, and items
/// archived and restored.
fn generated_commits(dice: &mut Dice) -> Vec<(Changes, Changes)> {
    let mut commits = Vec::new();
    let mut command = 0;
    for _ in 0..5 {
        let mut changes = Changes::new();
        let mut commands = Changes::new();
        for _ in 0..1 + dice.below(3) {
            let item = format!("item:{}", dice.below(4));
            match dice.below(9) {
                0..=2 => {
                    command += 1;
                    touch_into(&mut commands, command, dice.below(4), dice.below(2));
                }
                3 => assert(
                    &mut changes,
                    "item/list",
                    &item,
                    Value::Entity(entity(&format!("list:{}", dice.below(2)))),
                    Pick::Last,
                ),
                4 => assert(
                    &mut changes,
                    "item/tag",
                    &item,
                    Value::String(["red", "green", "blue"][dice.below(3)].into()),
                    Pick::All,
                ),
                5 => changes.dissociate(
                    relation("item/tag"),
                    entity(&item),
                    Value::String(["red", "green"][dice.below(2)].into()),
                ),
                6 => assert(
                    &mut changes,
                    "item/rank",
                    &item,
                    [
                        Value::UnsignedInt(3),
                        Value::SignedInt(-2),
                        Value::Float(1.5),
                    ][dice.below(3)]
                    .clone(),
                    Pick::Last,
                ),
                7 => assert(
                    &mut changes,
                    "list/owner",
                    &format!("list:{}", dice.below(2)),
                    Value::Entity(entity(&format!("user:{}", dice.below(3)))),
                    Pick::Last,
                ),
                _ => {
                    if dice.below(2) == 0 {
                        assert(
                            &mut changes,
                            "item/archived",
                            &item,
                            Value::Boolean(true),
                            Pick::Last,
                        );
                    } else {
                        changes.dissociate(
                            relation("item/archived"),
                            entity(&item),
                            Value::Boolean(true),
                        );
                    }
                }
            }
        }
        commits.push((changes, commands));
    }
    commits
}

/// How many generated rule sets the differential test runs. Each is
/// run twice, eleven commits a run.
const GENERATED: u64 = 24;

/// Over generated rule sets and commits, a branch committed to with
/// sharing on holds what one committed to with sharing off holds, and
/// every commit reports the same transients. Rule sets differ in which
/// bodies they repeat, how the variables are named, the order of the
/// premises, what each head writes, under which type and pick, whether
/// it asserts or retracts, and whether a command joins two rounds. The
/// commits fire the rules seeded and in full.
///
/// Beside the comparison of outcomes, every rule fired from another's
/// matches in these runs is checked against its own evaluation as it
/// fires.
#[dialog_common::test]
async fn it_writes_what_the_unshared_path_writes_over_generated_rules() -> Result<()> {
    let mut read = 0;
    let mut alone = 0;
    let mut saved = 0;
    for seed in 0..GENERATED {
        let mut dice = Dice(seed);
        let (rules, transient) = generated_rules(&mut dice)?;
        let commits = generated_commits(&mut dice);
        let (shared, unshared) = both(&rules, &transient, &commits)
            .await
            .map_err(|error| anyhow::anyhow!("seed {seed}: {error}"))?;
        assert!(
            shared.evaluations <= unshared.evaluations,
            "seed {seed}: sharing never evaluates more"
        );
        read += shared.shared.0;
        alone += shared.shared.1;
        saved += unshared.evaluations - shared.evaluations;
    }
    println!(
        "{GENERATED} rule sets: {read} rules read another's matches, {alone} were evaluated alone for the order's sake, {saved} evaluations saved"
    );
    assert!(
        read >= GENERATED as usize,
        "the generated sets share bodies: {read} rules read another's matches"
    );
    assert!(
        alone > 0,
        "and some of them meet matches whose order decides: {alone}"
    );
    assert!(saved > 0, "sharing saved evaluations: {saved}");
    Ok(())
}
