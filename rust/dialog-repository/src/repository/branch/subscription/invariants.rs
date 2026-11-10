//! Invariants the pull request's subscription changes claim, pinned as
//! tests.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::env::{self, VarError};
use std::fmt;
use std::thread;

use super::{Delta, Subscription};
use crate::Branch;
use crate::RepositoryExt as _;
use crate::helpers::test_repo;
use anyhow::Context as _;
use dialog_capability::Subject;
use dialog_common::ConditionalSync;
use dialog_corpus::{Changes, Entity};
use dialog_peer::helpers::{test_session_with_peer, unique_name};
use dialog_peer::{Peer, Session};
use dialog_query::rule::{DeductiveRule, DeductiveRuleDescriptor};
use dialog_query::{Attribute, Concept, Conclusion, Output as _, Predicate, Query, Relation};
use dialog_storage::provider::storage::VolatileSpace;

/// A badge number (`credential/badge`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("credential")]
pub struct Badge(pub String);

/// Someone holding a badge.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct BadgeHolder {
    /// The badge holder entity.
    pub this: Entity,
    /// Their badge number.
    pub badge: Badge,
}

/// "Subscription demand records one `derives` slice per attribute of
/// the subscribed concept, so ... a rule landing on a subscribed one
/// does" wake it. Bob has a legacy code and no badge when the
/// subscription first polls. A rule deriving `credential/badge` from
/// the legacy code then lands, and the next poll asserts bob. The pull
/// request removed the one test that installed a rule after a first
/// poll.
#[dialog_common::test]
async fn a_rule_landing_on_a_subscribed_attribute_wakes_the_subscription() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    let bob: Entity = "id:bob".parse()?;
    branch
        .transaction()
        .assert(Badge::of(alice.clone()).is("A-1"))
        .assert(
            dialog_query::the!("credential/legacy-code")
                .of(bob.clone())
                .is("B-2".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let mut subscription = branch.subscribe(Query::<BadgeHolder>::default());
    let initial = subscription.poll(&operator).await?.expect("initial");
    assert_eq!(
        initial.asserted,
        vec![BadgeHolder {
            this: alice.clone(),
            badge: Badge("A-1".into()),
        }]
    );

    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": {
            "badge": { "the": "credential/badge", "as": "text:" }
        }},
        "when": [{
            "assert": { "with": {
                "code": { "the": "credential/legacy-code", "as": "text:" }
            }},
            "where": {
                "this": { "?": { "name": "this" } },
                "code": { "?": { "name": "badge" } }
            }
        }]
    }))?;
    branch
        .transaction()
        .assert(&descriptor.compile()?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let delta = subscription
        .poll(&operator)
        .await?
        .expect("the rule's landing changes the result");
    assert_eq!(
        delta.asserted,
        vec![BadgeHolder {
            this: bob,
            badge: Badge("B-2".into()),
        }]
    );
    Ok(())
}

/// A value the relation `x/a` derives (`x/a(e) := v :- x/src1(e) = v`,
/// the same from `x/src2` and `x/src3`, and `x/a(e) := v :- x/a(e) =
/// v`, which makes the relation recursive).
#[derive(Attribute, Clone, PartialEq)]
#[domain("x")]
pub struct A(pub String);

/// An entity's `x/a`, read under `last`.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct HeldA {
    /// The entity.
    pub this: Entity,
    /// Its elected `x/a`.
    pub a: A,
}

/// "A derived value stands by the fact that bound it", kept by a
/// subscription that maintains the relation's fixpoint across polls
/// rather than rebuilding it. `v` is derived from `x/src1` (oldest) and
/// later from `x/src3` too; `w` from `x/src2`, in between. A poll after
/// `x/src3` lands reads `v`: the known row stands newer than before. A
/// poll after `x/src3` is retracted reads `w`: `v` keeps its older
/// derivation, and stands as that one again.
#[dialog_common::test]
async fn a_maintained_fixpoint_keeps_each_row_at_its_newest_surviving_derivation()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let e: Entity = "id:e".parse()?;
    let rule = |from: &str| -> anyhow::Result<_> {
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": { "a": { "the": "x/a", "as": "text:" } } },
            "when": [{
                "assert": { "with": { "v": { "the": from, "as": "text:" } } },
                "where": { "this": { "?": { "name": "this" } }, "v": { "?": { "name": "a" } } }
            }]
        }))?;
        Ok(descriptor.compile()?)
    };
    let source = |attribute: &str, value: &str| {
        attribute
            .parse::<Relation>()
            .expect("attribute")
            .of(e.clone())
            .is(value.to_string())
    };
    branch
        .transaction()
        .assert(&rule("x/src1")?)
        .assert(&rule("x/src2")?)
        .assert(&rule("x/src3")?)
        .assert(&rule("x/a")?)
        .assert(source("x/src1", "v"))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch
        .transaction()
        .assert(source("x/src2", "w"))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let held = |value: &str| HeldA {
        this: e.clone(),
        a: A(value.into()),
    };
    let mut subscription = branch.subscribe(Query::<HeldA>::default());
    let initial = subscription.poll(&operator).await?.expect("initial");
    assert_eq!(initial.asserted, vec![held("w")], "w stands newest");

    branch
        .transaction()
        .assert(source("x/src3", "v"))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let raised = subscription
        .poll(&operator)
        .await?
        .expect("v now stands newest");
    assert_eq!(raised.asserted, vec![held("v")]);
    assert_eq!(raised.retracted, vec![held("w")]);

    branch
        .transaction()
        .retract(source("x/src3", "v"))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let lowered = subscription
        .poll(&operator)
        .await?
        .expect("v falls back to its older derivation");
    assert_eq!(lowered.asserted, vec![held("w")]);
    assert_eq!(lowered.retracted, vec![held("v")]);
    assert!(
        subscription.maintenances() >= 1,
        "the fixpoint was maintained across a poll, not only rebuilt"
    );
    Ok(())
}
/// A display name (`report/name`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct Name(pub String);

/// A manager (`report/manager`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct Manager(pub Entity);

/// A mentor (`report/mentor`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct Mentor(pub Entity);

/// A staff level (`staff/level`), which rules read as an optional
/// field: none counts as 0.
#[derive(Attribute, Clone, PartialEq)]
#[domain("staff")]
pub struct Level(pub u32);

/// What [`unvouched_rule`] derives (`report/alone`): the name of
/// someone whose manager and mentor both hold no badge.
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct Alone(pub String);

/// What [`junior_led_rule`] derives (`report/junior-led`): the name of
/// someone whose manager is a junior.
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct JuniorLed(pub String);

/// What [`junior_taught_rule`] derives (`report/junior-taught`): the
/// name of someone whose mentor's level reads below 2.
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct JuniorTaught(pub String);

/// An entity's name.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Named {
    /// The named entity.
    pub this: Entity,
    /// Its name.
    pub name: Name,
}

/// An entity's manager.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Managed {
    /// The managed entity.
    pub this: Entity,
    /// Its manager.
    pub manager: Manager,
}

/// An entity's mentor.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Mentored {
    /// The mentored entity.
    pub this: Entity,
    /// Its mentor.
    pub mentor: Mentor,
}

/// An entity's level, where it has one.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Levelled {
    /// The levelled entity.
    pub this: Entity,
    /// Its level.
    pub level: Level,
}

/// Someone whose manager and mentor both hold a badge: two
/// conformance premises, neither binding `?this`, in one non-local
/// rule.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Vouched {
    /// The report.
    pub this: Entity,
    /// Their name.
    pub name: Name,
    /// Their manager, who must hold a badge.
    #[dialog(conforms = BadgeHolder)]
    pub manager: Manager,
    /// Their mentor, who must hold a badge.
    #[dialog(conforms = BadgeHolder)]
    pub mentor: Mentor,
}

/// Someone neither of whose manager and mentor holds a badge, read
/// from `report/alone`, which only [`unvouched_rule`] derives.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Unvouched {
    /// The report.
    pub this: Entity,
    /// Their name.
    pub alone: Alone,
}

/// Someone whose manager is a junior, read from `report/junior-led`,
/// which only [`junior_led_rule`] derives.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct JuniorLedReport {
    /// The report.
    pub this: Entity,
    /// Their name.
    pub junior_led: JuniorLed,
}

/// Someone whose mentor's level reads below 2, read from
/// `report/junior-taught`, which only [`junior_taught_rule`] derives.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct JuniorTaughtReport {
    /// The report.
    pub this: Entity,
    /// Their name.
    pub junior_taught: JuniorTaught,
}

/// Everyone above someone (`report/above`): their manager, their
/// manager's manager, and on up the chain, which [`above_rules`]
/// derive recursively. Every entity above is a value of its own, so
/// the attribute is read under `all`.
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
#[pick(all)]
pub struct Above(pub Entity);

/// What [`alone_led_rule`] derives (`report/alone-led`): the name of
/// someone whose manager is [`Unvouched`].
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct AloneLed(pub String);

/// Someone with someone above them, read from `report/above`. Its
/// rules are recursive, so a subscription to it is maintained by the
/// fixpoint continuation (DRed for what stopped being readable,
/// semi-naive extension for what became readable), not by the
/// delta-join.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Superior {
    /// The one below.
    pub this: Entity,
    /// Someone above them.
    pub above: Above,
}

/// Someone whose mentor is [`Vouched`], which in turn needs its
/// manager and mentor to be [`BadgeHolder`]s: a badge change reaches
/// this head through two layers of concept premises.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct VouchedMentee {
    /// The mentee.
    pub this: Entity,
    /// Their name.
    pub name: Name,
    /// Their mentor, who must be vouched for.
    #[dialog(conforms = Vouched)]
    pub mentor: Mentor,
}

/// Someone whose manager is [`Unvouched`], read from
/// `report/alone-led`, which only [`alone_led_rule`] derives: two
/// non-local layers, the inner one negated.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct AloneLedReport {
    /// The report.
    pub this: Entity,
    /// Their name.
    pub alone_led: AloneLed,
}

/// Someone whose manager is a [`VouchedMentee`]: three non-local
/// layers, all positive, so a badge change on the manager's mentor's
/// manager or mentor moves this row through three delta-join layers.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct VouchedMenteeLed {
    /// The report.
    pub this: Entity,
    /// Their name.
    pub name: Name,
    /// Their manager, who must be a vouched mentee.
    #[dialog(conforms = VouchedMentee)]
    pub manager: Manager,
}

/// What [`alone_led_led_rule`] derives (`report/alone-led-led`): the
/// name of someone whose manager is an [`AloneLedReport`].
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct AloneLedLed(pub String);

/// Someone whose manager is [`AloneLedReport`], read from
/// `report/alone-led-led`, which only [`alone_led_led_rule`] derives:
/// three non-local layers, the innermost negated.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct AloneLedLedReport {
    /// The report.
    pub this: Entity,
    /// Their name.
    pub alone_led_led: AloneLedLed,
}

/// A variable term in rule notation.
fn var(name: &str) -> serde_json::Value {
    serde_json::json!({ "?": { "name": name } })
}

/// A rule in notation, compiled.
fn rule(notation: serde_json::Value) -> anyhow::Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(notation)?;
    Ok(descriptor.compile()?)
}

/// The premises reading `of`'s name as `name` and its level, none
/// taken as 0, as below 2: a row exists or not by an optional field's
/// value.
fn level_below_two(of: &str, name: &str) -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "assert": { "with": {
                "name": { "the": "report/name", "as": "text:" },
                "level": { "the": "staff/level", "as": "natural:", "optional": true }
            }},
            "where": { "this": var(of), "name": var(name), "level": var("level") }
        }),
        serde_json::json!({
            "assert": "coalesce",
            "where": { "source": var("level"), "fallback": 0, "is": var("rank") }
        }),
        serde_json::json!({
            "assert": "<",
            "where": { "of": var("rank"), "with": 2 }
        }),
    ]
}

/// A premise reading `of`'s `field` (`report/<field>`) as `is`.
fn report(field: &str, kind: &str, of: &str, is: &str) -> serde_json::Value {
    serde_json::json!({
        "assert": { "with": {
            field: { "the": format!("report/{field}"), "as": kind }
        }},
        "where": { "this": var(of), field: var(is) }
    })
}

/// `report/alone(this) := name :- report/name(this) = name,
/// report/manager(this) = m, report/mentor(this) = t,
/// unless BadgeHolder(m), unless BadgeHolder(t)`: the mirror of
/// [`Vouched`], two concept premises under negation.
fn unvouched_rule() -> anyhow::Result<DeductiveRule> {
    let badge = |of: &str| {
        serde_json::json!({
            "assert": { "with": { "badge": { "the": "credential/badge", "as": "text:" } } },
            "where": { "this": var(of) }
        })
    };
    rule(serde_json::json!({
        "deduce": { "with": { "alone": { "the": "report/alone", "as": "text:" } } },
        "when": [
            report("name", "text:", "this", "alone"),
            report("manager", "entity:", "this", "manager"),
            report("mentor", "entity:", "this", "mentor")
        ],
        "unless": [badge("manager"), badge("mentor")]
    }))
}

/// `report/junior(this) := name :- report/name(this) = name,
/// coalesce(staff/level(this), 0) < 2`: an entity-local concept whose
/// rows exist by an optional field's value.
fn junior_rule() -> anyhow::Result<DeductiveRule> {
    rule(serde_json::json!({
        "deduce": { "with": { "junior": { "the": "report/junior", "as": "text:" } } },
        "when": level_below_two("this", "junior")
    }))
}

/// `report/junior-led(this) := name :- report/name(this) = name,
/// report/manager(this) = m, Junior(m)`: the optional field is read
/// through a target concept, so a level change on the manager moves
/// the report's row.
fn junior_led_rule() -> anyhow::Result<DeductiveRule> {
    rule(serde_json::json!({
        "deduce": { "with": { "junior_led": { "the": "report/junior-led", "as": "text:" } } },
        "when": [
            report("name", "text:", "this", "junior_led"),
            report("manager", "entity:", "this", "manager"),
            {
                "assert": { "with": { "junior": { "the": "report/junior", "as": "text:" } } },
                "where": { "this": var("manager"), "junior": var("managers") }
            }
        ]
    }))
}

/// `report/junior-taught(this) := name :- report/name(this) = name,
/// report/mentor(this) = t, coalesce(staff/level(t), 0) < 2`: the
/// optional field read in the non-local rule itself, for another
/// entity.
fn junior_taught_rule() -> anyhow::Result<DeductiveRule> {
    let mut when = vec![
        report("name", "text:", "this", "junior_taught"),
        report("mentor", "entity:", "this", "mentor"),
    ];
    when.extend(level_below_two("mentor", "mentors"));
    rule(serde_json::json!({
        "deduce": { "with": {
            "junior_taught": { "the": "report/junior-taught", "as": "text:" }
        }},
        "when": when
    }))
}

/// The `report/above` field, read under `all`.
fn above_field() -> serde_json::Value {
    serde_json::json!({ "above": { "the": "report/above", "as": "entity:", "pick": "all" } })
}

/// `report/above(this) := up :- report/<edge>(this) = up` and
/// `report/above(this) := up :- report/<edge>(this) = next,
/// report/above(next) = up`: the chain of `edge` (`manager` or
/// `mentor`) closed transitively. The second rule reads its own head,
/// which makes [`Superior`] recursive.
fn above_rules(edge: &str) -> anyhow::Result<Vec<DeductiveRule>> {
    Ok(vec![
        rule(serde_json::json!({
            "deduce": { "with": above_field() },
            "when": [report(edge, "entity:", "this", "above")]
        }))?,
        rule(serde_json::json!({
            "deduce": { "with": above_field() },
            "when": [
                report(edge, "entity:", "this", "next"),
                {
                    "assert": { "with": above_field() },
                    "where": { "this": var("next"), "above": var("above") }
                }
            ]
        }))?,
    ])
}

/// `report/alone-led(this) := name :- report/name(this) = name,
/// report/manager(this) = m, Unvouched(m)`: a concept premise over a
/// concept whose own rule reads other entities, so a badge change on
/// the manager's manager or mentor moves this row through two
/// delta-join layers.
fn alone_led_rule() -> anyhow::Result<DeductiveRule> {
    rule(serde_json::json!({
        "deduce": { "with": { "alone_led": { "the": "report/alone-led", "as": "text:" } } },
        "when": [
            report("name", "text:", "this", "alone_led"),
            report("manager", "entity:", "this", "manager"),
            {
                "assert": { "with": { "alone": { "the": "report/alone", "as": "text:" } } },
                "where": { "this": var("manager"), "alone": var("managers") }
            }
        ]
    }))
}

/// `report/alone-led-led(this) := name :- report/name(this) = name,
/// report/manager(this) = m, report/alone-led(m) = _`: a concept
/// premise over [`AloneLedReport`], itself two non-local layers deep.
fn alone_led_led_rule() -> anyhow::Result<DeductiveRule> {
    rule(serde_json::json!({
        "deduce": { "with": { "alone_led_led": { "the": "report/alone-led-led", "as": "text:" } } },
        "when": [
            report("name", "text:", "this", "alone_led_led"),
            report("manager", "entity:", "this", "manager"),
            {
                "assert": { "with": { "alone_led": { "the": "report/alone-led", "as": "text:" } } },
                "where": { "this": var("manager"), "alone_led": var("managers") }
            }
        ]
    }))
}

/// A premise holding when `of`'s badge is `B1`.
fn badged_b1(of: &str) -> serde_json::Value {
    serde_json::json!({
        "assert": { "with": { "badge": { "the": "credential/badge", "as": "text:" } } },
        "where": { "this": var(of), "badge": "B1" }
    })
}

/// `report/alone(this) := name :- report/name(this) = name,
/// credential/badge(this) = "B1"`: a second, entity-local derivation
/// of [`Unvouched`] that rule steps install and remove.
fn alone_by_own_badge_rule() -> anyhow::Result<DeductiveRule> {
    rule(serde_json::json!({
        "deduce": { "with": { "alone": { "the": "report/alone", "as": "text:" } } },
        "when": [report("name", "text:", "this", "alone"), badged_b1("this")]
    }))
}

/// `report/alone(this) := name :- report/name(this) = name,
/// report/mentor(this) = t, credential/badge(t) = "B1"`: a non-local
/// derivation of [`Unvouched`], which a rule step replaces
/// [`alone_by_own_badge_rule`] with, and the other way around.
fn alone_by_mentors_badge_rule() -> anyhow::Result<DeductiveRule> {
    rule(serde_json::json!({
        "deduce": { "with": { "alone": { "the": "report/alone", "as": "text:" } } },
        "when": [
            report("name", "text:", "this", "alone"),
            report("mentor", "entity:", "this", "mentor"),
            badged_b1("mentor")
        ]
    }))
}

/// The rules every run installs with its first commit.
fn installed_rules() -> anyhow::Result<Vec<DeductiveRule>> {
    let mut rules = vec![
        unvouched_rule()?,
        junior_rule()?,
        junior_led_rule()?,
        junior_taught_rule()?,
        alone_led_rule()?,
        alone_led_led_rule()?,
    ];
    rules.extend(above_rules("manager")?);
    Ok(rules)
}

/// The session every randomized run commits and reads through.
type Operator = Peer<VolatileSpace, Session>;

/// A seeded xorshift64* generator: the same seed yields the same
/// steps on every platform, with no dependency.
struct Xorshift(u64);

impl Xorshift {
    fn new(seed: u64) -> Self {
        // Splitmix the seed so small seeds start far apart, and keep
        // the state off zero, which xorshift never leaves.
        let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        Self((z ^ (z >> 31)).max(1))
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A number in `0..n`.
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    /// True with probability `numerator / denominator`.
    fn chance(&mut self, numerator: usize, denominator: usize) -> bool {
        self.below(denominator) < numerator
    }
}

/// How many entities the randomized runs write about. Few enough that
/// changes collide on the same rows, and that one entity is often
/// both someone's manager and someone's mentor.
const PEOPLE: usize = 4;

/// The attributes a randomized step writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Field {
    Badge,
    Name,
    Manager,
    Mentor,
    Level,
}

/// A value under a [`Field`]: text, one of the [`PEOPLE`] by index,
/// or a level.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Datum {
    Text(&'static str),
    Person(usize),
    Number(u32),
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Datum::Text(text) => write!(f, "{text:?}"),
            Datum::Person(index) => write!(f, "p{index}"),
            Datum::Number(number) => write!(f, "{number}"),
        }
    }
}

/// One fact a step asserts or retracts.
#[derive(Clone, Debug)]
struct Change {
    assert: bool,
    field: Field,
    of: usize,
    is: Datum,
}

impl fmt::Display for Change {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.assert { '+' } else { '-' };
        let the = match self.field {
            Field::Badge => "credential/badge",
            Field::Name => "report/name",
            Field::Manager => "report/manager",
            Field::Mentor => "report/mentor",
            Field::Level => "staff/level",
        };
        write!(f, "{sign} {the} p{} = {}", self.of, self.is)
    }
}

/// How a step lands.
#[derive(Clone, Copy, Debug)]
enum Via {
    Commit,
    Overlay,
    /// The overlay drops every fact and tombstone it holds.
    Clear,
    /// The branch's first commit, landing the rules with the step's
    /// changes, in a run that subscribed before it.
    Install,
}

/// The `index`th of the [`PEOPLE`].
fn person(index: usize) -> Entity {
    format!("id:p{index}").parse().expect("a valid entity")
}

/// The index of `entity` among the [`PEOPLE`].
fn index_of(entity: &Entity) -> usize {
    (0..PEOPLE)
        .find(|index| person(*index) == *entity)
        .expect("only the people are written about")
}

/// Every fact readable on the branch now, the session overlay folded
/// in, as `(field, of, is)`. The generator retracts from these, so a
/// retraction usually removes something.
async fn visible(
    branch: &Branch,
    operator: &Operator,
) -> anyhow::Result<Vec<(Field, usize, Datum)>> {
    let mut facts = Vec::new();
    for row in fresh::<BadgeHolder>(branch, operator).await? {
        let text = text_datum(&row.badge.0);
        facts.push((Field::Badge, index_of(&row.this), text));
    }
    for row in fresh::<Named>(branch, operator).await? {
        let text = text_datum(&row.name.0);
        facts.push((Field::Name, index_of(&row.this), text));
    }
    for row in fresh::<Managed>(branch, operator).await? {
        let to = Datum::Person(index_of(&row.manager.0));
        facts.push((Field::Manager, index_of(&row.this), to));
    }
    for row in fresh::<Mentored>(branch, operator).await? {
        let to = Datum::Person(index_of(&row.mentor.0));
        facts.push((Field::Mentor, index_of(&row.this), to));
    }
    for row in fresh::<Levelled>(branch, operator).await? {
        let level = Datum::Number(row.level.0);
        facts.push((Field::Level, index_of(&row.this), level));
    }
    facts.sort();
    Ok(facts)
}

/// The texts a step writes, so a read text maps back to a `'static`.
const TEXTS: [&str; 4] = ["B0", "B1", "N0", "N1"];

fn text_datum(text: &str) -> Datum {
    Datum::Text(
        TEXTS
            .iter()
            .find(|candidate| **candidate == text)
            .expect("only the generator's texts are written"),
    )
}

/// A fresh evaluation of the concept `C` on the branch now.
async fn fresh<C>(branch: &Branch, operator: &Operator) -> anyhow::Result<Vec<C>>
where
    C: Predicate<Conclusion = C> + Conclusion + ConditionalSync,
    C::Application: Default + Clone + ConditionalSync,
{
    Ok(branch
        .query()
        .select(Query::<C>::default())
        .perform(operator)
        .try_vec()
        .await?)
}

/// Draw one step: one to four changes on distinct `(field, of)` cells,
/// so no fact is both asserted and retracted in one step, whose order
/// a commit or an overlay instant does not record. A cell holding a
/// value is retracted or overwritten (a single-valued assert
/// supersedes the old value, so it moves both ways at once); an empty
/// one is asserted. Badges are drawn most, since they feed the
/// non-local premises of two rules. Levels range over 0 to 3, so an
/// overwrite can cross the threshold of 2 either way, and a
/// retraction makes the level read as 0.
fn draw(rng: &mut Xorshift, visible: &[(Field, usize, Datum)]) -> Vec<Change> {
    let count = 1 + rng.below(4);
    let mut cells: Vec<(Field, usize)> = Vec::new();
    while cells.len() < count {
        let field = match rng.below(6) {
            0 | 1 => Field::Badge,
            2 => Field::Name,
            3 => Field::Manager,
            4 => Field::Mentor,
            _ => Field::Level,
        };
        let cell = (field, rng.below(PEOPLE));
        if !cells.contains(&cell) {
            cells.push(cell);
        }
    }
    cells
        .into_iter()
        .map(|(field, of)| {
            let held: Vec<&Datum> = visible
                .iter()
                .filter(|(f, o, _)| *f == field && *o == of)
                .map(|(_, _, is)| is)
                .collect();
            if !held.is_empty() && rng.chance(1, 2) {
                let is = held[rng.below(held.len())].clone();
                return Change {
                    assert: false,
                    field,
                    of,
                    is,
                };
            }
            let is = match field {
                Field::Badge => Datum::Text(TEXTS[rng.below(2)]),
                Field::Name => Datum::Text(TEXTS[2 + rng.below(2)]),
                Field::Manager | Field::Mentor => Datum::Person(rng.below(PEOPLE)),
                Field::Level => Datum::Number(rng.below(4) as u32),
            };
            Change {
                assert: true,
                field,
                of,
                is,
            }
        })
        .collect()
}

/// `changes` as one batch of instructions.
fn batch(changes: &[Change]) -> Changes {
    let mut batch = Changes::new();
    for change in changes {
        let of = person(change.of);
        macro_rules! stage {
            ($statement:expr) => {
                if change.assert {
                    batch.assert($statement);
                } else {
                    batch.retract($statement);
                }
            };
        }
        match (change.field, &change.is) {
            (Field::Badge, Datum::Text(text)) => stage!(Badge::of(of).is(text.to_string())),
            (Field::Name, Datum::Text(text)) => stage!(Name::of(of).is(text.to_string())),
            (Field::Manager, Datum::Person(to)) => stage!(Manager::of(of).is(person(*to))),
            (Field::Mentor, Datum::Person(to)) => stage!(Mentor::of(of).is(person(*to))),
            (Field::Level, Datum::Number(level)) => stage!(Level::of(of).is(*level)),
            (field, is) => unreachable!("{field:?} never holds {is}"),
        }
    }
    batch
}

/// What a randomized run did, for the failure message: the seed and
/// each step's changes, with the facts readable before it.
struct Log {
    seed: u64,
    steps: Vec<String>,
}

impl Log {
    /// The steps from `from` on, one per line.
    fn since(&self, from: usize) -> String {
        self.steps[from..]
            .iter()
            .enumerate()
            .map(|(offset, step)| format!("  step {}: {step}", from + offset))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// `rows` in a canonical order, so result sets compare whatever order
/// an evaluation produced them in, duplicates included.
fn canonical<C: fmt::Debug>(rows: &[C]) -> Vec<String> {
    let mut rows: Vec<String> = rows.iter().map(|row| format!("{row:?}")).collect();
    rows.sort();
    rows
}

/// The rows of `left` that are not in `right`, canonically ordered.
fn minus(left: &[String], right: &[String]) -> Vec<String> {
    left.iter()
        .filter(|row| !right.contains(row))
        .cloned()
        .collect()
}

/// A subscription under test, with the result it held after its last
/// poll and the step that poll followed.
struct Watch<C: Predicate> {
    label: String,
    subscription: Subscription<Query<C>>,
    previous: Vec<String>,
    polled_at: usize,
}

impl<C> Watch<C>
where
    C: Predicate<Conclusion = C> + Conclusion + PartialEq + Clone + fmt::Debug + ConditionalSync,
    C::Application: Default + Clone + ConditionalSync,
{
    async fn new(label: String, branch: &Branch, operator: &Operator) -> anyhow::Result<Self> {
        let mut subscription = branch.subscribe(Query::<C>::default());
        subscription.poll(operator).await?;
        let previous = canonical(subscription.results());
        Ok(Self {
            label,
            subscription,
            previous,
            polled_at: 0,
        })
    }

    /// Poll after `step` and hold the invariant: the maintained result
    /// is a fresh evaluation's, and the reported delta is exactly the
    /// difference from the previous result.
    async fn check(
        &mut self,
        step: usize,
        branch: &Branch,
        operator: &Operator,
        log: &Log,
    ) -> anyhow::Result<()> {
        let delta = self.subscription.poll(operator).await?;
        let fresh = canonical(&fresh::<C>(branch, operator).await?);
        let results = canonical(self.subscription.results());
        let (asserted, retracted) = match &delta {
            Some(Delta {
                asserted,
                retracted,
            }) => (canonical(asserted), canonical(retracted)),
            None => (Vec::new(), Vec::new()),
        };
        let expected_asserted = minus(&results, &self.previous);
        let expected_retracted = minus(&self.previous, &results);
        if results != fresh || asserted != expected_asserted || retracted != expected_retracted {
            anyhow::bail!(
                "{label}: the maintained subscription diverged at step {step} of seed {seed}\n\
                 steps since its last poll (after step {last}):\n{steps}\n\
                 previous results: {previous:#?}\n\
                 maintained results: {results:#?}\n\
                 fresh evaluation: {fresh:#?}\n\
                 reported delta: {delta:#?}\n\
                 recomputes: {recomputes}, maintenances: {maintenances}",
                label = self.label,
                seed = log.seed,
                last = self.polled_at,
                steps = log.since(self.polled_at + 1),
                previous = self.previous,
                recomputes = self.subscription.recomputes(),
                maintenances = self.subscription.maintenances(),
            );
        }
        self.previous = results;
        self.polled_at = step;
        Ok(())
    }
}

/// Two subscriptions to the concept `C`: one polled after every step,
/// and one polled only now and then, so one delta can span several
/// commits and overlay instants.
struct Watches<C: Predicate> {
    every: Watch<C>,
    lagging: Watch<C>,
}

impl<C> Watches<C>
where
    C: Predicate<Conclusion = C> + Conclusion + PartialEq + Clone + fmt::Debug + ConditionalSync,
    C::Application: Default + Clone + ConditionalSync,
{
    async fn new(concept: &str, branch: &Branch, operator: &Operator) -> anyhow::Result<Self> {
        Ok(Self {
            every: Watch::new(format!("{concept}, polled every step"), branch, operator).await?,
            lagging: Watch::new(format!("{concept}, polled now and then"), branch, operator)
                .await?,
        })
    }

    async fn check(
        &mut self,
        step: usize,
        rng: &mut Xorshift,
        branch: &Branch,
        operator: &Operator,
        log: &Log,
    ) -> anyhow::Result<()> {
        self.every.check(step, branch, operator, log).await?;
        if rng.chance(1, 3) {
            self.lagging.check(step, branch, operator, log).await?;
        }
        Ok(())
    }

    /// The invariant holding says something only if the polls were
    /// maintained: a subscription that recomputed every time would
    /// pass trivially.
    fn ensure_maintained(&self, seed: u64) -> anyhow::Result<()> {
        let recomputes = self.every.subscription.recomputes();
        let maintenances = self.every.subscription.maintenances();
        anyhow::ensure!(
            maintenances > recomputes,
            "{} (seed {seed}): {maintenances} maintained polls against {recomputes} \
             recomputes; the delta-join path was not what the run exercised",
            self.every.label
        );
        Ok(())
    }

    /// A rule move under the subscribed concept can change any row, so
    /// each one the run made recomputes, beyond the first poll.
    fn ensure_recomputed(&self, toggle: &Toggle, seed: u64) -> anyhow::Result<()> {
        let recomputes = self.every.subscription.recomputes();
        anyhow::ensure!(
            toggle.moves > 0 && recomputes > toggle.moves,
            "{} (seed {seed}): {recomputes} recomputes after {} moves of {}",
            self.every.label,
            toggle.moves,
            toggle.name
        );
        Ok(())
    }
}

/// Every subscription a randomized run holds to the invariant, a pair
/// of [`Watches`] per rule shape: [`Vouched`] (positive concept
/// premises), [`Unvouched`] (negated ones), [`JuniorLedReport`] (an
/// optional field read through a target concept),
/// [`JuniorTaughtReport`] (an optional field read in the rule itself),
/// [`Superior`] (recursive rules), [`VouchedMentee`] and
/// [`AloneLedReport`] (concept premises two layers deep, positive and
/// negated), and [`VouchedMenteeLed`] and [`AloneLedLedReport`] (three
/// layers deep, positive and negated).
struct Watched {
    vouched: Watches<Vouched>,
    unvouched: Watches<Unvouched>,
    junior_led: Watches<JuniorLedReport>,
    junior_taught: Watches<JuniorTaughtReport>,
    superior: Watches<Superior>,
    vouched_mentee: Watches<VouchedMentee>,
    alone_led: Watches<AloneLedReport>,
    vouched_mentee_led: Watches<VouchedMenteeLed>,
    alone_led_led: Watches<AloneLedLedReport>,
}

impl Watched {
    async fn new(branch: &Branch, operator: &Operator) -> anyhow::Result<Self> {
        Ok(Self {
            vouched: Watches::new("Vouched", branch, operator).await?,
            unvouched: Watches::new("Unvouched", branch, operator).await?,
            junior_led: Watches::new("JuniorLedReport", branch, operator).await?,
            junior_taught: Watches::new("JuniorTaughtReport", branch, operator).await?,
            superior: Watches::new("Superior", branch, operator).await?,
            vouched_mentee: Watches::new("VouchedMentee", branch, operator).await?,
            alone_led: Watches::new("AloneLedReport", branch, operator).await?,
            vouched_mentee_led: Watches::new("VouchedMenteeLed", branch, operator).await?,
            alone_led_led: Watches::new("AloneLedLedReport", branch, operator).await?,
        })
    }

    /// Poll every subscription after `step`. The four shapes #711
    /// added decide their lagging polls from `rng`, in the order they
    /// always did, and the rest from `extra`, so a seed's steps are
    /// the steps it drew before the rest were added.
    async fn check(
        &mut self,
        step: usize,
        rng: &mut Xorshift,
        extra: &mut Xorshift,
        branch: &Branch,
        operator: &Operator,
        log: &Log,
    ) -> anyhow::Result<()> {
        self.vouched.check(step, rng, branch, operator, log).await?;
        self.unvouched
            .check(step, rng, branch, operator, log)
            .await?;
        self.junior_led
            .check(step, rng, branch, operator, log)
            .await?;
        self.junior_taught
            .check(step, rng, branch, operator, log)
            .await?;
        self.superior
            .check(step, extra, branch, operator, log)
            .await?;
        self.vouched_mentee
            .check(step, extra, branch, operator, log)
            .await?;
        self.alone_led
            .check(step, extra, branch, operator, log)
            .await?;
        self.vouched_mentee_led
            .check(step, extra, branch, operator, log)
            .await?;
        self.alone_led_led
            .check(step, extra, branch, operator, log)
            .await?;
        Ok(())
    }

    fn ensure_maintained(&self, seed: u64) -> anyhow::Result<()> {
        self.vouched.ensure_maintained(seed)?;
        self.unvouched.ensure_maintained(seed)?;
        self.junior_led.ensure_maintained(seed)?;
        self.junior_taught.ensure_maintained(seed)?;
        self.superior.ensure_maintained(seed)?;
        self.vouched_mentee.ensure_maintained(seed)?;
        self.alone_led.ensure_maintained(seed)?;
        self.vouched_mentee_led.ensure_maintained(seed)?;
        self.alone_led_led.ensure_maintained(seed)?;
        Ok(())
    }
}

/// Rules a rule step moves between. A toggle stands at one of its
/// variants, the first of which installs nothing; a move retracts the
/// rules of the variant it stands at and asserts another's in one
/// commit, so a move between two installing variants replaces one
/// rule with another.
struct Toggle {
    name: &'static str,
    variants: Vec<(&'static str, Vec<DeductiveRule>)>,
    at: usize,
    moves: usize,
}

/// The toggles of a run that moves rules: the mentor chains of the
/// recursive [`Superior`], whose fixpoint a move must rebuild, and a
/// second derivation of [`Unvouched`], local or not, which
/// [`AloneLedReport`] reads a layer up.
fn toggles() -> anyhow::Result<[Toggle; 2]> {
    Ok([
        Toggle {
            name: "mentor chains",
            variants: vec![("none", Vec::new()), ("installed", above_rules("mentor")?)],
            at: 0,
            moves: 0,
        },
        Toggle {
            name: "alone by badge",
            variants: vec![
                ("none", Vec::new()),
                ("own", vec![alone_by_own_badge_rule()?]),
                ("mentor's", vec![alone_by_mentors_badge_rule()?]),
            ],
            at: 0,
            moves: 0,
        },
    ])
}

/// Move one of `toggles` to another of its variants, in one commit,
/// and describe the move for the log.
async fn move_rule(
    rng: &mut Xorshift,
    toggles: &mut [Toggle],
    branch: &Branch,
    operator: &Operator,
) -> anyhow::Result<String> {
    let index = rng.below(toggles.len());
    let toggle = &mut toggles[index];
    let count = toggle.variants.len();
    let to = (toggle.at + 1 + rng.below(count - 1)) % count;
    let mut transaction = branch.transaction();
    for rule in &toggle.variants[toggle.at].1 {
        transaction = transaction.retract(rule);
    }
    for rule in &toggle.variants[to].1 {
        transaction = transaction.assert(rule);
    }
    transaction.commit().publish().perform(operator).await?;
    let described = format!(
        "Rules {}: {} -> {}",
        toggle.name, toggle.variants[toggle.at].0, toggle.variants[to].0
    );
    toggle.at = to;
    toggle.moves += 1;
    Ok(described)
}

/// Commit [`installed_rules`] together with `changes`.
async fn install(branch: &Branch, operator: &Operator, changes: Changes) -> anyhow::Result<()> {
    let mut transaction = branch.transaction();
    for rule in installed_rules()? {
        transaction = transaction.assert(rule);
    }
    transaction
        .integrate(changes)
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(())
}

/// How many steps a run that starts on an unborn branch takes through
/// the session overlay alone before its first commit lands the rules.
const UNBORN_STEPS: usize = 20;

/// What a randomized run exercises beyond the fact steps every run
/// takes.
#[derive(Clone, Copy, Debug)]
struct Shape {
    seed: u64,
    /// Subscribe before the branch's first commit, and change facts
    /// only through the session overlay for [`UNBORN_STEPS`] steps,
    /// until a commit lands the rules: the pin the subscriptions
    /// maintain from has no revision to view the state before a
    /// change in.
    unborn: bool,
    /// Interleave rule steps, each moving a [`Toggle`].
    rules: bool,
}

impl fmt::Display for Shape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match (self.unborn, self.rules) {
            (true, _) => "unborn",
            (false, true) => "moving",
            (false, false) => "fixed",
        };
        write!(f, "{kind} {}", self.seed)
    }
}

/// A run of `seed` with the rules fixed from the first commit on.
const fn fixed(seed: u64) -> Shape {
    Shape {
        seed,
        unborn: false,
        rules: false,
    }
}

/// A run of `seed` whose rules move between fact steps.
const fn moving(seed: u64) -> Shape {
    Shape {
        seed,
        unborn: false,
        rules: true,
    }
}

/// A run of `seed` subscribed before the branch's first commit.
const fn unborn(seed: u64) -> Shape {
    Shape {
        seed,
        unborn: true,
        rules: false,
    }
}

/// Run `steps` random fact steps of `shape`, holding the invariant on
/// every subscription in [`Watched`] after each, and after each rule
/// step a run that moves rules interleaves.
async fn run(shape: Shape, steps: usize) -> anyhow::Result<()> {
    let Shape {
        seed,
        unborn,
        rules,
    } = shape;
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut rng = Xorshift::new(seed);
    // Everything added after #711 that draws (the new shapes' lagging
    // polls, rule steps) draws from a stream of its own, so a fixed
    // run of a seed takes the fact steps it took before.
    let mut extra = Xorshift::new(seed ^ 0xA5A5_A5A5_A5A5_A5A5);
    let mut toggles = toggles()?;
    let mut log = Log {
        seed,
        steps: Vec::new(),
    };

    if unborn {
        log.steps
            .push("subscribed on an unborn branch, nothing installed".into());
    } else {
        // Step 0 installs the rules with a first population, so every
        // subscription first polls a born branch and maintains from
        // then on.
        log.steps.push("the rules, and a first population".into());
        let mut population = Vec::new();
        for _ in 0..3 {
            population.extend(draw(&mut rng, &population_facts(&population)));
        }
        install(&branch, &operator, batch(&population)).await?;
    }
    let mut watched = Watched::new(&branch, &operator).await?;

    let mut step = 0;
    for taken in 0..steps {
        let installed = !unborn || taken > UNBORN_STEPS;
        if rules && installed && extra.chance(1, 8) {
            step += 1;
            let described = move_rule(&mut extra, &mut toggles, &branch, &operator).await?;
            log.steps.push(described);
            watched
                .check(step, &mut rng, &mut extra, &branch, &operator, &log)
                .await?;
        }

        step += 1;
        let before = visible(&branch, &operator).await?;
        let via = if unborn && taken < UNBORN_STEPS {
            if rng.below(16) == 0 {
                Via::Clear
            } else {
                Via::Overlay
            }
        } else if unborn && taken == UNBORN_STEPS {
            Via::Install
        } else {
            match rng.below(16) {
                0 => Via::Clear,
                1..=7 => Via::Commit,
                _ => Via::Overlay,
            }
        };
        let changes = match via {
            Via::Clear => Vec::new(),
            Via::Commit | Via::Overlay | Via::Install => draw(&mut rng, &before),
        };
        let described: Vec<String> = changes.iter().map(ToString::to_string).collect();
        let readable: Vec<String> = before
            .iter()
            .map(|(field, of, is)| format!("{field:?}(p{of}) = {is}"))
            .collect();
        log.steps.push(format!(
            "{via:?} [{}], over [{}]",
            described.join(", "),
            readable.join(", ")
        ));

        match via {
            Via::Commit => {
                branch
                    .transaction()
                    .integrate(batch(&changes))
                    .commit()
                    .publish()
                    .perform(&operator)
                    .await?;
            }
            Via::Install => install(&branch, &operator, batch(&changes)).await?,
            Via::Overlay => {
                branch.overlay().apply(batch(&changes))?;
            }
            Via::Clear => {
                branch.overlay().clear();
            }
        }

        watched
            .check(step, &mut rng, &mut extra, &branch, &operator, &log)
            .await?;
    }

    watched.ensure_maintained(seed)?;
    if rules {
        let [mentor, alone] = &toggles;
        watched.superior.ensure_recomputed(mentor, seed)?;
        watched.unvouched.ensure_recomputed(alone, seed)?;
        watched.alone_led.ensure_recomputed(alone, seed)?;
    }
    Ok(())
}

/// The cells `changes` leave holding a value, as [`draw`] reads them,
/// for drawing a population before anything is committed.
fn population_facts(changes: &[Change]) -> Vec<(Field, usize, Datum)> {
    let mut facts: Vec<(Field, usize, Datum)> = Vec::new();
    for change in changes {
        facts.retain(|(field, of, _)| !(*field == change.field && *of == change.of));
        if change.assert {
            facts.push((change.field, change.of, change.is.clone()));
        }
    }
    facts
}

/// The runs the randomized check takes, unless `DIALOG_SUBSCRIPTION_SEEDS`
/// names seeds, each then a run with its rules fixed (see [`runs`]).
/// To reproduce a failure, its message names the run and step; run a
/// fixed run's seed alone with `DIALOG_SUBSCRIPTION_SEEDS=<seed>`, or
/// put the run here alone.
///
/// The six fixed runs are the seeds #711 and #714 chose. Their fact
/// steps are the ones they always drew: what the shapes added since
/// draw from a stream of their own. Each was checked to fail, for
/// every rule it names, under three mutations of the delta-join
/// discovery: every join reading the state after the change (all four
/// of [`Vouched`], [`Unvouched`], [`JuniorLedReport`] and
/// [`JuniorTaughtReport`]), and an optional premise read on one side
/// only, the changed fact's or the after side ([`JuniorLedReport`],
/// [`JuniorTaughtReport`]). The first was checked again with the shapes
/// added since.
///
/// Seeds 6, 11 and 35 diverged before the overlay rewind restored
/// how each instant found its facts: each reaches a fact stored over
/// its own tombstone, then a step that moves it (an overlay clear, an
/// overlay instant, a commit), where the rewind that undid readable
/// moves alone lost the fact from the view before the change.
///
/// The shapes added since were checked to fail under mutations of the
/// paths they reach, on these runs:
///
/// - [`Superior`], on all ten, when the fixpoint continuation ignores
///   what became readable (extension returns the retained table as
///   is), when its retraction skips re-deriving the suspects, and when
///   it skips cascading suspicion through recursive occurrences.
/// - [`VouchedMentee`] and [`AloneLedReport`], on all ten, when the
///   delta-join discovery keeps only the subscribed concept's own
///   candidate heads, so a change reaches the head through one layer of
///   concept premises and no more. [`JuniorLedReport`] fails too: its
///   target's rule reads a formula, which the analysis does not count
///   as entity-local, so it was already two layers deep.
/// - When a commit's change to a rule-discovery range is taken for no
///   change: [`Superior`], [`Unvouched`] and [`AloneLedReport`], whose
///   rules the toggles move, on both runs that move rules; and every
///   rule-derived shape on the unborn runs, whose first commit lands
///   the rules. [`Superior`], on both runs that move rules, when a
///   recompute reuses the retained fixpoint instead of rebuilding it.
/// - [`Vouched`] (seed 4) and [`VouchedMentee`] (seeds 2 and 4) on the
///   unborn runs, when the discovery joins against the state after the
///   change where an unborn pin has no state before to view.
/// - [`VouchedMenteeLed`] and [`AloneLedLedReport`], on all ten, when
///   the discovery drops the heads of every concept three layers of
///   concept premises below the subscribed one. On the first run every
///   shallower shape was still fresh when they diverged, so the third
///   layer is theirs alone to cover.
///
/// The shapes reaching a cell through another entity ([`Superior`],
/// [`VouchedMentee`], [`AloneLedReport`]) diverged on every run until
/// #716 widened the poll's changes by every other value of each
/// changed cell: a write under a single-valued attribute can flip
/// which of a cell's values is readable, and the diff reports only the
/// write. The three tests after this one pin it in small. Changing
/// these runs, check all of it again, so each shape keeps its teeth.
const RUNS: [Shape; 10] = [
    fixed(1),
    fixed(708),
    fixed(710),
    fixed(6),
    fixed(11),
    fixed(35),
    moving(3),
    moving(8),
    unborn(2),
    unborn(4),
];

/// Fact steps per run, unless `DIALOG_SUBSCRIPTION_STEPS` names another
/// count.
const STEPS: usize = 100;

/// The value of the environment variable `name`, if it is set.
fn knob(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("reading {name}")),
    }
}

/// The runs to take: [`RUNS`], or a run with fixed rules per seed in
/// the comma-separated list in `DIALOG_SUBSCRIPTION_SEEDS`, each item
/// a seed (`4711`) or a half-open range of seeds (`1000..1300`). A
/// bare number is one seed, never a count, so the seed a failure names
/// reproduces it alone. A malformed list fails the test rather than
/// falling back, so a typo cannot shrink a sweep to the committed runs
/// unnoticed.
fn runs() -> anyhow::Result<Vec<Shape>> {
    let Some(list) = knob("DIALOG_SUBSCRIPTION_SEEDS")? else {
        return Ok(RUNS.to_vec());
    };
    let seed = |text: &str| -> anyhow::Result<u64> {
        text.trim()
            .parse()
            .with_context(|| format!("DIALOG_SUBSCRIPTION_SEEDS={list:?}: {text:?} is not a seed"))
    };
    let mut seeds = Vec::new();
    for item in list.split(',').filter(|item| !item.trim().is_empty()) {
        match item.split_once("..") {
            Some((start, end)) => {
                let (start, end) = (seed(start)?, seed(end)?);
                anyhow::ensure!(
                    start < end,
                    "DIALOG_SUBSCRIPTION_SEEDS={list:?}: the range {item:?} is empty"
                );
                seeds.extend(start..end);
            }
            None => seeds.push(seed(item)?),
        }
    }
    anyhow::ensure!(
        !seeds.is_empty(),
        "DIALOG_SUBSCRIPTION_SEEDS={list:?} names no seed"
    );
    Ok(seeds.into_iter().map(fixed).collect())
}

/// Steps per seed: [`STEPS`], or the count in
/// `DIALOG_SUBSCRIPTION_STEPS`.
fn steps() -> anyhow::Result<usize> {
    match knob("DIALOG_SUBSCRIPTION_STEPS")? {
        Some(steps) => steps
            .trim()
            .parse()
            .with_context(|| format!("DIALOG_SUBSCRIPTION_STEPS={steps:?} is not a count")),
        None => Ok(STEPS),
    }
}

/// Names its run on stderr if the run panics, which, unlike a
/// divergence, carries no message naming it.
struct Panicking(Shape);

impl Drop for Panicking {
    fn drop(&mut self) {
        if thread::panicking() {
            eprintln!("run {} panicked", self.0);
        }
    }
}

/// After any sequence of multi-fact commits and session-overlay
/// instants, a subscription's result is a fresh evaluation of its
/// query, and the delta it reports is exactly the difference from its
/// previous result. Each step changes several facts at once, in both
/// directions, through each way a rule reads a changed fact: positive
/// concept premises ([`Vouched`]), negated ones ([`Unvouched`]), an
/// optional field whose value decides whether a row exists
/// ([`JuniorLedReport`], [`JuniorTaughtReport`]), concept premises two
/// layers deep ([`VouchedMentee`], [`AloneLedReport`]), and recursive
/// rules, maintained by the fixpoint continuation instead of the
/// delta-join ([`Superior`]). Some runs also move rules between fact
/// steps, and some subscribe before the branch's first commit. #708
/// and the holes #710 closed with it appeared only when such changes
/// moved together, or moved a row through an optional field, which no
/// single-fact test reaches.
///
/// Every seed runs even after one fails, so a wide sweep (the nightly
/// `soak:subscription` arm) reports each seed that diverges, with the
/// first failure in full.
#[dialog_common::test]
async fn a_maintained_subscription_matches_a_fresh_evaluation_after_random_changes()
-> anyhow::Result<()> {
    let runs = runs()?;
    let steps = steps()?;
    let mut failures = Vec::new();
    for &shape in &runs {
        let _panicking = Panicking(shape);
        if let Err(error) = run(shape, steps).await {
            failures.push((shape, error));
        }
    }
    let Some((first, error)) = failures.first() else {
        return Ok(());
    };
    let failing: Vec<String> = failures
        .iter()
        .map(|(shape, _)| shape.to_string())
        .collect();
    anyhow::bail!(
        "{failed} of {total} runs diverged at {steps} steps per run: {failing}\n\
         reproduce a fixed run with DIALOG_SUBSCRIPTION_SEEDS=<seed> DIALOG_SUBSCRIPTION_STEPS={steps}, \
         another by putting it in RUNS alone\n\
         the first, {first}: {error:#}",
        failed = failures.len(),
        total = runs.len(),
        failing = failing.join(", "),
    )
}

/// A maintained subscription after `poll`, held against a fresh
/// evaluation: the rows each holds, canonically ordered.
async fn maintained_and_fresh<C>(
    subscription: &mut Subscription<Query<C>>,
    branch: &Branch,
    operator: &Operator,
) -> anyhow::Result<(Vec<String>, Vec<String>)>
where
    C: Predicate<Conclusion = C> + Conclusion + PartialEq + Clone + fmt::Debug + ConditionalSync,
    C::Application: Default + Clone + ConditionalSync,
{
    subscription.poll(operator).await?;
    Ok((
        canonical(subscription.results()),
        canonical(&fresh::<C>(branch, operator).await?),
    ))
}

/// A session write under a single-valued attribute outvotes the
/// committed value of the cell, which stops being readable without the
/// overlay's instant reporting it: the overlay holds only its own
/// store's moves. A recursive subscription maintains through the
/// fixpoint continuation, whose retraction step is seeded by the
/// changes the poll reports, so the row the outvoted value supported
/// has to go all the same. p1's manager is committed as p0, then set
/// to p2 in the session: p1 is below p2 alone.
#[dialog_common::test]
async fn an_overlay_value_outvoting_a_committed_one_retracts_what_it_derived_recursively()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut transaction = branch.transaction();
    for rule in above_rules("manager")? {
        transaction = transaction.assert(rule);
    }
    transaction
        .assert(Manager::of(person(1)).is(person(0)))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let mut subscription = branch.subscribe(Query::<Superior>::default());
    subscription.poll(&operator).await?;

    branch
        .overlay()
        .assert(Manager::of(person(1)).is(person(2)))?;
    let (maintained, fresh) =
        maintained_and_fresh::<Superior>(&mut subscription, &branch, &operator).await?;
    assert_eq!(
        fresh,
        canonical(&[Superior {
            this: person(1),
            above: Above(person(2)),
        }]),
        "the session's manager outvotes the committed one"
    );
    assert_eq!(maintained, fresh);
    Ok(())
}

/// The delta-join has the same hole when a rule reads another
/// entity's single-valued attribute by its value: p1 is alone while
/// their mentor p0's committed badge is B1 ([`alone_by_mentors_badge_rule`]).
/// The session sets p0's badge to B0, which outvotes B1; the only
/// change reported is B0 arriving, which binds no derivation, so
/// nothing finds p1 to re-derive.
#[dialog_common::test]
async fn an_overlay_value_outvoting_a_committed_one_retracts_a_row_read_through_another_entity()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(&alone_by_mentors_badge_rule()?)
        .assert(Name::of(person(1)).is("N0".to_string()))
        .assert(Mentor::of(person(1)).is(person(0)))
        .assert(Badge::of(person(0)).is("B1".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let mut subscription = branch.subscribe(Query::<Unvouched>::default());
    let initial = subscription.poll(&operator).await?.expect("initial");
    assert_eq!(
        initial.asserted.len(),
        1,
        "p1 is alone by their mentor's badge"
    );

    branch
        .overlay()
        .assert(Badge::of(person(0)).is("B0".to_string()))?;
    let (maintained, fresh) =
        maintained_and_fresh::<Unvouched>(&mut subscription, &branch, &operator).await?;
    assert!(fresh.is_empty(), "B0 outvotes B1, so p1 is not alone");
    assert_eq!(maintained, fresh);
    Ok(())
}

/// A single-valued cell can hold two committed values: while the
/// session's value wins the cell, a committed write succeeds the claim
/// its reader observes, the session's, and leaves the committed one
/// beneath it in place. Retracting the newer committed value then makes the older one
/// readable again, and the commit's diff reports only the retraction.
/// p1's manager is committed as p3, set to p1 in the session, committed
/// as p2, and the session cleared before subscribing, so p1 is below
/// p2. Retracting p2 makes p3 p1's manager again.
#[dialog_common::test]
async fn retracting_a_committed_winner_restores_the_value_it_outvoted() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut transaction = branch.transaction();
    for rule in above_rules("manager")? {
        transaction = transaction.assert(rule);
    }
    transaction
        .assert(Manager::of(person(1)).is(person(3)))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch
        .overlay()
        .assert(Manager::of(person(1)).is(person(1)))?;
    branch
        .transaction()
        .assert(Manager::of(person(1)).is(person(2)))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.overlay().clear();
    let below = |manager: usize| {
        canonical(&[Superior {
            this: person(1),
            above: Above(person(manager)),
        }])
    };
    let mut subscription = branch.subscribe(Query::<Superior>::default());
    subscription.poll(&operator).await?;
    assert_eq!(canonical(subscription.results()), below(2));

    branch
        .transaction()
        .retract(Manager::of(person(1)).is(person(2)))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let (maintained, fresh) =
        maintained_and_fresh::<Superior>(&mut subscription, &branch, &operator).await?;
    assert_eq!(fresh, below(3), "the committed p3 is readable again");
    assert_eq!(maintained, fresh);
    Ok(())
}

/// A recursive rule whose recursion steps only through badged managers:
/// `above(this) = m :- manager(this) = m, badge(m) = "B1"`, and
/// `above(this) = a :- manager(this) = m, badge(m) = "B1", above(m) = a`.
/// The fixpoint continuation's suspicion cascade has to follow the join
/// to the manager's badge, not only the recursive edge.
fn badged_above_rules() -> anyhow::Result<Vec<DeductiveRule>> {
    Ok(vec![
        rule(serde_json::json!({
            "deduce": { "with": above_field() },
            "when": [report("manager", "entity:", "this", "above"), badged_b1("above")]
        }))?,
        rule(serde_json::json!({
            "deduce": { "with": above_field() },
            "when": [
                report("manager", "entity:", "this", "next"),
                badged_b1("next"),
                {
                    "assert": { "with": above_field() },
                    "where": { "this": var("next"), "above": var("above") }
                }
            ]
        }))?,
    ])
}

/// A badge on a recursive chain going and coming back, by commit and
/// through the session overlay, retracts and restores every row derived
/// through it. p1 reports to p0, p0 to p2, both badged: p1 is above p0
/// and p2, p0 above p2. Without p2's badge the chain stops at p0;
/// without p0's it stops at once.
#[dialog_common::test]
async fn a_recursive_rule_follows_a_badge_on_its_chain() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut transaction = branch.transaction();
    for rule in badged_above_rules()? {
        transaction = transaction.assert(rule);
    }
    transaction
        .assert(Manager::of(person(1)).is(person(0)))
        .assert(Manager::of(person(0)).is(person(2)))
        .assert(Badge::of(person(0)).is("B1".to_string()))
        .assert(Badge::of(person(2)).is("B1".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let mut subscription = branch.subscribe(Query::<Superior>::default());
    let initial = subscription.poll(&operator).await?.expect("initial");
    assert_eq!(initial.asserted.len(), 3, "p1 above p0 and p2, p0 above p2");

    let badge = |who: usize| Badge::of(person(who)).is("B1".to_string());
    branch
        .transaction()
        .retract(badge(2))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let (maintained, fresh) =
        maintained_and_fresh::<Superior>(&mut subscription, &branch, &operator).await?;
    assert_eq!(fresh.len(), 1, "the chain stops at p0");
    assert_eq!(
        maintained, fresh,
        "a committed badge retraction on the chain"
    );
    assert_eq!(subscription.maintenances(), 1, "maintained, not recomputed");

    branch
        .transaction()
        .assert(badge(2))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let (maintained, fresh) =
        maintained_and_fresh::<Superior>(&mut subscription, &branch, &operator).await?;
    assert_eq!(fresh.len(), 3, "the chain is whole again");
    assert_eq!(
        maintained, fresh,
        "a committed badge returning to the chain"
    );

    branch.overlay().retract(badge(0))?;
    let (maintained, fresh) =
        maintained_and_fresh::<Superior>(&mut subscription, &branch, &operator).await?;
    assert_eq!(fresh.len(), 1, "p1 is above nobody; p0 is still above p2");
    assert_eq!(maintained, fresh, "a session badge retraction on the chain");

    branch.overlay().assert(badge(0))?;
    let (maintained, fresh) =
        maintained_and_fresh::<Superior>(&mut subscription, &branch, &operator).await?;
    assert_eq!(fresh.len(), 3);
    assert_eq!(maintained, fresh, "a session badge returning to the chain");
    Ok(())
}

/// A rule installed through the session overlay, and removed through
/// it, moves a subscription the way a committed rule change does: the
/// rule range in the cover is touched, and the poll recomputes.
#[dialog_common::test]
async fn a_rule_installed_through_the_overlay_moves_the_subscription() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(Name::of(person(1)).is("N0".to_string()))
        .assert(Badge::of(person(1)).is("B1".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let mut subscription = branch.subscribe(Query::<Unvouched>::default());
    let initial = subscription.poll(&operator).await?.expect("initial");
    assert!(initial.asserted.is_empty(), "no rule yet, no rows");

    branch.overlay().assert(&alone_by_own_badge_rule()?)?;
    let (maintained, fresh) =
        maintained_and_fresh::<Unvouched>(&mut subscription, &branch, &operator).await?;
    assert_eq!(fresh.len(), 1, "the session rule derives p1");
    assert_eq!(maintained, fresh, "a rule arriving through the overlay");
    assert_eq!(
        (subscription.recomputes(), subscription.maintenances()),
        (2, 0),
        "a rule change recomputes"
    );

    branch.overlay().retract(&alone_by_own_badge_rule()?)?;
    let (maintained, fresh) =
        maintained_and_fresh::<Unvouched>(&mut subscription, &branch, &operator).await?;
    assert!(fresh.is_empty());
    assert_eq!(maintained, fresh, "a rule leaving through the overlay");
    Ok(())
}

/// Two writers each commit a manager for p1 on their own first commit;
/// one pulls the other, and the merge leaves both claims in the cell,
/// with the pick electing one. A subscription on the puller sees the
/// merge as a poll over the pulled changes; retracting the elected
/// claim then makes the other readable, which the diff does not report.
#[dialog_common::test]
async fn a_merge_leaving_two_claims_in_a_cell_keeps_a_recursive_subscription_fresh()
-> anyhow::Result<()> {
    let (writer, profile) = test_session_with_peer().await;
    let name = unique_name("repo");
    profile.space(name.clone()).open().perform(&writer).await?;
    let one = profile
        .session(b"one")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let two = profile
        .session(b"two")
        .space(profile.state())
        .allow(Subject::any())
        .await?;
    let w1 = profile
        .space(name.clone())
        .open()
        .perform(&one)
        .await?
        .branch("one")
        .open()
        .perform(&one)
        .await?;
    let w2 = profile
        .space(name.clone())
        .open()
        .perform(&two)
        .await?
        .branch("two")
        .open()
        .perform(&two)
        .await?;

    // Concurrent first commits: w2 knows nothing of w1's claim.
    w2.transaction()
        .assert(Manager::of(person(1)).is(person(2)))
        .commit()
        .publish()
        .perform(&two)
        .await?;
    let mut transaction = w1.transaction();
    for rule in above_rules("manager")? {
        transaction = transaction.assert(rule);
    }
    transaction
        .assert(Manager::of(person(1)).is(person(0)))
        .commit()
        .publish()
        .perform(&one)
        .await?;

    let mut subscription = w1.subscribe(Query::<Superior>::default());
    let initial = subscription.poll(&one).await?.expect("initial");
    assert_eq!(initial.asserted.len(), 1, "p1 is above p0");

    w1.pull().from(&w2).perform(&one).await?;
    let (maintained, expected) =
        maintained_and_fresh::<Superior>(&mut subscription, &w1, &one).await?;
    assert_eq!(
        expected.len(),
        1,
        "the cell holds two claims and the pick elects one"
    );
    assert_eq!(maintained, expected, "after the merge");
    let elected = fresh::<Superior>(&w1, &one).await?[0].above.0.clone();
    let other = if elected == person(0) {
        person(2)
    } else {
        person(0)
    };

    w1.transaction()
        .retract(Manager::of(person(1)).is(elected))
        .commit()
        .publish()
        .perform(&one)
        .await?;
    let rows = fresh::<Superior>(&w1, &one).await?;
    assert_eq!(rows.len(), 1, "the other claim is readable now");
    assert_eq!(rows[0].above.0, other);
    let (maintained, expected) =
        maintained_and_fresh::<Superior>(&mut subscription, &w1, &one).await?;
    assert_eq!(
        maintained, expected,
        "retracting the elected claim uncovers the other"
    );
    Ok(())
}
