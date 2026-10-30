//! Invariants the pull request's subscription changes claim, pinned as
//! tests.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::fmt;

use super::{Delta, Subscription};
use crate::Branch;
use crate::helpers::test_repo;
use dialog_artifacts::{Changes, Entity};
use dialog_common::ConditionalSync;
use dialog_peer::helpers::test_session_with_peer;
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

/// What the rule with negated premises derives (`report/alone`): the
/// name of someone whose manager and mentor both hold no badge.
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct Alone(pub String);

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

/// `report/alone(this) := name :- report/name(this) = name,
/// report/manager(this) = m, report/mentor(this) = t,
/// unless BadgeHolder(m), unless BadgeHolder(t)`: the mirror of
/// [`Vouched`], two concept premises under negation.
fn unvouched_rule() -> anyhow::Result<DeductiveRule> {
    let var = |name: &str| serde_json::json!({ "?": { "name": name } });
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "alone": { "the": "report/alone", "as": "text:" } } },
        "when": [
            {
                "assert": { "with": { "name": { "the": "report/name", "as": "text:" } } },
                "where": { "this": var("this"), "name": var("alone") }
            },
            {
                "assert": { "with": { "manager": { "the": "report/manager", "as": "entity:" } } },
                "where": { "this": var("this"), "manager": var("manager") }
            },
            {
                "assert": { "with": { "mentor": { "the": "report/mentor", "as": "entity:" } } },
                "where": { "this": var("this"), "mentor": var("mentor") }
            }
        ],
        "unless": [
            {
                "assert": { "with": { "badge": { "the": "credential/badge", "as": "text:" } } },
                "where": { "this": var("manager") }
            },
            {
                "assert": { "with": { "badge": { "the": "credential/badge", "as": "text:" } } },
                "where": { "this": var("mentor") }
            }
        ]
    }))?;
    Ok(descriptor.compile()?)
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
}

/// A value under a [`Field`]: text, or one of the [`PEOPLE`] by index.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Datum {
    Text(&'static str),
    Person(usize),
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Datum::Text(text) => write!(f, "{text:?}"),
            Datum::Person(index) => write!(f, "p{index}"),
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
/// non-local premises of both rules.
fn draw(rng: &mut Xorshift, visible: &[(Field, usize, Datum)]) -> Vec<Change> {
    let count = 1 + rng.below(4);
    let mut cells: Vec<(Field, usize)> = Vec::new();
    while cells.len() < count {
        let field = match rng.below(5) {
            0 | 1 => Field::Badge,
            2 => Field::Name,
            3 => Field::Manager,
            _ => Field::Mentor,
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
        let (text, to) = match &change.is {
            Datum::Text(text) => (text.to_string(), None),
            Datum::Person(index) => (String::new(), Some(person(*index))),
        };
        macro_rules! stage {
            ($statement:expr) => {
                if change.assert {
                    batch.assert($statement);
                } else {
                    batch.retract($statement);
                }
            };
        }
        match change.field {
            Field::Badge => stage!(Badge::of(of).is(text)),
            Field::Name => stage!(Name::of(of).is(text)),
            Field::Manager => stage!(Manager::of(of).is(to.expect("a person"))),
            Field::Mentor => stage!(Mentor::of(of).is(to.expect("a person"))),
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
    label: &'static str,
    subscription: Subscription<Query<C>>,
    previous: Vec<String>,
    polled_at: usize,
}

impl<C> Watch<C>
where
    C: Predicate<Conclusion = C> + Conclusion + PartialEq + Clone + fmt::Debug + ConditionalSync,
    C::Application: Default + Clone + ConditionalSync,
{
    async fn new(
        label: &'static str,
        branch: &Branch,
        operator: &Operator,
    ) -> anyhow::Result<Self> {
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

/// Run `steps` random steps from `seed`, holding the invariant on four
/// subscriptions: [`Vouched`] and [`Unvouched`], each polled after
/// every step and polled only now and then, so one delta can span
/// several commits and overlay instants.
async fn run(seed: u64, steps: usize) -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut rng = Xorshift::new(seed);
    let mut log = Log {
        seed,
        steps: vec!["the rule, and a first population".into()],
    };

    // Step 0 installs the rule with a first population, so every
    // subscription first polls a born branch and maintains from then on.
    let mut population = Vec::new();
    for _ in 0..3 {
        population.extend(draw(&mut rng, &population_facts(&population)));
    }
    branch
        .transaction()
        .assert(&unvouched_rule()?)
        .integrate(batch(&population))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let mut vouched =
        Watch::<Vouched>::new("Vouched, polled every step", &branch, &operator).await?;
    let mut unvouched =
        Watch::<Unvouched>::new("Unvouched, polled every step", &branch, &operator).await?;
    let mut vouched_lagging =
        Watch::<Vouched>::new("Vouched, polled now and then", &branch, &operator).await?;
    let mut unvouched_lagging =
        Watch::<Unvouched>::new("Unvouched, polled now and then", &branch, &operator).await?;

    for step in 1..=steps {
        let before = visible(&branch, &operator).await?;
        let via = match rng.below(16) {
            0 => Via::Clear,
            1..=7 => Via::Commit,
            _ => Via::Overlay,
        };
        let changes = match via {
            Via::Clear => Vec::new(),
            Via::Commit | Via::Overlay => draw(&mut rng, &before),
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
            Via::Overlay => {
                branch.overlay().apply(batch(&changes))?;
            }
            Via::Clear => {
                branch.overlay().clear();
            }
        }

        vouched.check(step, &branch, &operator, &log).await?;
        unvouched.check(step, &branch, &operator, &log).await?;
        if rng.chance(1, 3) {
            vouched_lagging
                .check(step, &branch, &operator, &log)
                .await?;
        }
        if rng.chance(1, 3) {
            unvouched_lagging
                .check(step, &branch, &operator, &log)
                .await?;
        }
    }

    // The invariant held, but it says something only if the polls
    // were maintained: a subscription that recomputed every time
    // would pass trivially.
    for (label, recomputes, maintenances) in [
        (
            vouched.label,
            vouched.subscription.recomputes(),
            vouched.subscription.maintenances(),
        ),
        (
            unvouched.label,
            unvouched.subscription.recomputes(),
            unvouched.subscription.maintenances(),
        ),
    ] {
        anyhow::ensure!(
            maintenances > recomputes,
            "{label} (seed {seed}): {maintenances} maintained polls against {recomputes} \
             recomputes; the delta-join path was not what the run exercised"
        );
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

/// The seeds the randomized check runs. To reproduce a failure, its
/// message names the seed and step; put that seed here alone.
///
/// Each was checked to fail when every delta-join reads the state
/// after the change (#708's class of bug): seed 1 through [`Vouched`],
/// 708 and 710 through [`Unvouched`] too, on its own. Changing them,
/// check that again, so both rules keep their teeth.
const SEEDS: [u64; 3] = [1, 708, 710];

/// Steps per seed.
const STEPS: usize = 100;

/// After any sequence of multi-fact commits and session-overlay
/// instants, a subscription's result is a fresh evaluation of its
/// query, and the delta it reports is exactly the difference from its
/// previous result. Each step changes several facts at once, in both
/// directions, through a non-local rule with two conformance premises
/// ([`Vouched`]) and its mirror under two negated ones
/// ([`Unvouched`]): the shapes #708 found only when two facts
/// supporting one row moved together, which no single-fact test
/// reaches.
#[dialog_common::test]
async fn a_maintained_subscription_matches_a_fresh_evaluation_after_random_changes()
-> anyhow::Result<()> {
    for seed in SEEDS {
        run(seed, STEPS).await?;
    }
    Ok(())
}
