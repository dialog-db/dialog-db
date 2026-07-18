//! Semi-naive fixpoint evaluation for recursive concepts.
//!
//! When the queried concept participates in a dependency cycle (a
//! non-trivial SCC in the [`ProgramAnalysis`] graph), top-down
//! evaluation would recurse unboundedly. This module evaluates the
//! whole strongly connected component bottom-up instead:
//!
//! 1. **Seed round.** Every rule of every component member with no
//!    in-component concept premise evaluates on the ordinary
//!    top-down path; its rows seed the [`AnswerTable`].
//! 2. **Delta rounds.** Each round re-derives every rule with
//!    in-component premises, semi-naive style: per rule and per
//!    recursive occurrence, that occurrence reads the previous
//!    round's *delta* while the other occurrences read the running
//!    *total*, so only derivations involving at least one new row
//!    are recomputed. The occurrences bind their rows into a
//!    [`Match`]; the remaining (non-recursive) premises are planned
//!    against that binding scope and evaluated on the ordinary
//!    top-down path.
//! 3. **Termination.** The fixpoint is reached when a round stages
//!    nothing new. [`MAX_ROUNDS`] is the loud-failure safety valve
//!    for rule sets that generate unboundedly (e.g. through
//!    formulas).
//!
//! The stratification contract makes this sound: negation inside a
//! component is rejected before evaluation (see
//! [`ProgramAnalysis::check`]), so every premise a component member
//! negates is fully derivable before the component's fixpoint runs.
//!
//! Goal-directed (magic-set) filtering of the component's answer
//! space is future work: this evaluator computes the component's
//! full fixpoint and joins the caller's bindings against the result
//! afterwards.

use super::ConceptQuery;
use crate::concept::descriptor::ConceptDescriptor;
use crate::error::EvaluationError;
use crate::negation::Negation;
use crate::parameters::Parameters;
use crate::planner::Planner;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::rule::deductive::DeductiveRule;
use crate::selection::{Binding, Match};
use crate::session::ProgramAnalysis;
use crate::source::SelectRules;
use crate::term::Term;
use crate::types::Any;
use crate::{Entity, Environment, Value};
use core::{iter, mem};
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use futures_util::TryStreamExt;
use std::collections::{BTreeMap, HashMap};

/// Upper bound on fixpoint rounds before the query fails loudly. A
/// round derives at least one new row (otherwise the fixpoint has
/// converged), so purely fact-driven recursion terminates well under
/// this; hitting it means the rule set generates fresh values every
/// round (e.g. through a formula) and would spin forever.
pub const MAX_ROUNDS: usize = 1000;

/// One derived conclusion: operand name to value. Operands resolved
/// to `Absent` (optional fields) are omitted.
pub type Row = BTreeMap<String, Value>;

/// Storage for the answers accumulated during a fixpoint run. The
/// trait is the swap point for bounded-memory (disk-backed)
/// implementations; the evaluator only ever appends, advances, and
/// scans.
pub trait AnswerTable {
    /// Stage a freshly derived row. Returns `false` when the row is
    /// already known (in the total or already staged), `true` when
    /// it is new.
    fn insert(&mut self, concept: &Entity, row: Row) -> bool;

    /// End the round: staged rows become the new delta and join the
    /// total. Returns `true` when the new delta is non-empty (the
    /// fixpoint has not converged).
    fn advance(&mut self) -> bool;

    /// Every row derived for the concept so far, including the
    /// current delta.
    fn total(&self, concept: &Entity) -> Vec<Row>;

    /// The rows first derived in the previous round.
    fn delta(&self, concept: &Entity) -> Vec<Row>;
}

/// In-memory [`AnswerTable`]. [`Value`] has no total order (floats),
/// so rows are deduplicated by their canonical dag-cbor encoding and
/// stored in sorted maps for deterministic iteration.
#[derive(Debug, Default)]
pub struct InMemoryAnswerTable {
    total: HashMap<Entity, BTreeMap<Vec<u8>, Row>>,
    delta: HashMap<Entity, BTreeMap<Vec<u8>, Row>>,
    staged: HashMap<Entity, BTreeMap<Vec<u8>, Row>>,
}

/// The canonical identity of a row: its dag-cbor bytes. dag-cbor
/// sorts map keys per spec, so the encoding is a pure function of
/// the row's contents.
fn row_key(row: &Row) -> Vec<u8> {
    serde_ipld_dagcbor::to_vec(row).expect("a row of values encodes")
}

impl AnswerTable for InMemoryAnswerTable {
    fn insert(&mut self, concept: &Entity, row: Row) -> bool {
        let key = row_key(&row);
        if self
            .total
            .get(concept)
            .is_some_and(|rows| rows.contains_key(&key))
        {
            return false;
        }
        self.staged
            .entry(concept.clone())
            .or_default()
            .insert(key, row)
            .is_none()
    }

    fn advance(&mut self) -> bool {
        self.delta = mem::take(&mut self.staged);
        for (concept, rows) in &self.delta {
            self.total
                .entry(concept.clone())
                .or_default()
                .extend(rows.iter().map(|(key, row)| (key.clone(), row.clone())));
        }
        self.delta.values().any(|rows| !rows.is_empty())
    }

    fn total(&self, concept: &Entity) -> Vec<Row> {
        self.total
            .get(concept)
            .map(|rows| rows.values().cloned().collect())
            .unwrap_or_default()
    }

    fn delta(&self, concept: &Entity) -> Vec<Row> {
        self.delta
            .get(concept)
            .map(|rows| rows.values().cloned().collect())
            .unwrap_or_default()
    }
}

/// Join one caller row against one derived row: bind the caller's
/// terms to the row's operand values, treating conflicts (a term
/// already bound to a different value, or a constant that doesn't
/// match) as a non-match. Operands the row resolved to `Absent`
/// bind absent, so optional concept fields keep their left-join
/// semantics.
pub fn join(
    input: &Match,
    terms: &Parameters,
    row: &Row,
) -> Result<Option<Match>, EvaluationError> {
    let mut merged = input.clone();
    for (param, term) in terms.iter() {
        match (row.get(param), term) {
            (Some(value), Term::Variable { name: Some(_), .. }) => {
                if merged.bind(term, value.clone()).is_err() {
                    return Ok(None);
                }
            }
            (Some(value), Term::Constant(expected)) => {
                if expected != value {
                    return Ok(None);
                }
            }
            (None, Term::Variable { name: Some(_), .. }) => {
                if merged.bind_absent(term).is_err() {
                    return Ok(None);
                }
            }
            (None, Term::Constant(_)) => return Ok(None),
            (_, Term::Variable { name: None, .. }) => {}
        }
    }
    Ok(Some(merged))
}

/// A component member's rule split into its in-component concept
/// premises (the *recursive occurrences*, evaluated from the answer
/// table) and everything else (the *base premises*, evaluated
/// top-down).
struct SplitRule {
    rule: DeductiveRule,
    occurrences: Vec<ConceptQuery>,
    base: Vec<Premise>,
}

/// A component member: its descriptor (for row projection) and its
/// split rules.
struct Member {
    descriptor: ConceptDescriptor,
    rules: Vec<SplitRule>,
}

/// Whether the premise applies a concept in the same cycle as
/// `root`, returning the application when so.
fn in_component<'p>(
    premise: &'p Premise,
    analysis: &ProgramAnalysis,
    root: &Entity,
) -> Option<&'p ConceptQuery> {
    let query = match premise {
        Premise::Assert(Proposition::Concept(query)) => query,
        // Negation into the component is rejected by the
        // stratification check before evaluation begins.
        Premise::Unless(Negation(Proposition::Concept(query))) => query,
        _ => return None,
    };
    analysis
        .in_same_cycle(root, &query.predicate.this())
        .then_some(query)
}

/// Bind one recursive occurrence's terms from a table row. Returns
/// `false` when the row conflicts with the bindings accumulated so
/// far (the combination is a non-match).
fn bind_occurrence(matched: &mut Match, occurrence: &ConceptQuery, row: &Row) -> bool {
    for (param, term) in occurrence.terms.iter() {
        let Some(value) = row.get(param) else {
            // The row resolved this operand to Absent (an optional
            // field); nothing to bind.
            continue;
        };
        match term {
            Term::Variable { name: Some(_), .. } => {
                if matched.bind(term, value.clone()).is_err() {
                    return false;
                }
            }
            Term::Constant(expected) => {
                if expected != value {
                    return false;
                }
            }
            Term::Variable { name: None, .. } => {}
        }
    }
    true
}

/// Project a rule's result match into a conclusion [`Row`]: one
/// entry per conclusion operand bound to a present value; operands
/// resolved to `Absent` (or never bound) are omitted.
fn project(descriptor: &ConceptDescriptor, matched: &Match) -> Row {
    let mut row = Row::new();
    let operands = iter::once("this").chain(descriptor.with().keys());
    for operand in operands {
        if let Ok(Binding::Present(value)) = matched.lookup(&Term::<Any>::var(operand)) {
            row.insert(operand.to_string(), value);
        }
    }
    row
}

/// Enumerate the cartesian product of per-occurrence row choices as
/// index vectors. An empty choice list for any occurrence yields no
/// combinations.
struct Combinations {
    sizes: Vec<usize>,
    cursor: Vec<usize>,
    done: bool,
}

impl Combinations {
    fn new(sizes: Vec<usize>) -> Self {
        let done = sizes.contains(&0);
        let cursor = vec![0; sizes.len()];
        Combinations {
            sizes,
            cursor,
            done,
        }
    }
}

impl Iterator for Combinations {
    type Item = Vec<usize>;

    fn next(&mut self) -> Option<Vec<usize>> {
        if self.done {
            return None;
        }
        let current = self.cursor.clone();
        // Odometer increment; overflow past the last position ends
        // the iteration.
        self.done = true;
        for position in (0..self.cursor.len()).rev() {
            self.cursor[position] += 1;
            if self.cursor[position] < self.sizes[position] {
                self.done = false;
                break;
            }
            self.cursor[position] = 0;
        }
        // A zero-occurrence rule has exactly one (empty) combination.
        if self.cursor.is_empty() {
            self.done = true;
        }
        Some(current)
    }
}

/// Compute the full fixpoint of the queried concept's strongly
/// connected component and return the rows derived for the queried
/// concept itself.
pub async fn evaluate<'a, Env>(
    root: &ConceptDescriptor,
    analysis: &ProgramAnalysis,
    env: &'a Env,
) -> Result<Vec<Row>, EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    let root_entity = root.this();

    // Discover the component: starting from the queried concept,
    // follow in-component premises (each embeds its target's full
    // descriptor) and collect every member's rules.
    let mut members: HashMap<Entity, Member> = HashMap::new();
    let mut queue = vec![root.clone()];
    while let Some(descriptor) = queue.pop() {
        let entity = descriptor.this();
        if members.contains_key(&entity) || !analysis.in_same_cycle(&root_entity, &entity) {
            continue;
        }
        let bundle = Provider::<SelectRules>::execute(env, descriptor.clone()).await?;
        let mut rules = Vec::new();
        for rule in bundle.rules() {
            let mut occurrences = Vec::new();
            let mut base = Vec::new();
            for premise in rule.analysis().premises() {
                match in_component(premise, analysis, &root_entity) {
                    Some(query) => {
                        queue.push(query.predicate.clone());
                        occurrences.push(query.clone());
                    }
                    None => base.push(premise.clone()),
                }
            }
            rules.push(SplitRule {
                rule: rule.clone(),
                occurrences,
                base,
            });
        }
        members.insert(entity, Member { descriptor, rules });
    }

    let mut table = InMemoryAnswerTable::default();

    // Seed round: rules with no recursive occurrence evaluate fully
    // top-down.
    for member in members.values() {
        for split in &member.rules {
            if !split.occurrences.is_empty() {
                continue;
            }
            let plan = split.rule.plan(&Environment::new());
            let results: Vec<Match> = plan
                .evaluate(Match::new().seed(), env)
                .try_collect()
                .await?;
            for matched in results {
                table.insert(
                    &member.descriptor.this(),
                    project(&member.descriptor, &matched),
                );
            }
        }
    }

    // Delta rounds: per rule and per recursive occurrence, that
    // occurrence reads the delta while its siblings read the total.
    let mut rounds = 0;
    while table.advance() {
        rounds += 1;
        if rounds > MAX_ROUNDS {
            return Err(EvaluationError::FixpointDivergence {
                concept: root_entity.to_string(),
                rounds,
            });
        }

        for member in members.values() {
            for split in &member.rules {
                if split.occurrences.is_empty() {
                    continue;
                }
                for delta_index in 0..split.occurrences.len() {
                    let choices: Vec<Vec<Row>> = split
                        .occurrences
                        .iter()
                        .enumerate()
                        .map(|(index, occurrence)| {
                            let target = occurrence.predicate.this();
                            if index == delta_index {
                                table.delta(&target)
                            } else {
                                table.total(&target)
                            }
                        })
                        .collect();

                    for combination in Combinations::new(choices.iter().map(Vec::len).collect()) {
                        let mut matched = Match::new();
                        let mut scope = Environment::new();
                        let mut compatible = true;
                        for (index, (occurrence, row_index)) in
                            split.occurrences.iter().zip(&combination).enumerate()
                        {
                            let row = &choices[index][*row_index];
                            if !bind_occurrence(&mut matched, occurrence, row) {
                                compatible = false;
                                break;
                            }
                            for (_, term) in occurrence.terms.iter() {
                                if let Some(name) = term.name() {
                                    scope.add(name);
                                }
                            }
                        }
                        if !compatible {
                            continue;
                        }

                        let results: Vec<Match> = if split.base.is_empty() {
                            vec![matched]
                        } else {
                            let plan = Planner::with_types(
                                split.base.clone(),
                                split.rule.analysis().types.clone(),
                            )
                            .plan(&scope)
                            .map_err(|error| {
                                EvaluationError::Planning {
                                    message: error.to_string(),
                                }
                            })?;
                            plan.evaluate(matched.seed(), env).try_collect().await?
                        };
                        for result in results {
                            table.insert(
                                &member.descriptor.this(),
                                project(&member.descriptor, &result),
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(table.total(&root_entity))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::attribute::query::AttributeQuery;
    use crate::attribute::{AttributeDescriptor, Cardinality, Type};
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use dialog_repository::helpers::{test_operator_with_profile, test_repo};
    use futures_util::TryStreamExt;

    /// The `ancestor` concept: `this` plus one entity-valued
    /// `ancestor` field.
    fn ancestor_concept() -> ConceptDescriptor {
        ConceptDescriptor::try_from(vec![(
            "ancestor",
            AttributeDescriptor::new(
                the!("family/ancestor"),
                "",
                Cardinality::Many,
                Some(Type::Entity),
            ),
        )])
        .unwrap()
    }

    /// The classic pair of rules:
    ///
    /// ```text
    /// ancestor(this, a) :- parent(this, a).
    /// ancestor(this, a) :- parent(this, p), ancestor(p, a).
    /// ```
    fn ancestor_rules(concept: &ConceptDescriptor) -> Vec<DeductiveRule> {
        let base = DeductiveRule::new(
            concept.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("family/parent")),
                    Term::<Entity>::var("this"),
                    Term::var("ancestor"),
                    Term::blank(),
                    Some(Cardinality::Many),
                )
                .into(),
            ],
        )
        .expect("base rule compiles");

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("p"));
        terms.insert("ancestor".to_string(), Term::<Any>::var("ancestor"));
        let step = DeductiveRule::new(
            concept.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("family/parent")),
                    Term::<Entity>::var("this"),
                    Term::var("p"),
                    Term::blank(),
                    Some(Cardinality::Many),
                )
                .into(),
                Premise::Assert(Proposition::Concept(ConceptQuery {
                    terms,
                    predicate: concept.clone(),
                })),
            ],
        )
        .expect("recursive rule compiles");

        vec![base, step]
    }

    /// Evaluate the ancestor concept against the source and return
    /// the derived `(this, ancestor)` pairs.
    async fn ancestor_pairs(
        source: &TestEnv<'_>,
        terms: Parameters,
    ) -> anyhow::Result<Vec<(Value, Value)>> {
        let premise = Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: ancestor_concept(),
        }));
        let plan = Planner::from(vec![premise])
            .plan(&Environment::new())
            .expect("plans");
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), source)
            .try_collect()
            .await?;
        let mut pairs = Vec::new();
        for matched in results {
            let who = matched.lookup(&Term::<Any>::var("who"))?.content()?;
            let ancestor = matched.lookup(&Term::<Any>::var("relative"))?.content()?;
            pairs.push((who, ancestor));
        }
        // Deliberately no dedup: the fixpoint must emit exactly one
        // row per distinct pair, so a duplicate is a test failure.
        pairs.sort_by_key(|pair| format!("{pair:?}"));
        Ok(pairs)
    }

    fn query_terms() -> Parameters {
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("who"));
        terms.insert("ancestor".to_string(), Term::<Any>::var("relative"));
        terms
    }

    /// Transitive closure over a linear chain: carol -> bob -> alice
    /// derives all three ancestor pairs.
    #[dialog_common::test]
    async fn it_derives_transitive_closure_over_a_chain() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let carol = Entity::new()?;
        branch
            .transaction()
            .assert(the!("family/parent").of(carol.clone()).is(bob.clone()))
            .assert(the!("family/parent").of(bob.clone()).is(alice.clone()))
            .commit()
            .perform(&operator)
            .await?;

        let concept = ancestor_concept();
        let mut registry = RuleRegistry::new();
        for rule in ancestor_rules(&concept) {
            registry.register(rule)?;
        }
        assert!(registry.is_recursive(&concept.this())?);

        let source = TestEnv::new(&branch, &operator, registry);
        let pairs = ancestor_pairs(&source, query_terms()).await?;
        let mut expected = vec![
            (Value::Entity(carol.clone()), Value::Entity(bob.clone())),
            (Value::Entity(bob.clone()), Value::Entity(alice.clone())),
            (Value::Entity(carol.clone()), Value::Entity(alice.clone())),
        ];
        expected.sort_by_key(|pair| format!("{pair:?}"));
        assert_eq!(pairs, expected, "the closure includes the transitive pair");
        Ok(())
    }

    /// A diamond family tree: d's ancestors are b, c, and a, with
    /// the two derivations of (d, a) deduplicated to one row.
    #[dialog_common::test]
    async fn it_deduplicates_diamond_derivations() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let a = Entity::new()?;
        let b = Entity::new()?;
        let c = Entity::new()?;
        let d = Entity::new()?;
        branch
            .transaction()
            .assert(the!("family/parent").of(d.clone()).is(b.clone()))
            .assert(the!("family/parent").of(d.clone()).is(c.clone()))
            .assert(the!("family/parent").of(b.clone()).is(a.clone()))
            .assert(the!("family/parent").of(c.clone()).is(a.clone()))
            .commit()
            .perform(&operator)
            .await?;

        let concept = ancestor_concept();
        let mut registry = RuleRegistry::new();
        for rule in ancestor_rules(&concept) {
            registry.register(rule)?;
        }

        let source = TestEnv::new(&branch, &operator, registry);
        let pairs = ancestor_pairs(&source, query_terms()).await?;
        let mut expected = vec![
            (Value::Entity(d.clone()), Value::Entity(b.clone())),
            (Value::Entity(d.clone()), Value::Entity(c.clone())),
            (Value::Entity(b.clone()), Value::Entity(a.clone())),
            (Value::Entity(c.clone()), Value::Entity(a.clone())),
            (Value::Entity(d.clone()), Value::Entity(a.clone())),
        ];
        expected.sort_by_key(|pair| format!("{pair:?}"));
        assert_eq!(
            pairs, expected,
            "one row per distinct pair; the diamond's two paths to (d, a) collapse"
        );
        Ok(())
    }

    /// A caller binding `this` joins against the fixpoint rows: only
    /// the bound entity's ancestors come back.
    #[dialog_common::test]
    async fn it_joins_caller_bindings_against_the_fixpoint() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let carol = Entity::new()?;
        branch
            .transaction()
            .assert(the!("family/parent").of(carol.clone()).is(bob.clone()))
            .assert(the!("family/parent").of(bob.clone()).is(alice.clone()))
            .commit()
            .perform(&operator)
            .await?;

        let concept = ancestor_concept();
        let mut registry = RuleRegistry::new();
        for rule in ancestor_rules(&concept) {
            registry.register(rule)?;
        }

        let mut terms = Parameters::new();
        terms.insert(
            "this".to_string(),
            Term::Constant(Value::Entity(bob.clone())),
        );
        terms.insert("ancestor".to_string(), Term::<Any>::var("relative"));
        let source = TestEnv::new(&branch, &operator, registry);
        let premise = Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: concept,
        }));
        let plan = Planner::from(vec![premise])
            .plan(&Environment::new())
            .expect("plans");
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        assert_eq!(results.len(), 1, "bob has exactly one ancestor");
        assert_eq!(
            results[0]
                .lookup(&Term::<Any>::var("relative"))?
                .content()?,
            Value::Entity(alice),
            "the caller's binding filtered the fixpoint rows"
        );
        Ok(())
    }

    /// Ported from `query/test/loop.spec.js` "test ancestor": a
    /// six-person linear chain (Eve is the root ancestor) derives
    /// all fifteen ancestor pairs.
    #[dialog_common::test]
    async fn it_derives_all_pairs_over_a_deep_chain() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // alice -> bob -> mallory -> jack -> adam -> eve
        let chain: Vec<Entity> = (0..6).map(|_| Entity::new()).collect::<Result<_, _>>()?;
        let mut transaction = branch.transaction();
        for pair in chain.windows(2) {
            transaction = transaction.assert(
                the!("family/parent")
                    .of(pair[0].clone())
                    .is(pair[1].clone()),
            );
        }
        transaction.commit().perform(&operator).await?;

        let concept = ancestor_concept();
        let mut registry = RuleRegistry::new();
        for rule in ancestor_rules(&concept) {
            registry.register(rule)?;
        }

        let source = TestEnv::new(&branch, &operator, registry);
        let pairs = ancestor_pairs(&source, query_terms()).await?;

        // Every strictly-later chain member is an ancestor of every
        // earlier one: 5 + 4 + 3 + 2 + 1 = 15 pairs.
        let mut expected = Vec::new();
        for (child_index, child) in chain.iter().enumerate() {
            for ancestor in chain.iter().skip(child_index + 1) {
                expected.push((
                    Value::Entity(child.clone()),
                    Value::Entity(ancestor.clone()),
                ));
            }
        }
        expected.sort_by_key(|pair| format!("{pair:?}"));
        assert_eq!(pairs.len(), 15, "fifteen distinct pairs, no duplicates");
        assert_eq!(pairs, expected);
        Ok(())
    }

    /// Ported from `query/test/loop.spec.js` "complex ancestor
    /// test": a family tree with multiple paths to the same nodes
    /// derives exactly 29 unique ancestor relationships (10 direct,
    /// 19 transitive), with every multi-path derivation collapsed
    /// to one row.
    #[dialog_common::test]
    async fn it_derives_the_multi_path_family_tree() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let charlie = Entity::new()?;
        let david = Entity::new()?;
        let mallory = Entity::new()?;
        let jack = Entity::new()?;
        let adam = Entity::new()?;
        let eve = Entity::new()?;
        let dave = Entity::new()?;

        // Three branches from alice converging on jack, then a
        // common tail jack -> adam -> eve. Both bob and david have
        // mallory as parent; both mallory and dave have jack.
        let edges = [
            (&alice, &bob),
            (&bob, &mallory),
            (&mallory, &jack),
            (&alice, &charlie),
            (&charlie, &dave),
            (&dave, &jack),
            (&alice, &david),
            (&david, &mallory),
            (&jack, &adam),
            (&adam, &eve),
        ];
        let mut transaction = branch.transaction();
        for (child, parent) in edges {
            transaction = transaction.assert(
                the!("family/parent")
                    .of((*child).clone())
                    .is((*parent).clone()),
            );
        }
        transaction.commit().perform(&operator).await?;

        let concept = ancestor_concept();
        let mut registry = RuleRegistry::new();
        for rule in ancestor_rules(&concept) {
            registry.register(rule)?;
        }

        let source = TestEnv::new(&branch, &operator, registry);
        let pairs = ancestor_pairs(&source, query_terms()).await?;

        assert_eq!(
            pairs.len(),
            29,
            "10 direct + 19 transitive unique relationships, multi-path \
             derivations deduplicated"
        );
        let expect = |child: &Entity, ancestor: &Entity| {
            let pair = (
                Value::Entity(child.clone()),
                Value::Entity(ancestor.clone()),
            );
            assert!(pairs.contains(&pair), "missing pair {pair:?}");
        };
        // The multi-path derivations called out in the original test.
        expect(&alice, &mallory); // via bob or via david
        expect(&alice, &jack); // via bob/mallory, david/mallory, or charlie/dave
        expect(&alice, &adam); // via any path to jack
        expect(&alice, &eve); // via any path to adam
        expect(&bob, &eve); // via mallory/jack/adam
        expect(&charlie, &eve); // via dave/jack/adam
        Ok(())
    }

    /// Ported from `query/test/loop.spec.js` "test recursion": a
    /// non-recursive concept consumes a recursive one. `List` is the
    /// transitive closure of `list/next`; `Connection` joins each
    /// derived link with the target's `meta/name`. The outer concept
    /// evaluates top-down and enters the fixpoint at the `List`
    /// boundary.
    #[dialog_common::test]
    async fn it_consumes_a_recursive_concept_from_a_plain_rule() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let n0 = Entity::new()?;
        let n1 = Entity::new()?;
        let n2 = Entity::new()?;
        let n3 = Entity::new()?;
        branch
            .transaction()
            .assert(the!("list/next").of(n0.clone()).is(n1.clone()))
            .assert(the!("list/next").of(n1.clone()).is(n2.clone()))
            .assert(the!("list/next").of(n2.clone()).is(n3.clone()))
            .assert(the!("meta/name").of(n1.clone()).is("a".to_string()))
            .assert(the!("meta/name").of(n2.clone()).is("b".to_string()))
            .assert(the!("meta/name").of(n3.clone()).is("c".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        // List { this, next }: transitive closure of list/next.
        let list = ConceptDescriptor::try_from(vec![(
            "next",
            AttributeDescriptor::new(the!("list/link"), "", Cardinality::Many, Some(Type::Entity)),
        )])?;
        let direct = DeductiveRule::new(
            list.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("list/next")),
                    Term::<Entity>::var("this"),
                    Term::var("next"),
                    Term::blank(),
                    Some(Cardinality::Many),
                )
                .into(),
            ],
        )?;
        let mut step_terms = Parameters::new();
        step_terms.insert("this".to_string(), Term::<Any>::var("hop"));
        step_terms.insert("next".to_string(), Term::<Any>::var("next"));
        let transitive = DeductiveRule::new(
            list.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("list/next")),
                    Term::<Entity>::var("this"),
                    Term::var("hop"),
                    Term::blank(),
                    Some(Cardinality::Many),
                )
                .into(),
                Premise::Assert(Proposition::Concept(ConceptQuery {
                    terms: step_terms,
                    predicate: list.clone(),
                })),
            ],
        )?;

        // Connection { this, to, name }: every reachable node with
        // its name. Not itself recursive.
        let connection = ConceptDescriptor::try_from(vec![
            (
                "to",
                AttributeDescriptor::new(
                    the!("conn/to"),
                    "",
                    Cardinality::Many,
                    Some(Type::Entity),
                ),
            ),
            (
                "name",
                AttributeDescriptor::new(
                    the!("conn/name"),
                    "",
                    Cardinality::Many,
                    Some(Type::String),
                ),
            ),
        ])?;
        let mut link_terms = Parameters::new();
        link_terms.insert("this".to_string(), Term::<Any>::var("this"));
        link_terms.insert("next".to_string(), Term::<Any>::var("to"));
        let connection_rule = DeductiveRule::new(
            connection.clone(),
            vec![
                Premise::Assert(Proposition::Concept(ConceptQuery {
                    terms: link_terms,
                    predicate: list.clone(),
                })),
                AttributeQuery::new(
                    Term::from(the!("meta/name")),
                    Term::<Entity>::var("to"),
                    Term::var("name"),
                    Term::blank(),
                    Some(Cardinality::One),
                )
                .into(),
            ],
        )?;

        let mut registry = RuleRegistry::new();
        registry.register(direct)?;
        registry.register(transitive)?;
        registry.register(connection_rule)?;
        assert!(registry.is_recursive(&list.this())?);
        assert!(
            !registry.is_recursive(&connection.this())?,
            "the consuming concept is outside the cycle"
        );

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("from"));
        terms.insert("to".to_string(), Term::<Any>::var("to"));
        terms.insert("name".to_string(), Term::<Any>::var("name"));
        let source = TestEnv::new(&branch, &operator, registry);
        let plan = Planner::from(vec![Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: connection,
        }))])
        .plan(&Environment::new())
        .expect("plans");
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        let mut connections = Vec::new();
        for matched in results {
            connections.push((
                matched.lookup(&Term::<Any>::var("from"))?.content()?,
                matched.lookup(&Term::<Any>::var("to"))?.content()?,
                matched.lookup(&Term::<Any>::var("name"))?.content()?,
            ));
        }
        connections.sort_by_key(|row| format!("{row:?}"));
        let mut expected = vec![
            (
                Value::Entity(n0.clone()),
                Value::Entity(n1.clone()),
                Value::String("a".into()),
            ),
            (
                Value::Entity(n1.clone()),
                Value::Entity(n2.clone()),
                Value::String("b".into()),
            ),
            (
                Value::Entity(n0.clone()),
                Value::Entity(n2.clone()),
                Value::String("b".into()),
            ),
            (
                Value::Entity(n2.clone()),
                Value::Entity(n3.clone()),
                Value::String("c".into()),
            ),
            (
                Value::Entity(n1.clone()),
                Value::Entity(n3.clone()),
                Value::String("c".into()),
            ),
            (
                Value::Entity(n0.clone()),
                Value::Entity(n3.clone()),
                Value::String("c".into()),
            ),
        ];
        expected.sort_by_key(|row| format!("{row:?}"));
        assert_eq!(connections, expected, "every reachable node, named");
        Ok(())
    }

    /// Ported from `query/test/loop.spec.js` "test not really
    /// recursive": a concept with a base rule plus a rule that
    /// re-derives the concept from itself. The recursive rule adds
    /// nothing new, so the fixpoint converges to exactly the base
    /// rows.
    #[dialog_common::test]
    async fn it_converges_when_the_recursive_rule_adds_nothing() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let x = Entity::new()?;
        let y = Entity::new()?;
        let z = Entity::new()?;
        branch
            .transaction()
            .assert(the!("meta/name").of(x.clone()).is("a".to_string()))
            .assert(the!("meta/name").of(y.clone()).is("b".to_string()))
            .assert(the!("meta/name").of(z.clone()).is("c".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let person = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(the!("query/name"), "", Cardinality::One, Some(Type::String)),
        )])?;
        let base = DeductiveRule::new(
            person.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("meta/name")),
                    Term::<Entity>::var("this"),
                    Term::var("name"),
                    Term::blank(),
                    Some(Cardinality::One),
                )
                .into(),
            ],
        )?;
        let mut self_terms = Parameters::new();
        self_terms.insert("this".to_string(), Term::<Any>::var("this"));
        self_terms.insert("name".to_string(), Term::<Any>::var("name"));
        let recur = DeductiveRule::new(
            person.clone(),
            vec![Premise::Assert(Proposition::Concept(ConceptQuery {
                terms: self_terms,
                predicate: person.clone(),
            }))],
        )?;

        let mut registry = RuleRegistry::new();
        registry.register(base)?;
        registry.register(recur)?;

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("who"));
        terms.insert("name".to_string(), Term::<Any>::var("name"));
        let source = TestEnv::new(&branch, &operator, registry);
        let plan = Planner::from(vec![Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: person,
        }))])
        .plan(&Environment::new())
        .expect("plans");
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        let mut names = Vec::new();
        for matched in &results {
            names.push(matched.lookup(&Term::<Any>::var("name"))?.content()?);
        }
        names.sort_by_key(|name| format!("{name:?}"));
        assert_eq!(
            names,
            vec![
                Value::String("a".into()),
                Value::String("b".into()),
                Value::String("c".into()),
            ],
            "exactly the base rows, once each"
        );
        Ok(())
    }

    /// Ported from `query/test/loop.spec.js` "test tautology", with
    /// least-fixpoint semantics: a single rule that conjoins the
    /// base scan with the concept itself has no non-recursive rule
    /// to seed the table, so nothing is ever derivable and the
    /// fixpoint converges to the empty set. (The legacy JS engine
    /// returned the base rows here; classical Datalog says empty,
    /// and that is what this engine implements.) The implicit rule
    /// scans the conclusion attribute's own facts, of which there
    /// are none.
    #[dialog_common::test]
    async fn it_derives_nothing_for_a_tautology() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let x = Entity::new()?;
        branch
            .transaction()
            .assert(the!("meta/name").of(x.clone()).is("a".to_string()))
            .commit()
            .perform(&operator)
            .await?;

        let tautology = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("tautology/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])?;
        let mut self_terms = Parameters::new();
        self_terms.insert("this".to_string(), Term::<Any>::var("this"));
        self_terms.insert("name".to_string(), Term::<Any>::var("name"));
        let rule = DeductiveRule::new(
            tautology.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("meta/name")),
                    Term::<Entity>::var("this"),
                    Term::var("name"),
                    Term::blank(),
                    Some(Cardinality::One),
                )
                .into(),
                Premise::Assert(Proposition::Concept(ConceptQuery {
                    terms: self_terms,
                    predicate: tautology.clone(),
                })),
            ],
        )?;

        let mut registry = RuleRegistry::new();
        registry.register(rule)?;

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("who"));
        terms.insert("name".to_string(), Term::<Any>::var("name"));
        let source = TestEnv::new(&branch, &operator, registry);
        let plan = Planner::from(vec![Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: tautology,
        }))])
        .plan(&Environment::new())
        .expect("plans");
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;

        assert!(
            results.is_empty(),
            "no base case, no derivations; and the query terminates"
        );
        Ok(())
    }

    /// The nl-datalog demo program (alexwarth.github.io/projects/
    /// nl-datalog): Simpsons facts with derived `parent` and
    /// `grandfather` rules, extended with the recursive `ancestor`.
    /// "Homer is Bart's father. Homer is Lisa's father. Abe is
    /// Homer's father." Abe is the grandfather of Bart and Lisa,
    /// and the ancestor closure has exactly five pairs.
    #[dialog_common::test]
    async fn it_derives_the_simpsons_program() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let homer = Entity::new()?;
        let bart = Entity::new()?;
        let lisa = Entity::new()?;
        let abe = Entity::new()?;
        branch
            .transaction()
            .assert(the!("family/father").of(bart.clone()).is(homer.clone()))
            .assert(the!("family/father").of(lisa.clone()).is(homer.clone()))
            .assert(the!("family/father").of(homer.clone()).is(abe.clone()))
            .commit()
            .perform(&operator)
            .await?;

        // parent(this, parent) :- father(this, parent).
        let parent = ConceptDescriptor::try_from(vec![(
            "parent",
            AttributeDescriptor::new(
                the!("family/derived-parent"),
                "",
                Cardinality::Many,
                Some(Type::Entity),
            ),
        )])?;
        let parent_rule = DeductiveRule::new(
            parent.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("family/father")),
                    Term::<Entity>::var("this"),
                    Term::var("parent"),
                    Term::blank(),
                    Some(Cardinality::One),
                )
                .into(),
            ],
        )?;

        // grandfather(this, g) :- father(z, g), parent(this, z).
        let grandfather = ConceptDescriptor::try_from(vec![(
            "grandfather",
            AttributeDescriptor::new(
                the!("family/grandfather"),
                "",
                Cardinality::Many,
                Some(Type::Entity),
            ),
        )])?;
        let mut parent_terms = Parameters::new();
        parent_terms.insert("this".to_string(), Term::<Any>::var("this"));
        parent_terms.insert("parent".to_string(), Term::<Any>::var("z"));
        let grandfather_rule = DeductiveRule::new(
            grandfather.clone(),
            vec![
                Premise::Assert(Proposition::Concept(ConceptQuery {
                    terms: parent_terms,
                    predicate: parent.clone(),
                })),
                AttributeQuery::new(
                    Term::from(the!("family/father")),
                    Term::<Entity>::var("z"),
                    Term::var("grandfather"),
                    Term::blank(),
                    Some(Cardinality::One),
                )
                .into(),
            ],
        )?;

        // ancestor: the recursive extension over the same facts.
        let ancestor = ConceptDescriptor::try_from(vec![(
            "ancestor",
            AttributeDescriptor::new(
                the!("family/forebear"),
                "",
                Cardinality::Many,
                Some(Type::Entity),
            ),
        )])?;
        let ancestor_base = DeductiveRule::new(
            ancestor.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("family/father")),
                    Term::<Entity>::var("this"),
                    Term::var("ancestor"),
                    Term::blank(),
                    Some(Cardinality::One),
                )
                .into(),
            ],
        )?;
        let mut step_terms = Parameters::new();
        step_terms.insert("this".to_string(), Term::<Any>::var("f"));
        step_terms.insert("ancestor".to_string(), Term::<Any>::var("ancestor"));
        let ancestor_step = DeductiveRule::new(
            ancestor.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("family/father")),
                    Term::<Entity>::var("this"),
                    Term::var("f"),
                    Term::blank(),
                    Some(Cardinality::One),
                )
                .into(),
                Premise::Assert(Proposition::Concept(ConceptQuery {
                    terms: step_terms,
                    predicate: ancestor.clone(),
                })),
            ],
        )?;

        let mut registry = RuleRegistry::new();
        registry.register(parent_rule)?;
        registry.register(grandfather_rule)?;
        registry.register(ancestor_base)?;
        registry.register(ancestor_step)?;

        let source = TestEnv::new(&branch, &operator, registry);

        // Who is whose grandfather?
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("grandchild"));
        terms.insert("grandfather".to_string(), Term::<Any>::var("grandfather"));
        let plan = Planner::from(vec![Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: grandfather,
        }))])
        .plan(&Environment::new())
        .expect("plans");
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;
        let mut grandchildren = Vec::new();
        for matched in &results {
            let grandchild = matched.lookup(&Term::<Any>::var("grandchild"))?.content()?;
            assert_eq!(
                matched
                    .lookup(&Term::<Any>::var("grandfather"))?
                    .content()?,
                Value::Entity(abe.clone()),
                "Abe is the only grandfather"
            );
            grandchildren.push(grandchild);
        }
        grandchildren.sort_by_key(|value| format!("{value:?}"));
        let mut expected = vec![Value::Entity(bart.clone()), Value::Entity(lisa.clone())];
        expected.sort_by_key(|value| format!("{value:?}"));
        assert_eq!(grandchildren, expected, "Abe has two grandchildren");

        // The full ancestor closure: five pairs.
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("who"));
        terms.insert("ancestor".to_string(), Term::<Any>::var("relative"));
        let plan = Planner::from(vec![Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: ancestor,
        }))])
        .plan(&Environment::new())
        .expect("plans");
        let results: Vec<Match> = plan
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;
        let mut pairs = Vec::new();
        for matched in &results {
            pairs.push((
                matched.lookup(&Term::<Any>::var("who"))?.content()?,
                matched.lookup(&Term::<Any>::var("relative"))?.content()?,
            ));
        }
        pairs.sort_by_key(|pair| format!("{pair:?}"));
        let mut expected = vec![
            (Value::Entity(bart.clone()), Value::Entity(homer.clone())),
            (Value::Entity(lisa.clone()), Value::Entity(homer.clone())),
            (Value::Entity(homer.clone()), Value::Entity(abe.clone())),
            (Value::Entity(bart.clone()), Value::Entity(abe.clone())),
            (Value::Entity(lisa.clone()), Value::Entity(abe.clone())),
        ];
        expected.sort_by_key(|pair| format!("{pair:?}"));
        assert_eq!(pairs, expected);
        Ok(())
    }
}
