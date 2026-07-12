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
use crate::artifact::Artifact;
use crate::attribute::The;
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
use crate::types::{Any, Typed};
use crate::{Entity, Environment, Value};
use core::fmt;
use core::{iter, mem};
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use futures_util::TryStreamExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

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

impl InMemoryAnswerTable {
    /// Remove a row from the total (DRed's over-delete step).
    /// Between evaluations the delta and staging areas are drained,
    /// so the total is the only place a retained row lives.
    fn remove(&mut self, concept: &Entity, row: &Row) {
        if let Some(rows) = self.total.get_mut(concept) {
            rows.remove(&row_key(row));
        }
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

/// Discover the queried concept's strongly connected component:
/// starting from the root, follow in-component premises (each embeds
/// its target's full descriptor) and collect every member's rules,
/// split into recursive occurrences and base premises.
async fn discover<'a, Env>(
    root: &ConceptDescriptor,
    analysis: &ProgramAnalysis,
    env: &'a Env,
) -> Result<HashMap<Entity, Member>, EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    let root_entity = root.this();
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
    Ok(members)
}

/// Evaluate one rule with the given occurrence-and-source bindings:
/// join the `rest` premises sideways and return every projected
/// conclusion row.
async fn collect_rule_rows<'a, Env>(
    member: &Member,
    split: &SplitRule,
    rest: Vec<Premise>,
    matched: Match,
    scope: &Environment,
    env: &'a Env,
) -> Result<Vec<Row>, EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    let results: Vec<Match> = if rest.is_empty() {
        vec![matched]
    } else {
        let plan = Planner::with_types(rest, split.rule.analysis().types.clone())
            .plan(scope)
            .map_err(|error| EvaluationError::Planning {
                message: error.to_string(),
            })?;
        plan.evaluate(matched.seed(), env).try_collect().await?
    };
    Ok(results
        .into_iter()
        .map(|result| project(&member.descriptor, &result))
        .collect())
}

/// [`collect_rule_rows`], staging every row into the table.
async fn stage_rule_rows<'a, Env>(
    member: &Member,
    split: &SplitRule,
    rest: Vec<Premise>,
    matched: Match,
    scope: &Environment,
    table: &mut InMemoryAnswerTable,
    env: &'a Env,
) -> Result<(), EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    for row in collect_rule_rows(member, split, rest, matched, scope, env).await? {
        table.insert(&member.descriptor.this(), row);
    }
    Ok(())
}

/// Run semi-naive delta rounds to convergence: per rule and per
/// recursive occurrence, that occurrence reads the previous round's
/// delta while its siblings read the running total.
async fn delta_rounds<'a, Env>(
    root: &Entity,
    members: &HashMap<Entity, Member>,
    table: &mut InMemoryAnswerTable,
    env: &'a Env,
) -> Result<(), EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    let mut rounds = 0;
    while table.advance() {
        rounds += 1;
        if rounds > MAX_ROUNDS {
            return Err(EvaluationError::FixpointDivergence {
                concept: root.to_string(),
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
                        stage_rule_rows(
                            member,
                            split,
                            split.base.clone(),
                            matched,
                            &scope,
                            table,
                            env,
                        )
                        .await?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Compute the full fixpoint of the queried concept's strongly
/// connected component into `table` and return the rows derived for
/// the queried concept itself.
pub async fn evaluate_table<'a, Env>(
    root: &ConceptDescriptor,
    analysis: &ProgramAnalysis,
    env: &'a Env,
    table: &mut InMemoryAnswerTable,
) -> Result<Vec<Row>, EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    let root_entity = root.this();
    let members = discover(root, analysis, env).await?;

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

    delta_rounds(&root_entity, &members, table, env).await?;
    Ok(table.total(&root_entity))
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
    let mut table = InMemoryAnswerTable::default();
    evaluate_table(root, analysis, env, &mut table).await
}

/// How a base premise participates in addition-seeding.
enum BaseSource {
    /// A positive attribute premise: a matching new fact binds its
    /// slots directly. Boxed to keep the variant sizes even.
    Attribute(Box<(Term<The>, Term<Entity>, Term<Any>)>),
    /// A positive out-of-component concept premise over an
    /// entity-local target: a new fact's subject binds its `this`
    /// slot (over-approximated; re-derivation settles truth).
    LocalConcept { this: Term<Any> },
    /// Never sourced by a fact (constraints, formulas).
    Inert,
    /// A shape additive seeding cannot handle soundly: a negated or
    /// optional premise a new fact matches can *retract* derived
    /// rows, and a non-local nested concept cannot bound its heads.
    Unsupported,
}

/// Classify a base premise for addition-seeding, given the changed
/// facts. Negated and optional premises are only `Unsupported` when
/// an addition can actually match them.
async fn classify_base<'a, Env>(
    premise: &Premise,
    additions: &[Artifact],
    env: &'a Env,
) -> Result<BaseSource, EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    fn slots(query: &crate::AttributeQuery) -> (Term<The>, Term<Entity>, Term<Any>) {
        (query.the().clone(), query.of().clone(), query.is().clone())
    }
    fn any_match(the: &Term<The>, of: &Term<Entity>, is: &Term<Any>, facts: &[Artifact]) -> bool {
        facts.iter().any(|fact| {
            constant_admits(the.as_constant(), &Value::from(The::from(fact.the.clone())))
                && constant_admits(of.as_constant(), &Value::Entity(fact.of.clone()))
                && constant_admits(is.as_constant(), &fact.is)
        })
    }

    Ok(match premise {
        Premise::Assert(Proposition::Attribute(query)) => {
            BaseSource::Attribute(Box::new(slots(query)))
        }
        Premise::Assert(Proposition::OptionalAttribute(query)) => {
            // A new fact turning an Absent slot Present replaces the
            // row rather than adding one: not additively seedable.
            let (the, of, is) = (
                query.query().the().clone(),
                query.of().clone(),
                query.is().clone(),
            );
            if any_match(&the, &of, &is, additions) {
                BaseSource::Unsupported
            } else {
                BaseSource::Inert
            }
        }
        Premise::Unless(Negation(Proposition::Attribute(query))) => {
            let (the, of, is) = slots(query);
            if any_match(&the, &of, &is, additions) {
                BaseSource::Unsupported
            } else {
                BaseSource::Inert
            }
        }
        Premise::Assert(Proposition::Concept(query)) => {
            let bundle = Provider::<SelectRules>::execute(env, query.predicate.clone()).await?;
            let local = bundle.recursion().is_none()
                && bundle.rules().all(|rule| rule.analysis().is_entity_local());
            let Some(this) = query.terms.get("this") else {
                return Ok(BaseSource::Unsupported);
            };
            if local {
                BaseSource::LocalConcept { this: this.clone() }
            } else {
                BaseSource::Unsupported
            }
        }
        Premise::Unless(Negation(Proposition::Concept(_))) => {
            if additions.is_empty() {
                BaseSource::Inert
            } else {
                // An addition can grow the negated set and retract
                // derived rows: not additively seedable.
                BaseSource::Unsupported
            }
        }
        _ => BaseSource::Inert,
    })
}

/// Whether a constant slot admits the value (a variable or blank
/// slot admits anything).
fn constant_admits(constant: Option<&Value>, value: &Value) -> bool {
    constant.map(|expected| expected == value).unwrap_or(true)
}

/// Bind one slot of a source premise from a changed value: a
/// constant filters, a named variable binds, a blank matches.
fn bind_source_slot(
    matched: &mut Match,
    scope: &mut Environment,
    constant: Option<&Value>,
    name: Option<&str>,
    value: Value,
) -> bool {
    if let Some(expected) = constant {
        return *expected == value;
    }
    match name {
        Some(name) => {
            if matched.bind(&Term::<Any>::var(name), value).is_err() {
                return false;
            }
            scope.add(name);
            true
        }
        None => true,
    }
}

/// Fold one term slot of a deleted-fact source into a suspicion
/// pattern: a mismatching constant rejects the fact, a head-operand
/// variable contributes a `(operand, value)` constraint, anything
/// else constrains nothing. Returns `false` on rejection.
fn pattern_slot<T: Typed>(
    term: &Term<T>,
    value: Value,
    operands: &BTreeSet<&str>,
    pattern: &mut Vec<(String, Value)>,
) -> bool {
    if let Some(expected) = term.as_constant() {
        return *expected == value;
    }
    if let Some(name) = term.name()
        && operands.contains(name)
    {
        pattern.push((name.to_string(), value));
    }
    true
}

/// Extend a previously computed fixpoint with newly added base
/// facts: semi-naive continuation. For every rule, each new fact is
/// bound into the base premise it can match (or, for an
/// out-of-component entity-local concept premise, the fact's
/// subject binds the premise's `this` slot); recursive occurrences
/// read the *retained totals*; the remaining base premises join
/// sideways top-down. Newly staged rows then drive the ordinary
/// delta rounds to convergence.
///
/// Returns `Ok(None)` when the rule set cannot be extended
/// additively — a negated or optional premise a new fact matches
/// (the addition could *retract* rows), or a non-local nested
/// concept — in which case the caller rebuilds from scratch, which
/// is always sound. Deletions must never reach this function; the
/// caller rebuilds for those.
pub async fn extend<'a, Env>(
    root: &ConceptDescriptor,
    analysis: &ProgramAnalysis,
    env: &'a Env,
    table: &mut InMemoryAnswerTable,
    additions: &[Artifact],
) -> Result<Option<Vec<Row>>, EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    let root_entity = root.this();
    let members = discover(root, analysis, env).await?;
    let subjects: BTreeSet<Entity> = additions.iter().map(|fact| fact.of.clone()).collect();

    for member in members.values() {
        for split in &member.rules {
            // Classify every base premise first: any unsupported
            // shape aborts the extension before rows are staged.
            let mut classified = Vec::with_capacity(split.base.len());
            for premise in &split.base {
                match classify_base(premise, additions, env).await? {
                    BaseSource::Unsupported => return Ok(None),
                    source => classified.push(source),
                }
            }

            for source in classified.iter() {
                // The bindings each new fact contributes through
                // this source premise.
                let mut seeds: Vec<(Match, Environment)> = Vec::new();
                match source {
                    BaseSource::Attribute(slots) => {
                        let (the, of, is) = slots.as_ref();
                        for fact in additions {
                            let mut matched = Match::new();
                            let mut scope = Environment::new();
                            if bind_source_slot(
                                &mut matched,
                                &mut scope,
                                the.as_constant(),
                                the.name(),
                                Value::from(The::from(fact.the.clone())),
                            ) && bind_source_slot(
                                &mut matched,
                                &mut scope,
                                of.as_constant(),
                                of.name(),
                                Value::Entity(fact.of.clone()),
                            ) && bind_source_slot(
                                &mut matched,
                                &mut scope,
                                is.as_constant(),
                                is.name(),
                                fact.is.clone(),
                            ) {
                                seeds.push((matched, scope));
                            }
                        }
                    }
                    BaseSource::LocalConcept { this } => {
                        for entity in &subjects {
                            let mut matched = Match::new();
                            let mut scope = Environment::new();
                            if bind_source_slot(
                                &mut matched,
                                &mut scope,
                                this.as_constant(),
                                this.name(),
                                Value::Entity(entity.clone()),
                            ) {
                                seeds.push((matched, scope));
                            }
                        }
                    }
                    BaseSource::Inert | BaseSource::Unsupported => continue,
                }

                // The source premise stays in the sideways join: with
                // its slots bound it re-verifies as a point lookup
                // (preserving cardinality-one winner semantics), and
                // a concept source's remaining fields bind from its
                // goal-directed evaluation.
                let rest: Vec<Premise> = split.base.clone();

                for (seed_match, seed_scope) in seeds {
                    // Occurrences read the retained totals: the new
                    // fact is the delta position.
                    let choices: Vec<Vec<Row>> = split
                        .occurrences
                        .iter()
                        .map(|occurrence| table.total(&occurrence.predicate.this()))
                        .collect();
                    for combination in Combinations::new(choices.iter().map(Vec::len).collect()) {
                        let mut matched = seed_match.clone();
                        let mut scope = seed_scope.clone();
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
                        stage_rule_rows(member, split, rest.clone(), matched, &scope, table, env)
                            .await?;
                    }
                }
            }
        }
    }

    delta_rounds(&root_entity, &members, table, env).await?;
    Ok(Some(table.total(&root_entity)))
}

/// Retract deleted base facts from a retained fixpoint: DRed.
///
/// 1. **Over-delete.** Every row forward-reachable from a deleted
///    fact is suspected: deleted facts bind into the base premises
///    they matched (occurrences reading the *pre-deletion* totals),
///    and suspicion cascades through occurrence positions until
///    closed. All suspects are removed.
/// 2. **Re-derive.** Each suspect is checked for a surviving
///    derivation: its head operands bind the rule body, occurrences
///    read the *post-removal* table, base premises read the current
///    store. Survivors re-insert; passes repeat until stable, so
///    chains re-derive bottom-up.
/// 3. **Insert.** The ordinary delta rounds propagate the
///    re-insertions.
///
/// Sound only when no single rule body could have consumed more
/// than one deleted thing at once — one body position is the delta,
/// the rest are read from surviving state. The gate is
/// conservative: if two or more of a rule's base premises match any
/// deletion, or a deletion matches a negated or optional premise (a
/// deletion there can make rows *appear*), `Ok(None)` is returned
/// and the caller rebuilds.
pub async fn retract<'a, Env>(
    root: &ConceptDescriptor,
    analysis: &ProgramAnalysis,
    env: &'a Env,
    table: &mut InMemoryAnswerTable,
    deletions: &[Artifact],
) -> Result<Option<()>, EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    let root_entity = root.this();
    let members = discover(root, analysis, env).await?;
    let subjects: BTreeSet<Entity> = deletions.iter().map(|fact| fact.of.clone()).collect();

    // Suspects per member, keyed like the table.
    let mut suspects: HashMap<Entity, BTreeMap<Vec<u8>, Row>> = HashMap::new();

    // Phase 1a: seed suspicion from the deleted facts. A deleted
    // fact cannot be re-joined (it is gone from the store), so the
    // rows it may have supported are found by *pattern-matching the
    // retained table*: the head-operand bindings the fact imposes
    // through the premise it matched select every consistent row.
    // A source that binds no head operand suspects the member's
    // whole table — conservative, settled by re-derivation.
    for member in members.values() {
        let operands: BTreeSet<&str> = iter::once("this")
            .chain(member.descriptor.with().keys())
            .collect();
        for split in &member.rules {
            let mut matchable = 0usize;
            let mut patterns: Vec<Vec<(String, Value)>> = Vec::new();
            for premise in &split.base {
                match classify_base(premise, deletions, env).await? {
                    BaseSource::Unsupported => return Ok(None),
                    BaseSource::Inert => {}
                    BaseSource::Attribute(slots) => {
                        let (the, of, is) = slots.as_ref();
                        let mut hit = false;
                        for fact in deletions {
                            let mut pattern = Vec::new();
                            let matches =
                                pattern_slot(
                                    the,
                                    Value::from(The::from(fact.the.clone())),
                                    &operands,
                                    &mut pattern,
                                ) && pattern_slot(
                                    of,
                                    Value::Entity(fact.of.clone()),
                                    &operands,
                                    &mut pattern,
                                ) && pattern_slot(is, fact.is.clone(), &operands, &mut pattern);
                            if matches {
                                hit = true;
                                patterns.push(pattern);
                            }
                        }
                        if hit {
                            matchable += 1;
                        }
                    }
                    BaseSource::LocalConcept { this } => {
                        if !subjects.is_empty() {
                            matchable += 1;
                        }
                        for entity in &subjects {
                            let value = Value::Entity(entity.clone());
                            match this.as_constant() {
                                Some(expected) if *expected != value => {}
                                Some(_) => patterns.push(Vec::new()),
                                None => match this.name() {
                                    Some(name) if operands.contains(name) => {
                                        patterns.push(vec![(name.to_string(), value)]);
                                    }
                                    _ => patterns.push(Vec::new()),
                                },
                            }
                        }
                    }
                }
            }
            // Two matchable premises in one body: a derivation could
            // have consumed two deleted things at once, and the
            // one-position-at-a-time walk would miss it.
            if matchable >= 2 {
                return Ok(None);
            }

            if patterns.is_empty() {
                continue;
            }
            let entity = member.descriptor.this();
            for row in table.total(&entity) {
                let suspect = patterns.iter().any(|pattern| {
                    pattern
                        .iter()
                        .all(|(operand, value)| row.get(operand) == Some(value))
                });
                if suspect {
                    suspects
                        .entry(entity.clone())
                        .or_default()
                        .insert(row_key(&row), row);
                }
            }
        }
    }

    // Phase 1b: cascade suspicion through occurrence positions,
    // reading pre-removal totals.
    let mut frontier = suspects.clone();
    while !frontier.values().all(BTreeMap::is_empty) {
        let mut next: HashMap<Entity, BTreeMap<Vec<u8>, Row>> = HashMap::new();
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
                                frontier
                                    .get(&target)
                                    .map(|rows| rows.values().cloned().collect())
                                    .unwrap_or_default()
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
                        let rows = collect_rule_rows(
                            member,
                            split,
                            split.base.clone(),
                            matched,
                            &scope,
                            env,
                        )
                        .await?;
                        let entity = member.descriptor.this();
                        for row in rows {
                            let key = row_key(&row);
                            let known = suspects
                                .get(&entity)
                                .is_some_and(|rows| rows.contains_key(&key));
                            if !known {
                                suspects
                                    .entry(entity.clone())
                                    .or_default()
                                    .insert(key.clone(), row.clone());
                                next.entry(entity.clone()).or_default().insert(key, row);
                            }
                        }
                    }
                }
            }
        }
        frontier = next;
    }

    // Over-delete.
    for (entity, rows) in &suspects {
        for row in rows.values() {
            table.remove(entity, row);
        }
    }

    // Phase 2: re-derive survivors, bottom-up until stable.
    loop {
        let mut rederived = false;
        for member in members.values() {
            let entity = member.descriptor.this();
            let Some(rows) = suspects.get(&entity) else {
                continue;
            };
            let mut survived: Vec<Vec<u8>> = Vec::new();
            for (key, row) in rows {
                if derivable(member, row, table, env).await? {
                    table.insert(&entity, row.clone());
                    survived.push(key.clone());
                    rederived = true;
                }
            }
            if !survived.is_empty() {
                // Promote the survivors immediately so this pass's
                // later checks (and the next pass) see them.
                table.advance();
                let bucket = suspects.get_mut(&entity).expect("bucket exists");
                for key in survived {
                    bucket.remove(&key);
                }
            }
        }
        if !rederived {
            break;
        }
    }

    // Phase 3: propagate anything the re-insertions enable.
    delta_rounds(&root_entity, &members, table, env).await?;
    Ok(Some(()))
}

/// Whether a suspect row still has a derivation: bind its head
/// operands into each rule body (occurrences read the current
/// table, base premises the current store) and check whether any
/// result projects back to the row.
async fn derivable<'a, Env>(
    member: &Member,
    row: &Row,
    table: &InMemoryAnswerTable,
    env: &'a Env,
) -> Result<bool, EvaluationError>
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    for split in &member.rules {
        let mut seed = Match::new();
        let mut seed_scope = Environment::new();
        let mut consistent = true;
        for (operand, value) in row {
            if seed
                .bind(&Term::<Any>::var(operand), value.clone())
                .is_err()
            {
                consistent = false;
                break;
            }
            seed_scope.add(operand);
        }
        if !consistent {
            continue;
        }

        let choices: Vec<Vec<Row>> = split
            .occurrences
            .iter()
            .map(|occurrence| table.total(&occurrence.predicate.this()))
            .collect();
        let combinations: Vec<Vec<usize>> = if split.occurrences.is_empty() {
            vec![Vec::new()]
        } else {
            Combinations::new(choices.iter().map(Vec::len).collect()).collect()
        };
        for combination in combinations {
            let mut matched = seed.clone();
            let mut scope = seed_scope.clone();
            let mut compatible = true;
            for (index, (occurrence, row_index)) in
                split.occurrences.iter().zip(&combination).enumerate()
            {
                let bound = &choices[index][*row_index];
                if !bind_occurrence(&mut matched, occurrence, bound) {
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
            let rows =
                collect_rule_rows(member, split, split.base.clone(), matched, &scope, env).await?;
            if rows.iter().any(|candidate| candidate == row) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// A retained fixpoint carried across evaluations by a standing
/// subscription: the answer table handle plus, when the poll's
/// changes were additions only, the new facts to seed a semi-naive
/// continuation from. Attached to a concept's
/// [`ConceptRules`](crate::ConceptRules) by the query environment;
/// consumed by `ConceptQuery::evaluate`'s fixpoint branch.
#[derive(Clone, Default)]
pub struct Continuation {
    table: Arc<Mutex<Option<InMemoryAnswerTable>>>,
    additions: Arc<Vec<Artifact>>,
    deletions: Arc<Vec<Artifact>>,
}

impl fmt::Debug for Continuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Continuation")
            .field("additions", &self.additions.len())
            .field("deletions", &self.deletions.len())
            .finish_non_exhaustive()
    }
}

impl Continuation {
    /// A continuation over the subscription-held `table`. Without
    /// changes attached, evaluation rebuilds into the handle (the
    /// first evaluation, or a recompute).
    pub fn new(table: Arc<Mutex<Option<InMemoryAnswerTable>>>) -> Self {
        Continuation {
            table,
            additions: Arc::new(Vec::new()),
            deletions: Arc::new(Vec::new()),
        }
    }

    /// Attach the poll's changed facts: deletions retract via DRed,
    /// additions extend semi-naively, in that order. Either phase
    /// declining (non-additive shapes) falls back to a rebuild.
    pub fn with_changes(
        mut self,
        additions: Arc<Vec<Artifact>>,
        deletions: Arc<Vec<Artifact>>,
    ) -> Self {
        self.additions = additions;
        self.deletions = deletions;
        self
    }

    /// The queried concept's fixpoint rows, reusing the retained
    /// table when possible. The table is taken out of the handle
    /// for the duration (an error mid-evaluation leaves the handle
    /// empty, so the next evaluation rebuilds).
    pub(crate) async fn rows<'a, Env>(
        &self,
        root: &ConceptDescriptor,
        analysis: &ProgramAnalysis,
        env: &'a Env,
    ) -> Result<Vec<Row>, EvaluationError>
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        let prior = self.table.lock().expect("fixpoint table lock").take();
        let has_changes = !self.additions.is_empty() || !self.deletions.is_empty();
        let maintained = match prior {
            Some(mut table) if has_changes => {
                let retracted = if self.deletions.is_empty() {
                    Some(())
                } else {
                    retract(root, analysis, env, &mut table, &self.deletions).await?
                };
                match retracted {
                    None => None,
                    Some(()) => {
                        if self.additions.is_empty() {
                            let rows = table.total(&root.this());
                            Some((table, rows))
                        } else {
                            extend(root, analysis, env, &mut table, &self.additions)
                                .await?
                                .map(|rows| (table, rows))
                        }
                    }
                }
            }
            _ => None,
        };
        let (table, rows) = match maintained {
            Some(outcome) => outcome,
            None => {
                let mut table = InMemoryAnswerTable::default();
                let rows = evaluate_table(root, analysis, env, &mut table).await?;
                (table, rows)
            }
        };
        *self.table.lock().expect("fixpoint table lock") = Some(table);
        Ok(rows)
    }
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

/// Recursion whose base facts come from another *derived* concept
/// rather than raw attributes: the seed round and the sideways joins
/// of the delta rounds both evaluate the edge concept through its own
/// rule.
#[cfg(test)]
mod derived_edge_tests {
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

    /// A derived edge concept: concluded by a rule over the raw
    /// `family/parent` attribute rather than stored directly.
    fn edge_concept() -> ConceptDescriptor {
        ConceptDescriptor::try_from(vec![(
            "parent",
            AttributeDescriptor::new(
                the!("family.derived/parent"),
                "",
                Cardinality::Many,
                Some(Type::Entity),
            ),
        )])
        .unwrap()
    }

    fn edge_rule(concept: &ConceptDescriptor) -> DeductiveRule {
        DeductiveRule::new(
            concept.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("family/parent")),
                    Term::<Entity>::var("this"),
                    Term::var("parent"),
                    Term::blank(),
                    Some(Cardinality::Many),
                )
                .into(),
            ],
        )
        .expect("edge rule compiles")
    }

    fn ancestor_concept() -> ConceptDescriptor {
        ConceptDescriptor::try_from(vec![(
            "ancestor",
            AttributeDescriptor::new(
                the!("family.derived/ancestor"),
                "",
                Cardinality::Many,
                Some(Type::Entity),
            ),
        )])
        .unwrap()
    }

    fn edge_premise(edge: &ConceptDescriptor, this: &str, parent: &str) -> Premise {
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var(this));
        terms.insert("parent".to_string(), Term::<Any>::var(parent));
        Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: edge.clone(),
        }))
    }

    fn ancestor_rules(concept: &ConceptDescriptor, edge: &ConceptDescriptor) -> Vec<DeductiveRule> {
        let base = DeductiveRule::new(
            concept.clone(),
            vec![edge_premise(edge, "this", "ancestor")],
        )
        .expect("base rule compiles");

        let mut step_terms = Parameters::new();
        step_terms.insert("this".to_string(), Term::<Any>::var("p"));
        step_terms.insert("ancestor".to_string(), Term::<Any>::var("ancestor"));
        let step = DeductiveRule::new(
            concept.clone(),
            vec![
                edge_premise(edge, "this", "p"),
                Premise::Assert(Proposition::Concept(ConceptQuery {
                    terms: step_terms,
                    predicate: concept.clone(),
                })),
            ],
        )
        .expect("step rule compiles");
        vec![base, step]
    }

    #[dialog_common::test]
    async fn it_derives_transitive_closure_over_a_derived_edge_concept() -> anyhow::Result<()> {
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

        let edge = edge_concept();
        let concept = ancestor_concept();
        let mut registry = RuleRegistry::new();
        registry.register(edge_rule(&edge))?;
        for rule in ancestor_rules(&concept, &edge) {
            registry.register(rule)?;
        }
        assert!(registry.is_recursive(&concept.this())?);

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("who"));
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
        let mut pairs = Vec::new();
        for matched in results {
            let who = matched.lookup(&Term::<Any>::var("who"))?.content()?;
            let relative = matched.lookup(&Term::<Any>::var("relative"))?.content()?;
            pairs.push((who, relative));
        }
        pairs.sort_by_key(|pair| format!("{pair:?}"));
        let mut expected = vec![
            (Value::Entity(carol.clone()), Value::Entity(bob.clone())),
            (Value::Entity(bob.clone()), Value::Entity(alice.clone())),
            (Value::Entity(carol.clone()), Value::Entity(alice.clone())),
        ];
        expected.sort_by_key(|pair| format!("{pair:?}"));
        assert_eq!(pairs, expected, "closure includes the transitive pair");
        Ok(())
    }
}
