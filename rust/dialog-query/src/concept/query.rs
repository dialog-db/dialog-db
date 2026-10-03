/// Adornment types for parameter binding pattern caching.
pub mod adornment;
/// Affected-entity discovery for incremental maintenance.
pub mod affected;
/// Semi-naive fixpoint evaluation for recursive concepts.
pub mod fixpoint;
/// Shared, branch-owned plan cache keyed by (rule identity, adornment).
mod plan_cache;
/// Per-concept rule management with adornment-keyed plan caching.
pub mod rules;

pub use plan_cache::PlanCache;
pub use rules::{ConceptRules, Exact};

use std::fmt;

use crate::artifact::Value;
use crate::attribute::Relation;
use crate::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use crate::planner::{Disjunction, Plan};
use crate::rule::deductive::DeductiveRule;
use crate::schema::CONCEPT_OVERHEAD;
use crate::selection::{Selection, Standing};
use crate::source::SelectRules;
use crate::stream::{fork_stream, stream_select};
use crate::types::Any;
use crate::{
    Binding, Cardinality, Environment, EvaluationError, Match, Parameters, Requirement, Schema,
    Term, try_stream,
};
use dialog_artifacts::encode_value_owned;
use dialog_capability::Provider;
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

/// Extract a Match with parameter names from a Match with user
/// variable names. Maps values from user-specified variable names
/// to internal parameter names for scoped evaluation. Both Present
/// and Absent bindings are propagated.
///
/// Parameters are bound by name: a concept's parameter variables are
/// untyped (`Term::var`), so no kind check applies to them.
fn extract_parameters(source: &Match, terms: &Parameters) -> Result<Match, EvaluationError> {
    let mut matched = Match::new();

    for (param_name, user_param) in terms.iter() {
        match user_param {
            Term::Variable {
                name: Some(name), ..
            } => match source.get(name) {
                Some(Binding::Present(value)) => {
                    matched.bind_variable(param_name, None, value.clone())?;
                }
                Some(Binding::Absent) => {
                    matched.bind_absent_variable(param_name)?;
                }
                // Unbound is expected here: the user supplied a
                // placeholder term (e.g. `Term::var("alice")` in
                // `Query<Person> { this: ..., ... }`) that the
                // concept query is about to bind. Skip it:
                // downstream evaluation fills it in.
                None => {}
            },
            Term::Constant(value) => {
                matched.bind_variable(param_name, None, value.clone())?;
            }
            Term::Variable { name: None, .. } => {}
        }
    }

    Ok(matched)
}

/// Merge a Match with parameter names back into a Match with user
/// variable names after evaluation. Both Present and Absent
/// bindings are propagated.
fn merge_parameters(
    base: &Match,
    result: &Match,
    terms: &Parameters,
) -> Result<Match, EvaluationError> {
    let mut merged = base.clone();

    for (param_name, user_param) in terms.iter() {
        if matches!(user_param, Term::Constant(_)) {
            continue;
        }

        match result.get(param_name) {
            Some(Binding::Present(value)) => {
                merged.bind(user_param, value.clone())?;
            }
            Some(Binding::Absent) => {
                merged.bind_absent(user_param)?;
            }
            // Unbound is expected: not every parameter survives the
            // concept evaluation (e.g. a blank slot the rule never
            // touched). Skip and let the user variable stay
            // un-extended.
            None => {}
        }
    }
    merged.adopt_citations(result);

    Ok(merged)
}

/// Represents an application of a concept with specific term bindings.
/// This is used when querying for entities that match a concept pattern.
///
/// Serializes as the formal notation:
/// `{ "assert": <ConceptDescriptor>, "where": <Parameters> }`.
///
/// A keyed-collection field is bound as an **entry**: under the field,
/// `{"the": <key term>, "is": <value term>}` — a mini fact, in the
/// slots an attribute query already uses. Internally the pair is two
/// operands, the field and its key operand
/// ([`Relation::key_operand`]), and the two forms convert on the
/// wire: the entry is what a document holds, the operands are what
/// the rule binds.
#[derive(Debug, Clone, PartialEq)]
pub struct ConceptQuery {
    /// The concept predicate being applied.
    pub predicate: ConceptDescriptor,
    /// The term bindings for this concept application.
    pub terms: Parameters,
}

/// One binding of a `where` map on the wire: a term, or a
/// collection entry.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum Bound {
    Entry {
        #[serde(default = "Term::blank", skip_serializing_if = "Term::is_blank")]
        the: Term<Any>,
        #[serde(default = "Term::blank", skip_serializing_if = "Term::is_blank")]
        is: Term<Any>,
    },
    Term(Term<Any>),
}

#[derive(Serialize)]
struct ConceptQueryOut<'a> {
    assert: &'a ConceptDescriptor,
    #[serde(rename = "where")]
    terms: BTreeMap<&'a str, Bound>,
}

#[derive(Deserialize)]
struct ConceptQueryIn {
    assert: ConceptDescriptor,
    #[serde(rename = "where")]
    terms: BTreeMap<String, serde_json::Value>,
}

impl Serialize for ConceptQuery {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut terms: BTreeMap<&str, Bound> = BTreeMap::new();
        let mut keys: BTreeMap<String, &str> = BTreeMap::new();
        for (name, _) in self.predicate.collections() {
            keys.insert(Relation::key_operand(name), name);
        }
        for (name, term) in self.terms.iter() {
            if let Some(field) = keys.get(name) {
                // The key half of an entry: folded under its field.
                let is = self.terms.get(field).cloned().unwrap_or_else(Term::blank);
                terms.insert(
                    field,
                    Bound::Entry {
                        the: term.clone(),
                        is,
                    },
                );
            } else if self.predicate.collections().any(|(field, _)| field == name) {
                terms.entry(name).or_insert_with(|| Bound::Entry {
                    the: Term::blank(),
                    is: term.clone(),
                });
            } else {
                terms.insert(name, Bound::Term(term.clone()));
            }
        }
        ConceptQueryOut {
            assert: &self.predicate,
            terms,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ConceptQuery {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let raw = ConceptQueryIn::deserialize(deserializer)?;
        let mut terms = Parameters::new();
        for (name, value) in raw.terms {
            let collection = raw.assert.collections().any(|(field, _)| field == name);
            // An entry object carries `the`/`is` and no `?`; anything
            // else under a collection field is a bare value term,
            // which binds every entry (`{_: ?value}`).
            let entry = collection
                && value.as_object().is_some_and(|map| {
                    !map.contains_key("?") && (map.contains_key("the") || map.contains_key("is"))
                });
            let bound: Bound = if entry {
                serde_json::from_value(value).map_err(D::Error::custom)?
            } else {
                Bound::Term(serde_json::from_value(value).map_err(D::Error::custom)?)
            };
            match bound {
                Bound::Entry { the, is } => {
                    terms.insert(Relation::key_operand(&name), the);
                    terms.insert(name, is);
                }
                Bound::Term(term) => {
                    terms.insert(name, term);
                }
            }
        }
        Ok(ConceptQuery {
            predicate: raw.assert,
            terms,
        })
    }
}

impl ConceptQuery {
    /// Estimate the cost of this concept application given the current environment.
    /// A concept is essentially a join over N fact lookups (one per attribute).
    /// Each fact lookup has the form: (this, attribute_i, value_i).
    ///
    /// Cost model:
    /// - If "this" is bound: Sum of costs for each attribute lookup
    ///   - For both 2/3 and 3/3 constraint:
    ///     - Cardinality::One: LOOKUP_COST
    ///     - Cardinality::Many: RANGE_READ_COST
    ///
    /// - If "this" is unbound but any attribute value is bound:
    ///   - Prefer Cardinality::One attribute (nearly free - just returns `this`)
    ///   - Otherwise use Cardinality::Many (expensive - scan + lookups for each result)
    ///
    /// - If nothing is bound: Returns None (should be blocked)
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        // Check if "this" parameter is bound
        let this_bound = if let Some(this) = self.terms.get("this") {
            this.is_bound(env)
        } else {
            false
        };

        if this_bound {
            // Entity is known - each attribute is a lookup (the + of known)
            let mut total = CONCEPT_OVERHEAD; // Add overhead for potential rule evaluation
            for (name, attribute) in self.predicate.with().iter() {
                // Check if this attribute's value is also bound
                total += attribute.estimate(
                    true,
                    if let Some(param) = self.terms.get(name) {
                        param.is_bound(env)
                    } else {
                        false
                    },
                );
            }
            Some(total)
        } else {
            // Entity is not bound - categorize attributes to find best execution strategy
            let mut bound_one: Option<&ConceptFieldDescriptor> = None;
            let mut bound_many: Option<&ConceptFieldDescriptor> = None;
            let mut unbound_one: Option<&ConceptFieldDescriptor> = None;
            let mut unbound_many: Option<&ConceptFieldDescriptor> = None;

            for (name, attribute) in self.predicate.with().iter() {
                if let Some(param) = self.terms.get(name) {
                    if param.is_bound(env) {
                        match attribute.cardinality() {
                            Cardinality::One => {
                                bound_one = Some(attribute);
                                break; // Best case found, stop searching
                            }
                            Cardinality::Many if bound_many.is_none() => {
                                bound_many = Some(attribute);
                            }
                            _ => {}
                        }
                    } else {
                        // Term exists but not bound
                        match attribute.cardinality() {
                            Cardinality::One if unbound_one.is_none() => {
                                unbound_one = Some(attribute);
                            }
                            Cardinality::Many if unbound_many.is_none() => {
                                unbound_many = Some(attribute);
                            }
                            _ => {}
                        }
                    }
                } else {
                    // No term at all
                    match attribute.cardinality() {
                        Cardinality::One if unbound_one.is_none() => {
                            unbound_one = Some(attribute);
                        }
                        Cardinality::Many if unbound_many.is_none() => {
                            unbound_many = Some(attribute);
                        }
                        _ => {}
                    }
                }
            }

            // Determine initial scan strategy based on what we found
            // For lead attribute: of=false (finding entity), is=bound (value bound or not)
            let (lead, bound) = if let Some(attribute) = bound_one {
                // Best case: bound Cardinality::One - lookup returns `this` directly
                (attribute, true)
            } else if let Some(attribute) = bound_many {
                // Bound Cardinality::Many - scan with value constraint
                (attribute, true)
            } else if let Some(attribute) = unbound_one {
                // No bound attributes but have Cardinality::One - cheaper scan
                (attribute, false)
            } else if let Some(attribute) = unbound_many {
                // Worst case: use unbound Cardinality::Many
                (attribute, false)
            } else {
                unreachable!("concept without attributes is not possible")
            };

            // Start with initial cost including overhead for potential rule evaluation
            // of=false (finding entity), is=bound
            let mut total = CONCEPT_OVERHEAD + lead.estimate(false, bound);

            for (name, attribute) in self.predicate.with().iter() {
                if lead != attribute {
                    total += attribute.estimate(
                        true,
                        if let Some(param) = self.terms.get(name) {
                            param.is_bound(env)
                        } else {
                            false
                        },
                    );
                }
            }

            Some(total)
        }
    }

    /// Returns the parameters for this concept application
    pub fn parameters(&self) -> Parameters {
        self.terms.clone()
    }

    /// Returns the schema describing this concept's attributes and their types.
    pub fn schema(&self) -> Schema {
        let mut schema = self.predicate.schema();
        // A set-widened read of an attribute concept binds its value
        // `Absent` where nothing matched, so the slot admits `Nothing`
        // for inference and planning alike.
        if let Some((name, _)) = self.predicate.attribute_field()
            && self.widens()
        {
            if let Some(field) = schema.get_mut(name) {
                field.content_type = field.content_type.take().map(|kind| kind.optional());
                field.requirement = Requirement::Optional;
            }
            // A left join is per entity: the entity must come from a
            // premise before it, as an optional scan's does.
            if let Some(this) = schema.get_mut("this") {
                this.requirement = Requirement::Required(None);
            }
        }
        schema
    }

    /// Evaluates this concept application within the given context, producing
    /// a selection stream.
    ///
    /// Rather than threading a scope through the entire evaluation pipeline,
    /// we derive the binding pattern (adornment) from the first match and
    /// use it to obtain a specialized, cached execution plan. This is the
    /// key insight from magic set optimization applied locally: the adornment
    /// is computed at the point of use from what's actually bound, rather
    /// than carried globally through every evaluation step.
    ///
    /// Every incoming row goes through one evaluation of that plan: each is
    /// renamed into the concept's parameters and placed in a scope nested in
    /// it ([`Match::within`]), and each result is merged back into the row it
    /// came from. Evaluating the plan once per row instead rebuilt the whole
    /// pipeline, every step's stream and a copy of the plan, for every row.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        let app = self.canonical();
        let this = app.terms.get("this").cloned();

        try_stream! {
            let mut selection = Box::pin(selection);
            let Some(first) = selection.next().await else {
                return;
            };
            let first = first?;

            let rules = Provider::<SelectRules>::execute(env, app.predicate.clone()).await?;
            // An attribute concept read with its entity free is a merge
            // input (see `Conjunction::merge_variable`): its rows leave
            // sorted on the entity, as a scan's would. Derived rows arrive
            // in their rules' order, so the read is buffered and sorted
            // whenever something derives the attribute.
            let sort = app.predicate.attribute_field().is_some()
                && !rules.installed().is_empty()
                && this
                    .as_ref()
                    .is_some_and(|term| term.name().is_some() && !first.contains(term));
            if sort {
                let unsorted = app.clone().evaluate_unsorted(
                    rules,
                    stream::once(async { Ok(first) }).chain(selection),
                    env,
                );
                let mut rows: Vec<(Vec<u8>, Match)> = Vec::new();
                for await row in unsorted {
                    let row = row?;
                    let key = this
                        .as_ref()
                        .and_then(|term| row.value_of(term.name()?))
                        .map(dialog_artifacts::encode_value_owned)
                        .unwrap_or_default();
                    rows.push((key, row));
                }
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                for (_, row) in rows {
                    yield row;
                }
                return;
            }
            for await row in app.evaluate_unsorted(
                rules,
                stream::once(async { Ok(first) }).chain(selection),
                env,
            ) {
                yield row?;
            }
        }
    }

    /// [`evaluate`](Self::evaluate) with the rules already resolved,
    /// yielding rows in evaluation order.
    fn evaluate_unsorted<'a, Env, M: Selection + 'a>(
        self,
        rules: ConceptRules,
        selection: M,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        let app = self;

        try_stream! {
            let mut selection = Box::pin(selection);
            let Some(first) = selection.next().await else {
                return;
            };
            let first = first?;

            // A concept on a dependency cycle cannot evaluate top-down (it
            // would recurse unboundedly): its component's semi-naive fixpoint
            // is computed once and the caller's bindings join against the
            // rows.
            let widen = app.widens();
            if let Some(analysis) = rules.recursion() {
                let table = match rules.continuation() {
                    Some(continuation) => {
                        continuation.rows(&app.predicate, analysis, env).await?
                    }
                    None => fixpoint::evaluate(&app.predicate, analysis, env).await?,
                };
                let rows = stream::once(async { Ok(first) }).chain(selection);
                for await each in rows {
                    let input = each?;
                    let mut matched = false;
                    for row in table.iter() {
                        if let Some(merged) = fixpoint::join(&input, &app.terms, row)? {
                            matched = true;
                            yield merged;
                        }
                    }
                    if widen && !matched {
                        yield widened(&input, &app.terms)?;
                    }
                }
                return;
            }

            // A reducing rule's fold reads its whole body relation, so caller
            // bindings must never restrict the body: each reducing rule's
            // folded rows are computed once, over the full relation, and the
            // caller's bindings join against the output (the fixpoint-table
            // shape). The plain rules plan as usual.
            let mut reduced: Vec<fixpoint::Row> = Vec::new();
            for rule in rules.reducing() {
                reduced.extend(reduce_rows(rule, env).await?);
            }
            // All matches in the selection share the first one's binding
            // pattern (same variables bound), only the values differ.
            // One rule derives every derived attribute of this concept:
            // while nothing is stored under those attributes, the rule
            // re-headed onto the concept is its whole answer, evaluated
            // once rather than once per attribute.
            if let Some(exact) = rules.exact()
                && stored_absent(&exact.attributes, env).await?
                && let Some(plan) = rules.plan_exact(&app.terms, &first)
            {
                let rows = stream::once(async { Ok(first) }).chain(selection);
                for await merged in app.through(&plan, rows, env, false, widen) {
                    yield merged?;
                }
                return;
            }

            // An attribute concept some rule derives: candidates from the
            // stored scan, every head and every fold are elected per
            // entity and one row is built per winner.
            if app.predicate.attribute_field().is_some() && !rules.installed().is_empty() {
                let rows = stream::once(async { Ok(first) }).chain(selection);
                for await row in app.relation(rules, reduced, rows, env) {
                    yield row?;
                }
                return;
            }

            let elect = app.elects(&rules);
            let plan = rules.plan(&app.terms, &first);
            let rows = stream::once(async { Ok(first) }).chain(selection);

            if reduced.is_empty() {
                for await merged in app.through(&plan, rows, env, elect, widen) {
                    yield merged?;
                }
            } else if widen {
                // Folded rows and scanned rows both count as matches for
                // the widening, so each input is evaluated on its own:
                // the one shape where the pipeline is per row.
                for await each in rows {
                    let input = each?;
                    let mut matched = false;
                    for row in reduced.iter() {
                        if let Some(merged) = fixpoint::join(&input, &app.terms, row)? {
                            matched = true;
                            yield merged;
                        }
                    }
                    let copy = input.clone();
                    let single = stream::once(async move { Ok(copy) });
                    for await merged in app.through(&plan, single, env, elect, false) {
                        matched = true;
                        yield merged?;
                    }
                    if !matched {
                        yield widened(&input, &app.terms)?;
                    }
                }
            } else {
                let (joining, planned) = fork_stream(rows);
                let terms = app.terms.clone();
                let joined = joining.map_ok(move |input| {
                    let rows: Vec<Result<Match, EvaluationError>> = reduced
                        .iter()
                        .filter_map(|row| fixpoint::join(&input, &terms, row).transpose())
                        .collect();
                    stream::iter(rows)
                })
                .try_flatten();
                let planned = app.through(&plan, planned, env, elect, widen);
                for await merged in stream_select!(Box::pin(joined), planned) {
                    yield merged?;
                }
            }
        }
    }

    /// Evaluate `plan` once for every row of `rows`: each row is renamed
    /// into this concept's parameters in a scope nested in it, and each
    /// result is merged back into the row it came from.
    fn through<'a, Env, M: Selection + 'a>(
        &self,
        plan: &Disjunction,
        rows: M,
        env: &'a Env,
        elect: bool,
        widen: bool,
    ) -> Pin<Box<dyn Selection + 'a>>
    where
        Env: crate::Scope<'a>,
    {
        let into = self.terms.clone();
        let back = self.terms.clone();
        // A set-widened read keeps every caller to tell, once the plan
        // has run, which of them nothing matched.
        let callers: Arc<Mutex<Vec<Arc<Match>>>> = Arc::new(Mutex::new(Vec::new()));
        let kept = callers.clone();
        let scoped = rows.map(move |each| {
            let mut input = each?;
            // Every result merges back into a clone of this row: share its
            // bindings rather than copy them into each.
            input.share();
            let inner = extract_parameters(&input, &into)
                .map_err(|e| EvaluationError::Store(e.to_string()))?;
            let caller = Arc::new(input);
            if widen {
                kept.lock().expect("callers lock").push(caller.clone());
            }
            Ok(inner.within(caller))
        });
        let results = plan.clone().evaluate(scoped, env);
        if !elect && !widen {
            return Box::pin(results.map(move |result| {
                let mut result = result?;
                let caller = result.take_caller().ok_or_else(|| {
                    EvaluationError::Store(
                        "a concept's result lost the row it was evaluated for".to_string(),
                    )
                })?;
                merge_parameters(&caller, &result, &back)
                    .map_err(|e| EvaluationError::Store(e.to_string()))
            }));
        }
        // A cardinality-one attribute concept with derived rows: one
        // value per entity leaves here, elected by the rows' standing,
        // the stored winner and every derived candidate alike. The
        // candidates for an entity arrive in no particular order, so
        // the election buffers the results of the whole input.
        Box::pin(try_stream! {
            let mut winners: BTreeMap<(usize, Vec<u8>), Winner> = BTreeMap::new();
            let mut matched: HashSet<usize> = HashSet::new();
            for await result in results {
                let mut result = result?;
                let caller = result.take_caller().ok_or_else(|| {
                    EvaluationError::Store(
                        "a concept's result lost the row it was evaluated for".to_string(),
                    )
                })?;
                let caller_id = Arc::as_ptr(&caller) as usize;
                matched.insert(caller_id);
                if !elect {
                    yield merge_parameters(&caller, &result, &back)
                        .map_err(|e| EvaluationError::Store(e.to_string()))?;
                    continue;
                }
                let entity = match result.lookup(&Term::<Any>::var("this")) {
                    Ok(Binding::Present(value)) => entity_key(&value)?,
                    _ => continue,
                };
                let value = match result.lookup(&Term::<Any>::var(ConceptDescriptor::VALUE)) {
                    Ok(Binding::Present(value)) => value,
                    _ => continue,
                };
                let standing = result.standing();
                let key = (caller_id, entity);
                match winners.get(&key) {
                    Some((best, best_value, _, _))
                        if best > &standing
                            || (best == &standing && !value_beats(&value, best_value)?) => {}
                    _ => {
                        winners.insert(key, (standing, value, result, caller));
                    }
                }
            }
            for (_, (_, _, result, caller)) in winners {
                yield merge_parameters(&caller, &result, &back)
                    .map_err(|e| EvaluationError::Store(e.to_string()))?;
            }
            if widen {
                let callers = std::mem::take(&mut *callers.lock().expect("callers lock"));
                for caller in callers {
                    if !matched.contains(&(Arc::as_ptr(&caller) as usize)) {
                        yield widened(&caller, &back)?;
                    }
                }
            }
        })
    }

    /// Evaluate an attribute concept some rule derives. Per input row,
    /// the candidates for the attribute are gathered from every source
    /// at once: the stored scan and any attribute-headed rule evaluated
    /// in scope, every head split from a source rule through the rows
    /// its body was remembered to yield, and every fold's rows. A
    /// cardinality-one attribute keeps one candidate per entity, the
    /// best standing then the greatest value; a cardinality-many one
    /// keeps every distinct value. One row is built per survivor,
    /// citing the facts it came from, and a set-widened read yields an
    /// `Absent` row where nothing survived.
    fn relation<'a, Env, M: Selection + 'a>(
        self,
        rules: ConceptRules,
        reduced: Vec<fixpoint::Row>,
        rows: M,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        let app = self;
        try_stream! {
            let mut rows = Box::pin(rows);
            let Some(first) = rows.next().await else {
                return;
            };
            let first = first?;
            let plan = rules.plan(&app.terms, &first);
            let widen = app.widens();
            let one = app
                .predicate
                .attribute_field()
                .is_some_and(|(_, field)| field.cardinality() == Cardinality::One);
            let this_term = app.terms.get("this").cloned();
            let key_operand = Relation::key_operand(ConceptDescriptor::VALUE);

            let rows = stream::once(async { Ok(first) }).chain(rows);
            for await input in rows {
                let input = input?;
                // The entity the caller bound, if any: a constant, or a
                // variable an earlier premise bound.
                let entity: Option<Value> = match &this_term {
                    Some(Term::Constant(value)) => Some(value.clone()),
                    Some(term @ Term::Variable { name: Some(name), .. }) if input.contains(term) => {
                        input.value_of(name).cloned()
                    }
                    _ => None,
                };

                let mut candidates: Vec<Candidate> = Vec::new();
                for conjunction in plan.conjunctions() {
                    match conjunction.steps.as_slice() {
                        [Plan::Recall(_, recall)] => {
                            let shared = recall.rows_for(entity.as_ref(), env).await?;
                            for (index, row) in shared.iter().enumerate() {
                                let Some(this) = row.value_of("this").cloned() else { continue };
                                let Some(value) = row.value_of(&recall.value).cloned() else { continue };
                                let key = match &recall.key {
                                    Some(operand) => match row.value_of(operand).cloned() {
                                        Some(key) => Some(key),
                                        None => continue,
                                    },
                                    None => None,
                                };
                                candidates.push(Candidate {
                                    entity: encode_value_owned(&this),
                                    this,
                                    value,
                                    key,
                                    standing: row.standing(),
                                    source: Source::Shared(shared.clone(), index),
                                });
                            }
                        }
                        _ => {
                            let mut scoped = extract_parameters(&input, &app.terms)
                                .map_err(|e| EvaluationError::Store(e.to_string()))?;
                            scoped.share();
                            let results: Vec<Match> = conjunction
                                .clone()
                                .evaluate(scoped.seed(), env)
                                .try_collect()
                                .await?;
                            for row in results {
                                let Some(this) = row.value_of("this").cloned() else { continue };
                                let Some(value) = row.value_of(ConceptDescriptor::VALUE).cloned() else {
                                    continue;
                                };
                                let key = row.value_of(&key_operand).cloned();
                                candidates.push(Candidate {
                                    entity: encode_value_owned(&this),
                                    this,
                                    value,
                                    key,
                                    standing: row.standing(),
                                    source: Source::Owned(row),
                                });
                            }
                        }
                    }
                }
                for row in &reduced {
                    let (Some(this), Some(value)) = (row.get("this"), row.get(ConceptDescriptor::VALUE)) else {
                        continue;
                    };
                    if let Some(bound) = &entity
                        && bound != this
                    {
                        continue;
                    }
                    candidates.push(Candidate {
                        entity: encode_value_owned(this),
                        this: this.clone(),
                        value: value.clone(),
                        key: row.get(&key_operand).cloned(),
                        standing: None,
                        source: Source::Folded,
                    });
                }

                let survivors = elect(candidates, one)?;
                if survivors.is_empty() && widen {
                    yield widened(&input, &app.terms)?;
                    continue;
                }
                for candidate in survivors {
                    let mut row = fixpoint::Row::new();
                    row.insert("this".to_string(), candidate.this);
                    row.insert(ConceptDescriptor::VALUE.to_string(), candidate.value);
                    if let Some(key) = candidate.key {
                        row.insert(key_operand.clone(), key);
                    }
                    let Some(mut merged) = fixpoint::join(&input, &app.terms, &row)? else {
                        continue;
                    };
                    match &candidate.source {
                        Source::Owned(source) => merged.adopt_citations(source),
                        Source::Shared(rows, index) => merged.adopt_citations(&rows[*index]),
                        Source::Folded => {}
                    }
                    yield merged;
                }
            }
        }
    }

    /// Whether this query reads its attribute concept set-widened: the
    /// caller's value term admits `Nothing`, so an entity no row
    /// matched yields one row with the value `Absent` instead of none.
    pub(crate) fn widens(&self) -> bool {
        self.predicate
            .attribute_field()
            .is_some_and(|(name, _)| self.terms.get(name).is_some_and(|term| term.is_optional()))
    }

    /// Whether this query's rows are elected on their way out: the
    /// query reads a cardinality-one attribute concept to which some
    /// rule contributes rows, so several candidates per entity may
    /// arrive and one must leave.
    fn elects(&self, rules: &ConceptRules) -> bool {
        match self.predicate.attribute_field() {
            Some((_, field)) => {
                field.cardinality() == Cardinality::One && !rules.installed().is_empty()
            }
            None => false,
        }
    }

    /// This query over the concept narrowed to the fields it binds:
    /// an optional field the terms leave out, or bind to an anonymous
    /// variable, is dropped from the predicate. A set-widened read of
    /// a field nobody binds yields every row once whatever the field
    /// holds, so it decides nothing; and when the field is derived by
    /// rules whose bodies read this concept, selecting it would put
    /// the concept on a cycle with those rules through a read the
    /// caller never asked for. Required fields stay: an entity
    /// without one is not an instance. An attribute concept is left
    /// as it is.
    pub fn narrowed(self) -> Self {
        if self.predicate.attribute_field().is_some() {
            return self;
        }
        let unbound = |name: &str| match self.terms.get(name) {
            None | Some(Term::Variable { name: None, .. }) => true,
            Some(_) => false,
        };
        let dropped: Vec<&str> = self
            .predicate
            .with()
            .iter()
            .filter(|(name, field)| field.is_optional() && unbound(name))
            .map(|(name, _)| name)
            .collect();
        if dropped.is_empty() {
            return self;
        }
        let kept: Vec<(String, ConceptFieldDescriptor)> = self
            .predicate
            .with()
            .iter()
            .filter(|(name, _)| !dropped.contains(name))
            .map(|(name, field)| (name.to_string(), field.clone()))
            .collect();
        match ConceptDescriptor::try_from(kept) {
            Ok(predicate) => ConceptQuery {
                predicate,
                terms: self.terms,
            },
            Err(_) => self,
        }
    }

    /// This query over the canonical spelling of its concept: an
    /// attribute concept under a field name of the caller's choosing
    /// is the attribute concept, and its rules bind the attribute's
    /// own operand name, so the caller's terms are re-keyed onto it.
    pub(crate) fn canonical(self) -> Self {
        let Some((name, field)) = self.predicate.attribute_field() else {
            return self;
        };
        if name == ConceptDescriptor::VALUE {
            return self;
        }
        let key = Relation::key_operand(name);
        let canonical_key = Relation::key_operand(ConceptDescriptor::VALUE);
        let mut terms = Parameters::new();
        for (param, term) in self.terms.iter() {
            let param = if param == name {
                ConceptDescriptor::VALUE.to_string()
            } else if *param == key {
                canonical_key.clone()
            } else {
                param.clone()
            };
            terms.insert(param, term.clone());
        }
        ConceptQuery {
            predicate: ConceptDescriptor::of_attribute(field),
            terms,
        }
    }
}

/// Whether nothing is stored under any of `attributes`, in any layer
/// the environment reads: each attribute's range is opened and must
/// yield no row. A range estimate would not do, since it reads the
/// committed tree alone and an overlay may hold the fact.
async fn stored_absent<'a, Env>(
    attributes: &[crate::artifact::ArtifactsAttribute],
    env: &'a Env,
) -> Result<bool, EvaluationError>
where
    Env: crate::Scope<'a>,
{
    for attribute in attributes {
        let selector = dialog_artifacts::ArtifactSelector::new().the(attribute.clone());
        let mut rows = Provider::<dialog_artifacts::Select<'_>>::execute(env, selector)
            .await
            .map_err(|error| EvaluationError::Store(error.to_string()))?;
        if rows.next().await.is_some() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// A cardinality-one election's incumbent for one caller and entity:
/// its standing, its value, the row it came from and the caller's row.
type Winner = (Option<Standing>, Value, Match, Arc<Match>);

/// One value a source offers for an attribute of an entity.
struct Candidate {
    /// The entity's merge key, for grouping.
    entity: Vec<u8>,
    this: Value,
    value: Value,
    key: Option<Value>,
    standing: Option<Standing>,
    source: Source,
}

/// Where a candidate's citations live.
enum Source {
    /// A row the stored scan or an attribute-headed rule built in scope.
    Owned(Match),
    /// A row of a source rule's remembered body, by index.
    Shared(Arc<Vec<Match>>, usize),
    /// A fold's row, which cites nothing.
    Folded,
}

/// The candidates that survive the attribute's election: one per
/// entity for a cardinality-one attribute, chosen by standing then by
/// value; every distinct value per entity otherwise.
fn elect(candidates: Vec<Candidate>, one: bool) -> Result<Vec<Candidate>, EvaluationError> {
    if candidates.len() <= 1 {
        return Ok(candidates);
    }
    let mut survivors: Vec<Candidate> = Vec::with_capacity(candidates.len());
    let mut best: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut seen: HashSet<(Vec<u8>, Vec<u8>)> = HashSet::new();
    for candidate in candidates {
        if one {
            match best.get(&candidate.entity) {
                Some(&index) => {
                    let incumbent = &survivors[index];
                    let wins = candidate.standing > incumbent.standing
                        || (candidate.standing == incumbent.standing
                            && value_beats(&candidate.value, &incumbent.value)?);
                    if wins {
                        survivors[index] = candidate;
                    }
                }
                None => {
                    best.insert(candidate.entity.clone(), survivors.len());
                    survivors.push(candidate);
                }
            }
        } else {
            let value = match &candidate.key {
                Some(key) => [encode_value(&candidate.value)?, encode_value(key)?].concat(),
                None => encode_value(&candidate.value)?,
            };
            if seen.insert((candidate.entity.clone(), value)) {
                survivors.push(candidate);
            }
        }
    }
    Ok(survivors)
}

/// `input` extended with the query's value bound `Absent`: the row a
/// set-widened read yields where nothing matched.
fn widened(input: &Match, terms: &Parameters) -> Result<Match, EvaluationError> {
    let mut widened = input.clone();
    if let Some(term) = terms.get(ConceptDescriptor::VALUE) {
        widened.bind_absent(term)?;
    }
    Ok(widened)
}

/// A value's canonical bytes, for grouping and tie-breaking.
fn encode_value(value: &Value) -> Result<Vec<u8>, EvaluationError> {
    serde_ipld_dagcbor::to_vec(value).map_err(|error| EvaluationError::Store(error.to_string()))
}

/// The bytes an election groups an entity under: the entity's own,
/// without encoding, when the value is one.
fn entity_key(value: &Value) -> Result<Vec<u8>, EvaluationError> {
    match value {
        Value::Entity(entity) => Ok(entity.to_string().into_bytes()),
        other => encode_value(other),
    }
}

/// Whether `candidate` wins the tie-break against `incumbent`: by the
/// values' own order where they have one, else by their bytes.
fn value_beats(candidate: &Value, incumbent: &Value) -> Result<bool, EvaluationError> {
    match candidate.partial_cmp(incumbent) {
        Some(ordering) => Ok(ordering.is_gt()),
        None => Ok(encode_value(candidate)? > encode_value(incumbent)?),
    }
}

/// Evaluate one reducing rule to its folded conclusion rows: the
/// body plans and evaluates at *empty* scope (the fold must see the
/// full relation, never a caller-restricted slice), the [`Reduce`]
/// fold groups by the non-reduced head fields and computes each
/// entry, and every folded match projects to a conclusion row for
/// the caller join. Recomputed per query — incremental maintenance
/// is milestone A5.
///
/// [`Reduce`]: crate::reduce::Reduce
async fn reduce_rows<'a, Env>(
    rule: &DeductiveRule,
    env: &'a Env,
) -> Result<Vec<fixpoint::Row>, EvaluationError>
where
    Env: crate::Scope<'a>,
{
    let reducer = rule
        .reducer()
        .expect("only rules with a reduce clause reach reduce_rows");
    let body = rule
        .plan(&Environment::new())
        .evaluate(Match::new().seed(), env);
    let folded = reducer.fold(body).await?;
    Ok(folded
        .iter()
        .filter_map(|matched| fixpoint::project_complete(rule.conclusion(), matched))
        .collect())
}

impl Display for ConceptQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {{", self.predicate.this())?;
        for (name, term) in self.terms.iter() {
            write!(f, "{}: {},", name, term)?;
        }

        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    mod entry_form {
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

        use crate::attribute::{AttributeDescriptor, Keyed, Relation};
        use crate::concept::descriptor::ConceptFieldDescriptor;
        use crate::{Cardinality, ConceptDescriptor, ConceptQuery, Term, Type};
        use dialog_artifacts::Symbol;
        use std::str::FromStr;

        fn ordered() -> ConceptDescriptor {
            ConceptDescriptor::try_from(vec![(
                "member".to_owned(),
                ConceptFieldDescriptor::required(AttributeDescriptor::over(
                    Relation::collection(
                        Symbol::from_str("todo.list").expect("a valid domain"),
                        Keyed::Sequence,
                    ),
                    "members",
                    Cardinality::Many,
                    Some(Type::String),
                )),
            )])
            .expect("a collection field builds a concept")
        }

        /// `member: {the: ?key, is: ?member}` on the wire is the two
        /// operands `member/key` and `member` in the query, and
        /// writes back as the entry.
        #[dialog_common::test]
        fn it_reads_and_writes_a_collection_entry() {
            let wire = serde_json::json!({
                "assert": ordered(),
                "where": {
                    "this": {"?": {"name": "list"}},
                    "member": {"the": {"?": {"name": "key"}}, "is": {"?": {"name": "member"}}}
                }
            });
            let query: ConceptQuery = serde_json::from_value(wire.clone()).expect("parses");
            assert_eq!(query.terms.get("member/key"), Some(&Term::var("key")));
            assert_eq!(query.terms.get("member"), Some(&Term::var("member")));
            assert_eq!(query.terms.get("this"), Some(&Term::var("list")));

            let written = serde_json::to_value(&query).expect("serializes");
            assert_eq!(
                written["where"], wire["where"],
                "the entry form round-trips"
            );
        }

        /// A literal key is a constant `the`; a bare term under a
        /// collection field binds every entry with the key blank, and
        /// the operand form is accepted on the way in.
        #[dialog_common::test]
        fn it_accepts_literal_bare_and_operand_forms() {
            let literal: ConceptQuery = serde_json::from_value(serde_json::json!({
                "assert": ordered(),
                "where": {"member": {"the": "N5", "is": {"?": {"name": "m"}}}}
            }))
            .expect("parses");
            assert_eq!(
                literal.terms.get("member/key"),
                Some(&Term::constant("N5".to_string()))
            );

            let bare: ConceptQuery = serde_json::from_value(serde_json::json!({
                "assert": ordered(),
                "where": {"member": {"?": {"name": "m"}}}
            }))
            .expect("parses");
            assert_eq!(bare.terms.get("member"), Some(&Term::var("m")));
            assert!(bare.terms.get("member/key").is_none());
            let written = serde_json::to_value(&bare).expect("serializes");
            assert_eq!(
                written["where"]["member"],
                serde_json::json!({"is": {"?": {"name": "m"}}}),
                "a bare value writes as an entry with no key"
            );

            let operands: ConceptQuery = serde_json::from_value(serde_json::json!({
                "assert": ordered(),
                "where": {
                    "member": {"?": {"name": "m"}},
                    "member/key": {"?": {"name": "k"}}
                }
            }))
            .expect("parses");
            assert_eq!(operands.terms.get("member/key"), Some(&Term::var("k")));
        }
    }

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Binding;
    use crate::attribute::query::AttributeQuery;
    use crate::concept::descriptor::ConceptDescriptor;
    use crate::error::{AnalyzerError, TypeError};
    use crate::the;
    use crate::types::Any;
    use std::collections::{BTreeSet, HashSet};

    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::{
        AttributeDescriptor, Cardinality, DeductiveRule, Negation, Parameters, Premise,
        Proposition, Query, Term, Type, Value,
    };
    use dialog_artifacts::Entity;
    use dialog_peer::helpers::{test_repo, test_session_with_peer};
    use futures_util::TryStreamExt;

    /// A premise reads the fields it binds and the required ones: an
    /// optional field left out, or bound to a blank, is dropped from
    /// the concept it reads; one bound to a variable stays, and so
    /// does every required field, named or not.
    #[dialog_common::test]
    fn it_narrows_a_premise_to_the_fields_it_binds() {
        let descriptor: ConceptDescriptor = serde_json::from_value(serde_json::json!({
            "with": {
                "name": { "the": "narrow/name", "as": "Text" },
                "nick": { "the": "narrow/nick", "as": "Text", "optional": true },
                "mood": { "the": "narrow/mood", "as": "Text", "optional": true }
            }
        }))
        .unwrap();
        let fields = |query: &ConceptQuery| -> Vec<String> {
            query.predicate.with().keys().map(String::from).collect()
        };

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        let bare = ConceptQuery {
            predicate: descriptor.clone(),
            terms,
        }
        .narrowed();
        assert_eq!(
            fields(&bare),
            vec!["name"],
            "unnamed optionals go, required stays"
        );

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("nick".into(), Term::var("nick"));
        terms.insert("mood".into(), Term::<Any>::blank());
        let bound = ConceptQuery {
            predicate: descriptor.clone(),
            terms,
        }
        .narrowed();
        assert_eq!(
            fields(&bound),
            vec!["name", "nick"],
            "a variable keeps its field, a blank does not"
        );

        let mut terms = Parameters::new();
        terms.insert("nick".into(), Term::var("nick"));
        terms.insert("mood".into(), Term::var("mood"));
        let full = ConceptQuery {
            predicate: descriptor.clone(),
            terms,
        }
        .narrowed();
        assert_eq!(
            full.predicate, descriptor,
            "nothing to drop leaves it as it is"
        );
    }

    // Note: Async tests are commented out due to Rust recursion limit issues in test compilation
    // with deeply nested async streams. The functionality is tested indirectly through integration
    // tests and the planning tests above verify the core logic.

    #[dialog_common::test]
    async fn it_executes_concept_query() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/age").of(alice.clone()).is(25u32))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .assert(the!("person/age").of(bob.clone()).is(30u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        // Create a person concept
        let concept = ConceptDescriptor::try_from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("person"));
        terms.insert("name".to_string(), Term::var("name"));
        terms.insert("age".to_string(), Term::var("age"));

        let application = ConceptQuery {
            terms,
            predicate: concept,
        };

        // Execute the query
        let selection =
            TryStreamExt::try_collect::<Vec<_>>(application.evaluate(Match::new().seed(), &source))
                .await?;

        // Should find both Alice and Bob with their name and age
        assert_eq!(selection.len(), 2, "Should find 2 people");

        let name_param = Term::var("name");
        let age_param = Term::var("age");

        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let name = match_result.lookup(&name_param)?.content()?;
            let age = match_result.lookup(&age_param)?.content()?;

            match name {
                Value::String(n) if n == "Alice" => {
                    assert_eq!(age, Value::UnsignedInt(25), "Alice should be 25");
                    found_alice = true;
                }
                Value::String(n) if n == "Bob" => {
                    assert_eq!(age, Value::UnsignedInt(30), "Bob should be 30");
                    found_bob = true;
                }
                _ => panic!("Unexpected person: {:?}", name),
            }
        }

        assert!(found_alice, "Should find Alice");
        assert!(found_bob, "Should find Bob");

        Ok(())
    }

    /// End-to-end: a concept with a `maybe` attribute returns
    /// rows for entities that lack the optional fact, with the
    /// optional slot bound to `Binding::Absent`. Entities that
    /// have the fact get `Binding::Present(value)` for the slot.
    /// This is the v2 set-widening behavior at the concept
    /// projection layer, realized by the `OptionalAttributeQuery` left-join the
    /// concept lowering emits for `maybe` fields.
    #[dialog_common::test]
    async fn it_executes_concept_with_optional_field() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Alice has both name and nickname; Bob has only name.
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("person/nickname")
                    .of(alice.clone())
                    .is("Ali".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let concept = ConceptDescriptor::try_from(vec![
            (
                "name".to_string(),
                ConceptFieldDescriptor::required(AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
            (
                "nickname".to_string(),
                ConceptFieldDescriptor::optional(AttributeDescriptor::new(
                    the!("person/nickname"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
        ])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("person"));
        terms.insert("name".to_string(), Term::var("name"));
        terms.insert("nickname".to_string(), Term::var("nickname"));

        let application = ConceptQuery {
            terms,
            predicate: concept,
        };

        let selection =
            TryStreamExt::try_collect::<Vec<_>>(application.evaluate(Match::new().seed(), &source))
                .await?;

        assert_eq!(
            selection.len(),
            2,
            "Should find 2 people (both Alice and Bob)"
        );

        let nickname_param = Term::var("nickname");
        let name_param = Term::var("name");

        let mut found_alice_with_nickname = false;
        let mut found_bob_without_nickname = false;
        for match_result in selection.iter() {
            let name = match_result.lookup(&name_param)?.content()?;
            let nickname = match_result.lookup(&nickname_param)?;
            match (&name, nickname) {
                (Value::String(n), Binding::Present(Value::String(nick)))
                    if n == "Alice" && nick == "Ali" =>
                {
                    found_alice_with_nickname = true;
                }
                (Value::String(n), Binding::Absent) if n == "Bob" => {
                    found_bob_without_nickname = true;
                }
                _ => panic!(
                    "unexpected (name, nickname): ({:?}, {:?})",
                    name,
                    match_result.lookup(&nickname_param)
                ),
            }
        }
        assert!(
            found_alice_with_nickname,
            "Alice should have nickname Present"
        );
        assert!(
            found_bob_without_nickname,
            "Bob should have nickname Absent"
        );

        Ok(())
    }

    /// Regression (PR #348): a `this`-unbound concept query whose
    /// alphabetically-first field is *optional* must still set-widen
    /// it, not drop entities that lack the optional fact. `bio`
    /// (optional) sorts before `name` (required); Alice has only
    /// `name`, Bob has both. Both must be returned (Alice's `bio`
    /// Absent, Bob's Present).
    ///
    /// Fixed by making an optional attribute *require* its entity
    /// (`of`) bound rather than letting the choice group bind it: an
    /// unbound-entity optional scan suppresses its `Absent` fallback,
    /// so it must never lead an unbound scan. Feasibility now forces a
    /// required premise (`name`) to bind `this` first; the optional
    /// `bio` then runs with `this` known and set-widens correctly.
    #[dialog_common::test]
    async fn it_set_widens_optional_field_sorted_before_required() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Alice has only name; Bob has both name and bio.
        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .assert(the!("person/bio").of(bob.clone()).is("Hi".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        // `bio` (optional) sorts before `name` (required).
        let concept = ConceptDescriptor::try_from(vec![
            (
                "bio".to_string(),
                ConceptFieldDescriptor::optional(AttributeDescriptor::new(
                    the!("person/bio"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
            (
                "name".to_string(),
                ConceptFieldDescriptor::required(AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
        ])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("person"));
        terms.insert("name".to_string(), Term::var("name"));
        terms.insert("bio".to_string(), Term::var("bio"));

        let application = ConceptQuery {
            terms,
            predicate: concept,
        };

        let selection =
            TryStreamExt::try_collect::<Vec<_>>(application.evaluate(Match::new().seed(), &source))
                .await?;

        assert_eq!(
            selection.len(),
            2,
            "Should find 2 people (both Alice and Bob), even though the \
             optional `bio` field sorts before the required `name`"
        );

        let name_param = Term::var("name");
        let bio_param = Term::var("bio");

        let mut found_alice_without_bio = false;
        let mut found_bob_with_bio = false;
        for match_result in selection.iter() {
            let name = match_result.lookup(&name_param)?.content()?;
            let bio = match_result.lookup(&bio_param)?;
            match (&name, bio) {
                (Value::String(n), Binding::Absent) if n == "Alice" => {
                    found_alice_without_bio = true;
                }
                (Value::String(n), Binding::Present(Value::String(b)))
                    if n == "Bob" && b == "Hi" =>
                {
                    found_bob_with_bio = true;
                }
                _ => panic!(
                    "unexpected (name, bio): ({:?}, {:?})",
                    name,
                    match_result.lookup(&bio_param)
                ),
            }
        }
        assert!(found_alice_without_bio, "Alice should have bio Absent");
        assert!(found_bob_with_bio, "Bob should have bio Present");

        Ok(())
    }

    /// End-to-end: a `#[derive(Concept)]` struct with an `Option<T>`
    /// field round-trips through the full query pipeline. Alice has
    /// both `name` and `nickname`; Bob has only `name`. The macro
    /// emits `Term<Option<String>>` for the `nickname` field; at
    /// realize time, Alice's nickname appears as `Some(_)` and Bob's
    /// as `None`.
    #[dialog_common::test]
    async fn it_executes_macro_concept_with_optional_field() -> anyhow::Result<()> {
        mod employee {
            use crate::Attribute;

            /// Employee given name
            #[derive(Attribute, Clone, PartialEq)]
            #[domain("person")]
            pub struct Name(pub String);

            /// Employee preferred nickname
            #[derive(Attribute, Clone, PartialEq)]
            #[domain("person")]
            pub struct Nickname(pub String);
        }

        /// Employee with required name and optional nickname.
        #[derive(crate::Concept, Debug, Clone)]
        pub struct Employee {
            /// Employee entity
            pub this: Entity,
            /// Required given name
            pub name: employee::Name,
            /// Optional nickname
            pub nickname: Option<employee::Nickname>,
        }

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(
                the!("person/nickname")
                    .of(alice.clone())
                    .is("Ali".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let query = Query::<Employee>::default();
        let employees: Vec<Employee> = query.perform(&source).try_collect().await?;

        assert_eq!(employees.len(), 2);

        let mut found_alice = false;
        let mut found_bob = false;
        for emp in employees {
            match emp.name.0.as_str() {
                "Alice" => {
                    assert_eq!(
                        emp.nickname.as_ref().map(|n| n.0.as_str()),
                        Some("Ali"),
                        "Alice should have nickname Some(Ali)"
                    );
                    found_alice = true;
                }
                "Bob" => {
                    assert!(emp.nickname.is_none(), "Bob should have nickname None");
                    found_bob = true;
                }
                other => panic!("Unexpected name: {other}"),
            }
        }
        assert!(found_alice && found_bob);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_executes_query_with_bound_entity() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/age").of(alice.clone()).is(25u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        // Create a person concept
        let concept = ConceptDescriptor::try_from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("person"));
        terms.insert("name".to_string(), Term::var("name"));
        terms.insert("age".to_string(), Term::var("age"));

        let application = ConceptQuery {
            terms,
            predicate: concept,
        };

        // Create evaluation context with bound entity in the match
        let mut input = Match::new();
        let person_param = Term::var("person");
        input.bind(&person_param, Value::from(alice))?;

        // Execute with bound entity via match
        application
            .evaluate(input.seed(), &source)
            .try_vec()
            .await?;

        Ok(())
    }

    #[dialog_common::test]
    fn it_operates_on_concept_conclusion() {
        let concept = ConceptDescriptor::try_from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
        .unwrap();

        // Test that attributes are present
        let param_names: Vec<&str> = concept.with().keys().collect();
        assert!(param_names.contains(&"name"));
        assert!(param_names.contains(&"age"));
        assert!(!param_names.contains(&"height"));
        // "this" parameter is implied but not in attributes
    }

    #[dialog_common::test]
    fn it_creates_concept_descriptor() {
        let concept = ConceptDescriptor::try_from(vec![(
            "name".to_string(),
            AttributeDescriptor::new(
                the!("person/name"),
                "Person name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        // Operator is now a computed URI
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );
        assert_eq!(concept.with().iter().count(), 1);
        assert!(concept.with().keys().any(|k| k == "name"));
    }

    #[dialog_common::test]
    fn it_analyzes_concept_application() {
        let concept = ConceptDescriptor::try_from(vec![
            (
                "name".to_string(),
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age".to_string(),
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("name".to_string(), Term::var("person_name"));
        terms.insert("age".to_string(), Term::var("person_age"));

        let concept_app = ConceptQuery {
            terms,
            predicate: concept,
        };

        let cost = concept_app.estimate(&Environment::new());
        assert_eq!(cost, Some(2200));

        let schema = concept_app.schema();

        assert_eq!(schema.iter().count(), 3);
        assert!(schema.get("this").is_some());
        assert!(schema.get("name").is_some());
        assert!(schema.get("age").is_some());
    }

    #[dialog_common::test]
    fn it_extracts_deductive_rule_parameters() {
        let predicate = ConceptDescriptor::try_from([
            (
                "name".to_string(),
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age".to_string(),
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
        .unwrap();
        let rule = DeductiveRule::from(&predicate);

        let params: HashSet<String> = rule.parameters().collect();
        assert!(params.contains("this"));
        assert!(params.contains("name"));
        assert!(params.contains("age"));
        assert_eq!(params.len(), 3);
    }

    #[dialog_common::test]
    fn it_constructs_premises() {
        let relation = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::constant("Alice".to_string()),
            Term::blank(),
            None,
        );

        let premise = Premise::from(relation);

        match premise {
            Premise::Assert(Proposition::Attribute(_)) => {
                // Expected case - AttributeQuery produces Attribute premise
            }
            _ => panic!("Expected Attribute application"),
        }
    }

    #[dialog_common::test]
    fn it_produces_expected_error_types() {
        // Test AnalyzerError creation
        let predicate = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(the!("test/name"), "", Cardinality::One, Some(Type::String)),
        )])
        .unwrap();
        let rule = DeductiveRule::from(&predicate);

        let analyzer_error = AnalyzerError::UnusedParameter {
            rule: Box::new(rule.clone().into()),
            parameter: "test_param".to_string(),
        };

        // Test conversion to TypeError
        let type_error: TypeError = analyzer_error.into();
        match &type_error {
            TypeError::UnusedParameter { rule: r, parameter } => {
                // Operator is now a computed URI
                assert!(
                    r.conclusion().this().to_string().starts_with("concept:"),
                    "Operator should be a concept URI"
                );
                assert_eq!(parameter, "test_param");
            }
            _ => panic!("Expected UnusedParameter variant"),
        }
    }

    #[dialog_common::test]
    fn it_handles_application_variants() {
        // Test Attribute application
        let relation = AttributeQuery::new(
            Term::from(the!("test/attr")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );
        let app = Proposition::Attribute(Box::new(relation));

        match app {
            Proposition::Attribute(_) => {
                // Expected
            }
            _ => panic!("Expected Attribute variant"),
        }

        // Test other variants exist
        let mut terms = Parameters::new();
        terms.insert("test".to_string(), Term::var("test_var"));
        let concept_app = Proposition::Concept(ConceptQuery {
            terms,
            predicate: ConceptDescriptor::try_from([(
                "name",
                AttributeDescriptor::new(
                    the!("test/name"),
                    "Test name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            )])
            .unwrap(),
        });

        match concept_app {
            Proposition::Concept(_) => {
                // Expected
            }
            _ => panic!("Expected Realize variant"),
        }
    }

    #[dialog_common::test]
    fn it_constructs_negation() {
        let relation = AttributeQuery::new(
            Term::from(the!("test/attr")),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );
        let app = Proposition::Attribute(Box::new(relation));
        let negation = Negation(app);

        // Test that negation wraps the application
        match negation {
            Negation(Proposition::Attribute(_)) => {
                // Expected
            }
            _ => panic!("Expected wrapped Attribute application"),
        }
    }

    #[dialog_common::test]
    async fn it_respects_constant_entity_parameter() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let concept = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "Person name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        // Query with constant entity - should only return Alice
        let mut terms = Parameters::new();
        terms.insert(
            "this".to_string(),
            Term::Constant(Value::Entity(alice.clone())),
        );
        terms.insert("name".to_string(), Term::var("name"));

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let app = ConceptQuery {
            terms,
            predicate: concept,
        };
        let selection =
            TryStreamExt::try_collect::<Vec<_>>(app.evaluate(Match::new().seed(), &source)).await?;

        assert_eq!(
            selection.len(),
            1,
            "Should find only Alice, not both people"
        );
        assert_eq!(
            selection[0].lookup(&Term::var("name"))?.content()?,
            Value::String("Alice".to_string())
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_respects_constant_attribute_parameter() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/age").of(alice.clone()).is(25u32))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .assert(the!("person/age").of(bob.clone()).is(30u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let concept = ConceptDescriptor::try_from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
        .unwrap();

        // Query with constant name value - should only return Bob
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert("name".to_string(), Term::constant("Bob".to_string()));
        terms.insert("age".to_string(), Term::var("age"));

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let app = ConceptQuery {
            terms,
            predicate: concept,
        };
        let selection =
            TryStreamExt::try_collect::<Vec<_>>(app.evaluate(Match::new().seed(), &source)).await?;

        assert_eq!(selection.len(), 1, "Should find only Bob");
        assert_eq!(
            selection[0].lookup(&Term::var("entity"))?.content()?,
            Value::Entity(bob.clone())
        );
        assert_eq!(
            selection[0].lookup(&Term::var("age"))?.content()?,
            Value::UnsignedInt(30)
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_respects_multiple_constant_parameters() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/age").of(alice.clone()).is(25u32))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .assert(the!("person/age").of(bob.clone()).is(30u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let concept = ConceptDescriptor::try_from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
        .unwrap();

        // Query with both name and age constants - should only match Alice
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("entity"));
        terms.insert("name".to_string(), Term::constant("Alice".to_string()));
        terms.insert("age".to_string(), Term::constant(25u32));

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let app = ConceptQuery {
            terms,
            predicate: concept,
        };
        let selection =
            TryStreamExt::try_collect::<Vec<_>>(app.evaluate(Match::new().seed(), &source)).await?;

        assert_eq!(
            selection.len(),
            1,
            "Should find only Alice with exact name and age match"
        );
        assert_eq!(
            selection[0].lookup(&Term::var("entity"))?.content()?,
            Value::Entity(alice.clone())
        );

        Ok(())
    }

    /// Build a representative two-attribute Person concept query with mixed
    /// variable / constant bindings for the serde round-trip tests.
    fn sample_concept_query() -> ConceptQuery {
        let predicate = ConceptDescriptor::try_from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::<Any>::var("entity"));
        terms.insert("name".into(), Term::Constant(Value::String("Alice".into())));
        terms.insert("age".into(), Term::<Any>::var("age"));

        ConceptQuery { predicate, terms }
    }

    #[dialog_common::test]
    fn it_serializes_concept_query_in_formal_notation_shape() {
        let cq = sample_concept_query();
        let value: serde_json::Value = serde_json::to_value(&cq).expect("serialize");

        let obj = value.as_object().expect("object");
        assert_eq!(
            obj.keys().collect::<BTreeSet<_>>(),
            ["assert".to_string(), "where".to_string()]
                .iter()
                .collect::<BTreeSet<_>>(),
            "ConceptQuery must serialize as {{assert, where}}"
        );

        assert!(
            value["assert"].is_object(),
            "`assert` must hold the concept descriptor"
        );
        assert!(
            value["where"].is_object(),
            "`where` must hold the parameter map"
        );
    }

    #[dialog_common::test]
    fn it_round_trips_concept_query_through_json() {
        let cq = sample_concept_query();

        let json = serde_json::to_string(&cq).expect("serialize");
        let restored: ConceptQuery = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(
            restored.predicate.this(),
            cq.predicate.this(),
            "predicate content hash must survive round-trip"
        );

        let original_keys: BTreeSet<&String> = cq.terms.keys().collect();
        let restored_keys: BTreeSet<&String> = restored.terms.keys().collect();
        assert_eq!(
            original_keys, restored_keys,
            "parameter names must survive round-trip"
        );

        for (name, original_term) in cq.terms.iter() {
            assert_eq!(
                restored.terms.get(name),
                Some(original_term),
                "binding for `{name}` must survive round-trip"
            );
        }
    }

    #[dialog_common::test]
    fn it_matches_proposition_concept_serialization() {
        let cq = sample_concept_query();
        let prop: Proposition = cq.clone().into();

        let cq_json = serde_json::to_value(&cq).expect("serialize");
        let prop_json = serde_json::to_value(&prop).expect("serialize");

        assert_eq!(
            cq_json, prop_json,
            "ConceptQuery and Proposition::Concept must produce identical JSON"
        );
    }

    #[dialog_common::test]
    fn it_validates_concept_query_deserialization() {
        let only_assert = serde_json::json!({ "assert": { "with": {
            "name": { "the": "person/name", "as": "Text" }
        }}});
        assert!(
            serde_json::from_value::<ConceptQuery>(only_assert).is_err(),
            "missing `where` must fail to deserialize"
        );

        let only_where = serde_json::json!({ "where": {} });
        assert!(
            serde_json::from_value::<ConceptQuery>(only_where).is_err(),
            "missing `assert` must fail to deserialize"
        );

        // Unknown fields are ignored, consistent with other formal-notation
        // types in this crate. This keeps wire-format readers tolerant of
        // forward-compatible additions.
        let extra_field = serde_json::json!({
            "assert": { "with": { "name": { "the": "person/name", "as": "Text" } } },
            "where": {},
            "stranger": true,
        });
        assert!(
            serde_json::from_value::<ConceptQuery>(extra_field).is_ok(),
            "unknown fields must be ignored on deserialize"
        );
    }

    /// Reducing-rule evaluation: the `reduce` clause end to end
    /// through `ConceptQuery::evaluate` (milestone A3,
    /// `notes/aggregation.md`).
    mod reducing {
        use super::*;
        use crate::reduce::{Aggregator, ReduceSpec};
        use crate::rule::DeductiveRuleDescriptor;
        use std::collections::BTreeMap;

        fn compile(json: serde_json::Value) -> DeductiveRule {
            let descriptor: DeductiveRuleDescriptor =
                serde_json::from_value(json).expect("descriptor parses");
            descriptor.compile().expect("rule compiles")
        }

        /// `DeptTotal { total: sum(?salary) }` grouped by the
        /// department entity: the body reads the anonymous employee
        /// concept, binding the department as `?this`.
        fn dept_total_rule() -> DeductiveRule {
            compile(serde_json::json!({
                "deduce": { "with": {
                    "total": { "the": "org.dept/total", "as": "UnsignedInteger" }
                }},
                "when": [{
                    "assert": { "with": {
                        "dept": { "the": "org.employee/dept", "as": "Entity" },
                        "salary": { "the": "org.employee/salary", "as": "UnsignedInteger" }
                    }},
                    "where": {
                        "this": { "?": { "name": "employee" } },
                        "dept": { "?": { "name": "this" } },
                        "salary": { "?": { "name": "salary" } }
                    }
                }],
                "reduce": {
                    "total": { "apply": "sum", "of": { "?": { "name": "salary" } } }
                }
            }))
        }

        #[dialog_common::test]
        async fn it_evaluates_grouped_sum() -> anyhow::Result<()> {
            let (operator, profile) = test_session_with_peer().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let dept_a: Entity = "id:dept-a".parse()?;
            let dept_b: Entity = "id:dept-b".parse()?;
            let alice: Entity = "id:alice".parse()?;
            let bob: Entity = "id:bob".parse()?;
            let carol: Entity = "id:carol".parse()?;

            branch
                .transaction()
                .assert(
                    the!("org.employee/dept")
                        .of(alice.clone())
                        .is(dept_a.clone()),
                )
                .assert(the!("org.employee/salary").of(alice.clone()).is(100u32))
                .assert(the!("org.employee/dept").of(bob.clone()).is(dept_a.clone()))
                .assert(the!("org.employee/salary").of(bob.clone()).is(50u32))
                .assert(
                    the!("org.employee/dept")
                        .of(carol.clone())
                        .is(dept_b.clone()),
                )
                .assert(the!("org.employee/salary").of(carol.clone()).is(70u32))
                .commit()
                .publish()
                .perform(&operator)
                .await?;

            let rule = dept_total_rule();
            let conclusion = rule.conclusion().clone();
            let mut registry = RuleRegistry::new();
            registry.register(rule)?;
            let source = TestEnv::new(&branch, &operator, registry);

            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("dept"));
            terms.insert("total".into(), Term::var("total"));
            let rows = ConceptQuery {
                terms,
                predicate: conclusion,
            }
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;

            let mut totals: Vec<(String, Value)> = rows
                .iter()
                .map(|row| {
                    Ok((
                        Entity::try_from(row.lookup(&Term::var("dept"))?.content()?)?.to_string(),
                        row.lookup(&Term::var("total"))?.content()?,
                    ))
                })
                .collect::<Result<_, EvaluationError>>()?;
            totals.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(
                totals,
                vec![
                    ("id:dept-a".to_string(), Value::UnsignedInt(150)),
                    ("id:dept-b".to_string(), Value::UnsignedInt(70)),
                ],
                "one folded row per department"
            );
            Ok(())
        }

        /// A caller arriving with a grouping field bound joins into
        /// the folded output; the fold itself still ran over the full
        /// relation, so the dept-bound total equals the unrestricted
        /// query's row for that dept.
        #[dialog_common::test]
        async fn it_folds_the_full_group_under_caller_binding() -> anyhow::Result<()> {
            let (operator, profile) = test_session_with_peer().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let dept_a: Entity = "id:dept-a".parse()?;
            let alice: Entity = "id:alice".parse()?;
            let bob: Entity = "id:bob".parse()?;

            branch
                .transaction()
                .assert(
                    the!("org.employee/dept")
                        .of(alice.clone())
                        .is(dept_a.clone()),
                )
                .assert(the!("org.employee/salary").of(alice.clone()).is(100u32))
                .assert(the!("org.employee/dept").of(bob.clone()).is(dept_a.clone()))
                .assert(the!("org.employee/salary").of(bob.clone()).is(50u32))
                .commit()
                .publish()
                .perform(&operator)
                .await?;

            let rule = dept_total_rule();
            let conclusion = rule.conclusion().clone();
            let mut registry = RuleRegistry::new();
            registry.register(rule)?;
            let source = TestEnv::new(&branch, &operator, registry);

            // Unrestricted: one group, total 150.
            let mut open_terms = Parameters::new();
            open_terms.insert("this".into(), Term::var("dept"));
            open_terms.insert("total".into(), Term::var("total"));
            let open = ConceptQuery {
                terms: open_terms,
                predicate: conclusion.clone(),
            }
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;
            assert_eq!(open.len(), 1);
            let unrestricted = open[0].lookup(&Term::var("total"))?.content()?;

            // Dept bound as a constant: the same total, not a
            // caller-sliced fold.
            let mut bound_terms = Parameters::new();
            bound_terms.insert("this".into(), Term::Constant(Value::Entity(dept_a.clone())));
            bound_terms.insert("total".into(), Term::var("total"));
            let bound = ConceptQuery {
                terms: bound_terms,
                predicate: conclusion.clone(),
            }
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;
            assert_eq!(bound.len(), 1, "the bound dept still yields its group row");
            assert_eq!(
                bound[0].lookup(&Term::var("total"))?.content()?,
                unrestricted,
                "aggregation is over the relation, not the caller-restricted slice"
            );

            // A *reduced* field bound by the caller filters the
            // folded output (join semantics), never the body.
            let mut filter_terms = Parameters::new();
            filter_terms.insert("this".into(), Term::var("dept"));
            filter_terms.insert("total".into(), Term::Constant(Value::UnsignedInt(150)));
            let filtered = ConceptQuery {
                terms: filter_terms,
                predicate: conclusion.clone(),
            }
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;
            assert_eq!(
                filtered.len(),
                1,
                "the matching group row survives the join"
            );

            let mut miss_terms = Parameters::new();
            miss_terms.insert("this".into(), Term::var("dept"));
            miss_terms.insert("total".into(), Term::Constant(Value::UnsignedInt(7)));
            let missed = ConceptQuery {
                terms: miss_terms,
                predicate: conclusion,
            }
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;
            assert!(
                missed.is_empty(),
                "a non-matching total filters the group out"
            );
            Ok(())
        }

        /// A fold groups by the entity: a rule concluding a count of
        /// salaries per department counts every employee of the
        /// department, however many salaries it sees. A group keyed
        /// by anything finer than the entity is an entity of its own.
        #[dialog_common::test]
        async fn it_counts_within_the_entity_group() -> anyhow::Result<()> {
            let (operator, profile) = test_session_with_peer().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let dept: Entity = "id:dept-a".parse()?;
            let alice: Entity = "id:alice".parse()?;
            let bob: Entity = "id:bob".parse()?;
            let carol: Entity = "id:carol".parse()?;

            branch
                .transaction()
                .assert(the!("org.employee/dept").of(alice.clone()).is(dept.clone()))
                .assert(the!("org.employee/salary").of(alice.clone()).is(100u32))
                .assert(the!("org.employee/dept").of(bob.clone()).is(dept.clone()))
                .assert(the!("org.employee/salary").of(bob.clone()).is(100u32))
                .assert(the!("org.employee/dept").of(carol.clone()).is(dept.clone()))
                .assert(the!("org.employee/salary").of(carol.clone()).is(200u32))
                .commit()
                .publish()
                .perform(&operator)
                .await?;

            let rule = compile(serde_json::json!({
                "deduce": { "with": {
                    "headcount": { "the": "org.dept/headcount", "as": "UnsignedInteger" }
                }},
                "when": [{
                    "assert": { "with": {
                        "dept": { "the": "org.employee/dept", "as": "Entity" },
                        "salary": { "the": "org.employee/salary", "as": "UnsignedInteger" }
                    }},
                    "where": {
                        "this": { "?": { "name": "employee" } },
                        "dept": { "?": { "name": "this" } },
                        "salary": { "?": { "name": "salary" } }
                    }
                }],
                "reduce": {
                    "headcount": { "apply": "count", "of": { "?": { "name": "salary" } } }
                }
            }));
            let conclusion = rule.conclusion().clone();
            let mut registry = RuleRegistry::new();
            registry.register(rule)?;
            let source = TestEnv::new(&branch, &operator, registry);

            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("dept"));
            terms.insert("headcount".into(), Term::var("n"));
            let rows = ConceptQuery {
                terms,
                predicate: conclusion,
            }
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;

            let counts: Vec<(Value, Value)> = rows
                .iter()
                .map(|row| {
                    Ok((
                        row.lookup(&Term::var("dept"))?.content()?,
                        row.lookup(&Term::var("n"))?.content()?,
                    ))
                })
                .collect::<Result<_, EvaluationError>>()?;
            assert_eq!(
                counts,
                vec![(Value::Entity(dept.clone()), Value::UnsignedInt(3))],
                "the fold counts within the entity's group"
            );
            Ok(())
        }

        /// Optional-input `max`: a group with a present input derives
        /// the maximum; a group whose inputs are all Absent derives
        /// nothing for the reduced attribute, while its `count`, which
        /// has an identity, still derives zero.
        #[dialog_common::test]
        async fn it_binds_absent_for_the_all_absent_group() -> anyhow::Result<()> {
            let (operator, profile) = test_session_with_peer().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let dept_a: Entity = "id:dept-a".parse()?;
            let dept_b: Entity = "id:dept-b".parse()?;
            let alice: Entity = "id:alice".parse()?;
            let bob: Entity = "id:bob".parse()?;

            // Alice (dept a) has a bonus; Bob (dept b) has none.
            branch
                .transaction()
                .assert(
                    the!("org.employee/dept")
                        .of(alice.clone())
                        .is(dept_a.clone()),
                )
                .assert(the!("org.employee/bonus").of(alice.clone()).is(25u32))
                .assert(the!("org.employee/dept").of(bob.clone()).is(dept_b.clone()))
                .commit()
                .publish()
                .perform(&operator)
                .await?;

            let rule = compile(serde_json::json!({
                "deduce": { "with": {
                    "headcount": { "the": "org.dept/headcount", "as": "UnsignedInteger" },
                    "top": {
                        "the": "org.dept/top-bonus",
                        "as": "UnsignedInteger",
                        "optional": true
                    }
                }},
                "when": [{
                    "assert": { "with": {
                        "dept": { "the": "org.employee/dept", "as": "Entity" },
                        "bonus": {
                            "the": "org.employee/bonus",
                            "as": "UnsignedInteger",
                            "optional": true
                        }
                    }},
                    "where": {
                        "this": { "?": { "name": "employee" } },
                        "dept": { "?": { "name": "this" } },
                        "bonus": { "?": { "name": "bonus" } }
                    }
                }],
                "reduce": {
                    "headcount": { "apply": "count", "of": { "?": { "name": "bonus" } } },
                    "top": { "apply": "max", "of": { "?": { "name": "bonus" } } }
                }
            }));
            let conclusion = rule.conclusion().clone();
            let mut registry = RuleRegistry::new();
            registry.register(rule)?;
            let source = TestEnv::new(&branch, &operator, registry);

            let field = |name: &str| {
                conclusion
                    .with()
                    .iter()
                    .find(|(field, _)| *field == name)
                    .map(|(_, field)| field.clone())
                    .expect("the head declares the field")
            };
            let rows_of = |attribute: ConceptDescriptor, value: &'static str| {
                let mut terms = Parameters::new();
                terms.insert("this".into(), Term::var("dept"));
                terms.insert("is".into(), Term::var(value));
                ConceptQuery {
                    terms,
                    predicate: attribute,
                }
            };

            let counts = rows_of(ConceptDescriptor::of_attribute(&field("headcount")), "n")
                .evaluate(Match::new().seed(), &source)
                .try_vec()
                .await?;
            let mut counts: Vec<(Value, Value)> = counts
                .iter()
                .map(|row| {
                    Ok((
                        row.lookup(&Term::var("dept"))?.content()?,
                        row.lookup(&Term::var("n"))?.content()?,
                    ))
                })
                .collect::<Result<_, EvaluationError>>()?;
            counts.sort_by_key(|(dept, _)| format!("{dept:?}"));
            assert_eq!(
                counts,
                vec![
                    (Value::Entity(dept_a.clone()), Value::UnsignedInt(1)),
                    (Value::Entity(dept_b.clone()), Value::UnsignedInt(0)),
                ],
                "count has an identity, so both departments fold"
            );

            let tops = rows_of(ConceptDescriptor::of_attribute(&field("top")), "top")
                .evaluate(Match::new().seed(), &source)
                .try_vec()
                .await?;
            let tops: Vec<(Value, Value)> = tops
                .iter()
                .map(|row| {
                    Ok((
                        row.lookup(&Term::var("dept"))?.content()?,
                        row.lookup(&Term::var("top"))?.content()?,
                    ))
                })
                .collect::<Result<_, EvaluationError>>()?;
            assert_eq!(
                tops,
                vec![(Value::Entity(dept_a.clone()), Value::UnsignedInt(25))],
                "an identity-less max over an all-absent group derives nothing"
            );
            Ok(())
        }

        /// Composition, stratum 1 over stratum 0: a *plain* rule
        /// consumes a reducing rule's concept like any other.
        #[dialog_common::test]
        async fn it_composes_plain_rule_over_reducing_concept() -> anyhow::Result<()> {
            let (operator, profile) = test_session_with_peer().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let dept_a: Entity = "id:dept-a".parse()?;
            let alice: Entity = "id:alice".parse()?;
            let bob: Entity = "id:bob".parse()?;

            branch
                .transaction()
                .assert(
                    the!("org.employee/dept")
                        .of(alice.clone())
                        .is(dept_a.clone()),
                )
                .assert(the!("org.employee/salary").of(alice.clone()).is(100u32))
                .assert(the!("org.employee/dept").of(bob.clone()).is(dept_a.clone()))
                .assert(the!("org.employee/salary").of(bob.clone()).is(50u32))
                .commit()
                .publish()
                .perform(&operator)
                .await?;

            let inner = dept_total_rule();
            let inner_concept = serde_json::to_value(inner.conclusion())?;
            let outer = compile(serde_json::json!({
                "deduce": { "with": {
                    "grand": { "the": "org.report/grand", "as": "UnsignedInteger" }
                }},
                "when": [{
                    "assert": inner_concept,
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "total": { "?": { "name": "grand" } }
                    }
                }]
            }));
            let report = outer.conclusion().clone();
            let mut registry = RuleRegistry::new();
            registry.register(inner)?;
            registry.register(outer)?;
            let source = TestEnv::new(&branch, &operator, registry);

            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("dept"));
            terms.insert("grand".into(), Term::var("grand"));
            let rows = ConceptQuery {
                terms,
                predicate: report,
            }
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].lookup(&Term::var("grand"))?.content()?,
                Value::UnsignedInt(150),
                "the plain rule reads the folded row"
            );
            Ok(())
        }

        /// Composition, reducing over reducing (two strata): org
        /// totals fold the department totals, which fold the
        /// employees.
        #[dialog_common::test]
        async fn it_composes_reducing_rule_over_reducing_concept() -> anyhow::Result<()> {
            let (operator, profile) = test_session_with_peer().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            let org_x: Entity = "id:org-x".parse()?;
            let org_y: Entity = "id:org-y".parse()?;
            let dept_a: Entity = "id:dept-a".parse()?;
            let dept_b: Entity = "id:dept-b".parse()?;
            let dept_c: Entity = "id:dept-c".parse()?;
            let alice: Entity = "id:alice".parse()?;
            let bob: Entity = "id:bob".parse()?;
            let carol: Entity = "id:carol".parse()?;

            branch
                .transaction()
                .assert(the!("org.dept/org").of(dept_a.clone()).is(org_x.clone()))
                .assert(the!("org.dept/org").of(dept_b.clone()).is(org_x.clone()))
                .assert(the!("org.dept/org").of(dept_c.clone()).is(org_y.clone()))
                .assert(
                    the!("org.employee/dept")
                        .of(alice.clone())
                        .is(dept_a.clone()),
                )
                .assert(the!("org.employee/salary").of(alice.clone()).is(10u32))
                .assert(the!("org.employee/dept").of(bob.clone()).is(dept_b.clone()))
                .assert(the!("org.employee/salary").of(bob.clone()).is(20u32))
                .assert(
                    the!("org.employee/dept")
                        .of(carol.clone())
                        .is(dept_c.clone()),
                )
                .assert(the!("org.employee/salary").of(carol.clone()).is(7u32))
                .commit()
                .publish()
                .perform(&operator)
                .await?;

            let inner = dept_total_rule();
            let inner_concept = serde_json::to_value(inner.conclusion())?;
            let outer = compile(serde_json::json!({
                "deduce": { "with": {
                    "grand": { "the": "org.report/grand", "as": "UnsignedInteger" }
                }},
                "when": [
                    {
                        "assert": inner_concept,
                        "where": {
                            "this": { "?": { "name": "dept" } },
                            "total": { "?": { "name": "t" } }
                        }
                    },
                    {
                        "assert": { "with": {
                            "org": { "the": "org.dept/org", "as": "Entity" }
                        }},
                        "where": {
                            "this": { "?": { "name": "dept" } },
                            "org": { "?": { "name": "this" } }
                        }
                    }
                ],
                "reduce": {
                    "grand": { "apply": "sum", "of": { "?": { "name": "t" } } }
                }
            }));
            let org_total = outer.conclusion().clone();
            let mut registry = RuleRegistry::new();
            registry.register(inner)?;
            registry.register(outer)?;
            let source = TestEnv::new(&branch, &operator, registry);

            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("org"));
            terms.insert("grand".into(), Term::var("grand"));
            let rows = ConceptQuery {
                terms,
                predicate: org_total,
            }
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;

            let mut grands: Vec<(String, Value)> = rows
                .iter()
                .map(|row| {
                    Ok((
                        Entity::try_from(row.lookup(&Term::var("org"))?.content()?)?.to_string(),
                        row.lookup(&Term::var("grand"))?.content()?,
                    ))
                })
                .collect::<Result<_, EvaluationError>>()?;
            grands.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(
                grands,
                vec![
                    ("id:org-x".to_string(), Value::UnsignedInt(30)),
                    ("id:org-y".to_string(), Value::UnsignedInt(7)),
                ],
                "two strata of folds compose"
            );
            Ok(())
        }

        /// Rule-level determinism: the same base facts inserted in
        /// different orders yield identical folded rows.
        #[dialog_common::test]
        async fn it_is_deterministic_across_insertion_orders() -> anyhow::Result<()> {
            let dept_a: Entity = "id:dept-a".parse()?;
            let dept_b: Entity = "id:dept-b".parse()?;
            let people: Vec<(Entity, Entity, u32)> = vec![
                ("id:alice".parse()?, dept_a.clone(), 100),
                ("id:bob".parse()?, dept_a.clone(), 50),
                ("id:carol".parse()?, dept_b.clone(), 70),
                ("id:dave".parse()?, dept_b.clone(), 5),
            ];

            let mut observed = Vec::new();
            for order in [
                people.clone(),
                people.iter().rev().cloned().collect::<Vec<_>>(),
            ] {
                let (operator, profile) = test_session_with_peer().await;
                let repo = test_repo(&operator, &profile).await;
                let branch = repo.branch("main").open().perform(&operator).await?;
                let mut tx = branch.transaction();
                for (person, dept, salary) in &order {
                    tx = tx
                        .assert(
                            the!("org.employee/dept")
                                .of(person.clone())
                                .is(dept.clone()),
                        )
                        .assert(the!("org.employee/salary").of(person.clone()).is(*salary));
                }
                tx.commit().publish().perform(&operator).await?;

                let rule = dept_total_rule();
                let conclusion = rule.conclusion().clone();
                let mut registry = RuleRegistry::new();
                registry.register(rule)?;
                let source = TestEnv::new(&branch, &operator, registry);

                let mut terms = Parameters::new();
                terms.insert("this".into(), Term::var("dept"));
                terms.insert("total".into(), Term::var("total"));
                let rows = ConceptQuery {
                    terms,
                    predicate: conclusion,
                }
                .evaluate(Match::new().seed(), &source)
                .try_vec()
                .await?;
                let mut totals: Vec<(String, Value)> = rows
                    .iter()
                    .map(|row| {
                        Ok((
                            Entity::try_from(row.lookup(&Term::var("dept"))?.content()?)?
                                .to_string(),
                            row.lookup(&Term::var("total"))?.content()?,
                        ))
                    })
                    .collect::<Result<_, EvaluationError>>()?;
                totals.sort_by(|a, b| a.0.cmp(&b.0));
                observed.push(totals);
            }
            assert_eq!(
                observed[0], observed[1],
                "folded rows are independent of base-fact insertion order"
            );
            assert_eq!(observed[0].len(), 2);
            Ok(())
        }

        /// A reducing rule whose body reads its own conclusion is an
        /// aggregating edge inside its own strongly connected
        /// component: rejected at acquire with the structured
        /// stratification error, never silently mis-evaluated.
        #[dialog_common::test]
        async fn it_rejects_recursive_reducing_rule() -> anyhow::Result<()> {
            let conclusion = ConceptDescriptor::try_from(vec![(
                "total",
                AttributeDescriptor::new(
                    the!("org.dept/total"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            )])
            .unwrap();

            // Body reads the rule's own conclusion concept.
            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("this"));
            terms.insert("total".into(), Term::<Any>::var("s"));
            let premises = vec![Premise::Assert(Proposition::Concept(ConceptQuery {
                terms,
                predicate: conclusion.clone(),
            }))];
            let mut reduce = BTreeMap::new();
            reduce.insert(
                "total".to_string(),
                ReduceSpec {
                    apply: Aggregator::Sum,
                    of: Term::var("s"),
                },
            );
            let rule = DeductiveRule::with_reduce(conclusion.clone(), premises, reduce)
                .expect("locally the rule compiles; the cycle is a program property");

            let mut registry = RuleRegistry::new();
            registry.register(rule)?;
            match registry.acquire(&conclusion) {
                Err(EvaluationError::AggregationThroughRecursion {
                    concept,
                    aggregated,
                }) => {
                    assert_eq!(concept, conclusion.this().to_string());
                    assert_eq!(
                        aggregated,
                        conclusion.this().to_string(),
                        "the self-referential body is the aggregated concept"
                    );
                }
                other => panic!("expected AggregationThroughRecursion, got {other:?}"),
            }
            Ok(())
        }
    }
}
