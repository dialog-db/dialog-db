/// Adornment types for parameter binding pattern caching.
pub mod adornment;
/// Affected-entity discovery for incremental maintenance.
pub mod affected;
/// Semi-naive fixpoint evaluation for recursive concepts.
pub mod fixpoint;
#[cfg(test)]
mod invariants;
/// Shared, branch-owned plan cache keyed by (rule identity, adornment).
mod plan_cache;
/// Per-concept rule management with adornment-keyed plan caching.
pub mod rules;

pub use plan_cache::{PLAN_CACHE_CAPACITY, PlanCache};
pub use rules::{ConceptRules, Exact, Installed};

use std::fmt;

use crate::attribute::The;
use crate::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use crate::corpus::{CorpusRelation, Type, Value};
use crate::planner::{Disjunction, Plan};
use crate::rule::deductive::DeductiveRule;
use crate::schema::CONCEPT_OVERHEAD;
use crate::selection::{Selection, Standing};
use crate::selective::Ranking;
use crate::source::SelectRules;
use crate::stream::{fork_stream, stream_select};
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::{
    Binding, Cardinality, Environment, EvaluationError, Match, Parameters, Requirement, Schema,
    Term, try_stream,
};
use dialog_capability::Provider;
use dialog_corpus::encode_value_owned;
use futures_util::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Display;
use std::future;
use std::mem;
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
    let mut matched = Match::for_scope_of(source);

    for (param_name, user_param) in terms.iter() {
        match user_param {
            // The slot moves in as it is: a value the caller's row holds in
            // place is not hydrated to enter the scope. Unbound is expected
            // here: the user supplied a placeholder term (e.g.
            // `Term::var("alice")` in `Query<Person> { this: ..., ... }`)
            // that the concept query is about to bind; downstream
            // evaluation fills it in.
            Term::Variable {
                name: Some(name), ..
            } => {
                matched.bind_slot_of(&Term::<Any>::var(param_name.as_str()), source, name)?;
            }
            Term::Constant(value) => {
                matched.bind_variable(param_name, None, value.clone())?;
            }
            Term::Variable { name: None, .. } => {}
        }
    }

    Ok(matched)
}

/// The type the caller's value term reads through, when it is a typed
/// variable. An attribute's type is part of which attribute it is, so
/// to a variable of a type a value of another type is a fact of
/// another attribute: it takes no part in an election and does not
/// make the entity one that has a value. `None` reads every value.
fn lens(terms: &Parameters) -> Option<Kind> {
    match terms.get(ConceptDescriptor::VALUE) {
        Some(term @ Term::Variable { name: Some(_), .. }) => term.kind(),
        _ => None,
    }
}

/// Whether a bind took. A value the term's type does not read, and one
/// that disagrees with what the term is already bound to, make the row
/// one the caller's terms do not match: it is passed over, as a scan
/// into the same term passes over it. Any other failure is an error.
fn took(bound: Result<(), EvaluationError>) -> Result<bool, EvaluationError> {
    match bound {
        Ok(()) => Ok(true),
        Err(EvaluationError::KindMismatch { .. } | EvaluationError::Assignment { .. }) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Merge a Match with parameter names back into a Match with user
/// variable names after evaluation. Both Present and Absent
/// bindings are propagated. `None` when the caller's terms do not
/// match the result (see [`took`]).
fn merge_parameters(
    base: &Match,
    result: &Match,
    terms: &Parameters,
) -> Result<Option<Match>, EvaluationError> {
    let mut merged = base.clone();

    for (param_name, user_param) in terms.iter() {
        if matches!(user_param, Term::Constant(_)) {
            continue;
        }

        // The slot moves over as it is: a binding a scan made in place
        // stays in place in the caller's row.
        match merged.bind_slot_of(user_param, result, param_name) {
            // Unbound is expected: not every parameter survives the
            // concept evaluation (e.g. a blank slot the rule never
            // touched). Skip and let the user variable stay
            // un-extended.
            Ok(None) | Ok(Some(false)) => {}
            Ok(Some(true)) => {
                if let Some(name) = user_param.shared_name()
                    && let Some(standing) = result.standing_of(param_name)
                {
                    merged.cite_variable_standing(name, standing);
                }
            }
            Err(error) => {
                if !took(Err(error))? {
                    return Ok(None);
                }
            }
        }
    }
    merged.adopt_citations(result);

    Ok(Some(merged))
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
/// ([`The::key_operand`]), and the two forms convert on the
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
            keys.insert(The::key_operand(name), name);
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
            // An entry object carries `the`/`is` and no `?`, and is
            // empty when both are blank, which are left out; anything
            // else under a collection field is a bare value term,
            // which binds every entry (`{_: ?value}`).
            let entry = collection
                && value.as_object().is_some_and(|map| {
                    map.is_empty()
                        || (!map.contains_key("?")
                            && (map.contains_key("the") || map.contains_key("is")))
                });
            let bound: Bound = if entry {
                serde_json::from_value(value).map_err(D::Error::custom)?
            } else {
                Bound::Term(serde_json::from_value(value).map_err(D::Error::custom)?)
            };
            match bound {
                Bound::Entry { the, is } => {
                    terms.insert(The::key_operand(&name), the);
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
    /// This application with each constant conformed to the type its
    /// field declares (see [`Value::conform`]), and a constant `this` to
    /// an entity. A constant written bare reads as the first type its
    /// payload fits, so it may name the field's value under another type,
    /// which no claim of the field would match.
    pub fn conformed(mut self) -> Self {
        let mut conformed = Vec::new();
        for (name, term) in self.terms.iter() {
            let Term::Constant(value) = term else {
                continue;
            };
            let kind = if name == "this" {
                Some(Type::Entity)
            } else {
                self.predicate
                    .with()
                    .iter()
                    .find(|(field, _)| field == name)
                    .and_then(|(_, field)| field.content_type())
            };
            if let Some(kind) = kind {
                let fitted = value.clone().conform(kind);
                if &fitted != value {
                    conformed.push((name.clone(), fitted));
                }
            }
        }
        for (name, value) in conformed {
            self.terms.insert(name, Term::Constant(value));
        }
        self
    }

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
    ///
    /// Each constant is first conformed to the type its field declares
    /// ([`conformed`](Self::conformed)): `+41` under a natural field reads
    /// the natural `41`, which is the value a claim of the field holds.
    ///
    /// The one implementation of [`Application::evaluate`] for a concept
    /// query, reached through the trait alone, so every caller (a plan's
    /// concept premise, an embedder's `select`) evaluates the same way.
    ///
    /// [`Application::evaluate`]: crate::query::Application::evaluate
    pub(crate) fn evaluate_application<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        // The caller's stream is erased at every entry (here, in
        // `evaluate_unsorted`, `through` and the conjunction's merge), and
        // the body lives in a function generic over the environment
        // alone. A rule body evaluates its premises through these entries
        // again, and a closure or async block inside a generic function
        // carries that function's arguments in its own type: a body
        // generic over the stream handed the next level a type that held
        // this level's, and the compiler unrolled the recursion forty
        // levels deep (a 50 KB type term) in a workload's binary.
        self.conformed().evaluate_erased(Box::pin(selection), env)
    }

    fn evaluate_erased<'a, Env>(
        self,
        selection: Pin<Box<dyn Selection + 'a>>,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        // The read of a concept of one field that picks is its own boxed
        // stream, entered through a pointer: held inline, its state would
        // be part of every concept read's, picking or not.
        let picking = self.predicate.picked_relation().is_some();
        let asked = picking.then(|| self.clone());
        let app = self.canonical();
        let this = app.terms.get("this").cloned();

        try_stream! {
            let mut selection = selection;
            if let Some(asked) = asked {
                match Box::pin(asked.picked(selection, env)).await? {
                    Picked::Answered(mut rows) => {
                        while let Some(row) = rows.next().await {
                            yield row?;
                        }
                        return;
                    }
                    Picked::Declined(rows) => selection = rows,
                }
            }
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
            // The inner evaluation is boxed. Held inline, its state would
            // be part of this stream's state, which every caller moves when
            // it returns and again when it boxes it: the inner state's size
            // copied per evaluation of a concept, before any row is read.
            let mut unsorted = Box::pin(app.evaluate_unsorted(
                rules,
                Box::pin(stream::iter([Ok(first)]).chain(selection)),
                env,
            ));
            if sort {
                let mut rows: Vec<(Vec<u8>, Match)> = Vec::new();
                while let Some(row) = unsorted.next().await {
                    let row = row?;
                    let key = this
                        .as_ref()
                        .and_then(|term| row.value_of(term.name()?))
                        .map(dialog_corpus::encode_value_owned)
                        .unwrap_or_default();
                    rows.push((key, row));
                }
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                for (_, row) in rows {
                    yield row;
                }
                return;
            }
            while let Some(row) = unsorted.next().await {
                yield row?;
            }
        }
    }

    /// [`evaluate`](Self::evaluate) with the rules already resolved,
    /// yielding rows in evaluation order.
    fn evaluate_unsorted<'a, Env>(
        self,
        rules: ConceptRules,
        selection: Pin<Box<dyn Selection + 'a>>,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: crate::Scope<'a>,
    {
        let app = self;

        try_stream! {
            let mut selection = selection;
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
                // Boxed, as the relation below is. The caller boxes this
                // stream, and a stream has room for the largest thing each
                // of its stops waits on, one beside the other, so without
                // these boxes that allocation would carry the fixpoint's two
                // futures and the relation's stream, which a plain read
                // never runs.
                let table = match rules.continuation() {
                    Some(continuation) => {
                        Box::pin(continuation.rows(&app.predicate, analysis, env)).await?
                    }
                    None => Box::pin(fixpoint::evaluate(&app.predicate, analysis, env)).await?,
                };
                // The component yields its candidates as a set; a read
                // under cardinality one elects one of them at the exit,
                // so a reader outside the component sees one value. Each
                // row stands as the newest fact any of its derivations
                // consumed, ranked past the values of its cell it
                // displaced, so it competes as it would had its relation
                // not been recursive.
                let mut table: Vec<fixpoint::Answer> = table.to_vec();
                if let Some((name, field)) = app.predicate.attribute_field() {
                    let election = Election::of(field);
                    if election.elects() {
                        fixpoint::rank_successions(&mut table, name);
                        table = election.elect_rows(table, name)?;
                    }
                }
                let rows = stream::iter([Ok(first)]).chain(selection);
                for await each in rows {
                    let input = each?;
                    let mut matched = false;
                    for answer in table.iter() {
                        if let Some(mut merged) = fixpoint::join(&input, &app.terms, &answer.row)? {
                            if let Some(standing) = &answer.standing {
                                fixpoint::cite(&mut merged, &app.terms, &answer.row, standing);
                            }
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

            // A reducing rule's fold reads every row of a group, so a
            // caller's bindings must never restrict the rows within one:
            // each reducing rule's folded rows are computed once and the
            // caller's bindings join against the output (the fixpoint-table
            // shape). A constant the query names for a grouping field
            // restricts which groups are folded, never a group's rows, so
            // it seeds the body. The plain rules plan as usual.
            let mut reduced: Vec<fixpoint::Answer> = Vec::new();
            for rule in rules.reducing() {
                reduced.extend(reduce_rows(rule, &app.terms, env).await?);
            }
            // All matches in the selection share the first one's binding
            // pattern (same variables bound), only the values differ.
            // One rule derives the attribute of this attribute concept:
            // while nothing is stored under it, the rule re-headed onto
            // the concept is its whole answer, its rows elected per
            // entity as the relation's would be. A concept of several
            // attributes selects each attribute and joins them, which a
            // body yielding several rows for one entity does not; it is
            // answered this way only when its rule's body yields at most
            // one row per entity, which is then each attribute's one
            // candidate and so what every election picks.
            if let Some(exact) = rules.exact()
                && (app.predicate.attribute_field().is_some() || exact.rule.single_valued())
                && stored_absent(&exact.attributes, env).await?
                && let Some(plan) =
                    rules.plan_exact(&app.seeds(app.election(&rules).is_some()), &first)
            {
                let elect = app.election(&rules);
                let rows = stream::iter([Ok(first)]).chain(selection);
                for await merged in app.through(&plan, rows, env, elect, widen) {
                    yield merged?;
                }
                return;
            }

            // An attribute concept some rule derives: candidates from the
            // stored scan, every head and every fold are elected per
            // entity and one row is built per winner.
            if app.predicate.attribute_field().is_some() && !rules.installed().is_empty() {
                let rows = stream::iter([Ok(first)]).chain(selection);
                for await row in Box::pin(app.relation(rules, reduced, None, rows, env)) {
                    yield row?;
                }
                return;
            }

            let elect = app.election(&rules);
            let plan = rules.plan(&app.seeds(elect.is_some()), &first);
            let rows = stream::iter([Ok(first)]).chain(selection);

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
                        if let Some(merged) = fixpoint::join(&input, &app.terms, &row.row)? {
                            matched = true;
                            yield merged;
                        }
                    }
                    let copy = input.clone();
                    let single = stream::once(async move { Ok(copy) });
                    for await merged in app.through(&plan, single, env, elect.clone(), false) {
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
                        .filter_map(|row| fixpoint::join(&input, &terms, &row.row).transpose())
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
        elect: Option<Election>,
        widen: bool,
    ) -> Pin<Box<dyn Selection + 'a>>
    where
        Env: crate::Scope<'a>,
    {
        // Erased at the entry; see `evaluate`.
        self.through_erased(plan, Box::pin(rows), env, elect, widen)
    }

    fn through_erased<'a, Env>(
        &self,
        plan: &Disjunction,
        rows: Pin<Box<dyn Selection + 'a>>,
        env: &'a Env,
        elect: Option<Election>,
        widen: bool,
    ) -> Pin<Box<dyn Selection + 'a>>
    where
        Env: crate::Scope<'a>,
    {
        let key_operand = The::key_operand(ConceptDescriptor::VALUE);
        let into = self.seeds(elect.is_some());
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
        if elect.is_none() && !widen {
            return Box::pin(results.filter_map(move |result| {
                let merged = result.and_then(|mut result| {
                    let caller = result.take_caller().ok_or_else(|| {
                        EvaluationError::Store(
                            "a concept's result lost the row it was evaluated for".to_string(),
                        )
                    })?;
                    merge_parameters(&caller, &result, &back)
                        .map_err(|e| EvaluationError::Store(e.to_string()))
                });
                future::ready(merged.transpose())
            }));
        }
        // An attribute concept read under its cardinality: what the
        // cardinality
        // keeps of an entity's candidates leaves here, elected from the
        // stored row and every derived candidate alike. The candidates
        // for an entity arrive in no particular order, so the election
        // buffers the results of the whole input.
        let lens = lens(&back);
        Box::pin(try_stream! {
            let mut groups: Groups = BTreeMap::new();
            let mut matched: HashSet<usize> = HashSet::new();
            for await result in results {
                let mut result = result?;
                let caller = result.take_caller().ok_or_else(|| {
                    EvaluationError::Store(
                        "a concept's result lost the row it was evaluated for".to_string(),
                    )
                })?;
                let caller_id = Arc::as_ptr(&caller) as usize;
                // A value the caller's variable does not read is no fact
                // of the attribute it asks for: it is not elected among,
                // and the entity is not one a set-widened read matched.
                if let Some(kind) = &lens
                    && let Some(Binding::Present(value)) = result.get(ConceptDescriptor::VALUE)
                    && !kind.reads(value)
                {
                    continue;
                }
                matched.insert(caller_id);
                if elect.is_none() {
                    let merged = merge_parameters(&caller, &result, &back)
                        .map_err(|e| EvaluationError::Store(e.to_string()))?;
                    if let Some(merged) = merged {
                        yield merged;
                    }
                    continue;
                }
                let this = match result.lookup(&Term::<Any>::var("this")) {
                    Ok(Binding::Present(value)) => value,
                    _ => continue,
                };
                let value = match result.lookup(&Term::<Any>::var(ConceptDescriptor::VALUE)) {
                    Ok(Binding::Present(value)) => value,
                    _ => continue,
                };
                let identity = match result.lookup(&Term::<Any>::var(&key_operand)) {
                    Ok(Binding::Present(key)) => encode_value(&key)?,
                    _ => Vec::new(),
                };
                let key = (caller_id, entity_key(&this)?);
                // A derived value stands as the newest fact its
                // derivation consumed. It is cited on the value, so the
                // caller's variable carries it out with the row: a read
                // further up that picks the newest of these candidates
                // has their standings to pick by.
                let standing = result
                    .standing_of(ConceptDescriptor::VALUE)
                    .or_else(|| result.standing());
                if result.standing_of(ConceptDescriptor::VALUE).is_none()
                    && let Some(standing) = &standing
                {
                    result.cite_variable_standing(
                        &Arc::from(ConceptDescriptor::VALUE),
                        standing.clone(),
                    );
                }
                groups.entry(key).or_default().push(Entry {
                    standing,
                    value,
                    identity,
                    carrier: (result, caller),
                });
            }
            if let Some(election) = &elect {
                for (_, entries) in groups {
                    for (result, caller) in election.resolve(entries)?.0 {
                        if !agrees(&result, &caller, &back, &[ConceptDescriptor::VALUE, &key_operand])? {
                            continue;
                        }
                        let merged = merge_parameters(&caller, &result, &back)
                            .map_err(|e| EvaluationError::Store(e.to_string()))?;
                        if let Some(merged) = merged {
                            yield merged;
                        }
                    }
                }
            }
            if widen {
                let callers = mem::take(&mut *callers.lock().expect("callers lock"));
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
        reduced: Vec<fixpoint::Answer>,
        ranking: Option<Ranking>,
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
            let plan = rules.plan(&app.seeds(true), &first);
            let widen = app.widens();
            let lens = lens(&app.terms);
            let election = match app.predicate.attribute_field() {
                Some((_, field)) => Election::of(field),
                None => Election::plain(Cardinality::One),
            };
            let this_term = app.terms.get("this").cloned();
            let key_operand = The::key_operand(ConceptDescriptor::VALUE);

            // The survivors of an election no input row informed: an input
            // binding none of the query's variables asks the same question
            // as every other such input, so it is answered once.
            let mut unconditional: Option<Vec<Candidate>> = None;

            let rows = stream::once(async { Ok(first) }).chain(rows);
            for await input in rows {
                let input = input?;
                let independent = app
                    .terms
                    .iter()
                    .all(|(_, term)| term.name().is_none() || !input.contains(term));
                if independent && let Some(survivors) = &unconditional {
                    if survivors.is_empty() && widen {
                        yield widened(&input, &app.terms)?;
                        continue;
                    }
                    for candidate in survivors.clone() {
                        if let Some(merged) = merge_candidate(&input, &app.terms, candidate, &key_operand)? {
                            yield merged;
                        }
                    }
                    continue;
                }
                // The entity the caller bound, if any: a constant, or a
                // variable an earlier premise bound.
                let entity: Option<Value> = match &this_term {
                    Some(Term::Constant(value)) => Some(value.clone()),
                    Some(term @ Term::Variable { name: Some(name), .. }) if input.contains(term) => {
                        input.value_of(name).cloned()
                    }
                    _ => None,
                };

                let conjunctions = plan.conjunctions();
                let mut candidates: Vec<Candidate> = Vec::new();
                for conjunction in conjunctions {
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
                                    standing: row
                                        .standing_of(&recall.value)
                                        .or_else(|| row.standing()),
                                    source: Source::Shared(shared.clone(), index),
                                });
                            }
                        }
                        _ => {
                            // Seeded with the caller's entity only: its
                            // value is tested after the election.
                            let seed = app.seeds(true);
                            let mut scoped = extract_parameters(&input, &seed)
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
                                    standing: row
                                        .standing_of(ConceptDescriptor::VALUE)
                                        .or_else(|| row.standing()),
                                    source: Source::Owned(row),
                                });
                            }
                        }
                    }
                }
                for folded in &reduced {
                    let row = &folded.row;
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
                        // A fold that chose one of its inputs stands
                        // as the fact it chose; one that computed a
                        // value cites none, and stands below every
                        // claim.
                        standing: folded.standing.clone(),
                        source: Source::Folded,
                    });
                }
                if let Some(kind) = &lens {
                    candidates.retain(|candidate| kind.reads(&candidate.value));
                }

                // A field's pick keeps one candidate per entity, chosen
                // among all of them; a plain read keeps what the
                // cardinality keeps.
                let survivors = match &ranking {
                    Some(ranking) => pick(candidates, ranking),
                    None => elect(candidates, &election)?,
                };
                if independent {
                    unconditional = Some(survivors.clone());
                }
                if survivors.is_empty() && widen {
                    yield widened(&input, &app.terms)?;
                    continue;
                }
                for candidate in survivors {
                    if let Some(merged) = merge_candidate(&input, &app.terms, candidate, &key_operand)? {
                        yield merged;
                    }
                }
            }
        }
    }

    /// The terms that seed a rule's body, and so the terms its plan is
    /// made for. Under an election the caller's value and key, bound or
    /// constant, are tested against what the election yields and never
    /// seed the body: seeding would elect among the candidates that
    /// happen to equal them, or sum only those. A plan made for the
    /// caller's terms as they are would take the value as bound and
    /// order a premise that reads it ahead of the one that binds it.
    fn seeds(&self, elected: bool) -> Parameters {
        let mut seeds = self.terms.clone();
        if elected {
            seeds.remove(ConceptDescriptor::VALUE);
            seeds.remove(&The::key_operand(ConceptDescriptor::VALUE));
        }
        seeds
    }

    /// Whether this query reads its attribute concept set-widened: the
    /// caller's value term admits `Nothing`, so an entity no row
    /// matched yields one row with the value `Absent` instead of none.
    pub(crate) fn widens(&self) -> bool {
        self.predicate
            .attribute_field()
            .is_some_and(|(name, _)| self.terms.get(name).is_some_and(|term| term.is_optional()))
    }

    /// How this query's rows are elected on their way out, if they
    /// are: the query reads an attribute concept some rule contributes
    /// rows to, so several candidates per entity may arrive and the
    /// cardinality decides what leaves: the newest under one, each
    /// distinct value once under many. A read of a relation nothing
    /// derives is the stored rows as they are, so it elects nothing here.
    fn election(&self, rules: &ConceptRules) -> Option<Election> {
        let (_, field) = self.predicate.attribute_field()?;
        if rules.installed().is_empty() {
            return None;
        }
        Some(Election::of(field))
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

    /// The read of a concept of one field that picks, over a relation
    /// rules derive outside any cycle: the relation's candidates are
    /// gathered as a read of it gathers them, and the pick chooses
    /// among them per entity. The selecting rule that folds the field
    /// answers the same, through a rule and a fold more. Declined, the
    /// rows are handed back for the concept's own rules to answer.
    async fn picked<'a, Env>(
        self,
        selection: Pin<Box<dyn Selection + 'a>>,
        env: &'a Env,
    ) -> Result<Picked<'a>, EvaluationError>
    where
        Env: crate::Scope<'a>,
    {
        let Some((relation, ranking)) = self.picking() else {
            return Ok(Picked::Declined(selection));
        };
        let rules = Provider::<SelectRules>::execute(env, relation.predicate.clone()).await?;
        if rules.installed().is_empty()
            || rules.recursion().is_some()
            || rules.reducing().next().is_some()
        {
            return Ok(Picked::Declined(selection));
        }
        Ok(Picked::Answered(Box::pin(relation.relation(
            rules,
            Vec::new(),
            Some(ranking),
            selection,
            env,
        ))))
    }

    /// When this query reads a concept of one required field that
    /// picks: the read of the field's relation, every claim of it, with
    /// the caller's terms re-keyed onto the relation's operands, and
    /// the ranking the pick chooses by.
    fn picking(&self) -> Option<(ConceptQuery, Ranking)> {
        let mut fields = self.predicate.with().iter();
        let (name, field) = fields.next()?;
        if fields.next().is_some() || field.is_optional() || field.conforms().is_some() {
            return None;
        }
        let ranking = field.ranking()?;
        let mut terms = Parameters::new();
        for (param, term) in self.terms.iter() {
            let param = if param == name {
                ConceptDescriptor::VALUE.to_string()
            } else {
                param.clone()
            };
            terms.insert(param, term.clone());
        }
        Some((
            ConceptQuery {
                predicate: ConceptDescriptor::of_attribute(field),
                terms,
            },
            ranking,
        ))
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
        let key = The::key_operand(name);
        let canonical_key = The::key_operand(ConceptDescriptor::VALUE);
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
/// committed tree alone and an overlay may hold the fact. The answer
/// per attribute is kept on the query's memo, since the facts do not
/// change within a query and a concept is evaluated many times in one.
async fn stored_absent<'a, Env>(
    attributes: &[CorpusRelation],
    env: &'a Env,
) -> Result<bool, EvaluationError>
where
    Env: crate::Scope<'a>,
{
    for attribute in attributes {
        let key = attribute.to_string().into_bytes();
        let absent = match env.memo().and_then(|memo| memo.stored_absent(&key)) {
            Some(absent) => absent,
            None => {
                let selector = dialog_corpus::Selector::new().the(attribute.clone());
                let mut rows = Provider::<dialog_corpus::Select<'_>>::execute(env, selector)
                    .await
                    .map_err(|error| EvaluationError::Store(error.to_string()))?;
                let absent = rows.next().await.is_none();
                if let Some(memo) = env.memo() {
                    memo.remember_stored_absent(key, absent);
                }
                absent
            }
        };
        if !absent {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The row for `candidate` merged into the caller's `input`, citing the
/// facts the candidate came from; `None` when the caller's bindings
/// disagree with it.
fn merge_candidate(
    input: &Match,
    terms: &Parameters,
    candidate: Candidate,
    key_operand: &str,
) -> Result<Option<Match>, EvaluationError> {
    let mut row = fixpoint::Row::new();
    row.insert("this".to_string(), candidate.this);
    row.insert(ConceptDescriptor::VALUE.to_string(), candidate.value);
    if let Some(key) = candidate.key {
        row.insert(key_operand.to_string(), key);
    }
    let Some(mut merged) = fixpoint::join(input, terms, &row)? else {
        return Ok(None);
    };
    match &candidate.source {
        Source::Owned(source) => merged.adopt_citations(source),
        Source::Shared(rows, index) => merged.adopt_citations(&rows[*index]),
        Source::Folded => {}
    }
    // The value stands as the claim it was elected as, stored or derived,
    // so a fold over the caller's variable ranks it as that claim, as it
    // ranks a value a scan bound.
    if let (Some(standing), Some(name)) = (
        candidate.standing,
        terms
            .get(ConceptDescriptor::VALUE)
            .and_then(|term| term.shared_name()),
    ) {
        merged.cite_variable_standing(name, standing);
    }
    Ok(Some(merged))
}

/// What became of a read of a concept of one field that picks.
enum Picked<'a> {
    /// The relation's candidates answer it: the rows.
    Answered(Pin<Box<dyn Selection + 'a>>),
    /// The concept's own rules answer it: the input, as it came.
    Declined(Pin<Box<dyn Selection + 'a>>),
}

/// One value a source offers for an attribute of an entity.
#[derive(Clone)]
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
#[derive(Clone)]
enum Source {
    /// A row the stored scan or an attribute-headed rule built in scope.
    Owned(Match),
    /// A row of a source rule's remembered body, by index.
    Shared(Arc<Vec<Match>>, usize),
    /// A fold's row, which cites nothing.
    Folded,
}

/// How an attribute concept read elects: under the field's
/// cardinality, the newest candidate of each entity under one, every
/// distinct value under many. The newest is decided by the order every
/// cardinality-one read and write shares ([`Cardinality::newer`]).
#[derive(Clone, Debug)]
pub struct Election {
    cardinality: Cardinality,
}

/// The candidates of `through`, grouped by caller and entity: each
/// group's entries carry the result and the caller it answers.
type Groups = BTreeMap<(usize, Vec<u8>), Vec<Entry<(Match, Arc<Match>)>>>;

/// One candidate under election: its standing and value, the key that
/// makes it a distinct fact when the relation is a keyed collection,
/// and whatever the caller keeps of it.
struct Entry<T> {
    standing: Option<Standing>,
    value: Value,
    identity: Vec<u8>,
    carrier: T,
}

/// What an election leaves of one entity's candidates: one under
/// cardinality one, every distinct value under many.
struct Resolved<T>(Vec<T>);

impl Election {
    /// The election a concept field declares.
    pub(crate) fn of(field: &ConceptFieldDescriptor) -> Self {
        Election::plain(field.cardinality())
    }

    /// An election under `cardinality`.
    fn plain(cardinality: Cardinality) -> Self {
        Election { cardinality }
    }

    /// Whether the election keeps one candidate of each entity.
    fn elects(&self) -> bool {
        self.cardinality == Cardinality::One
    }

    /// Resolve one entity's candidates. Under many every distinct value
    /// is kept, a keyed collection every distinct (value, key) pair;
    /// under one the newest.
    fn resolve<T>(&self, entries: Vec<Entry<T>>) -> Result<Resolved<T>, EvaluationError> {
        if !self.elects() {
            let mut seen: HashSet<Vec<u8>> = HashSet::new();
            let mut chosen = Vec::new();
            for entry in entries {
                let distinct = [encode_value(&entry.value)?, entry.identity].concat();
                if seen.insert(distinct) {
                    chosen.push(entry.carrier);
                }
            }
            return Ok(Resolved(chosen));
        }
        let mut best: Option<Entry<T>> = None;
        for entry in entries {
            best = Some(match best {
                Some(incumbent)
                    if !Cardinality::newer(
                        (&entry.value, entry.standing.as_ref()),
                        (&incumbent.value, incumbent.standing.as_ref()),
                    ) =>
                {
                    incumbent
                }
                _ => entry,
            });
        }
        Ok(Resolved(
            best.into_iter().map(|entry| entry.carrier).collect(),
        ))
    }
}

/// Whether `result` agrees with what the caller bound or fixed for
/// `operands`: a constant term must equal the result's value, a
/// variable the caller bound must too, and an unbound one agrees with
/// anything.
fn agrees(
    result: &Match,
    caller: &Match,
    terms: &Parameters,
    operands: &[&str],
) -> Result<bool, EvaluationError> {
    for name in operands {
        let Some(term) = terms.get(name) else {
            continue;
        };
        let Some(Binding::Present(actual)) = result.get(name) else {
            continue;
        };
        let expected: Option<Value> = match term {
            Term::Constant(value) => Some(value.clone()),
            Term::Variable {
                name: Some(variable),
                ..
            } => match caller.get(variable) {
                Some(Binding::Present(value)) => Some(value.clone()),
                _ => None,
            },
            _ => None,
        };
        if expected.is_some_and(|expected| expected != *actual) {
            return Ok(false);
        }
    }
    Ok(true)
}

impl Election {
    /// The rows a recursive component yields, elected per entity under
    /// this cardinality at the component's exit. The rows are keyed by the
    /// query's field name, `field`, as the fixpoint projects them.
    fn elect_rows(
        &self,
        rows: Vec<fixpoint::Answer>,
        field: &str,
    ) -> Result<Vec<fixpoint::Answer>, EvaluationError> {
        let key_operand = The::key_operand(field);
        let mut order: Vec<Vec<u8>> = Vec::new();
        let mut groups: HashMap<Vec<u8>, Vec<Entry<fixpoint::Answer>>> = HashMap::new();
        for answer in rows {
            let row = &answer.row;
            let (Some(this), Some(value)) = (row.get("this"), row.get(field)) else {
                continue;
            };
            let key = entity_key(this)?;
            if !groups.contains_key(&key) {
                order.push(key.clone());
            }
            let value = value.clone();
            let identity = match row.get(&key_operand) {
                Some(key) => encode_value(key)?,
                None => Vec::new(),
            };
            let entry = Entry {
                standing: answer.standing.clone(),
                value,
                identity,
                carrier: answer,
            };
            groups.entry(key).or_default().push(entry);
        }
        let mut elected = Vec::new();
        for key in order {
            let entries = groups.remove(&key).unwrap_or_default();
            elected.extend(self.resolve(entries)?.0);
        }
        Ok(elected)
    }
}

/// The candidate each entity's pick chooses among all of its
/// candidates, in the order the entities were first seen.
fn pick(candidates: Vec<Candidate>, ranking: &Ranking) -> Vec<Candidate> {
    let mut picked: Vec<Candidate> = Vec::new();
    let mut at: HashMap<Vec<u8>, usize> = HashMap::new();
    for candidate in candidates {
        match at.get(&candidate.entity) {
            Some(&index) => {
                let incumbent = &picked[index];
                if ranking.aggregate.prefers(
                    &ranking.listed,
                    (&candidate.value, candidate.standing.as_ref()),
                    (&incumbent.value, incumbent.standing.as_ref()),
                ) {
                    picked[index] = candidate;
                }
            }
            None => {
                at.insert(candidate.entity.clone(), picked.len());
                picked.push(candidate);
            }
        }
    }
    picked
}

/// The candidates that survive the attribute's election under the
/// field's cardinality: one per entity under one, every distinct value
/// per entity under many.
fn elect(
    candidates: Vec<Candidate>,
    election: &Election,
) -> Result<Vec<Candidate>, EvaluationError> {
    if candidates.len() <= 1 {
        return Ok(candidates);
    }
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut groups: HashMap<Vec<u8>, Vec<Entry<Candidate>>> = HashMap::new();
    for candidate in candidates {
        let identity = match &candidate.key {
            Some(key) => encode_value(key)?,
            None => Vec::new(),
        };
        let key = candidate.entity.clone();
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        groups.entry(key).or_default().push(Entry {
            standing: candidate.standing.clone(),
            value: candidate.value.clone(),
            identity,
            carrier: candidate,
        });
    }
    let mut survivors = Vec::new();
    for key in order {
        let entries = groups.remove(&key).unwrap_or_default();
        survivors.extend(election.resolve(entries)?.0);
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

/// The bytes an election tells values apart by: one key per value, the
/// natural 3 and the float 3.0 included (see [`Value::identity_key`]).
fn encode_value(value: &Value) -> Result<Vec<u8>, EvaluationError> {
    Ok(value.identity_key())
}

/// The bytes an election groups an entity under: the entity's own,
/// without encoding, when the value is one.
fn entity_key(value: &Value) -> Result<Vec<u8>, EvaluationError> {
    match value {
        Value::Entity(entity) => Ok(entity.to_string().into_bytes()),
        other => encode_value(other),
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
    terms: &Parameters,
    env: &'a Env,
) -> Result<Vec<fixpoint::Answer>, EvaluationError>
where
    Env: crate::Scope<'a>,
{
    let reducer = rule
        .reducer()
        .expect("only rules with a reduce clause reach reduce_rows");
    // The groups the query names: a constant for the entity or for a
    // head field the rule does not reduce is part of every wanted
    // group's key, so the body is evaluated with it bound and only
    // those groups are read. A constant for a reduced field restricts
    // nothing here: it is compared with the fold's result, by the join.
    let mut seed = Match::new();
    for group in &reducer.groups {
        if let Some(Term::Constant(value)) = terms.get(group) {
            seed.bind(&Term::var(group.as_str()), value.clone())?;
        }
    }
    let body = rule.plan(&seed.environment()).evaluate(seed.seed(), env);
    let folded = reducer.fold(body).await?;
    // A folded row cites a standing only for a field a selective fold
    // bound, from the fact it chose.
    let chosen = |matched: &Match| {
        rule.reduce()
            .iter()
            .filter_map(|entry| matched.standing_of(&entry.field))
            .max()
    };
    Ok(folded
        .iter()
        .filter_map(|matched| {
            let row = fixpoint::project_complete(rule.conclusion(), matched)?;
            Some(fixpoint::Answer::new(row, chosen(matched)))
        })
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
    use crate::premise::reading;
    use crate::query::Application as _;
    mod entry_form {
        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

        use crate::attribute::{AttributeDescriptor, Keyed, The};
        use crate::concept::descriptor::ConceptFieldDescriptor;
        use crate::{Cardinality, ConceptDescriptor, ConceptQuery, Term, Type};
        use dialog_corpus::Symbol;
        use std::str::FromStr;

        fn ordered() -> ConceptDescriptor {
            ConceptDescriptor::try_from(vec![(
                "member".to_owned(),
                ConceptFieldDescriptor::required(AttributeDescriptor::over(
                    The::collection(
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

        /// An entry whose key and value are both blank writes as `{}`,
        /// neither half said, and reads back as that entry, not as a
        /// constant.
        #[dialog_common::test]
        fn it_reads_back_an_entry_whose_key_and_value_are_blank() {
            let query: ConceptQuery = serde_json::from_value(serde_json::json!({
                "assert": ordered(),
                "where": {
                    "this": {"?": {"name": "list"}},
                    "member": {"?": {}},
                    "member/key": {"?": {}}
                }
            }))
            .expect("parses");
            let written = serde_json::to_value(&query).expect("serializes");
            assert_eq!(written["where"]["member"], serde_json::json!({}));
            assert_eq!(
                serde_json::from_value::<ConceptQuery>(written).map_err(|e| e.to_string()),
                Ok(query.clone())
            );

            let bytes = serde_ipld_dagcbor::to_vec(&query).expect("encodes");
            assert_eq!(
                serde_ipld_dagcbor::from_slice::<ConceptQuery>(&bytes).map_err(|e| e.to_string()),
                Ok(query)
            );
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
    use dialog_corpus::Entity;
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
                "name": { "the": "narrow/name", "as": "text:" },
                "nick": { "the": "narrow/nick", "as": "text:", "optional": true },
                "mood": { "the": "narrow/mood", "as": "text:", "optional": true }
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

    /// The exact path elects like any other: one rule derives the
    /// concept's only attribute and nothing is stored under it, so the
    /// rule re-headed onto the concept is its whole answer, and where
    /// its body offers two values for one entity a cardinality-one
    /// read sees one row.
    #[dialog_common::test]
    async fn it_elects_on_the_exact_path() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let thing = Entity::new()?;
        branch
            .transaction()
            .assert(the!("stuff/name").of(thing.clone()).is("a".to_string()))
            .assert(the!("stuff/name").of(thing.clone()).is("b".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let stuff = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("stuff/name"),
                "",
                Cardinality::Many,
                Some(Type::String),
            ),
        )])?;
        let member = ConceptDescriptor::try_from(vec![(
            "title",
            AttributeDescriptor::new(
                the!("member/title"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("this"));
        terms.insert("name".to_string(), Term::<Any>::var("title"));
        let rule = DeductiveRule::new(
            member.clone(),
            vec![Premise::Assert(Proposition::Concept(ConceptQuery {
                terms,
                predicate: stuff,
            }))],
        )?;
        let mut registry = RuleRegistry::new();
        registry.register(rule)?;
        let source = TestEnv::new(&branch, &operator, registry);

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("who"));
        terms.insert("title".to_string(), Term::<Any>::var("title"));
        let rows: Vec<Match> = ConceptQuery {
            terms,
            predicate: member,
        }
        .evaluate(Match::new().seed(), &source)
        .try_collect()
        .await?;
        assert_eq!(rows.len(), 1, "one title survives the election");
        assert_eq!(
            rows[0].lookup(&Term::<Any>::var("title"))?.content()?,
            Value::String("b".to_string()),
            "the greatest value wins"
        );
        Ok(())
    }

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

    /// The most a concept read's stream may take, in bytes. It measures
    /// 1,696 on a 64-bit target with rustc 1.97; the room above that is
    /// for a toolchain laying it out differently, not for growth.
    const READ_STREAM_LIMIT: usize = 2048;

    /// A read is returned by value and moved into the box it is polled
    /// from, so its size is copied on every read, whichever path the read
    /// takes. It was 12 KB, most of it two inline copies of the inner
    /// evaluation, which is now boxed (dialog-db/dialog-db#781,
    /// dialog-db/dialog-db#806).
    #[dialog_common::test]
    async fn it_keeps_a_read_small_enough_to_move() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

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
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("this"));
        terms.insert("name".to_string(), Term::var("name"));

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let app = ConceptQuery {
            terms,
            predicate: concept,
        };
        let read = app.evaluate(Match::new().seed(), &source);
        let size = mem::size_of_val(&read);
        assert!(
            size <= READ_STREAM_LIMIT,
            "a concept read is {size} bytes, over the {READ_STREAM_LIMIT} it is held to: \
             box what a plain read does not run"
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
            "name": { "the": "person/name", "as": "text:" }
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
            "assert": { "with": { "name": { "the": "person/name", "as": "text:" } } },
            "where": {},
            "stranger": true,
        });
        assert!(
            serde_json::from_value::<ConceptQuery>(extra_field).is_ok(),
            "unknown fields must be ignored on deserialize"
        );
    }

    /// The `(this, value)` rows an attribute concept over `the` yields,
    /// sorted, read with both free.
    async fn relation_rows(source: &TestEnv<'_>, the: &str) -> anyhow::Result<Vec<(Value, Value)>> {
        let predicate = ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(
            AttributeDescriptor::new(
                the.parse().expect("a selector"),
                "",
                Cardinality::Many,
                Some(Type::String),
            ),
        ));
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("who"));
        terms.insert(ConceptDescriptor::VALUE.into(), Term::var("what"));
        let rows = ConceptQuery { terms, predicate }
            .evaluate(Match::new().seed(), source)
            .try_vec()
            .await?;
        let mut pairs: Vec<(Value, Value)> = rows
            .iter()
            .map(|row| {
                Ok((
                    row.lookup(&Term::var("who"))?.content()?,
                    row.lookup(&Term::var("what"))?.content()?,
                ))
            })
            .collect::<Result<_, EvaluationError>>()?;
        pairs.sort_by_key(|pair| format!("{pair:?}"));
        Ok(pairs)
    }

    /// A cardinality-one read returns the newest claim of a cell, and a
    /// cardinality-many read every claim: with `org/salary` holding 300
    /// and then 200, one reads 200 and many reads both.
    #[dialog_common::test]
    async fn it_reads_a_cell_under_its_cardinality() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        for salary in [300u64, 200] {
            branch
                .transaction()
                .assert(
                    the!("org/salary")
                        .of(alice.clone())
                        .is(salary)
                        .cardinality(Cardinality::Many),
                )
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

        let read = async |cardinality: Cardinality| -> anyhow::Result<Vec<u64>> {
            let predicate = ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(
                AttributeDescriptor::new(
                    "org/salary".parse().expect("a selector"),
                    "",
                    cardinality,
                    Some(Type::UnsignedInt),
                ),
            ));
            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::<Any>::constant(alice.clone()));
            terms.insert(ConceptDescriptor::VALUE.into(), Term::var("salary"));
            let rows = ConceptQuery { terms, predicate }
                .evaluate(Match::new().seed(), &source)
                .try_vec()
                .await?;
            let mut values: Vec<u64> = rows
                .iter()
                .map(|row| {
                    Ok(match row.lookup(&Term::var("salary"))?.content()? {
                        Value::UnsignedInt(value) => value as u64,
                        other => anyhow::bail!("not a salary: {other:?}"),
                    })
                })
                .collect::<anyhow::Result<_>>()?;
            values.sort();
            Ok(values)
        };
        assert_eq!(read(Cardinality::One).await?, vec![200]);
        assert_eq!(read(Cardinality::Many).await?, vec![200, 300]);
        Ok(())
    }

    /// A rule body naming a derived relation by an attribute premise
    /// reads the derived candidates too: `graph/even(x) := n :-
    /// graph/odd(x) = n` sees the `graph/odd` a rule derives beside the
    /// one stored, and `graph/lonely(x) := t :- graph/tag(x) = t,
    /// unless graph/odd(x)` excludes an entity whose `graph/odd` is
    /// derived, where a stored scan would have missed it. `graph/twice`
    /// reads `graph/even`, under which nothing is stored, so its one
    /// rule is its covering rule, and the covering rule reads the
    /// derived relation the same way.
    #[dialog_common::test]
    async fn it_reads_derived_candidates_through_an_attribute_premise() -> anyhow::Result<()> {
        use crate::negation::Negation;

        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let a: Entity = "id:a".parse()?;
        let b: Entity = "id:b".parse()?;
        let c: Entity = "id:c".parse()?;
        let d: Entity = "id:d".parse()?;
        branch
            .transaction()
            .assert(the!("graph/node").of(a.clone()).is("a".to_string()))
            .assert(the!("graph/node").of(b.clone()).is("b".to_string()))
            .assert(the!("graph/odd").of(c.clone()).is("c".to_string()))
            .assert(the!("graph/tag").of(a.clone()).is("A".to_string()))
            .assert(the!("graph/tag").of(d.clone()).is("D".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let relation = |the: &str| -> ConceptDescriptor {
            ConceptDescriptor::try_from(vec![(
                "name",
                AttributeDescriptor::new(
                    the.parse().expect("a selector"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            )])
            .expect("a concept")
        };
        let scan = |the: &str, value: &str| -> Premise {
            reading(
                the.parse::<crate::Relation>().expect("a selector"),
                Term::<Entity>::var("this"),
                Term::var(value),
                Some(Cardinality::One),
            )
        };
        let odd = DeductiveRule::new(relation("graph/odd"), vec![scan("graph/node", "name")])?;
        let even = DeductiveRule::new(relation("graph/even"), vec![scan("graph/odd", "name")])?;
        let twice = DeductiveRule::new(relation("graph/twice"), vec![scan("graph/even", "name")])?;
        let lonely = DeductiveRule::new(
            relation("graph/lonely"),
            vec![
                scan("graph/tag", "name"),
                match reading(
                    the!("graph/odd"),
                    Term::<Entity>::var("this"),
                    Term::blank(),
                    None,
                ) {
                    Premise::Assert(read) => Premise::Unless(Negation(read)),
                    other => other,
                },
            ],
        )?;
        let mut registry = RuleRegistry::new();
        registry.register(odd)?;
        registry.register(even)?;
        registry.register(lonely)?;
        registry.register(twice)?;
        let source = TestEnv::new(&branch, &operator, registry);

        let text = |value: &str| Value::String(value.into());
        let all = vec![
            (Value::Entity(a.clone()), text("a")),
            (Value::Entity(b.clone()), text("b")),
            (Value::Entity(c.clone()), text("c")),
        ];
        assert_eq!(
            relation_rows(&source, "graph/even").await?,
            all,
            "the stored and the derived odd values are both even"
        );
        assert_eq!(
            relation_rows(&source, "graph/twice").await?,
            all,
            "the covering rule reads the derived relation too"
        );
        assert_eq!(
            relation_rows(&source, "graph/lonely").await?,
            vec![(Value::Entity(d.clone()), text("D"))],
            "a derived odd excludes its entity from the negation"
        );
        Ok(())
    }

    /// Reducing-rule evaluation: the `reduce` clause end to end
    /// through `ConceptQuery::evaluate` (milestone A3,
    /// `notes/aggregation.md`).
    mod reducing {
        use super::*;
        use crate::rule::DeductiveRuleDescriptor;

        fn compile(json: serde_json::Value) -> DeductiveRule {
            let descriptor: DeductiveRuleDescriptor =
                serde_json::from_value(json).expect("descriptor parses");
            descriptor.compile().expect("rule compiles")
        }

        /// A rule offers one candidate per employee salary under the
        /// department's `org.dept/salary`, and a field reading that
        /// relation picks among them: the greatest under `max`, the
        /// least under `min`, every distinct value under `all`. A
        /// candidate is a fact, so two employees on one salary are one
        /// value of the relation.
        #[dialog_common::test]
        async fn it_chooses_among_the_candidates_a_rule_offers() -> anyhow::Result<()> {
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
                    "salary": { "the": "org.dept/salary", "as": "natural:", "cardinality": "many" }
                }},
                "when": [{
                    "assert": { "with": {
                        "dept": { "the": "org.employee/dept", "as": "entity:" },
                        "salary": { "the": "org.employee/salary", "as": "natural:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "employee" } },
                        "dept": { "?": { "name": "this" } },
                        "salary": { "?": { "name": "salary" } }
                    }
                }]
            }));
            let mut registry = RuleRegistry::new();
            registry.register(rule)?;
            let source = TestEnv::new(&branch, &operator, registry);

            let read = |pick: &str| -> ConceptDescriptor {
                serde_json::from_value(serde_json::json!({ "with": {
                    "n": { "the": "org.dept/salary", "as": "natural:", "pick": pick }
                }}))
                .expect("a concept over the relation")
            };
            for (select, expected) in [
                ("max", vec![200u128]),
                ("min", vec![100]),
                ("all", vec![100, 200]),
            ] {
                let mut terms = Parameters::new();
                terms.insert("this".into(), Term::var("dept"));
                terms.insert("n".into(), Term::var("n"));
                let rows = ConceptQuery {
                    terms,
                    predicate: read(select),
                }
                .evaluate(Match::new().seed(), &source)
                .try_vec()
                .await?;
                let mut values: Vec<(Value, Value)> = rows
                    .iter()
                    .map(|row| {
                        Ok((
                            row.lookup(&Term::var("dept"))?.content()?,
                            row.lookup(&Term::var("n"))?.content()?,
                        ))
                    })
                    .collect::<Result<_, EvaluationError>>()?;
                values.sort_by_key(|pair| format!("{pair:?}"));
                let expected: Vec<(Value, Value)> = expected
                    .into_iter()
                    .map(|n| (Value::Entity(dept.clone()), Value::UnsignedInt(n)))
                    .collect();
                assert_eq!(
                    values, expected,
                    "`{select}` over the department's candidates"
                );
            }
            Ok(())
        }
    }
}
