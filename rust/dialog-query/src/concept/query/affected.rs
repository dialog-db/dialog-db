//! Affected-entity discovery: the delta-join step of incremental
//! subscription maintenance.
//!
//! Given a set of changed base facts and a subscribed concept, which
//! head entities can the changes affect? For an *entity-local* rule
//! (every premise reads `of ?this`) the answer is just the changed
//! facts' subjects. For a non-local rule — one whose body reads
//! other entities' facts through a concept premise (a concept-typed
//! field's conformance check, a variant's negation) — the answer is
//! discovered by a *delta-join*: bind the changed fact into the
//! premise it can match, evaluate the rule's remaining premises
//! sideways with those bindings, and project the head variable.
//! Each affected head is then re-derived goal-directedly by the
//! caller (DRed's delete/re-derive, per entity).
//!
//! A derivation the change touches exists in exactly one of the two
//! states around it, and its remaining premises hold only there: a
//! derivation through a fact that stopped being readable held
//! *before* the change, one through a fact that became readable
//! holds *after* it, and a negated premise swaps the two, since the
//! derivation holds where the fact is absent. So the sideways join
//! runs against the state the derivation exists in. Joining every
//! change against the new state alone misses a derivation two
//! retracted facts supported, since each is bound into its premise
//! and the other is already gone from the state the rest is joined
//! against (#708); the same holds mirrored for two facts asserted
//! under negated premises. An optional premise holds on both sides
//! with different bindings, so a change to its fact touches a
//! derivation on each: the one through the old binding and the one
//! through the new, and the premise is read again on each side for
//! the fact's subject rather than bound to the changed value. The
//! join keeps the premise the change was bound into, so the plan
//! reads it again on its side with what the change bound and binds
//! the rest of it: a target concept's fields, a fact's cause.
//!
//! The discovery over-approximates but never misses: a fact bound
//! into a premise it merely *could* match yields candidate heads,
//! and re-derivation settles the truth per head. `None` means the
//! discovery could not bound the affected set (recursion, a rule
//! shape it does not handle, an unplannable sideways join) and the
//! caller must fall back to full re-evaluation, which is always
//! sound.

use std::collections::{BTreeSet, HashMap};

use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_corpus::Pick;
use futures_util::TryStreamExt as _;

use crate::attribute::{Cardinality, Relation, The};
use crate::concept::descriptor::{ConceptDescriptor, ConceptFieldDescriptor};
use crate::concept::query::ConceptQuery;
use crate::constraint::Constraint;
use crate::corpus::{Fact, Type as ValueType};
use crate::error::EvaluationError;
use crate::formula::cell::Cells;
use crate::negation::Negation;
use crate::parameters::Parameters;
use crate::planner::Planner;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::rule::deductive::DeductiveRule;
use crate::selection::{Binding, Match};
use crate::source::SelectRules;
use crate::term::Term;
use crate::type_system::{Primitive, Type as Kind};
use crate::types::{Any, Typed};
use crate::{Entity, Environment, Value};

/// The attributes the rules reachable from a concept read as
/// single-valued: what such a read sees of a cell is the one claim the
/// attribute's pick elects, not every claim, so a change to one claim
/// of the cell can change what is read of the others.
#[derive(Clone, Debug, PartialEq)]
pub enum Elected {
    /// Some premise reads a variable attribute as single-valued: any
    /// cell may be read that way.
    Any,
    /// Exactly these attributes, as the constant terms naming them.
    Of(Vec<Value>),
}

impl Elected {
    /// Whether a cell under `the` may be read as single-valued.
    pub fn includes(&self, the: &dialog_corpus::Relation) -> bool {
        match self {
            Elected::Any => true,
            Elected::Of(attributes) => {
                attributes.contains(&Value::from(Relation::from(the.clone())))
            }
        }
    }
}

/// The attributes the rules reachable from `concept` read as
/// single-valued, through any depth of concept premises, recursion
/// included. A maintenance that learns of changes as moved rows widens
/// a move under one of these to the cell's other claims, since the
/// move can change which of them is read.
///
/// None when every reachable rule is entity-local and none recurs: a
/// change to an entity's facts then re-derives that entity whole,
/// which reads each of its cells again and elects afresh, so the moved
/// rows are all the maintenance has to learn of.
pub async fn elected_attributes<'a, Env>(
    concept: &ConceptDescriptor,
    env: &'a Env,
) -> Result<Elected, EvaluationError>
where
    Env: crate::Scope<'a>,
{
    let mut attributes = Vec::new();
    let mut joins = false;
    let mut seen = BTreeSet::new();
    let mut queue = vec![concept.clone()];
    while let Some(descriptor) = queue.pop() {
        if !seen.insert(descriptor.this()) {
            continue;
        }
        let bundle = Provider::<SelectRules>::execute(env, descriptor).await?;
        joins |= bundle.recursion().is_some();
        for rule in bundle.rules() {
            joins |= !reads_only_this(rule);
            for premise in rule.analysis().premises() {
                let (the, cardinality) = match premise {
                    Premise::Assert(Proposition::Attribute(query)) => {
                        (query.the(), query.cardinality())
                    }
                    Premise::Assert(Proposition::OptionalAttribute(query)) => {
                        (query.query().the(), query.query().cardinality())
                    }
                    Premise::Unless(Negation(Proposition::Attribute(query))) => {
                        (query.the(), query.cardinality())
                    }
                    _ => {
                        if let Some((query, _)) = concept_premise(premise) {
                            queue.push(query.predicate.clone());
                        }
                        continue;
                    }
                };
                if cardinality != Cardinality::One {
                    continue;
                }
                match the.as_constant() {
                    Some(value) => {
                        if !attributes.contains(value) {
                            attributes.push(value.clone());
                        }
                    }
                    None => {
                        attributes.clear();
                        return Ok(if joins {
                            Elected::Any
                        } else {
                            Elected::Of(attributes)
                        });
                    }
                }
            }
        }
    }
    if !joins {
        attributes.clear();
    }
    Ok(Elected::Of(attributes))
}

/// Whether every premise of `rule` is about the entity it derives:
/// an attribute of `?this`, a concept applied to `?this`, or a
/// constraint or formula over what those bind. Such a rule's rows for
/// an entity depend on that entity's facts alone, read through concepts
/// that are themselves about it when their rules read this way too,
/// which [`elected_attributes`] checks as it reaches them. Wider than
/// the analysis's entity-local, which admits no concept premise.
fn reads_only_this(rule: &DeductiveRule) -> bool {
    fn is_this<T: Typed>(term: &Term<T>) -> bool {
        matches!(term, Term::Variable { name: Some(name), .. } if &**name == "this")
    }
    rule.analysis().is_entity_local()
        || rule.analysis().premises().all(|premise| match premise {
            Premise::Assert(Proposition::Attribute(query)) => is_this(query.of()),
            Premise::Assert(Proposition::OptionalAttribute(query)) => is_this(query.of()),
            Premise::Unless(Negation(Proposition::Attribute(query))) => is_this(query.of()),
            Premise::Assert(Proposition::Constraint(_)) => true,
            Premise::Assert(Proposition::Formula(_)) => true,
            Premise::Unless(Negation(Proposition::Constraint(_))) => true,
            _ => concept_premise(premise)
                .and_then(|(query, _)| query.terms.get("this"))
                .is_some_and(is_this),
        })
}

/// Which of the two states around a change something exists in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    /// The state the change was applied to.
    Before,
    /// The state the change produced.
    After,
}

impl Side {
    fn opposite(self) -> Side {
        match self {
            Side::Before => Side::After,
            Side::After => Side::Before,
        }
    }

    /// The side a derivation through a premise bound to something
    /// that exists on `self` holds: the same side for a positive
    /// premise, the opposite for a negated one, which holds where
    /// what it is bound to is absent.
    fn derivation(self, negated: bool) -> Side {
        if negated { self.opposite() } else { self }
    }
}

/// A changed fact and the side it is readable on: an asserted fact
/// exists after the change, a retracted one before it.
struct Change<'f> {
    fact: &'f Fact,
    exists: Side,
}

/// The candidate heads of one concept, by the side the derivation
/// the change touches is on: a head in `before` may have lost a
/// derivation, one in `after` may have gained one. A head whose row
/// merely changed is on both.
#[derive(Clone, Debug, Default)]
struct Heads {
    before: BTreeSet<Entity>,
    after: BTreeSet<Entity>,
}

impl Heads {
    fn on(&mut self, side: Side) -> &mut BTreeSet<Entity> {
        match side {
            Side::Before => &mut self.before,
            Side::After => &mut self.after,
        }
    }

    fn sides(&self) -> impl Iterator<Item = (Side, &BTreeSet<Entity>)> {
        [(Side::Before, &self.before), (Side::After, &self.after)].into_iter()
    }

    fn into_union(self) -> BTreeSet<Entity> {
        let mut heads = self.before;
        heads.extend(self.after);
        heads
    }
}

/// The two states around the change, each as an environment to
/// evaluate against. Rules are resolved from `after`; a change that
/// touched them makes the caller recompute instead. The state before
/// is asked for only when a join needs it, since the caller may have
/// to build it, and may have none to offer.
struct States<'a, Env, Before> {
    before: Before,
    after: &'a Env,
}

impl<'a, Env, Before> States<'a, Env, Before>
where
    Before: Fn() -> Option<&'a Env>,
{
    /// The environment of `side`, or `None` when there is no state
    /// before the change to read.
    fn on(&self, side: Side) -> Option<&'a Env> {
        match side {
            Side::Before => (self.before)(),
            Side::After => Some(self.after),
        }
    }
}

/// The head entities of `concept` that the changed facts can
/// affect, or `None` when the set cannot be bounded and the caller
/// must re-evaluate in full.
///
/// `asserted` are the facts that became readable and `retracted`
/// the ones that stopped; `after` reads the state the change
/// produced and `before` yields the state it was applied to (see the
/// [module docs](self) for why the discovery needs both). `before`
/// is called only when a join needs that state, so a caller that has
/// to build it pays for it only then, and one that has none to
/// offer returns `None`, which makes the discovery return `None`.
///
/// Nested concept premises are followed to arbitrary depth: the
/// reachable concepts form an acyclic graph (recursion anywhere in
/// it returns `None`), each is resolved once, and affected sets are
/// computed bottom-up — a target's affected heads feed the premises
/// that apply it, so a change can propagate through any number of
/// derivation layers.
pub async fn affected_entities<'a, Env, Before>(
    concept: &ConceptDescriptor,
    asserted: &[Fact],
    retracted: &[Fact],
    after: &'a Env,
    before: Before,
) -> Result<Option<BTreeSet<Entity>>, EvaluationError>
where
    Env: crate::Scope<'a>,
    Before: Fn() -> Option<&'a Env> + ConditionalSync,
{
    let root = concept.this();
    let states = States { before, after };
    let changes: Vec<Change<'_>> = asserted
        .iter()
        .map(|fact| Change {
            fact,
            exists: Side::After,
        })
        .chain(retracted.iter().map(|fact| Change {
            fact,
            exists: Side::Before,
        }))
        .collect();

    // Collect the reachable concepts and their resolved rules.
    let mut rules_of: HashMap<Entity, Vec<DeductiveRule>> = HashMap::new();
    let mut targets_of: HashMap<Entity, BTreeSet<Entity>> = HashMap::new();
    // The concepts that are one keyed collection under `all` which no rule
    // derives:
    // each row of such a concept is one stored entry, so a changed entry
    // binds into a premise that reads it as a fact binds into an attribute
    // premise (see `collection_premise`).
    let mut stored: BTreeSet<Entity> = BTreeSet::new();
    // The concepts that are one keyed collection under a pick that elects
    // by value or by rank (see `elects_across`).
    let mut across: BTreeSet<Entity> = BTreeSet::new();
    let mut queue = vec![concept.clone()];
    while let Some(descriptor) = queue.pop() {
        let entity = descriptor.this();
        if rules_of.contains_key(&entity) {
            continue;
        }
        let bundle = Provider::<SelectRules>::execute(after, descriptor.clone()).await?;
        if bundle.recursion().is_some() {
            return Ok(None);
        }
        // A reducing rule's fold spans entities: one changed fact
        // moves its whole group's row, and a retraction can make a
        // group's row disappear without any change to the head
        // entity's own facts. Per-entity re-derivation cannot bound
        // that, so the caller recomputes (A3's recompute-per-poll
        // model; incremental aggregate maintenance is A5).
        if bundle.rules().any(|rule| !rule.reduce().is_empty()) {
            return Ok(None);
        }
        // A join of two numbers depends on the side it starts from (see
        // `joins_numbers_unevenly`). A goal-directed read of one head can
        // start from the other side than a read of the whole query, so
        // it can give other rows. The caller recomputes, which reads the
        // query as a first read does.
        if bundle.rules().any(joins_numbers_unevenly) {
            return Ok(None);
        }
        if bundle.installed().is_empty() && collection_of(&descriptor).is_some() {
            stored.insert(entity.clone());
        }
        if elects_across(&descriptor) {
            across.insert(entity.clone());
        }
        let rules: Vec<DeductiveRule> = bundle.rules().cloned().collect();
        let mut targets = BTreeSet::new();
        for rule in &rules {
            for premise in rule.analysis().premises() {
                if let Some((query, _)) = concept_premise(premise) {
                    targets.insert(query.predicate.this());
                    queue.push(query.predicate.clone());
                }
            }
        }
        targets_of.insert(entity.clone(), targets);
        rules_of.insert(entity, rules);
    }

    // Bottom-up: a concept computes once every target it applies
    // has. The graph is acyclic (recursion returned above), so this
    // always makes progress.
    let mut affected: HashMap<Entity, Heads> = HashMap::new();
    let mut pending: Vec<Entity> = rules_of.keys().cloned().collect();
    while !pending.is_empty() {
        let ready: Vec<Entity> = pending
            .iter()
            .filter(|entity| {
                targets_of[*entity]
                    .iter()
                    .all(|target| affected.contains_key(target))
            })
            .cloned()
            .collect();
        if ready.is_empty() {
            // Unreachable for an acyclic graph; bail rather than spin.
            return Ok(None);
        }
        for entity in &ready {
            let mut heads = Heads::default();
            for rule in &rules_of[entity] {
                if rule.analysis().is_entity_local() {
                    // A change to E's facts only affects E's rows, and
                    // only under an attribute the rule reads: a change
                    // under one it never reads leaves its rows for E as
                    // they were. Every target an entity-local rule has
                    // is a concept selecting stored attributes, so this
                    // is what keeps a change to one concept's facts
                    // from being joined through every other.
                    for change in &changes {
                        for bind in reading_attribute(rule, change.fact) {
                            for side in bind.sides(change.exists) {
                                heads.on(side).insert(change.fact.of.clone());
                            }
                        }
                    }
                    continue;
                }
                match rule_heads(rule, &changes, &affected, &stored, &states).await? {
                    Some(found) => {
                        heads.before.extend(found.before);
                        heads.after.extend(found.after);
                    }
                    None => return Ok(None),
                }
            }
            // A change to one claim of a holder can change which claim
            // such a concept elects for it, on either side: a new claim
            // can win over the old winner, and a retracted winner leaves
            // the place to another claim. So the row of each holder that
            // a change touches may change on both sides.
            if across.contains(entity) {
                let holders = heads.into_union();
                heads = Heads {
                    before: holders.clone(),
                    after: holders,
                };
            }
            affected.insert(entity.clone(), heads);
        }
        pending.retain(|entity| !ready.contains(entity));
    }

    Ok(affected.remove(&root).map(Heads::into_union))
}

/// How a changed fact reaches the derivations through an attribute
/// premise it can match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Bind {
    /// The premise holds by the fact (or, negated, by its absence):
    /// bind the fact into it; the derivation is on the fact's side,
    /// flipped when negated.
    Exact { negated: bool },
    /// The premise holds on both sides, binding its value from
    /// whatever fact the subject has there: the derivations through
    /// the old binding and the new both change, so the premise is
    /// read again on each side for the fact's subject.
    Optional,
}

impl Bind {
    /// The sides of the derivations a fact readable on `exists`
    /// touches through a premise bound this way.
    fn sides(self, exists: Side) -> impl Iterator<Item = Side> {
        match self {
            Bind::Exact { negated } => vec![exists.derivation(negated)],
            Bind::Optional => vec![Side::Before, Side::After],
        }
        .into_iter()
    }
}

/// The slots of an attribute premise a changed fact can bind into,
/// and how.
struct Slots<'p> {
    the: &'p Term<Relation>,
    of: &'p Term<Entity>,
    is: &'p Term<Any>,
    bind: Bind,
}

fn attribute_premise(premise: &Premise) -> Option<Slots<'_>> {
    match premise {
        Premise::Assert(Proposition::Attribute(query)) => Some(Slots {
            the: query.the(),
            of: query.of(),
            is: query.is(),
            bind: Bind::Exact { negated: false },
        }),
        Premise::Assert(Proposition::OptionalAttribute(query)) => Some(Slots {
            the: query.query().the(),
            of: query.of(),
            is: query.is(),
            bind: Bind::Optional,
        }),
        Premise::Unless(Negation(Proposition::Attribute(query))) => Some(Slots {
            the: query.the(),
            of: query.of(),
            is: query.is(),
            bind: Bind::Exact { negated: true },
        }),
        _ => None,
    }
}

/// The attribute premises of `rule` that read the attribute of
/// `fact` — one naming it, or one over a variable attribute, which
/// reads any — each as how the fact binds into it.
fn reading_attribute<'r>(
    rule: &'r DeductiveRule,
    fact: &'r Fact,
) -> impl Iterator<Item = Bind> + 'r {
    let attribute = Value::from(Relation::from(fact.the.clone()));
    rule.analysis().premises().filter_map(move |premise| {
        let slots = attribute_premise(premise)?;
        slots
            .the
            .as_constant()
            .is_none_or(|value| *value == attribute)
            .then_some(slots.bind)
    })
}

/// The collection field of a concept that is one keyed collection under
/// `all` and nothing else: its name and its descriptor.
///
/// A field typed by a concept is left out: its rows also depend on the
/// facts of each member, which must conform, so a row can change with no
/// change to an entry.
///
/// A field under a pick that elects is left out too. Under `all` each
/// claim is an entry of its own, so a changed claim changes the rows that
/// it matches and no other. Under a pick that elects, a claim can take the
/// place of another claim that the premise read: a claim of the same key
/// under `last`, and a claim of any key of the entity under `max`, `min`
/// and `top`, which elect among every claim of the entity before the key
/// is compared. A changed claim then stands for more than itself.
fn collection_of(concept: &ConceptDescriptor) -> Option<(&str, &ConceptFieldDescriptor)> {
    let mut fields = concept.with().iter();
    let (name, field) = fields.next()?;
    if fields.next().is_some() || field.conforms().is_some() {
        return None;
    }
    if field.descriptor().pick().elects() {
        return None;
    }
    matches!(field.the(), The::Collection { .. }).then_some((name, field))
}

/// Whether `rule` joins a variable whose numbers a scan can compare in two
/// ways.
///
/// A scan does not compare two numbers by equality. A read that is bound with
/// a number reads each stored number through the type of that number: a read
/// of the natural 7 finds a stored 7.5, and a read of 7.5 does not find a
/// stored 7. So where two premises bind one value, the rows of the join can
/// depend on the premise that binds it first. Both orders give the same rows
/// when each premise takes the value through one and the same number type of
/// full width, since each read then sees each number through that type. They
/// can differ when a premise has no type, when a type holds more than one
/// number type, when a sized type rounds or narrows a number, and when two
/// premises have two number types.
///
/// A value can pass from one variable to another. An equality (`==`) binds
/// one side from the other, and `coalesce` binds its result from its source
/// or its fallback. So the check takes each variable together with each
/// variable that such a premise links it to, as one value. A premise that
/// binds that value is a read of an attribute or of a field of a concept, a
/// constant that an equality or `coalesce` gives it, or the result of a
/// formula or a resolver. Where two such premises bind one value and one of
/// them can hold numbers of more than one type, or two of them hold two
/// number types, the rule joins numbers unevenly. The check is a fallback to
/// a full evaluation, which is always sound, so it counts a premise whose
/// type it cannot prove as a premise of more than one type.
///
/// A constant that an equality compares with the value is a filter, such as
/// `?score == 3`. A constant that is no number is never equal to a number, so
/// it counts for nothing. A natural and an integer read a number alike where
/// both see it: each reads a number with a fraction as its floor. So where
/// each other premise of the value reads it as naturals alone, or as integers
/// alone, at full width, a constant of either type that this type reads as
/// the same number finds the same numbers from either side. Such a constant
/// counts as a premise of the type of the value. Any other constant counts as
/// a premise of its own type: a float sees 3.5 as itself, and a natural or an
/// integer sees it as 3.
///
/// A value that a positive premise takes as its entity is an entity in each
/// row, and a scan compares an entity by equality, so such a value joins
/// evenly. A value that passes through `coalesce` is left out of that: the
/// result of `coalesce` can be the entity while its source or its fallback
/// holds a number.
fn joins_numbers_unevenly(rule: &DeductiveRule) -> bool {
    /// What the premises of one value of the rule take it as.
    #[derive(Default)]
    struct Reads {
        /// Whether some positive premise takes the value as its entity.
        entity: bool,
        /// Whether the value passes through `coalesce`.
        coalesced: bool,
        /// How many premises bind the value.
        values: usize,
        /// Whether some premise binds the value through no one number type
        /// of full width while it can hold a number.
        uneven: bool,
        /// The one number type of each other premise that can bind a number.
        numbers: Vec<ValueType>,
    }
    impl Reads {
        fn value(&mut self, kind: Option<Kind>) {
            self.values += 1;
            let Some(kind) = kind else {
                self.uneven = true;
                return;
            };
            let Some(numbers) = kind.primitive_part().intersect(Primitive::NUMERIC) else {
                return;
            };
            let sized = kind
                .refinement()
                .is_some_and(|refinement| !refinement.views.is_empty());
            match numbers.as_singleton() {
                Some(number) if !sized => {
                    if !self.numbers.contains(&number) {
                        self.numbers.push(number);
                    }
                }
                _ => self.uneven = true,
            }
        }

        /// A constant that an equality compares with the value (see
        /// `joins_numbers_unevenly`).
        fn compared(&mut self, constant: &Value) {
            if !constant.data_type().is_number() {
                return;
            }
            let whole = |number: &ValueType| {
                matches!(number, ValueType::UnsignedInt | ValueType::SignedInt)
            };
            if let [number] = self.numbers.as_slice()
                && !self.uneven
                && whole(number)
                && whole(&constant.data_type())
                && constant
                    .clone()
                    .viewed_as(*number)
                    .is_some_and(|viewed| viewed.same_number(constant))
            {
                self.values += 1;
                return;
            }
            self.value(Some(Kind::from(constant.data_type())));
        }

        fn uneven(&self) -> bool {
            !(self.entity && !self.coalesced)
                && self.values >= 2
                && (self.uneven || self.numbers.len() > 1)
        }
    }

    /// The terms of a premise that links the values of its variables: an
    /// equality or `coalesce`.
    fn links(premise: &Premise) -> Option<(Vec<&Term<Any>>, bool)> {
        match premise {
            Premise::Assert(Proposition::Constraint(Constraint::Equality(equality))) => {
                Some((vec![&equality.this, &equality.is], false))
            }
            Premise::Assert(Proposition::Constraint(Constraint::Coalesce(coalesce))) => Some((
                vec![&coalesce.source, &coalesce.fallback, &coalesce.is],
                true,
            )),
            _ => None,
        }
    }

    // A variable that a link joins to another names that other variable
    // here. The names lead to one variable, which stands for the value.
    let mut linked: HashMap<&str, &str> = HashMap::new();
    fn value_of<'r>(linked: &HashMap<&'r str, &'r str>, mut name: &'r str) -> &'r str {
        while let Some(next) = linked.get(name).filter(|next| **next != name) {
            name = next;
        }
        name
    }
    let premises: Vec<&Premise> = rule.analysis().premises().collect();
    for premise in &premises {
        let Some((terms, _)) = links(premise) else {
            continue;
        };
        let mut names = terms.iter().filter_map(|term| term.name());
        let Some(first) = names.next() else {
            continue;
        };
        for name in names {
            let (one, other) = (value_of(&linked, first), value_of(&linked, name));
            if one != other {
                linked.insert(other, one);
            }
        }
    }

    let mut reads: HashMap<String, Reads> = HashMap::new();
    /// The reads of the value of the variable `name`.
    fn read<'m>(
        reads: &'m mut HashMap<String, Reads>,
        linked: &HashMap<&str, &str>,
        name: &str,
    ) -> &'m mut Reads {
        reads.entry(value_of(linked, name).to_owned()).or_default()
    }
    // A formula or a resolver binds each of its cells that is not required:
    // its results.
    let results = |cells: &Cells, terms: Parameters| -> Vec<(String, Option<Kind>)> {
        cells
            .iter()
            .filter(|(_, cell)| !cell.requirement().is_required())
            .filter_map(|(cell, spec)| {
                let name = terms.get(cell)?.name()?.to_owned();
                Some((name, spec.content_type().clone()))
            })
            .collect()
    };
    // The constants that an equality compares with a value, by a variable of
    // that value.
    let mut compared: Vec<(&str, &Value)> = Vec::new();
    for premise in &premises {
        if let Some(name) = entity_of(premise) {
            read(&mut reads, &linked, name).entity = true;
        }
        if let Some(Slots { is, .. }) = attribute_premise(premise) {
            if let Some(name) = is.name() {
                read(&mut reads, &linked, name).value(is.kind());
            }
            continue;
        }
        if let Some((terms, coalesce)) = links(premise) {
            let names: Vec<&str> = terms.iter().filter_map(|term| term.name()).collect();
            let Some(first) = names.first() else {
                continue;
            };
            read(&mut reads, &linked, first).coalesced |= coalesce;
            for term in terms {
                match term.as_constant() {
                    Some(constant) if !coalesce => compared.push((first, constant)),
                    Some(_) => read(&mut reads, &linked, first).value(term.kind()),
                    None => {}
                }
            }
            continue;
        }
        let bound = match premise {
            Premise::Assert(Proposition::Formula(formula)) => {
                results(formula.cells(), formula.parameters())
            }
            Premise::Assert(Proposition::Resolver(resolver)) => {
                results(resolver.cells(), resolver.parameters())
            }
            _ => Vec::new(),
        };
        for (name, kind) in bound {
            read(&mut reads, &linked, &name).value(kind);
        }
        let Some((query, _)) = concept_premise(premise) else {
            continue;
        };
        for (slot, term) in query.terms.iter() {
            let Some(name) = term.name() else {
                continue;
            };
            if let Some((_, field)) = query
                .predicate
                .with()
                .iter()
                .find(|(field, _)| field == slot)
            {
                read(&mut reads, &linked, name).value(field.content_type().map(Kind::from));
            }
        }
    }
    // A constant of an equality counts once each other premise of its value
    // is known.
    for (name, constant) in compared {
        read(&mut reads, &linked, name).compared(constant);
    }
    reads.values().any(Reads::uneven)
}

/// Whether `concept` is one keyed collection under `max`, `min`, or `top`.
/// A read of such a concept elects one claim among every claim of the
/// holder, of each key, and compares the key of the premise after that. So
/// its row for a holder is the claim that wins, and a change to any claim of
/// the holder can change it.
fn elects_across(concept: &ConceptDescriptor) -> bool {
    concept.attribute_field().is_some_and(|(_, field)| {
        matches!(field.the(), The::Collection { .. })
            && matches!(
                field.descriptor().pick(),
                Pick::Max | Pick::Min | Pick::Top(_)
            )
    })
}

/// The terms of a premise that reads a concept of one keyed collection,
/// with the kind of the attributes of its entries.
struct Entry<'p> {
    /// The kind every attribute of an entry has: the collection's domain
    /// and the shape of its keys.
    kind: Kind,
    /// The kind the field reads each value of an entry through, when it
    /// declares one: the kind of the value slot of the collection's scan.
    value_kind: Option<Kind>,
    /// The entity that holds the collection.
    this: Option<&'p Term<Any>>,
    /// The key of an entry: the name half of its attribute.
    key: Option<&'p Term<Any>>,
    /// The value of an entry.
    value: Option<&'p Term<Any>>,
}

/// How a changed fact binds into `query`, when it reads a concept that is
/// one keyed collection.
fn collection_premise(query: &ConceptQuery) -> Option<Entry<'_>> {
    let (name, field) = collection_of(&query.predicate)?;
    Some(Entry {
        kind: field.the().kind()?,
        value_kind: field.content_type().map(Kind::from),
        this: query.terms.get("this"),
        key: query.terms.get(&The::key_operand(name)),
        value: query.terms.get(name),
    })
}

/// The variable that `premise` takes as its entity, where the premise must
/// hold for a row: a positive premise that is not optional, which reads an
/// attribute of the variable or applies a concept to it. Such a variable
/// holds an entity in each row. A negated premise holds where its fact is
/// absent, and an optional premise holds with no fact, so a variable that
/// only such a premise takes as its entity can hold any value in a row.
fn entity_of(premise: &Premise) -> Option<&str> {
    match premise {
        Premise::Assert(Proposition::Attribute(query)) => query.of().name(),
        Premise::Assert(Proposition::Concept(query)) => {
            query.terms.get("this").and_then(|term| term.name())
        }
        _ => None,
    }
}

/// The concept application of a premise, and whether it is negated.
fn concept_premise(premise: &Premise) -> Option<(&ConceptQuery, bool)> {
    match premise {
        Premise::Assert(Proposition::Concept(query)) => Some((query, false)),
        Premise::Unless(Negation(Proposition::Concept(query))) => Some((query, true)),
        _ => None,
    }
}

/// The candidate head entities one non-local rule can produce or
/// retract given the changed facts: per premise, bind what the
/// change can match and join the remaining premises sideways, on
/// the side the derivation through that premise holds. Concept
/// premises bind their `this` slot from the target's
/// already-computed affected heads, side by side.
async fn rule_heads<'a, Env, Before>(
    rule: &DeductiveRule,
    changes: &[Change<'_>],
    affected: &HashMap<Entity, Heads>,
    stored: &BTreeSet<Entity>,
    states: &States<'a, Env, Before>,
) -> Result<Option<Heads>, EvaluationError>
where
    Env: crate::Scope<'a>,
    Before: Fn() -> Option<&'a Env> + ConditionalSync,
{
    let premises = &rule.analysis().premises;
    let mut heads = Heads::default();
    // The head, and each variable that a premise must take as its entity
    // for a row to hold (see `entity_of`).
    let mut entities: BTreeSet<&str> = BTreeSet::from(["this"]);
    entities.extend(premises.iter().filter_map(entity_of));

    for premise in premises {
        // Attribute premises (positive, optional, or negated) are
        // matched directly by the changed facts. An exact binding
        // takes the fact's value too; an optional one binds the
        // subject only, so each side reads the value it has there.
        if let Some(Slots { the, of, is, bind }) = attribute_premise(premise) {
            for change in changes {
                let fact = change.fact;
                let mut matched = Match::new();
                let mut scope = Environment::new();
                if !bind_slot(
                    &mut matched,
                    &mut scope,
                    the.as_constant(),
                    the.name(),
                    Value::from(Relation::from(fact.the.clone())),
                ) {
                    continue;
                }
                if !bind_slot(
                    &mut matched,
                    &mut scope,
                    of.as_constant(),
                    of.name(),
                    Value::Entity(fact.of.clone()),
                ) {
                    continue;
                }
                if matches!(bind, Bind::Exact { .. })
                    && !bind_slot(
                        &mut matched,
                        &mut scope,
                        is.as_constant(),
                        is.name(),
                        fact.is.clone(),
                    )
                {
                    continue;
                }
                for side in bind.sides(change.exists) {
                    let Some(env) = states.on(side) else {
                        return Ok(None);
                    };
                    match sideways_heads(premises, matched.clone(), &scope, rule, env).await? {
                        Some(found) => heads.on(side).extend(found),
                        None => return Ok(None),
                    }
                }
            }
            continue;
        }

        // A premise that reads one stored keyed collection under `all` is matched
        // by the changed entries of that collection, as an attribute
        // premise is by the changed facts: an entry is a fact whose
        // attribute holds the key, and it binds the entity and the
        // key, and the value too unless it is a number. Bound by its
        // entity alone, as a concept premise is below, one new entry
        // would join every entry of its collection sideways, and
        // every head those reach would be derived again: the whole
        // window of a log, for one post.
        if let Some((query, negated)) = concept_premise(premise)
            && stored.contains(&query.predicate.this())
            && let Some(entry) = collection_premise(query)
        {
            for change in changes {
                let fact = change.fact;
                let attribute = Value::from(Relation::from(fact.the.clone()));
                if !entry.kind.admits(&attribute) {
                    continue;
                }
                // The value is the value as a scan of the collection gives
                // it: read through the type of the field (`Kind::view`). A
                // fact whose value the field does not read is no entry, and
                // it binds nothing.
                let value = match &entry.value_kind {
                    Some(kind) => match kind.view(fact.is.clone()) {
                        Some(value) => value,
                        None => continue,
                    },
                    None => fact.is.clone(),
                };
                let slots = [
                    (entry.this, Value::Entity(fact.of.clone())),
                    (entry.key, Value::String(fact.the.name().to_owned())),
                    (entry.value, value),
                ];
                // A head is an entity, and so is each variable that a
                // premise of the rule takes as its entity. A field with no
                // type can hold an entry of another value, and where the
                // premise names such a variable as the value of its entry,
                // such an entry reaches no head.
                if slots.iter().any(|(term, value)| {
                    term.and_then(|term| term.name())
                        .is_some_and(|name| entities.contains(name))
                        && !matches!(value, Value::Entity(_))
                }) {
                    continue;
                }
                // A number is left to the scan, and binds nothing. A scan
                // does not compare a number by equality: it reads each
                // stored number through the type of the number that it is
                // bound with, so 7.5 answers a read of the natural 7, and 7
                // does not answer a read of 7.5. A number bound here would
                // meet a constant of the premise, or the number of another
                // premise, from the side of the entry alone. With the
                // entity and the key bound, the read of the premise on each
                // side compares the number as a first read does.
                let mut matched = Match::new();
                let mut scope = Environment::new();
                if !slots.into_iter().all(|(term, value)| {
                    value.data_type().is_number()
                        || term.is_none_or(|term| {
                            bind_slot(
                                &mut matched,
                                &mut scope,
                                term.as_constant(),
                                term.name(),
                                value,
                            )
                        })
                }) {
                    continue;
                }
                for side in (Bind::Exact { negated }).sides(change.exists) {
                    let Some(env) = states.on(side) else {
                        return Ok(None);
                    };
                    match sideways_heads(premises, matched.clone(), &scope, rule, env).await? {
                        Some(found) => heads.on(side).extend(found),
                        None => return Ok(None),
                    }
                }
            }
            continue;
        }

        // Concept premises (positive or negated): the change's
        // effect on the target is its already-computed affected
        // set; each affected target head, bound into the premise's
        // `this` slot, joins sideways to candidate heads, on the
        // side the target's derivation is on.
        if let Some((query, negated)) = concept_premise(premise) {
            let Some(target_heads) = affected.get(&query.predicate.this()) else {
                return Ok(None);
            };
            let Some(this) = query.terms.get("this") else {
                return Ok(None);
            };
            for (target_side, entities) in target_heads.sides() {
                let side = target_side.derivation(negated);
                for entity in entities {
                    let mut matched = Match::new();
                    let mut scope = Environment::new();
                    if !bind_slot(
                        &mut matched,
                        &mut scope,
                        this.as_constant(),
                        this.name(),
                        Value::Entity(entity.clone()),
                    ) {
                        continue;
                    }
                    let Some(env) = states.on(side) else {
                        return Ok(None);
                    };
                    match sideways_heads(premises, matched, &scope, rule, env).await? {
                        Some(found) => heads.on(side).extend(found),
                        None => return Ok(None),
                    }
                }
            }
        }
    }

    Ok(Some(heads))
}

/// Bind one premise slot from a changed value. A constant slot
/// filters (the fact must match it); a named variable binds; a
/// blank slot matches anything. Returns `false` when the fact
/// cannot match the slot.
fn bind_slot(
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

/// Join the rule's premises against the bound match and project the
/// head (`?this`) of every result. The premise the change was bound
/// into is joined like the rest: what the change bound seeds it, and
/// reading it again on this side binds whatever else it binds.
/// `None` when the join cannot be planned from these bindings or a
/// result leaves the head unbound — the caller falls back.
async fn sideways_heads<'a, Env>(
    premises: &[Premise],
    matched: Match,
    scope: &Environment,
    rule: &DeductiveRule,
    env: &'a Env,
) -> Result<Option<BTreeSet<Entity>>, EvaluationError>
where
    Env: crate::Scope<'a>,
{
    // The head may already be bound by the source premise itself.
    if let Ok(Binding::Present(Value::Entity(entity))) = matched.lookup(&Term::<Any>::var("this")) {
        return Ok(Some(BTreeSet::from([entity])));
    }

    let Ok(plan) =
        Planner::with_types(premises.to_vec(), rule.analysis().types.clone()).plan(scope)
    else {
        return Ok(None);
    };

    let mut heads = BTreeSet::new();
    let results: Vec<Match> = plan.evaluate(matched.seed(), env).try_collect().await?;
    for result in results {
        match result.lookup(&Term::<Any>::var("this")) {
            Ok(Binding::Present(Value::Entity(entity))) => {
                heads.insert(entity);
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(heads))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::attribute::{AttributeDescriptor, Cardinality, Type};
    use crate::concept::descriptor::ConceptFieldDescriptor;
    use crate::constraint::{Coalesce, Constraint, LessThan};
    use crate::parameters::Parameters;
    use crate::rule::DeductiveRuleDescriptor;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use dialog_peer::helpers::{test_repo, test_session_with_peer};
    use std::slice;

    fn concept(field: &str, the: &str, kind: Type) -> ConceptDescriptor {
        ConceptDescriptor::try_from(vec![(
            field,
            AttributeDescriptor::new(
                the.parse().expect("an attribute"),
                "",
                Cardinality::One,
                Some(kind),
            ),
        )])
        .expect("a well-formed concept")
    }

    fn application(concept: &ConceptDescriptor, terms: &[(&str, &str)]) -> ConceptQuery {
        let mut parameters = Parameters::new();
        for (field, variable) in terms {
            parameters.insert(field.to_string(), Term::<Any>::var(*variable));
        }
        ConceptQuery {
            terms: parameters,
            predicate: concept.clone(),
        }
    }

    fn premise(concept: &ConceptDescriptor, terms: &[(&str, &str)]) -> Premise {
        Premise::Assert(Proposition::Concept(application(concept, terms)))
    }

    fn unless(concept: &ConceptDescriptor, terms: &[(&str, &str)]) -> Premise {
        Premise::Unless(Negation(Proposition::Concept(application(concept, terms))))
    }

    fn fact(the: &str, of: &Entity, is: &str) -> anyhow::Result<Fact> {
        Ok(Fact {
            the: the.parse()?,
            of: of.clone(),
            is: Value::String(is.to_string()),
            cause: None,
        })
    }

    /// Someone whose manager and mentor both hold a badge: a rule with
    /// two conformance premises, neither binding the head.
    fn mentored(
        named: &ConceptDescriptor,
        managed: &ConceptDescriptor,
        mentoring: &ConceptDescriptor,
        badged: &ConceptDescriptor,
    ) -> anyhow::Result<(ConceptDescriptor, DeductiveRule)> {
        let mentored = concept("name", "report/mentored", Type::String);
        let rule = DeductiveRule::new(
            mentored.clone(),
            vec![
                premise(named, &[("this", "this"), ("name", "name")]),
                premise(managed, &[("this", "this"), ("manager", "manager")]),
                premise(mentoring, &[("this", "this"), ("mentor", "mentor")]),
                premise(badged, &[("this", "manager"), ("badge", "managers")]),
                premise(badged, &[("this", "mentor"), ("badge", "mentors")]),
            ],
        )?;
        Ok((mentored, rule))
    }

    /// Someone neither of whose manager and mentor holds a badge: the
    /// same shape under negation.
    fn unmentored(
        named: &ConceptDescriptor,
        managed: &ConceptDescriptor,
        mentoring: &ConceptDescriptor,
        badged: &ConceptDescriptor,
    ) -> anyhow::Result<(ConceptDescriptor, DeductiveRule)> {
        let unmentored = concept("name", "report/alone", Type::String);
        let rule = DeductiveRule::new(
            unmentored.clone(),
            vec![
                premise(named, &[("this", "this"), ("name", "name")]),
                premise(managed, &[("this", "this"), ("manager", "manager")]),
                premise(mentoring, &[("this", "this"), ("mentor", "mentor")]),
                unless(badged, &[("this", "manager")]),
                unless(badged, &[("this", "mentor")]),
            ],
        )?;
        Ok((unmentored, rule))
    }

    /// Two facts retracted together, each under a different premise of
    /// one non-local rule, reach the head they jointly supported (#708).
    /// The two states are two branches: `before` holds both badges,
    /// `after` neither. Bound into its premise, a retracted badge
    /// finds the head only where the other badge still holds, which
    /// is the state before the change.
    #[dialog_common::test]
    async fn it_reaches_a_head_two_retracted_facts_supported() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let before = repo.branch("before").open().perform(&operator).await?;
        let after = repo.branch("after").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let dave = Entity::new()?;
        let bob = Entity::new()?;
        for branch in [&before, &after] {
            branch
                .transaction()
                .assert(
                    the!("report/name")
                        .of(alice.clone())
                        .is("Alice".to_string()),
                )
                .assert(the!("report/name").of(bob.clone()).is("Bob".to_string()))
                .assert(the!("report/manager").of(bob.clone()).is(alice.clone()))
                .assert(the!("report/mentor").of(bob.clone()).is(dave.clone()))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        before
            .transaction()
            .assert(
                the!("credential/badge")
                    .of(alice.clone())
                    .is("A-1".to_string()),
            )
            .assert(
                the!("credential/badge")
                    .of(dave.clone())
                    .is("D-4".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let named = concept("name", "report/name", Type::String);
        let managed = concept("manager", "report/manager", Type::Entity);
        let mentoring = concept("mentor", "report/mentor", Type::Entity);
        let badged = concept("badge", "credential/badge", Type::String);
        let (mentored, rule) = mentored(&named, &managed, &mentoring, &badged)?;
        let mut registry = RuleRegistry::new();
        registry.register(rule)?;
        let before_env = TestEnv::new(&before, &operator, registry.clone());
        let after_env = TestEnv::new(&after, &operator, registry);

        let retracted = [
            fact("credential/badge", &alice, "A-1")?,
            fact("credential/badge", &dave, "D-4")?,
        ];
        assert_eq!(
            affected_entities(&mentored, &[], &retracted, &after_env, || Some(&before_env)).await?,
            Some(BTreeSet::from([bob])),
            "the row both badges supported is reached through either"
        );
        Ok(())
    }

    /// The mirror image: two facts asserted together, each under a
    /// different negated premise of one rule, reach the head they
    /// jointly undo. Neither badge holds in `before`, both in `after`;
    /// a derivation under a negation holds where the fact is absent,
    /// so the asserted badge joins the rest of the rule before the
    /// change, where the other badge is still absent.
    #[dialog_common::test]
    async fn it_reaches_a_head_two_asserted_facts_negated() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let before = repo.branch("before").open().perform(&operator).await?;
        let after = repo.branch("after").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let dave = Entity::new()?;
        let bob = Entity::new()?;
        for branch in [&before, &after] {
            branch
                .transaction()
                .assert(the!("report/name").of(bob.clone()).is("Bob".to_string()))
                .assert(the!("report/manager").of(bob.clone()).is(alice.clone()))
                .assert(the!("report/mentor").of(bob.clone()).is(dave.clone()))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        after
            .transaction()
            .assert(
                the!("credential/badge")
                    .of(alice.clone())
                    .is("A-1".to_string()),
            )
            .assert(
                the!("credential/badge")
                    .of(dave.clone())
                    .is("D-4".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let named = concept("name", "report/name", Type::String);
        let managed = concept("manager", "report/manager", Type::Entity);
        let mentoring = concept("mentor", "report/mentor", Type::Entity);
        let badged = concept("badge", "credential/badge", Type::String);
        let (unmentored, rule) = unmentored(&named, &managed, &mentoring, &badged)?;
        let mut registry = RuleRegistry::new();
        registry.register(rule)?;
        let before_env = TestEnv::new(&before, &operator, registry.clone());
        let after_env = TestEnv::new(&after, &operator, registry);

        let asserted = [
            fact("credential/badge", &alice, "A-1")?,
            fact("credential/badge", &dave, "D-4")?,
        ];
        assert_eq!(
            affected_entities(&unmentored, &asserted, &[], &after_env, || Some(
                &before_env
            ))
            .await?,
            Some(BTreeSet::from([bob])),
            "the row both badges undo is reached through either"
        );
        Ok(())
    }

    /// A named staff member and their level, an optional field: a
    /// concept whose projection reads the level with a left join.
    fn levelled() -> ConceptDescriptor {
        ConceptDescriptor::try_from(vec![
            (
                "name".to_string(),
                ConceptFieldDescriptor::required(AttributeDescriptor::new(
                    "report/name".parse().expect("an attribute"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
            (
                "level".to_string(),
                ConceptFieldDescriptor::optional(AttributeDescriptor::new(
                    "staff/level".parse().expect("an attribute"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                )),
            ),
        ])
        .expect("a well-formed concept")
    }

    /// Whether `of`'s level, none taken as 0, reads below 3: the
    /// premises that make a row exist or not by an optional field's
    /// value. The level is read through [`levelled`], as an installed
    /// rule reads an attribute.
    fn level_below_three(of: &str) -> Vec<Premise> {
        vec![
            premise(
                &levelled(),
                &[("this", of), ("name", "levelled"), ("level", "level")],
            ),
            Constraint::Coalesce(Coalesce::new(
                Term::<Any>::var("level"),
                Term::<Any>::constant(0u32),
                Term::<Any>::var("rank"),
            ))
            .into(),
            Constraint::LessThan(LessThan::new(
                Term::<Any>::var("rank"),
                Term::<Any>::constant(3u32),
            ))
            .into(),
        ]
    }

    /// A change to an optional field of a target concept touches a
    /// derivation on each side: the row through the old binding and
    /// the row through the new one. Alice's level going from 5 to
    /// none makes her a junior, so bob, who reports to her, gains a
    /// row; a join on the retracted fact's own side alone would look
    /// where she was not a junior and find nothing. The mirror, the
    /// level arriving, loses bob's row and is found on the side
    /// before the change.
    #[dialog_common::test]
    async fn it_reaches_a_head_an_optional_field_of_a_target_moves() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let before = repo.branch("before").open().perform(&operator).await?;
        let after = repo.branch("after").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        for branch in [&before, &after] {
            branch
                .transaction()
                .assert(
                    the!("report/name")
                        .of(alice.clone())
                        .is("Alice".to_string()),
                )
                .assert(the!("report/name").of(bob.clone()).is("Bob".to_string()))
                .assert(the!("report/manager").of(bob.clone()).is(alice.clone()))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        before
            .transaction()
            .assert(the!("staff/level").of(alice.clone()).is(5u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let named = concept("name", "report/name", Type::String);
        let managed = concept("manager", "report/manager", Type::Entity);
        let junior = concept("name", "report/junior", Type::String);
        let mut premises = vec![premise(&named, &[("this", "this"), ("name", "name")])];
        premises.extend(level_below_three("this"));
        let junior_rule = DeductiveRule::new(junior.clone(), premises)?;
        let supervised = concept("name", "report/supervised", Type::String);
        let supervised_rule = DeductiveRule::new(
            supervised.clone(),
            vec![
                premise(&named, &[("this", "this"), ("name", "name")]),
                premise(&managed, &[("this", "this"), ("manager", "manager")]),
                premise(&junior, &[("this", "manager"), ("name", "managers")]),
            ],
        )?;
        let mut registry = RuleRegistry::new();
        registry.register(junior_rule)?;
        registry.register(supervised_rule)?;
        let before_env = TestEnv::new(&before, &operator, registry.clone());
        let after_env = TestEnv::new(&after, &operator, registry);

        let level = Fact {
            the: "staff/level".parse()?,
            of: alice.clone(),
            is: Value::from(5u32),
            cause: None,
        };
        assert_eq!(
            affected_entities(
                &supervised,
                &[],
                slice::from_ref(&level),
                &after_env,
                || Some(&before_env)
            )
            .await?,
            Some(BTreeSet::from([bob.clone()])),
            "the level going makes the manager a junior: the report gains a row"
        );
        // The mirror: the level arriving, read with the states swapped.
        assert_eq!(
            affected_entities(&supervised, &[level], &[], &before_env, || Some(&after_env)).await?,
            Some(BTreeSet::from([bob])),
            "the level arriving unmakes the junior: the report loses its row"
        );
        Ok(())
    }

    /// The same, with the level read in the non-local rule itself,
    /// for another entity: the level's own concept puts its subject
    /// on both sides, and the rule's join on each side reads the
    /// level that side has, absent where it is absent.
    #[dialog_common::test]
    async fn it_reaches_a_head_an_optional_premise_over_another_entity_moves() -> anyhow::Result<()>
    {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let before = repo.branch("before").open().perform(&operator).await?;
        let after = repo.branch("after").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        for branch in [&before, &after] {
            branch
                .transaction()
                .assert(
                    the!("report/name")
                        .of(alice.clone())
                        .is("Alice".to_string()),
                )
                .assert(the!("report/name").of(bob.clone()).is("Bob".to_string()))
                .assert(the!("report/manager").of(bob.clone()).is(alice.clone()))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        before
            .transaction()
            .assert(the!("staff/level").of(alice.clone()).is(5u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let named = concept("name", "report/name", Type::String);
        let managed = concept("manager", "report/manager", Type::Entity);
        let supervised = concept("name", "report/supervised", Type::String);
        let mut premises = vec![
            premise(&named, &[("this", "this"), ("name", "name")]),
            premise(&managed, &[("this", "this"), ("manager", "manager")]),
        ];
        premises.extend(level_below_three("manager"));
        let rule = DeductiveRule::new(supervised.clone(), premises)?;
        let mut registry = RuleRegistry::new();
        registry.register(rule)?;
        let before_env = TestEnv::new(&before, &operator, registry.clone());
        let after_env = TestEnv::new(&after, &operator, registry);

        let level = Fact {
            the: "staff/level".parse()?,
            of: alice.clone(),
            is: Value::from(5u32),
            cause: None,
        };
        assert_eq!(
            affected_entities(
                &supervised,
                &[],
                slice::from_ref(&level),
                &after_env,
                || Some(&before_env)
            )
            .await?,
            Some(BTreeSet::from([bob.clone()])),
            "the level going is read as absent on the side after"
        );
        assert_eq!(
            affected_entities(&supervised, &[level], &[], &before_env, || Some(&after_env)).await?,
            Some(BTreeSet::from([bob])),
            "the level arriving is read as absent on the side before"
        );
        Ok(())
    }

    /// A change under an attribute no rule reads affects nothing: the
    /// tag's colour is not what the label is made of, so the document
    /// linking to the tag is not re-derived. A change to the tag
    /// itself is.
    #[dialog_common::test]
    async fn it_ignores_a_change_under_an_attribute_the_target_never_reads() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let doc = Entity::new()?;
        let tag = Entity::new()?;
        branch
            .transaction()
            .assert(the!("doc/link").of(doc.clone()).is(tag.clone()))
            .assert(the!("doc/tag").of(tag.clone()).is("x".to_string()))
            .assert(the!("doc/color").of(tag.clone()).is("red".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let link = concept("to", "doc/link", Type::Entity);
        let tagged = concept("tag", "doc/tag", Type::String);
        let labelled = concept("label", "doc/label", Type::String);
        let rule = DeductiveRule::new(
            labelled.clone(),
            vec![
                premise(&link, &[("this", "this"), ("to", "tag")]),
                premise(&tagged, &[("this", "tag"), ("tag", "label")]),
            ],
        )?;
        let mut registry = RuleRegistry::new();
        registry.register(rule)?;
        let env = TestEnv::new(&branch, &operator, registry);

        let recoloured = Fact {
            the: "doc/color".parse()?,
            of: tag.clone(),
            is: Value::String("blue".to_string()),
            cause: None,
        };
        assert_eq!(
            affected_entities(&labelled, &[recoloured], &[], &env, || Some(&env)).await?,
            Some(BTreeSet::new()),
            "a colour change reaches no label"
        );

        let retagged = Fact {
            the: "doc/tag".parse()?,
            of: tag.clone(),
            is: Value::String("y".to_string()),
            cause: None,
        };
        assert_eq!(
            affected_entities(&labelled, &[retagged], &[], &env, || Some(&env)).await?,
            Some(BTreeSet::from([doc])),
            "a tag change reaches the document linking to it"
        );
        Ok(())
    }

    /// A rule of the JSON form, compiled.
    fn compiled(rule: serde_json::Value) -> anyhow::Result<DeductiveRule> {
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(rule)?;
        Ok(descriptor.compile()?)
    }

    /// The check takes a value together with each variable that an equality
    /// or `coalesce` links it to. Each of these joins an entry of a
    /// collection with no type and the rank of an item, and the check flags
    /// it. The same join over two typed reads of naturals is not flagged.
    #[dialog_common::test]
    async fn it_flags_a_join_of_numbers_through_a_link() -> anyhow::Result<()> {
        let variable = |name: &str| serde_json::json!({ "?": { "name": name } });
        let entries = |kind: &str, value: &str| {
            let mut scores = serde_json::json!({
                "the": { "domain": "test.scores", "keyed": "dictionary" },
                "pick": "all:",
            });
            if !kind.is_empty() {
                scores["as"] = serde_json::json!(kind);
            }
            serde_json::json!({
                "assert": { "with": { "scores": scores } },
                "where": {
                    "this": { "entity:": "board:1" },
                    "scores": { "the": variable("key"), "is": variable(value) },
                },
            })
        };
        let rank = |kind: &str| {
            let mut rank = serde_json::json!({ "the": "test.item/rank" });
            if !kind.is_empty() {
                rank["as"] = serde_json::json!(kind);
            }
            serde_json::json!({
                "assert": { "with": { "rank": rank } },
                "where": { "this": variable("this"), "rank": variable("rank") },
            })
        };
        let head = serde_json::json!({ "with": { "rank": { "the": "test.ranked/rank" } } });
        let rule = |when: Vec<serde_json::Value>| {
            compiled(serde_json::json!({ "deduce": head, "when": when }))
        };
        let equal = serde_json::json!({ "assert": "==", "where": {
            "this": variable("value"), "is": variable("rank"),
        } });
        assert!(joins_numbers_unevenly(&rule(vec![
            entries("", "value"),
            equal.clone(),
            rank("")
        ])?));
        let coalesce = serde_json::json!({ "assert": "coalesce", "where": {
            "source": variable("value"), "fallback": 0, "is": variable("rank"),
        } });
        assert!(joins_numbers_unevenly(&rule(vec![
            entries("", "value"),
            coalesce,
            rank("")
        ])?));
        assert!(!joins_numbers_unevenly(&rule(vec![
            entries("natural:", "value"),
            equal,
            rank("natural:")
        ])?));
        Ok(())
    }
}
