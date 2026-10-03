/// Serializable rule descriptor matching the formal notation.
pub mod descriptor;
/// A rule's heads, one per attribute.
pub mod head;
/// Renaming a body's variables.
pub(crate) mod rename;

pub use head::Head;

use crate::Formula;
use crate::artifact::Entity;
use crate::attribute::Relation;
use crate::attribute::query::AttributeQuery;
pub use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::descriptor::ConceptFieldDescriptor;
use crate::concept::query::ConceptQuery;
use crate::error::TypeError;
use crate::formula::attribute::AttributeParts;
use crate::memo::Memo;
use crate::negation::Negation;
use crate::optional::OptionalAttributeQuery;
pub use crate::planner::Plan;
pub use crate::planner::{Conjunction, Planner};
pub use crate::premise::Premise;
use crate::reduce::{Reduce, ReduceEntry, ReduceSpec};
use crate::rule::analyzer::AnalyzedRule;
use crate::rule::{Compile, RuleKind, compile_internal, compile_rule, fmt_rule_schema};
use crate::type_system::Primitive;
use crate::type_system::Type as Kind;
use crate::types::Any;
pub use crate::{Attribute, Cardinality, Parameters, Proposition, Requirement, Value};
use crate::{Environment, Term};
use descriptor::DeductiveRuleDescriptor;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::iter;
use std::sync::Arc;

/// A deductive rule that has passed analysis: verified for every
/// invariant and plannable by construction.
///
/// Holds the analysis (the narrowed premises, inferred types, and
/// dependency graph) rather than a pre-baked plan. A concrete
/// execution plan is produced per scope by [`plan`](Self::plan).
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// The narrowed premises, inferred types, and dependency graph
    /// produced by analysis. Shared: a rule never changes once analyzed,
    /// and rules are cloned per query (out of statics, caches, and rule
    /// sets), so a clone should not copy the analysis.
    analysis: Arc<AnalyzedRule>,
    /// The rule's content-addressed identity, computed on first use:
    /// plan-cache lookups ask for it on every query.
    identity: Memo<Option<Entity>>,
    /// For a rule re-headed onto one attribute of a source rule's head:
    /// the source rule and which of its operands this head projects, so
    /// every head of the source shares one evaluation of its body.
    origin: Option<Arc<Origin>>,
}

/// Where a re-headed rule's rows come from: the source rule whose body
/// it shares, and the body operands its head attribute projects.
#[derive(Debug, Clone, PartialEq)]
pub struct Origin {
    /// The rule this head was split from.
    pub rule: DeductiveRule,
    /// The source body's operand for the head attribute's value.
    pub value: String,
    /// The source body's operand for the head attribute's key, for a
    /// keyed collection.
    pub key: Option<String>,
}
impl Compile for DeductiveRule {
    const KIND: RuleKind = RuleKind::Deductive;

    fn from_analysis(analysis: AnalyzedRule) -> Self {
        DeductiveRule {
            analysis: Arc::new(analysis),
            identity: Memo::default(),
            origin: None,
        }
    }

    fn in_progress(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Self {
        DeductiveRule {
            analysis: Arc::new(AnalyzedRule::in_progress(conclusion, premises)),
            identity: Memo::default(),
            origin: None,
        }
    }
}

impl DeductiveRule {
    /// Analyze a rule from a conclusion and premises into a verified,
    /// plannable rule. Runs type inference + narrowing, validates that
    /// every conclusion variable is grounded by a positive premise and
    /// that required head variables are not bound only by optional
    /// (set-widened) sources, and confirms the body is plannable.
    pub fn new(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Result<Self, TypeError> {
        <Self as Compile>::compile(conclusion, premises)
    }

    /// Analyze a *reducing* rule: [`Self::new`] plus a `reduce`
    /// clause, keyed by head field name. Every reduce key must name
    /// a head (`deduce`) field — an unknown key fails with
    /// [`TypeError::ReducedFieldNotInHead`] before analysis — and
    /// the reduce-specific analysis (field collision, typed entry
    /// construction, output/head unification, input grounding) runs
    /// with the shared pipeline. An empty map compiles a plain rule.
    pub fn with_reduce(
        conclusion: ConceptDescriptor,
        premises: Vec<Premise>,
        reduce: BTreeMap<String, ReduceSpec>,
    ) -> Result<Self, TypeError> {
        if let Some(field) = reduce
            .keys()
            .find(|field| !conclusion.with().keys().any(|name| name == field.as_str()))
        {
            return Err(TypeError::ReducedFieldNotInHead {
                field: field.clone(),
            });
        }
        compile_rule::<Self>(conclusion, premises, reduce.into_iter().collect())
    }

    /// The checked `reduce` clause entries, in head-field order.
    /// Empty for a plain rule.
    pub fn reduce(&self) -> &[ReduceEntry] {
        &self.analysis.reduce
    }

    /// The runtime fold for a reducing rule: the [`Reduce`] over the
    /// *derived* grouping fields (`this` plus every head field not in
    /// the reduce clause) and the checked entries. `None` for a
    /// plain rule.
    pub fn reducer(&self) -> Option<Reduce> {
        if self.analysis.reduce.is_empty() {
            return None;
        }
        let reduced = |name: &str| self.analysis.reduce.iter().any(|entry| entry.field == name);
        let groups = iter::once("this".to_string())
            .chain(
                self.conclusion()
                    .with()
                    .keys()
                    .filter(|name| !reduced(name))
                    .map(String::from),
            )
            .collect();
        Some(Reduce::new(groups, self.analysis.reduce.clone()))
    }

    /// Returns the conclusion predicate for this rule.
    pub fn conclusion(&self) -> &ConceptDescriptor {
        &self.analysis.conclusion
    }

    /// Returns this rule's analysis (narrowed premises, inferred
    /// types, dependency graph).
    pub fn analysis(&self) -> &AnalyzedRule {
        &self.analysis
    }

    /// Plan this rule's premises against a scope, producing a concrete
    /// execution plan ([`Conjunction`]) ordered for the given bindings.
    /// Reuses the analysis-inferred types; planning never re-infers.
    pub fn plan(&self, scope: &Environment) -> Conjunction {
        Planner::with_types(self.analysis.premises.clone(), self.analysis.types.clone())
            .plan(scope)
            .expect("an analyzed rule is plannable by construction")
    }

    /// Returns an iterator over the required operand names of this
    /// rule's conclusion.
    pub fn required_operands(&self) -> impl Iterator<Item = String> + '_ {
        self.conclusion().required_operands()
    }
    /// Returns the names of the parameters for this rule.
    pub fn parameters(&self) -> impl Iterator<Item = String> + '_ {
        self.conclusion().required_operands()
    }

    /// Creates a rule application by binding the provided terms to this rule's parameters.
    /// Validates that all required parameters are provided and returns an error if the
    /// application would be invalid.
    pub fn apply(&self, parameters: Parameters) -> Result<Proposition, TypeError> {
        self.conclusion().apply(parameters)
    }

    /// Converts this compiled rule back into a serializable [`DeductiveRuleDescriptor`].
    ///
    /// Reconstructs the `when`/`unless` split from the analyzed premises.
    pub fn descriptor(&self) -> DeductiveRuleDescriptor {
        match &self.analysis.authored {
            Some(authored) => self.describe(&authored.premises, &authored.reduce),
            None => self.describe(&self.analysis.premises, &self.analysis.reduce),
        }
    }

    /// This rule in its canonical spelling (see
    /// [`canonical`](crate::rule::canonical)): locals renamed by the
    /// body's structure and premises sorted, the same for every way
    /// of writing the body. Its encoding is what the rule's identity
    /// hashes.
    pub fn canonical_descriptor(&self) -> DeductiveRuleDescriptor {
        match &self.analysis.canonical {
            Some(identity) => {
                let (when, unless) = split(&identity.premises);
                DeductiveRuleDescriptor {
                    description: None,
                    deduce: identity.conclusion.clone(),
                    when,
                    unless,
                    reduce: identity.reduce.iter().cloned().collect(),
                }
            }
            None => self.describe(&self.analysis.premises, &self.analysis.reduce),
        }
    }

    /// The head's spelling: what the identity leaves out. Two rules of
    /// one identity under different field names plan and remember
    /// their bodies under different variables, so what is keyed by
    /// identity is keyed by this as well.
    pub fn spelling(&self) -> Vec<u8> {
        let operands = self.conclusion().sorted_operands();
        let parts: Vec<&[u8]> = operands.iter().map(|name| name.as_bytes()).collect();
        blake3::hash(&parts.concat()).as_bytes()[..8].to_vec()
    }

    /// This rule re-headed onto `target`, a concept of the same
    /// attributes under other field names: the body's variables
    /// renamed onto the target's operands, so a caller binding the
    /// target's names reaches the head. `None` when the fields do not
    /// pair up one to one by attribute, and `Ok(None)` as well when
    /// nothing needs renaming.
    pub fn respelled(&self, target: &ConceptDescriptor) -> Result<Option<Self>, TypeError> {
        use rename::{Rename, fresh_name, rename_premises, variables};

        let mut unpaired: Vec<(&str, &ConceptFieldDescriptor)> =
            self.conclusion().with().iter().collect();
        let mut map = Rename::new();
        for (name, field) in target.with().iter() {
            let Some(index) = unpaired.iter().position(|(_, mine)| {
                same_attribute(mine, field) && mine.is_optional() == field.is_optional()
            }) else {
                return Ok(None);
            };
            let (mine, _) = unpaired.remove(index);
            if mine != name {
                map.insert(mine.to_string(), name.to_string());
                if matches!(field.the(), Relation::Collection { .. }) {
                    map.insert(Relation::key_operand(mine), Relation::key_operand(name));
                }
            }
        }
        if !unpaired.is_empty() || map.is_empty() {
            return Ok(None);
        }
        let premises = &self.analysis.premises;
        let taken = variables(premises);
        let targets: BTreeSet<&String> = map.values().collect();
        let mut aside = Rename::new();
        for variable in &taken {
            if targets.contains(variable) && !map.contains_key(variable) {
                let fresh = fresh_name(variable, &taken, &map);
                aside.insert(variable.clone(), fresh);
            }
        }
        map.extend(aside);
        let premises = rename_premises(premises, &map)?;
        let reduce: BTreeMap<String, ReduceSpec> = self
            .analysis
            .reduce
            .iter()
            .map(|entry| {
                let field = map
                    .get(&entry.field)
                    .cloned()
                    .unwrap_or_else(|| entry.field.clone());
                let mut spec = ReduceSpec::from(entry);
                spec.of = rename::rename_term(&spec.of, &map);
                (field, spec)
            })
            .collect();
        Ok(Some(Self::with_reduce(target.clone(), premises, reduce)?))
    }

    fn describe(&self, premises: &[Premise], reduce: &[ReduceEntry]) -> DeductiveRuleDescriptor {
        let mut when = Vec::new();
        let mut unless = Vec::new();

        for premise in premises {
            match premise {
                Premise::Assert(proposition) => when.push(proposition.clone()),
                Premise::Unless(Negation(proposition)) => unless.push(proposition.clone()),
            }
        }

        DeductiveRuleDescriptor {
            description: None,
            deduce: self.conclusion().clone(),
            when,
            unless,
            reduce: reduce
                .iter()
                .map(|entry| (entry.field.clone(), ReduceSpec::from(entry)))
                .collect(),
        }
    }

    /// Canonical encoding of this rule's descriptor, if it has one —
    /// the dag-cbor bytes, deterministic by construction.
    ///
    /// Returns `None` when the rule body can't be expressed in formal
    /// notation: the implicit per-descriptor rule and any rule built
    /// directly from raw [`AttributeQuery`] premises encode to nothing,
    /// because `Proposition`'s formal-notation `Serialize` rejects
    /// attribute propositions. Only rules with concept/formula bodies
    /// (what `rule!:` notation and stored `dialog.rule/*` rules produce)
    /// have a canonical encoding.
    ///
    /// dag-cbor canonicalizes map keys per the spec, so the encoding is
    /// a pure function of the descriptor even though a premise's terms
    /// come from a [`Parameters`] `HashMap` — no manual key sorting
    /// needed. This is the same encoding dialog content-addresses with
    /// elsewhere.
    pub fn try_encode(&self) -> Option<Vec<u8>> {
        serde_ipld_dagcbor::to_vec(&self.descriptor()).ok()
    }

    /// This rule's content-addressed identity, if it has an encodable
    /// body: `rule:<base58(blake3(dag-cbor(canonical descriptor)))>`.
    ///
    /// `None` for rules with no encodable body (implicit / attribute-query
    /// rules — see [`try_encode`](Self::try_encode)). A pure function of
    /// the rule's canonical spelling, so two bodies that differ only in
    /// what their locals are called or in the order of their premises
    /// are one rule: one entity its facts are stored under, one plan
    /// cache entry, one body memo.
    pub fn try_this(&self) -> Option<Entity> {
        self.identity
            .get_or_init(|| {
                use base58::ToBase58;
                let canonical = serde_ipld_dagcbor::to_vec(&self.canonical_descriptor()).ok()?;
                let hash = blake3::hash(&canonical);
                let encoded = hash.as_bytes().as_ref().to_base58();
                format!("rule:{encoded}").parse().ok()
            })
            .clone()
    }

    /// Whether this rule's body is what was stored under `entity`:
    /// `entity` is its identity, or the identity its spelling had
    /// before identities were canonical (the hash of the stored bytes),
    /// so a rule installed then stays live. Bytes stored under any other
    /// entity are forged or corrupt.
    pub fn stored_as(&self, entity: &Entity) -> bool {
        self.try_this().as_ref() == Some(entity)
            || legacy_identity(self.try_encode()).as_ref() == Some(entity)
    }

    /// The source this head was split from, when it was.
    pub fn origin(&self) -> Option<&Origin> {
        self.origin.as_deref()
    }

    /// This rule marked as a head split from `origin`.
    pub(crate) fn with_origin(mut self, origin: Origin) -> Self {
        self.origin = Some(Arc::new(origin));
        self
    }

    /// Bytes identifying this rule within one process: its content
    /// address when it has one, else the address of its analysis,
    /// which every clone shares. A memo keyed by this never confuses
    /// two rules, and a built-in rule without an encodable body still
    /// has a key.
    pub fn memo_key(&self) -> Vec<u8> {
        match self.try_this() {
            Some(entity) => [entity.to_string().into_bytes(), self.spelling()].concat(),
            None => (Arc::as_ptr(&self.analysis) as usize)
                .to_le_bytes()
                .to_vec(),
        }
    }

    /// Whether `other` is this rule: the same content address when both
    /// have one, else structural equality. Two hydrations of one stored
    /// body are not always equal, because analysis records its
    /// narrowings in a hash-map-dependent order, so a rule set
    /// deduplicates by this rather than by `==`.
    pub fn same(&self, other: &DeductiveRule) -> bool {
        if Arc::ptr_eq(&self.analysis, &other.analysis) {
            return true;
        }
        match (self.try_this(), other.try_this()) {
            (Some(a), Some(b)) => a == b,
            _ => self == other,
        }
    }

    /// Canonical dag-cbor encoding, panicking if the rule has no
    /// encodable body. Use on the storage path where the rule is known
    /// to be storable (concept/formula bodies). Prefer
    /// [`try_encode`](Self::try_encode) otherwise.
    pub fn encode(&self) -> Vec<u8> {
        self.try_encode()
            .expect("rule body must encode in formal notation")
    }

    /// Content-addressed identity, panicking if the rule has no
    /// encodable body. Use on the storage path; prefer
    /// [`try_this`](Self::try_this) otherwise.
    pub fn this(&self) -> Entity {
        self.try_this()
            .expect("storable rule must have a content-addressed identity")
    }

    /// Rebuild a rule from its canonical dag-cbor [`encode`](Self::encode)
    /// bytes. `Err` carries a human-readable reason — either the cbor
    /// decode failed or the decoded descriptor didn't compile.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let descriptor: DeductiveRuleDescriptor = serde_ipld_dagcbor::from_slice(bytes)
            .map_err(|e| format!("dag-cbor decode failed: {e}"))?;
        descriptor.compile().map_err(|e| e.to_string())
    }
}

impl Serialize for DeductiveRule {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.descriptor().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DeductiveRule {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let definition = DeductiveRuleDescriptor::deserialize(deserializer)?;
        definition.compile().map_err(D::Error::custom)
    }
}

impl Display for DeductiveRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        fmt_rule_schema(self.conclusion(), f)
    }
}

/// Lower a concept's fields into the body premises of its implicit
/// rule: one scan (or left-join) per field, plus a conjoined target
/// premise per concept-typed field. Shared by
/// `From<&ConceptDescriptor>` and [`DeductiveRule::variants`].
fn concept_premises(concept: &ConceptDescriptor) -> Vec<Premise> {
    selecting_premises(concept, &|_| false)
}

/// Lower a concept's fields into the body premises of its selecting
/// rule. A required field whose attribute `derived` says some rule
/// derives is read through the [attribute
/// concept](ConceptDescriptor::of_attribute) over it, so the field
/// sees stored and derived values alike; every other field is a scan
/// (or left-join) over stored facts, as in the implicit rule.
///
/// An optional field over a derived attribute reads the attribute
/// concept set-widened: the premise's value term admits `Nothing`, and
/// [`ConceptQuery`](crate::concept::query::ConceptQuery) yields one
/// `Absent` row for an entity no row matched.
fn selecting_premises(
    concept: &ConceptDescriptor,
    derived: &dyn Fn(&ConceptFieldDescriptor) -> bool,
) -> Vec<Premise> {
    let mut premises = Vec::new();
    for (name, field) in concept.with().iter() {
        premises.extend(field_premises(name, field, derived(field)));
    }
    premises
}

/// The premises by which a concept's rule reads one field: through the
/// attribute concept when `derived`, else from stored facts, plus the
/// key projection of a collection and the conformance of a
/// concept-typed field.
fn field_premises(name: &str, field: &ConceptFieldDescriptor, derived: bool) -> Vec<Premise> {
    use crate::type_system::ConceptRef;

    let mut premises = Vec::new();

    let this = Term::<Entity>::var("this");

    {
        if derived {
            // An optional field reads the attribute concept set-widened:
            // its value term admits `Nothing`, which the concept query
            // honours by yielding one `Absent` row where no row matched.
            let kind = match (
                field.descriptor().read_type().map(Kind::from),
                field.conforms(),
            ) {
                (Some(kind), Some(target)) => Some(
                    kind.with_conformance(ConceptRef(target.this().to_string()))
                        .expect("a conforming field is entity-valued by construction"),
                ),
                (kind, _) => kind,
            };
            let value = match (kind, field.is_optional()) {
                (Some(kind), true) => Term::<Any>::typed_var(name, kind.optional()),
                (Some(kind), false) => Term::<Any>::typed_var(name, kind),
                (None, true) => Term::<Any>::typed_var(name, Kind::from(Primitive::ANY)),
                (None, false) => Term::var(name),
            };
            let mut terms = Parameters::new();
            terms.insert("this".to_string(), Term::<Any>::var("this"));
            terms.insert(ConceptDescriptor::VALUE.to_string(), value.clone());
            if let Relation::Collection { .. } = field.the() {
                terms.insert(
                    Relation::key_operand(ConceptDescriptor::VALUE),
                    Term::var(Relation::key_operand(name)),
                );
            }
            premises.push(Premise::Assert(Proposition::Concept(ConceptQuery {
                terms,
                predicate: ConceptDescriptor::of_attribute(field),
            })));
            if let Some(target) = field.conforms() {
                let mut terms = Parameters::new();
                terms.insert("this".to_string(), value);
                premises.push(Premise::Assert(Proposition::Concept(ConceptQuery {
                    terms,
                    predicate: target.clone(),
                })));
            }
            return premises;
        }
        // The value term stays scalar in both cases; the
        // associative layer never carries optionality. A
        // required field lowers to a plain scan (a missing fact
        // filters the row out); an optional field lowers to a
        // `OptionalAttributeQuery` left-join, which set-widens at the
        // projection: `this` is bound by the required fields, so
        // a miss yields one row with the slot bound to
        // `Binding::Absent`.
        //
        // A concept-typed field's slot additionally carries the
        // conformance refinement, and the constraint itself is
        // enforced structurally: the target concept is conjoined
        // as a premise over the field's variable below.
        let kind = match (field.content_type().map(Kind::from), field.conforms()) {
            (Some(kind), Some(target)) => Some(
                kind.with_conformance(ConceptRef(target.this().to_string()))
                    .expect("a conforming field is entity-valued by construction"),
            ),
            (kind, _) => kind,
        };
        let value = match kind {
            Some(kind) => Term::<Any>::typed_var(name, kind),
            None => Term::var(name),
        };

        let premise: Premise = if field.is_optional() {
            OptionalAttributeQuery::new(
                field.the().term(name),
                this.clone(),
                value.clone(),
                Term::blank(),
                Some(field.cardinality()),
            )
            .into()
        } else {
            AttributeQuery::new(
                field.the().term(name),
                this.clone(),
                value.clone(),
                Term::blank(),
                Some(field.cardinality()),
            )
            .into()
        };
        premises.push(premise);

        // A keyed collection's scan binds the whole attribute
        // (`domain/key`) to an internal variable; the author-facing
        // key is its name half, projected onto the field's key
        // operand. One entry, one row, `(key, value)` bound flat.
        if let Relation::Collection { .. } = field.the() {
            let mut parts = Parameters::new();
            parts.insert(
                "of".to_string(),
                Term::var(Relation::attribute_variable(name)),
            );
            parts.insert("domain".to_string(), Term::blank());
            parts.insert("name".to_string(), Term::var(Relation::key_operand(name)));
            premises.push(
                AttributeParts::apply(parts)
                    .expect("attribute-parts operands are well-formed by construction")
                    .into(),
            );
        }

        // Conformance is "facts exist", not a property of the
        // scalar, so it desugars to the target concept applied
        // to the field's entity: the row survives only when the
        // target entity satisfies the concept. Only `this` is
        // projected; the target's own fields stay internal to
        // the premise.
        if let Some(target) = field.conforms() {
            let mut terms = Parameters::new();
            terms.insert("this".to_string(), value);
            premises.push(Premise::Assert(Proposition::Concept(ConceptQuery {
                terms,
                predicate: target.clone(),
            })));
        }
    }

    premises
}

impl From<&ConceptDescriptor> for DeductiveRule {
    fn from(concept: &ConceptDescriptor) -> Self {
        compile_internal::<Self>(concept.clone(), concept_premises(concept))
            .expect("Concept should compile")
    }
}

impl DeductiveRule {
    /// The rule by which `concept` selects its rows: every required
    /// field whose attribute `derived` says some rule derives is read
    /// through the attribute concept over it, and every other field
    /// from stored facts. With nothing derived this is the concept's
    /// implicit rule.
    pub fn selecting(
        concept: &ConceptDescriptor,
        derived: &dyn Fn(&ConceptFieldDescriptor) -> bool,
    ) -> Result<Self, TypeError> {
        compile_internal::<Self>(concept.clone(), selecting_premises(concept, derived))
    }

    /// This rule re-headed onto `concept`: its body, with the variables of
    /// the head fields it shares with the concept renamed to the concept's
    /// field names, joined with stored scans of the concept's other
    /// fields. This is the concept's exact evaluation when the rule is
    /// the only source of those attributes and nothing is stored under
    /// them. `None` when the heads share no attribute, when a shared
    /// field is optional on either side (an optional concept field
    /// admits entities the rule derives nothing for), or when the rule
    /// folds.
    pub fn covering(&self, concept: &ConceptDescriptor) -> Result<Option<Self>, TypeError> {
        use rename::{Rename, fresh_name, rename_premises, variables};
        use std::collections::BTreeSet;

        if !self.reduce().is_empty() {
            return Ok(None);
        }
        let premises: Vec<Premise> = self.analysis().premises().cloned().collect();
        let taken = variables(&premises);
        let targets: BTreeSet<String> = concept.operands().collect();

        let mut map = Rename::new();
        let mut shared: Vec<&str> = Vec::new();
        // The rule's own operands that stand for a concept field, under
        // either name: these are never captured variables.
        let mut kept: BTreeSet<String> = BTreeSet::from(["this".to_string()]);
        for (name, field) in concept.with().iter() {
            let Some((mine, head)) = self
                .conclusion()
                .with()
                .iter()
                .find(|(_, head)| same_attribute(head, field))
            else {
                continue;
            };
            // A concept field the rule derives is exact only when it is
            // required: an optional one admits entities the rule derives
            // nothing for, which the rule's body never yields.
            if field.is_optional() || head.is_optional() {
                return Ok(None);
            }
            shared.push(name);
            kept.insert(mine.to_string());
            if let Relation::Collection { .. } = field.the() {
                kept.insert(Relation::key_operand(mine));
            }
            if mine != name {
                map.insert(mine.to_string(), name.to_string());
                if let Relation::Collection { .. } = field.the() {
                    map.insert(Relation::key_operand(mine), Relation::key_operand(name));
                }
            }
        }
        if shared.is_empty() {
            return Ok(None);
        }
        // A body variable named like a concept operand it does not stand
        // for would be captured: move it aside.
        for variable in &taken {
            if targets.contains(variable) && !kept.contains(variable) {
                let fresh = fresh_name(variable, &taken, &map);
                map.insert(variable.clone(), fresh);
            }
        }

        let mut body = rename_premises(&premises, &map)?;
        for (name, field) in concept.with().iter() {
            if shared.contains(&name) {
                if let Some(target) = field.conforms() {
                    let mut terms = Parameters::new();
                    terms.insert("this".to_string(), Term::<Any>::var(name));
                    body.push(Premise::Assert(Proposition::Concept(ConceptQuery {
                        terms,
                        predicate: target.clone(),
                    })));
                }
            } else {
                body.extend(field_premises(name, field, false));
            }
        }
        compile_internal::<Self>(concept.clone(), body).map(Some)
    }
}

/// The premises split into the descriptor's `when` and `unless`.
fn split(premises: &[Premise]) -> (Vec<Proposition>, Vec<Proposition>) {
    let mut when = Vec::new();
    let mut unless = Vec::new();
    for premise in premises {
        match premise {
            Premise::Assert(proposition) => when.push(proposition.clone()),
            Premise::Unless(Negation(proposition)) => unless.push(proposition.clone()),
        }
    }
    (when, unless)
}

/// Whether two fields are the same attribute: the same relation,
/// cardinality and content type, which is what an attribute's identity
/// hashes.
pub(crate) fn same_attribute(a: &ConceptFieldDescriptor, b: &ConceptFieldDescriptor) -> bool {
    a.the() == b.the() && a.cardinality() == b.cardinality() && a.content_type() == b.content_type()
}

/// The identity a stored body had before identities were canonical:
/// `rule:<base58(blake3(bytes))>` over its encoding as stored. What
/// [`DeductiveRule::stored_as`] and [`InductiveRule::stored_as`] accept
/// beside the canonical identity.
///
/// [`InductiveRule::stored_as`]: crate::rule::inductive::InductiveRule::stored_as
pub fn legacy_identity(encoded: Option<Vec<u8>>) -> Option<Entity> {
    use base58::ToBase58;
    let hash = blake3::hash(&encoded?);
    let encoded = hash.as_bytes().as_ref().to_base58();
    format!("rule:{encoded}").parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Cause, Entity, Type};
    use crate::attribute::AttributeDescriptor;
    use crate::attribute::The;
    use crate::attribute::query::AttributeQuery;
    use crate::constraint::{Coalesce, Constraint};
    use crate::proposition::Proposition;
    use crate::rule::analyzer::DependencyGraph;
    use crate::the;
    use crate::types::Any;
    use crate::{ConceptFieldDescriptor, Premise};

    /// Helper: an optional (set-widening) premise over the given
    /// attribute. Optionality is structural (a `OptionalAttributeQuery`
    /// left-join wrapping a scalar lookup), so this is how a test
    /// makes a variable's inferred kind admit `Nothing`.
    fn optional_premise(the: Term<The>, is: Term<Any>, cause: Term<Cause>) -> Premise {
        OptionalAttributeQuery::new(
            the,
            Term::<Entity>::var("this"),
            is,
            cause,
            Some(Cardinality::One),
        )
        .into()
    }

    #[dialog_common::test]
    fn it_compiles_with_valid_premises() {
        let conclusion = ConceptDescriptor::try_from(vec![
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
        let this = Term::<Entity>::var("this");
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this.clone(),
                Term::var("name"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("user/age")),
                this,
                Term::var("age"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_ok());
    }

    /// A successfully compiled rule retains its analysis (the
    /// dependency graph / SIPS and inferred types) rather than
    /// discarding it. The retained graph must match what the
    /// planner's ordered steps yield, confirming the analysis phase
    /// and the planned plan are consistent.
    #[dialog_common::test]
    fn it_retains_analysis_matching_planned_steps() {
        let conclusion = ConceptDescriptor::try_from(vec![
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
        let this = Term::<Entity>::var("this");
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this.clone(),
                Term::var("name"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("user/age")),
                this,
                Term::var("age"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let rule = DeductiveRule::new(conclusion, premises).expect("rule compiles");

        let analysis = rule.analysis();
        assert_eq!(
            analysis.graph,
            DependencyGraph::from_premises(&analysis.premises),
            "retained graph must match the premises' dependency graph"
        );
    }

    /// A body stored before identities were canonical sits under the
    /// hash of its bytes: the rule is stored as that entity and as its
    /// canonical identity, and as nothing else.
    #[dialog_common::test]
    fn it_is_stored_as_its_legacy_identity_too() {
        use serde_json::json;
        let json = json!({
            "deduce": { "with": { "name": { "the": "org/employee-name", "as": "Text" } } },
            "when": [
                {
                    "assert": { "with": { "name": { "the": "org/person-name", "as": "Text" } } },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "name": { "?": { "name": "name" } }
                    }
                }
            ]
        });
        let descriptor: DeductiveRuleDescriptor =
            serde_json::from_value(json).expect("descriptor parses");
        let rule = descriptor.compile().expect("rule compiles");
        let legacy = legacy_identity(rule.try_encode()).expect("an encodable body");
        assert_ne!(
            legacy,
            rule.this(),
            "the legacy identity hashes the stored bytes"
        );
        assert!(rule.stored_as(&rule.this()));
        assert!(rule.stored_as(&legacy));
        let other: Entity = "rule:forged".parse().expect("an entity");
        assert!(!rule.stored_as(&other));
    }

    /// A concept-bodied rule (the storable kind) has a deterministic,
    /// content-addressed `this()` — same body ⇒ same `rule:` entity
    /// across independent compilations, regardless of premise-term map
    /// iteration order. This is the plan-cache / storage key.
    #[dialog_common::test]
    fn it_has_a_deterministic_content_addressed_identity() {
        use serde_json::json;
        let json = json!({
            "deduce": { "with": { "name": { "the": "org/employee-name", "as": "Text" } } },
            "when": [
                {
                    "assert": { "with": { "name": { "the": "org/person-name", "as": "Text" } } },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "name": { "?": { "name": "name" } }
                    }
                }
            ]
        });
        let build = || {
            let d: DeductiveRuleDescriptor =
                serde_json::from_value(json.clone()).expect("descriptor parses");
            d.compile().expect("rule compiles")
        };
        let a = build();
        let b = build();
        assert_eq!(a.this(), b.this(), "same rule body ⇒ same identity");
        assert_eq!(
            a.encode(),
            b.encode(),
            "dag-cbor encoding is stable across compilations"
        );
        assert!(
            a.this().to_string().starts_with("rule:"),
            "identity is a rule: URI, got {}",
            a.this()
        );
        // Round-trips through encode/decode.
        let decoded = DeductiveRule::decode(&a.encode()).expect("decodes");
        assert_eq!(decoded.this(), a.this(), "encode/decode preserves identity");
    }

    #[dialog_common::test]
    fn it_rejects_unconstrained_fact() {
        let conclusion = ConceptDescriptor::try_from(vec![
            (
                "key",
                AttributeDescriptor::new(
                    the!("person/key"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "value",
                AttributeDescriptor::new(
                    the!("person/value"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
        ])
        .unwrap();
        let premises = vec![
            AttributeQuery::new(
                Term::var("the"),
                Term::var("user"),
                Term::var("value"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        assert!(DeductiveRule::new(conclusion, premises).is_err());
    }

    #[dialog_common::test]
    fn it_rejects_unconstrained_relation() {
        let conclusion = ConceptDescriptor::try_from(vec![
            (
                "key",
                AttributeDescriptor::new(
                    the!("person/key"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "value",
                AttributeDescriptor::new(
                    the!("person/value"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
        ])
        .unwrap();

        // All terms are variables: no constants at all.
        // The planner should reject this at install time.
        let premises = vec![
            AttributeQuery::new(
                Term::var("the"),
                Term::var("user"),
                Term::var("value"),
                Term::var("cause"),
                None,
            )
            .into(),
        ];

        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_err(),
            "Rule with fully unconstrained relation premise should fail at install time"
        );
    }

    #[dialog_common::test]
    fn it_rejects_unused_parameter() {
        let conclusion = ConceptDescriptor::try_from(vec![
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
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                Term::var("this"),
                Term::var("name"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_err());
        if let Err(TypeError::UnboundVariable { variable, .. }) = result {
            assert_eq!(variable, "age", "Should report 'age' as unbound");
        }
    }

    #[dialog_common::test]
    fn it_rejects_empty_premises() {
        let conclusion = ConceptDescriptor::try_from(vec![
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
        assert!(DeductiveRule::new(conclusion, vec![]).is_err());
    }

    #[dialog_common::test]
    fn it_compiles_with_chained_dependencies() {
        let conclusion = ConceptDescriptor::try_from(vec![
            (
                "key",
                AttributeDescriptor::new(
                    the!("result/key"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "value",
                AttributeDescriptor::new(
                    the!("result/value"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
        ])
        .unwrap();
        let this = Term::<Entity>::var("this");
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this.clone(),
                Term::constant("jack".to_string()),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            // Use ?key as the the variable
            // to ensure the conclusion parameter "key" gets bound.
            AttributeQuery::new(
                Term::var("key"),
                this,
                Term::var("value"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
        assert_eq!(result.unwrap().analysis().premises.len(), 2);
    }

    #[dialog_common::test]
    fn it_rejects_mismatched_parameter_name() {
        let conclusion = ConceptDescriptor::try_from(vec![(
            "key",
            AttributeDescriptor::new(the!("result/key"), "", Cardinality::One, Some(Type::String)),
        )])
        .unwrap();

        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                Term::<Entity>::var("this"),
                Term::var("key_var"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];

        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_err(),
            "Should fail when variable name doesn't match parameter name"
        );
        if let Err(TypeError::UnboundVariable { variable, .. }) = result {
            assert_eq!(variable, "key", "Should report 'key' as unbound");
        }
    }

    #[dialog_common::test]
    fn it_rejects_negated_constraint_with_unbound_variable() {
        let conclusion = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let name = Term::<String>::var("name");
        let z = Term::<String>::var("z");
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                name.clone().into(),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            // ?z is never bound by any premise; should fail to compile
            !name.is(z),
        ];

        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_err(),
            "Should reject rule with negated constraint referencing unbound variable ?z"
        );
    }

    #[dialog_common::test]
    fn it_rejects_negated_constraint_with_unbound_variable_on_left() {
        let conclusion = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let name = Term::<String>::var("name");
        let z = Term::<String>::var("z");
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                name.clone().into(),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            // flipped: ?z (unbound) on the left, ?name (bound) on the right
            !z.is(name),
        ];

        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_err(),
            "Should reject rule with negated constraint referencing unbound variable ?z (flipped)"
        );
    }

    /// Concept projection emits one scalar scan per `with`
    /// attribute. A concept with no `maybe` attributes produces no
    /// `Maybe` left-joins.
    #[dialog_common::test]
    fn from_concept_with_only_required_emits_required_premises() {
        let concept = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let rule = DeductiveRule::from(&concept);

        let mut scans = 0;
        let mut maybes = 0;
        for premise in rule.analysis().premises.iter() {
            match premise {
                Premise::Assert(Proposition::Attribute(_)) => scans += 1,
                Premise::Assert(Proposition::OptionalAttribute(_)) => maybes += 1,
                _ => {}
            }
        }
        assert_eq!(scans, 1, "expected one scalar scan");
        assert_eq!(maybes, 0, "expected no Maybe left-joins");
    }

    /// Concept projection emits a scalar scan per required attribute
    /// and a `Maybe` left-join per optional attribute. The left-join
    /// wraps a *scalar* lookup; optionality is structural, not a
    /// property of the value term's kind.
    #[dialog_common::test]
    fn from_concept_with_optional_field_emits_maybe_left_join() {
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

        let rule = DeductiveRule::from(&concept);

        let mut scans = 0;
        let mut maybes = 0;
        for premise in rule.analysis().premises.iter() {
            match premise {
                Premise::Assert(Proposition::Attribute(_)) => scans += 1,
                Premise::Assert(Proposition::OptionalAttribute(query)) => {
                    assert!(
                        !query.is().is_optional(),
                        "the wrapped lookup's value term stays scalar"
                    );
                    maybes += 1;
                }
                _ => {}
            }
        }
        assert_eq!(scans, 1, "expected one scalar scan (name)");
        assert_eq!(maybes, 1, "expected one Maybe left-join (nickname)");
    }

    /// Entity locality: the implicit rule of a plain concept reads
    /// only `?this`'s facts; concept premises (conforming fields,
    /// variant negations) make a rule non-local.
    #[dialog_common::test]
    fn it_classifies_entity_locality() {
        let plain = ConceptDescriptor::try_from(vec![
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
        assert!(
            DeductiveRule::from(&plain).analysis().is_entity_local(),
            "attribute and optional premises over ?this are local"
        );

        let target = ConceptDescriptor::try_from(vec![(
            "badge",
            AttributeDescriptor::new(
                the!("employee/badge"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        let conforming = ConceptDescriptor::try_from(vec![
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
                "manager".to_string(),
                ConceptFieldDescriptor::conforming(
                    AttributeDescriptor::new(
                        the!("person/manager"),
                        "",
                        Cardinality::One,
                        Some(Type::Entity),
                    ),
                    target.clone(),
                )
                .unwrap(),
            ),
        ])
        .unwrap();
        assert!(
            !DeductiveRule::from(&conforming)
                .analysis()
                .is_entity_local(),
            "a concept premise reads another entity's facts"
        );
    }

    /// A deductive rule is open, so it is monotone: an `unless` premise
    /// is refused at compile time, before any other analysis, whatever
    /// else the rule does. Negation belongs to the closed places (a
    /// query, a subscription, an inductive rule).
    #[dialog_common::test]
    fn it_refuses_negation_in_a_deductive_rule() {
        use crate::negation::Negation;

        let conclusion = ConceptDescriptor::try_from(vec![(
            "handle",
            AttributeDescriptor::new(
                the!("contact/handle"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        let blocked = ConceptDescriptor::try_from(vec![(
            "blocked",
            AttributeDescriptor::new(
                the!("contact/blocked"),
                "",
                Cardinality::One,
                Some(Type::Boolean),
            ),
        )])
        .unwrap();

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Entity>::var("this").into());
        terms.insert("blocked".to_string(), Term::blank());
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/email")),
                Term::<Entity>::var("this"),
                Term::var("handle"),
                Term::blank(),
                Some(Cardinality::One),
            )
            .into(),
            Premise::Unless(Negation(Proposition::Concept(ConceptQuery {
                terms,
                predicate: blocked,
            }))),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            matches!(result, Err(TypeError::NegationInOpenRule { .. })),
            "a deductive rule admits no unless, got {result:?}"
        );
    }

    /// A `reduce` block is refused in a deductive rule for the same
    /// reason: a fold withdraws its previous result when a fact
    /// arrives. The attribute's `select` policy folds instead.
    #[dialog_common::test]
    fn it_refuses_reduce_in_a_deductive_rule() {
        use crate::reduce::{Aggregator, ReduceSpec};

        let conclusion = ConceptDescriptor::try_from(vec![(
            "total",
            AttributeDescriptor::new(
                the!("org/total"),
                "",
                Cardinality::One,
                Some(Type::UnsignedInt),
            ),
        )])
        .unwrap();
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("org/salary")),
                Term::<Entity>::var("this"),
                Term::var("salary"),
                Term::blank(),
                Some(Cardinality::Many),
            )
            .into(),
        ];
        let reduce = BTreeMap::from([(
            "total".to_string(),
            ReduceSpec {
                apply: Aggregator::Sum,
                of: Term::var("salary"),
            },
        )]);
        let result = DeductiveRule::with_reduce(conclusion, premises, reduce);
        assert!(
            matches!(result, Err(TypeError::ReduceInOpenRule { .. })),
            "a deductive rule admits no reduce, got {result:?}"
        );
    }

    /// A deductive rule neither concludes through nor reads a field
    /// whose policy changes the attribute's carrier: inside a recursive
    /// component such a field would read entities as a number, and the
    /// refusal is local to the rule so no merge of rule sets is ever
    /// rejected.
    #[dialog_common::test]
    fn it_refuses_a_carrier_changing_field_in_a_deductive_rule() {
        use crate::schema::Select;

        let counted = ConceptDescriptor::try_from(vec![(
            "members",
            AttributeDescriptor::new(
                the!("team/member"),
                "",
                Cardinality::Many,
                Some(Type::Entity),
            )
            .with_select(Select::Count, Vec::new()),
        )])
        .unwrap();
        let size = ConceptDescriptor::try_from(vec![(
            "size",
            AttributeDescriptor::new(
                the!("team/size"),
                "",
                Cardinality::One,
                Some(Type::UnsignedInt),
            ),
        )])
        .unwrap();

        // Concluding through a count.
        let result = DeductiveRule::new(
            counted.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("team/size")),
                    Term::<Entity>::var("this"),
                    Term::var("members"),
                    Term::blank(),
                    Some(Cardinality::One),
                )
                .into(),
            ],
        );
        assert!(
            matches!(
                result,
                Err(TypeError::PolicyInOpenRule {
                    role: "concludes",
                    ..
                })
            ),
            "a deductive rule concludes carrier-closed fields only, got {result:?}"
        );

        // Reading a count.
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Entity>::var("this").into());
        terms.insert("members".to_string(), Term::var("size"));
        let result = DeductiveRule::new(
            size,
            vec![Premise::Assert(Proposition::Concept(ConceptQuery {
                terms,
                predicate: counted,
            }))],
        );
        assert!(
            matches!(
                result,
                Err(TypeError::PolicyInOpenRule { role: "reads", .. })
            ),
            "a deductive rule reads carrier-closed fields only, got {result:?}"
        );
    }

    /// A concept-typed field conjoins the target concept as a
    /// premise over the field's variable, and the field's slot kind
    /// carries the conformance refinement.
    #[dialog_common::test]
    fn from_concept_with_conforming_field_conjoins_target_premise() {
        use crate::type_system::ConceptRef;

        let target = ConceptDescriptor::try_from(vec![(
            "badge",
            AttributeDescriptor::new(
                the!("employee/badge"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

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
                "manager".to_string(),
                ConceptFieldDescriptor::conforming(
                    AttributeDescriptor::new(
                        the!("person/manager"),
                        "",
                        Cardinality::One,
                        Some(Type::Entity),
                    ),
                    target.clone(),
                )
                .expect("entity-valued attribute conforms"),
            ),
        ])
        .unwrap();

        let rule = DeductiveRule::from(&concept);

        let mut scans = Vec::new();
        let mut concepts = Vec::new();
        for premise in rule.analysis().premises.iter() {
            match premise {
                Premise::Assert(Proposition::Attribute(query)) => scans.push(query),
                Premise::Assert(Proposition::Concept(query)) => concepts.push(query),
                other => panic!("unexpected premise {other:?}"),
            }
        }
        assert_eq!(scans.len(), 2, "one scan per attribute");
        assert_eq!(concepts.len(), 1, "one conjoined target premise");

        let conformance = &concepts[0];
        assert_eq!(
            conformance.predicate.this(),
            target.this(),
            "the conjoined premise applies the target concept"
        );
        assert_eq!(
            conformance.terms.iter().count(),
            1,
            "only `this` is projected into the target"
        );
        let this = conformance.terms.get("this").expect("this bound");
        assert_eq!(this.name(), Some("manager"), "joins on the field variable");

        let manager_scan = scans
            .iter()
            .find(|scan| scan.is().name() == Some("manager"))
            .expect("manager scan present");
        let kind = manager_scan.is().kind().expect("typed slot");
        assert!(
            kind.refinement()
                .expect("conformance refinement stamped")
                .conforms
                .contains(&ConceptRef(target.this().to_string())),
            "the slot kind names the target concept"
        );
    }

    /// The degenerate "rule body binds only optionals" shape, at the
    /// concept layer: rejected by construction.
    ///
    /// A concept with zero required (`with`) attributes constrains
    /// nothing, so every entity would match it; a rule built from it
    /// would have a body of only optional premises (each yielding an
    /// Absent fallback on miss). This is unsound, and it is now
    /// *unconstructable*: `ConceptDescriptor::try_from` of an empty
    /// required set returns [`TypeError::EmptyConcept`], so the
    /// degenerate concept can never reach the rule compiler at all.
    /// Optional fields do not change this; only required ones count.
    ///
    /// (A required head bound *only* by an optional premise, the
    /// distinct shape where a `with` field exists but is fed from an
    /// optional source, is caught separately by
    /// `RequiredHeadFromOptional`; see
    /// `it_rejects_required_head_from_optional_premise`.)
    #[dialog_common::test]
    fn it_rejects_concept_with_no_required_attributes_by_construction() {
        // Empty required set: construction fails outright.
        let empty: Vec<(&str, AttributeDescriptor)> = Vec::new();
        match ConceptDescriptor::try_from(empty) {
            Err(TypeError::EmptyConcept) => {}
            other => panic!("expected EmptyConcept, got {other:?}"),
        }
    }

    /// A required-only concept carries no optional fields; building
    /// with an optional field flags exactly that field optional.
    #[dialog_common::test]
    fn optional_field_is_flagged_optional() {
        let concept = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        assert!(
            concept.with().iter().all(|(_, field)| !field.is_optional()),
            "no optional fields by default"
        );

        let with_optional = ConceptDescriptor::try_from(vec![
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

        let optional: Vec<&str> = with_optional
            .with()
            .iter()
            .filter(|(_, field)| field.is_optional())
            .map(|(name, _)| name)
            .collect();
        assert_eq!(optional, vec!["nickname"], "one optional field installed");
    }

    /// A conclusion variable bound only by an optional attribute
    /// query carries `Nothing` in its meet. Required heads cannot
    /// accept that: the rule could produce an Absent value in a
    /// required slot. Reject.
    #[dialog_common::test]
    fn it_rejects_required_head_from_optional_premise() {
        let conclusion = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        // Bind ?name only through a left-join; the meet for ?name
        // includes Nothing.
        let premises = vec![optional_premise(
            Term::from(the!("user/name")),
            Term::var("name"),
            Term::var("cause"),
        )];
        let result = DeductiveRule::new(conclusion, premises);
        match result {
            Err(TypeError::RequiredHeadFromOptional { variable, .. }) => {
                assert_eq!(variable, "name");
            }
            other => panic!("expected RequiredHeadFromOptional, got {other:?}"),
        }
    }

    /// A conclusion variable bound by *both* an optional and a
    /// required premise (with a typed `is` slot) has the Nothing
    /// bit removed by the meet: at least one premise guarantees
    /// Present. Accept.
    #[dialog_common::test]
    fn it_accepts_required_head_when_inference_strips_nothing() {
        let conclusion = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        let this = Term::<Entity>::var("this");
        let typed_name: Term<Any> = Term::<String>::var("name").into();

        let premises = vec![
            // Left-join: contributes a slot type with Nothing.
            optional_premise(
                Term::from(the!("user/name")),
                Term::<String>::var("name").into(),
                Term::var("cause1"),
            ),
            // Required `is` term: contributes a slot type without Nothing.
            AttributeQuery::new(
                Term::from(the!("user/canonical-name")),
                this,
                typed_name,
                Term::var("cause2"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_ok(),
            "meet of Required + Optional strips Nothing, should compile (got {:?})",
            result.err()
        );
    }

    /// Symmetric case: an *untyped* Required premise paired with a
    /// typed Optional premise should also strip Nothing from the
    /// meet. The untyped Required contribution is "any present
    /// value" (`Primitive::ALL`), so intersected with
    /// `Optional<String>` (i.e. `{String, Nothing}`) the meet
    /// resolves to `{String}`: no Nothing. Rule compiles.
    #[dialog_common::test]
    fn it_accepts_untyped_required_paired_with_typed_optional() {
        let conclusion = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        let this = Term::<Entity>::var("this");

        let premises = vec![
            // Left-join (typed): contributes `{String, Nothing}`.
            optional_premise(
                Term::from(the!("user/name")),
                Term::<String>::var("name").into(),
                Term::var("cause1"),
            ),
            // Required with *untyped* `is` (Term::var without a
            // kind). Contributes "any present value" to the meet
            // via the None-content_type branch.
            AttributeQuery::new(
                Term::from(the!("user/canonical-name")),
                this,
                Term::<Any>::var("name"),
                Term::var("cause2"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_ok(),
            "untyped Required + typed Optional should compile (got {:?})",
            result.err()
        );
    }

    /// The `cause` slot of a `Maybe` left-join is set-widened in
    /// the schema (since the fallback row binds it to `Absent`). A
    /// rule where a required-head variable shares its name with
    /// such a cause is therefore rejected by the meet algebra.
    #[dialog_common::test]
    fn it_rejects_required_head_from_optional_cause() {
        // Conclusion has a required `mark` field expecting a
        // typed value (Bytes).
        let conclusion = ConceptDescriptor::try_from(vec![(
            "mark",
            AttributeDescriptor::new(the!("person/mark"), "", Cardinality::One, Some(Type::Bytes)),
        )])
        .unwrap();
        // The left-join's cause slot shares the name `?mark` with
        // the conclusion's required head; the meet's cause
        // contribution carries Nothing, so the required head sees
        // Optional.
        let premises = vec![optional_premise(
            Term::from(the!("user/name")),
            Term::var("name"),
            Term::<Cause>::var("mark"),
        )];
        let result = DeductiveRule::new(conclusion, premises);
        match result {
            Err(TypeError::RequiredHeadFromOptional { variable, .. }) => {
                assert_eq!(variable, "mark");
            }
            other => panic!("expected RequiredHeadFromOptional, got {other:?}"),
        }
    }

    /// The widening crosses the concept boundary: a required head
    /// bound only through an inner concept's *optional* field is
    /// rejected, because the concept's schema declares that the
    /// slot can deliver `Absent`.
    #[dialog_common::test]
    fn it_rejects_required_head_from_concept_optional_field() {
        let conclusion = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let inner = ConceptDescriptor::try_from(vec![
            (
                "title".to_string(),
                ConceptFieldDescriptor::required(AttributeDescriptor::new(
                    the!("person/title"),
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
        terms.insert("this".to_string(), Term::var("this"));
        terms.insert("title".to_string(), Term::var("title"));
        // The outer head's required `name` is fed by the inner
        // concept's optional `nickname`; the meet admits Nothing.
        terms.insert("nickname".to_string(), Term::var("name"));
        let premises = vec![Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: inner,
        }))];

        let result = DeductiveRule::new(conclusion, premises);
        match result {
            Err(TypeError::RequiredHeadFromOptional { variable, .. }) => {
                assert_eq!(variable, "name");
            }
            other => panic!("expected RequiredHeadFromOptional, got {other:?}"),
        }
    }

    /// A rule containing a malformed Coalesce (non-Optional source)
    /// is rejected at compile time. This is the regression test for
    /// validate-not-called: previously `Coalesce::validate` existed
    /// but no production path invoked it, so wire-format or
    /// raw-constructor mismatches silently passed.
    #[dialog_common::test]
    fn it_rejects_coalesce_with_non_optional_source() {
        let conclusion = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        let this = Term::<Entity>::var("this");
        let typed_name: Term<Any> = Term::<String>::var("name").into();

        // Source is a `Term<Any>` carrying `String` (not Optional<String>).
        let bad_source: Term<Any> = Term::<String>::var("source").into();
        let bad_coalesce = Coalesce::new(
            bad_source,
            Term::<Any>::constant("Anon".to_string()),
            typed_name.clone(),
        );

        let premises = vec![
            // Required premise so the rule has a chance of compiling
            // up to the coalesce-validation step.
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this,
                typed_name,
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            Premise::Assert(Proposition::Constraint(Constraint::Coalesce(bad_coalesce))),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        match result {
            Err(TypeError::CoalesceTypeMismatch { .. }) => {}
            other => panic!("expected CoalesceTypeMismatch, got {other:?}"),
        }
    }
}
