/// Serializable rule descriptor matching the formal notation.
pub mod descriptor;

use crate::artifact::Entity;
use crate::attribute::query::AttributeQuery;
pub use crate::concept::descriptor::ConceptDescriptor;
use crate::error::TypeError;
use crate::negation::Negation;
use crate::optional::OptionalAttributeQuery;
pub use crate::planner::Plan;
pub use crate::planner::{Conjunction, Planner};
pub use crate::premise::Premise;
use crate::rule::analyzer::AnalyzedRule;
use crate::rule::{Compile, fmt_rule_schema};
use crate::type_system::Type as Kind;
use crate::types::Any;
pub use crate::{Attribute, Cardinality, Parameters, Proposition, Requirement, Value};
use crate::{Environment, Term};
use descriptor::DeductiveRuleDescriptor;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter, Result as FmtResult};

/// A deductive rule that has passed analysis: verified for every
/// invariant and plannable by construction.
///
/// Holds the analysis (the narrowed premises, inferred types, and
/// dependency graph) rather than a pre-baked plan. A concrete
/// execution plan is produced per scope by [`plan`](Self::plan).
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// The narrowed premises, inferred types, and dependency graph
    /// produced by analysis.
    analysis: AnalyzedRule,
}
impl Compile for DeductiveRule {
    fn from_analysis(analysis: AnalyzedRule) -> Self {
        DeductiveRule { analysis }
    }

    fn in_progress(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Self {
        DeductiveRule {
            analysis: AnalyzedRule::in_progress(conclusion, premises),
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
    pub fn required_operands(&self) -> impl Iterator<Item = &str> {
        self.conclusion().required_operands()
    }
    /// Returns the names of the parameters for this rule.
    pub fn parameters(&self) -> impl Iterator<Item = &str> {
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
        let mut when = Vec::new();
        let mut unless = Vec::new();

        for premise in &self.analysis.premises {
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
    /// (what `rule!:` notation and stored `db.rule/*` rules produce)
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

    /// This rule's content-addressed identity, if it has a canonical
    /// encoding: `rule:<base58(blake3(dag-cbor(descriptor)))>`.
    ///
    /// `None` for rules with no encodable body (implicit / attribute-query
    /// rules — see [`try_encode`](Self::try_encode)). A pure function of
    /// the rule body, stable across compilations, so it is a
    /// collision-free key for plan caching and the entity a rule's facts
    /// are stored under.
    pub fn try_this(&self) -> Option<Entity> {
        use base58::ToBase58;
        let hash = blake3::hash(&self.try_encode()?);
        let encoded = hash.as_bytes().as_ref().to_base58();
        format!("rule:{encoded}").parse().ok()
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

impl DeductiveRule {
    /// Compile *ordered variants* of a concept: spec-style
    /// first-to-conform alternatives, each an alternative body for
    /// the same conclusion.
    ///
    /// Variant `k` desugars to a rule whose body is the variant's
    /// own premises plus one negated premise per *earlier* variant:
    /// the entity yields variant `k`'s row only when no earlier
    /// variant matched it. Order is semantic; the returned rules are
    /// installed together (e.g. via
    /// [`ConceptRules::install`](crate::ConceptRules)) and their
    /// disjunction is deterministic per entity because the
    /// negations make the variants pairwise disjoint.
    ///
    /// Every variant must ground the conclusion's required operands
    /// with its own fields (the ordinary head-grounding contract);
    /// a variant that doesn't fails compilation like any other rule.
    pub fn variants(
        conclusion: ConceptDescriptor,
        ordered: Vec<ConceptDescriptor>,
    ) -> Result<Vec<DeductiveRule>, TypeError> {
        use crate::concept::query::ConceptQuery;

        let mut rules = Vec::new();
        for (position, variant) in ordered.iter().enumerate() {
            let mut premises = concept_premises(variant);
            for earlier in &ordered[..position] {
                let mut terms = Parameters::new();
                terms.insert("this".to_string(), Term::<Entity>::var("this").into());
                premises.push(Premise::Unless(Negation(Proposition::Concept(
                    ConceptQuery {
                        terms,
                        predicate: earlier.clone(),
                    },
                ))));
            }
            rules.push(DeductiveRule::new(conclusion.clone(), premises)?);
        }
        Ok(rules)
    }
}

/// Lower a concept's fields into the body premises of its implicit
/// rule: one scan (or left-join) per field, plus a conjoined target
/// premise per concept-typed field. Shared by
/// `From<&ConceptDescriptor>` and [`DeductiveRule::variants`].
fn concept_premises(concept: &ConceptDescriptor) -> Vec<Premise> {
    use crate::concept::query::ConceptQuery;
    use crate::type_system::ConceptRef;

    let mut premises = Vec::new();

    let this = Term::<Entity>::var("this");

    for (name, field) in concept.with().iter() {
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
                Term::Constant(Value::from(field.the().clone())),
                this.clone(),
                value.clone(),
                Term::blank(),
                Some(field.cardinality()),
            )
            .into()
        } else {
            AttributeQuery::new(
                Term::Constant(Value::from(field.the().clone())),
                this.clone(),
                value.clone(),
                Term::blank(),
                Some(field.cardinality()),
            )
            .into()
        };
        premises.push(premise);

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
        DeductiveRule::new(concept.clone(), concept_premises(concept))
            .expect("Concept should compile")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Cause, Entity, Type};
    use crate::attribute::AttributeDescriptor;
    use crate::attribute::The;
    use crate::attribute::query::AttributeQuery;
    use crate::concept::query::ConceptQuery;
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

    /// Ordered variants desugar to negated premises: variant `k`
    /// carries one `Unless` per earlier variant, joined on `this`,
    /// so the first conforming variant wins.
    #[dialog_common::test]
    fn variants_desugar_to_ordered_negations() {
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
        let email = ConceptDescriptor::try_from(vec![(
            "handle",
            AttributeDescriptor::new(the!("user/email"), "", Cardinality::One, Some(Type::String)),
        )])
        .unwrap();
        let phone = ConceptDescriptor::try_from(vec![(
            "handle",
            AttributeDescriptor::new(the!("user/phone"), "", Cardinality::One, Some(Type::String)),
        )])
        .unwrap();

        let rules = DeductiveRule::variants(conclusion, vec![email.clone(), phone.clone()])
            .expect("variants compile");
        assert_eq!(rules.len(), 2);

        let negations = |rule: &DeductiveRule| {
            rule.analysis()
                .premises
                .iter()
                .filter_map(|premise| match premise {
                    Premise::Unless(Negation(Proposition::Concept(query))) => Some(query.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };

        assert!(
            negations(&rules[0]).is_empty(),
            "the first variant negates nothing"
        );

        let unless = negations(&rules[1]);
        assert_eq!(unless.len(), 1, "one negation per earlier variant");
        assert_eq!(
            unless[0].predicate.this(),
            email.this(),
            "the later variant excludes the earlier one"
        );
        assert_eq!(
            unless[0].terms.iter().count(),
            1,
            "the negation joins on `this` alone"
        );
        assert_eq!(
            unless[0].terms.get("this").and_then(|term| term.name()),
            Some("this")
        );
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
        let email = ConceptDescriptor::try_from(vec![(
            "handle",
            AttributeDescriptor::new(the!("user/email"), "", Cardinality::One, Some(Type::String)),
        )])
        .unwrap();
        let phone = ConceptDescriptor::try_from(vec![(
            "handle",
            AttributeDescriptor::new(the!("user/phone"), "", Cardinality::One, Some(Type::String)),
        )])
        .unwrap();
        let variants = DeductiveRule::variants(conclusion, vec![email, phone]).unwrap();
        assert!(
            variants[0].analysis().is_entity_local(),
            "the first variant is plain attribute premises"
        );
        assert!(
            !variants[1].analysis().is_entity_local(),
            "a negated concept premise is non-local"
        );
    }

    /// A rule that negates its own conclusion concept is a negative
    /// self-loop: rejected at analysis.
    #[dialog_common::test]
    fn it_rejects_self_negating_rule() {
        use crate::concept::query::ConceptQuery;
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

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Entity>::var("this").into());
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
                predicate: conclusion.clone(),
            }))),
        ];

        match DeductiveRule::new(conclusion, premises) {
            Err(TypeError::SelfNegation { concept, .. }) => {
                assert!(concept.starts_with("concept:"));
            }
            other => panic!("expected SelfNegation, got {other:?}"),
        }
    }

    /// Negation over *another* concept is a negative IDB edge,
    /// surfaced by the analysis for the stratification pass.
    #[dialog_common::test]
    fn analysis_surfaces_negative_idb_edges() {
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
        let email = ConceptDescriptor::try_from(vec![(
            "handle",
            AttributeDescriptor::new(the!("user/email"), "", Cardinality::One, Some(Type::String)),
        )])
        .unwrap();
        let phone = ConceptDescriptor::try_from(vec![(
            "handle",
            AttributeDescriptor::new(the!("user/phone"), "", Cardinality::One, Some(Type::String)),
        )])
        .unwrap();

        let rules =
            DeductiveRule::variants(conclusion, vec![email.clone(), phone]).expect("compiles");

        assert_eq!(
            rules[0].analysis().negated_concepts().count(),
            0,
            "the first variant carries no negative edges"
        );
        assert_eq!(
            rules[1].analysis().negated_concepts().collect::<Vec<_>>(),
            vec![email.this()],
            "the later variant's negative edge names the earlier variant"
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
