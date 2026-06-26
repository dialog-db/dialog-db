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

    /// Canonical JSON form of this rule's descriptor — a stable byte
    /// string independent of map iteration order.
    ///
    /// Canonicalization is load-bearing: a premise's `where` terms
    /// serialize from a [`Parameters`] `HashMap`, whose iteration order
    /// is non-deterministic, so serializing the descriptor directly
    /// would vary across compilations of the same rule. Round-tripping
    /// through a [`serde_json::Value`] with every object's keys sorted
    /// gives a deterministic form. This is the value stored under a
    /// rule's `source` claim and the input to [`this`](Self::this).
    pub fn canonical_source(&self) -> String {
        let mut value = serde_json::to_value(self.descriptor())
            .expect("DeductiveRuleDescriptor always serializes to JSON");
        sort_json_keys(&mut value);
        serde_json::to_string(&value).expect("a serde_json::Value always re-serializes")
    }

    /// This rule's content-addressed identity:
    /// `rule:<base58(blake3(canonical_source))>`.
    ///
    /// A pure function of the rule body, stable across compilations
    /// (via [`canonical_source`](Self::canonical_source)). Used as a
    /// collision-free key for plan caching and as the entity a rule's
    /// facts are stored under. Hashes canonical JSON rather than
    /// dag-cbor because a [`DeductiveRuleDescriptor`]'s premise
    /// propositions don't dag-cbor encode.
    pub fn this(&self) -> Entity {
        use base58::ToBase58;
        let hash = blake3::hash(self.canonical_source().as_bytes());
        let encoded = hash.as_bytes().as_ref().to_base58();
        format!("rule:{encoded}")
            .parse()
            .expect("rule:<base58> is a valid entity URI")
    }
}

/// Recursively sort every JSON object's keys so serialization is a pure
/// function of the value, independent of map iteration order.
fn sort_json_keys(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            let mut sorted = serde_json::Map::new();
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            for key in keys {
                let mut child = map.remove(&key).expect("key present");
                sort_json_keys(&mut child);
                sorted.insert(key, child);
            }
            *map = sorted;
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(sort_json_keys),
        _ => {}
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

impl From<&ConceptDescriptor> for DeductiveRule {
    fn from(concept: &ConceptDescriptor) -> Self {
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
            let value = match field.content_type() {
                Some(ty) => Term::<Any>::typed_var(name, Kind::from(ty)),
                None => Term::var(name),
            };

            let premise: Premise = if field.is_optional() {
                OptionalAttributeQuery::new(
                    Term::Constant(Value::from(field.the().clone())),
                    this.clone(),
                    value,
                    Term::blank(),
                    Some(field.cardinality()),
                )
                .into()
            } else {
                AttributeQuery::new(
                    Term::Constant(Value::from(field.the().clone())),
                    this.clone(),
                    value,
                    Term::blank(),
                    Some(field.cardinality()),
                )
                .into()
            };
            premises.push(premise);
        }

        DeductiveRule::new(concept.clone(), premises).expect("Concept should compile")
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
            a.canonical_source(),
            b.canonical_source(),
            "canonical source is stable across compilations"
        );
        assert!(
            a.this().to_string().starts_with("rule:"),
            "identity is a rule: URI, got {}",
            a.this()
        );
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
