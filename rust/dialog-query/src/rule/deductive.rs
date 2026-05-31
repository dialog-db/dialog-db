/// Serializable rule descriptor matching the formal notation.
pub mod descriptor;

use crate::artifact::Entity;
use crate::attribute::query::AttributeQuery;
pub use crate::concept::descriptor::ConceptDescriptor;
use crate::error::TypeError;
use crate::negation::Negation;
pub use crate::planner::Plan;
pub use crate::planner::{Conjunction, Planner};
pub use crate::premise::Premise;
use crate::rule::{Compile, fmt_rule_schema};
use crate::type_system::Primitive;
use crate::type_system::Type as Kind;
use crate::types::Any;
pub use crate::{Attribute, Cardinality, Parameters, Proposition, Requirement, Value};
use crate::{Environment, Term};
use descriptor::DeductiveRuleDescriptor;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// Conclusion that this rule reaches if all premises hold. This is
    /// typically what datalog calls rule head.
    conclusion: ConceptDescriptor,
    /// Execution plan for the rule's premises, ordered for optimal
    /// evaluation. Produced by [`Planner::plan`] during compilation.
    join: Conjunction,
}
impl Compile for DeductiveRule {
    fn from_parts(conclusion: ConceptDescriptor, join: Conjunction) -> Self {
        DeductiveRule { conclusion, join }
    }
}

impl DeductiveRule {
    /// Compile a rule from a conclusion and premises.
    ///
    /// Plans the optimal premise execution order, validates that every
    /// conclusion variable is grounded by at least one positive premise,
    /// and runs the type-inference check that required head variables
    /// are not bound only by optional (set-widened) sources.
    pub fn new(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Result<Self, TypeError> {
        <Self as Compile>::compile(conclusion, premises)
    }

    /// Returns the conclusion predicate for this rule.
    pub fn conclusion(&self) -> &ConceptDescriptor {
        &self.conclusion
    }

    /// Re-plan this rule's premises against a new scope.
    ///
    /// If replanning with the new bindings fails, falls back to the
    /// original compiled join plan.
    pub fn plan(&self, scope: &Environment) -> Conjunction {
        self.join.plan(scope).unwrap_or_else(|_| self.join.clone())
    }

    /// Returns an iterator over the operand names of this rule's conclusion.
    pub fn operands(&self) -> impl Iterator<Item = &str> {
        self.conclusion.operands()
    }
    /// Returns the names of the parameters for this rule.
    pub fn parameters(&self) -> impl Iterator<Item = &str> {
        self.conclusion.operands()
    }

    /// Creates a rule application by binding the provided terms to this rule's parameters.
    /// Validates that all required parameters are provided and returns an error if the
    /// application would be invalid.
    pub fn apply(&self, parameters: Parameters) -> Result<Proposition, TypeError> {
        self.conclusion.apply(parameters)
    }

    /// Converts this compiled rule back into a serializable [`DeductiveRuleDescriptor`].
    ///
    /// Reconstructs the `when`/`unless` split from the compiled premises.
    pub fn descriptor(&self) -> DeductiveRuleDescriptor {
        let mut when = Vec::new();
        let mut unless = Vec::new();

        for step in &self.join.steps {
            match &step.premise {
                Premise::Assert(proposition) => when.push(proposition.clone()),
                Premise::Unless(Negation(proposition)) => unless.push(proposition.clone()),
            }
        }

        DeductiveRuleDescriptor {
            description: None,
            deduce: self.conclusion.clone(),
            when,
            unless,
        }
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
        fmt_rule_schema(&self.conclusion, f)
    }
}

impl From<&ConceptDescriptor> for DeductiveRule {
    fn from(concept: &ConceptDescriptor) -> Self {
        let mut premises = Vec::new();

        let this = Term::<Entity>::var("this");

        for (name, field) in concept.with().iter() {
            // Required field: standard EAV semantics — a missing fact
            // filters the row out. Optional field: the `is` term is
            // typed set-widened, so `AttributeQuery` runs with
            // `Resolution::Optional` and a missing fact yields a
            // fallback row with the slot bound to `Binding::Absent`.
            let value = if field.is_optional() {
                let kind = match field.content_type() {
                    Some(ty) => Kind::primitive(ty).optional(),
                    None => Kind::primitive_set(Primitive::ALL).optional(),
                };
                Term::<Any>::typed_var(name, kind)
            } else {
                Term::var(name)
            };

            premises.push(
                AttributeQuery::new(
                    Term::Constant(Value::from(field.the().clone())),
                    this.clone(),
                    value,
                    Term::blank(),
                    Some(field.cardinality()),
                )
                .into(),
            );
        }

        DeductiveRule::new(concept.clone(), premises).expect("Concept should compile")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Cause, Entity, Type};
    use crate::attribute::AttributeDescriptor;
    use crate::attribute::query::AttributeQuery;
    use crate::the;
    use crate::types::Any;

    /// Helper: produce an optional `Term<Any>` — this is how an
    /// AttributeQuery is told to run with optional resolution, since
    /// resolution is derived from `is.is_optional()`.
    fn optional_term(name: &str, inner: Option<Type>) -> Term<Any> {
        let kind = match inner {
            Some(ty) => Kind::primitive(ty).optional(),
            None => Kind::primitive_set(Primitive::ALL).optional(),
        };
        Term::<Any>::typed_var(name, kind)
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

        // All terms are variables — no constants at all.
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
        assert_eq!(result.unwrap().join.steps.len(), 2);
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
            // ?z is never bound by any premise — should fail to compile
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

    /// Concept projection emits one premise per `with` attribute,
    /// each with `Resolution::Required` (the default). A concept
    /// with no `maybe` attributes produces only required
    /// premises.
    #[dialog_common::test]
    fn from_concept_with_only_required_emits_required_premises() {
        use crate::Premise;
        use crate::attribute::query::Resolution;
        use crate::proposition::Proposition;

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

        let mut required = 0;
        let mut optional = 0;
        for step in rule.join.steps.iter() {
            if let Premise::Assert(Proposition::Attribute(query)) = &step.premise {
                match query.resolution() {
                    Resolution::Required => required += 1,
                    Resolution::Optional => optional += 1,
                }
            }
        }
        assert_eq!(required, 1, "expected one required premise");
        assert_eq!(optional, 0, "expected no optional premises");
    }

    /// Concept projection emits one premise per required attribute
    /// (Required) and one per optional attribute (Optional).
    #[dialog_common::test]
    fn from_concept_with_optional_field_emits_optional_resolver() {
        use crate::ConceptFieldDescriptor;
        use crate::Premise;
        use crate::attribute::query::Resolution;
        use crate::proposition::Proposition;

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

        let mut required = 0;
        let mut optional = 0;
        for step in rule.join.steps.iter() {
            if let Premise::Assert(Proposition::Attribute(query)) = &step.premise {
                match query.resolution() {
                    Resolution::Required => required += 1,
                    Resolution::Optional => optional += 1,
                }
            }
        }
        assert_eq!(required, 1, "expected one required premise (name)");
        assert_eq!(optional, 1, "expected one optional premise (nickname)");
    }

    /// The degenerate "rule body binds only optionals" shape, at the
    /// concept layer — rejected by construction.
    ///
    /// A concept with zero required (`with`) attributes constrains
    /// nothing, so every entity would match it; a rule built from it
    /// would have a body of only optional premises (each yielding an
    /// Absent fallback on miss). This is unsound, and it is now
    /// *unconstructable*: `ConceptDescriptor::try_from` of an empty
    /// required set returns [`TypeError::EmptyConcept`], so the
    /// degenerate concept can never reach the rule compiler at all.
    /// Optional fields do not change this — only required ones count.
    ///
    /// (A required head bound *only* by an optional premise — the
    /// distinct shape where a `with` field exists but is fed from an
    /// optional source — is caught separately by
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
        use crate::ConceptFieldDescriptor;

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
    /// accept that — the rule could produce an Absent value in a
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
        let this = Term::<Entity>::var("this");
        // Bind ?name with an optional `is` term — the meet for ?name
        // includes Nothing.
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this,
                optional_term("name", None),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
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
    /// bit removed by the meet — at least one premise guarantees
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
        // The `is` slot must carry a typed kind so the meet has
        // information to combine. An untyped `Term::var` produces
        // `None` content_type, which contributes nothing to the
        // meet — the optional side then wins.
        let typed_name: Term<Any> = Term::<String>::var("name").into();
        let optional_name: Term<Any> = Term::<Option<String>>::var("name").into();

        let premises = vec![
            // Optional `is` term: contributes a slot type with Nothing.
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this.clone(),
                optional_name,
                Term::var("cause1"),
                Some(Cardinality::One),
            )
            .into(),
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
            "meet of Required + Optional strips Nothing — should compile (got {:?})",
            result.err()
        );
    }

    /// Symmetric case: an *untyped* Required premise paired with a
    /// typed Optional premise should also strip Nothing from the
    /// meet. The untyped Required contribution is "any present
    /// value" (`Primitive::ALL`), so intersected with
    /// `Optional<String>` (i.e. `{String, Nothing}`) the meet
    /// resolves to `{String}` — no Nothing. Rule compiles.
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
        let optional_name = optional_term("name", Some(Type::String));

        let premises = vec![
            // Optional `is` (typed): contributes `{String, Nothing}`.
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this.clone(),
                optional_name,
                Term::var("cause1"),
                Some(Cardinality::One),
            )
            .into(),
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

    /// The `cause` slot of an Optional attribute query is set-
    /// widened in the schema (since the fallback row binds it to
    /// `Absent`). A rule where a required-head variable shares
    /// its name with such a cause is therefore rejected by the
    /// meet algebra.
    #[dialog_common::test]
    fn it_rejects_required_head_from_optional_cause() {
        // Conclusion has a required `mark` field expecting a
        // typed value (Bytes).
        let conclusion = ConceptDescriptor::try_from(vec![(
            "mark",
            AttributeDescriptor::new(the!("person/mark"), "", Cardinality::One, Some(Type::Bytes)),
        )])
        .unwrap();
        let this = Term::<Entity>::var("this");
        // The optional attribute's cause slot shares the name
        // `?mark` with the conclusion's required head — the
        // meet's cause contribution carries Nothing, so the
        // required head sees Optional.
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this,
                optional_term("name", None),
                Term::<Cause>::var("mark"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        match result {
            Err(TypeError::RequiredHeadFromOptional { variable, .. }) => {
                assert_eq!(variable, "mark");
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
        use crate::constraint::{Coalesce, Constraint};
        use crate::premise::Premise;

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
