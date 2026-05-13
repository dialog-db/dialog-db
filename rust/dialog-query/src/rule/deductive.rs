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
use crate::rule::analyzer::{self, AnalysisError};
use crate::type_system::Primitive;
use crate::type_system::Type as Kind;
use crate::types::Any;
pub use crate::{Attribute, Cardinality, Parameters, Proposition, Requirement, Value};
use crate::{Environment, Term, Type};
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
impl DeductiveRule {
    /// Compile a rule from a conclusion and premises.
    ///
    /// Plans the optimal premise execution order, validates that every
    /// conclusion variable is grounded by at least one positive premise,
    /// and runs the meet-algebra check that required head variables
    /// are not bound only by optional (set-widened) sources.
    pub fn new(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Result<Self, TypeError> {
        // Plan the order of premises in a scope where none of the rule
        // parameters are bound to find the optimal execution order, or to
        // discover unsatisfiable premises (e.g. a formula whose required
        // cell is never derived by another premise).
        let join = Planner::from(premises).plan(&Environment::new())?;

        // Run rule-level type analysis: inference + required-head
        // check + Coalesce contract validation. Failures here are
        // wrapped into the corresponding `TypeError::*` variants so
        // the user sees the rule embedded in the error.
        if let Err(err) = analyzer::analyze(conclusion.clone(), &join.steps) {
            let rule = DeductiveRule { conclusion, join };
            return Err(match err {
                AnalysisError::RequiredHeadFromOptional { variable } => {
                    TypeError::RequiredHeadFromOptional {
                        rule: Box::new(rule),
                        variable,
                    }
                }
                AnalysisError::CoalesceTypeMismatch { reason } => TypeError::CoalesceTypeMismatch {
                    rule: Box::new(rule),
                    reason,
                },
            });
        }

        let rule = DeductiveRule { conclusion, join };

        // Verify that every conclusion parameter is derived by one of the
        // premises; otherwise the rule could never fully bind its output.
        // This check depends on the planner's `binds` set, so it lives
        // here rather than in analysis.
        let unbound = rule
            .conclusion
            .operands()
            .find(|name| !rule.join.binds.contains(name))
            .map(String::from);

        if let Some(variable) = unbound {
            return Err(TypeError::UnboundVariable {
                rule: Box::new(rule),
                variable,
            });
        }

        Ok(rule)
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
        write!(f, "{} {{", self.conclusion.this())?;
        write!(f, "this: {},", Type::Entity)?;
        for (name, attribute) in self.conclusion.with().iter() {
            match attribute.content_type() {
                Some(ty) => write!(f, "{}: {},", name, ty)?,
                None => write!(f, "{}: Any,", name)?,
            }
        }
        write!(f, "}}")
    }
}

impl From<&ConceptDescriptor> for DeductiveRule {
    fn from(concept: &ConceptDescriptor) -> Self {
        let mut premises = Vec::new();

        let this = Term::<Entity>::var("this");

        // Required (`with`) attributes — standard EAV semantics.
        // A missing fact filters the row out entirely.
        for (name, attribute) in concept.with().iter() {
            premises.push(
                AttributeQuery::new(
                    Term::Constant(Value::from(attribute.the().clone())),
                    this.clone(),
                    Term::var(name),
                    Term::blank(),
                    Some(attribute.cardinality()),
                )
                .into(),
            );
        }

        // Optional (`maybe`) attributes — a missing fact yields a
        // fallback row with the slot bound to `Binding::Absent`.
        // We encode this by typing the `is` term as optional;
        // `AttributeQuery` derives its resolution from that kind.
        if let Some(maybe) = concept.maybe() {
            for (name, attribute) in maybe.iter() {
                let kind = match attribute.content_type() {
                    Some(ty) => Kind::primitive(ty).optional(),
                    None => Kind::primitive_set(Primitive::ALL).optional(),
                };
                premises.push(
                    AttributeQuery::new(
                        Term::Constant(Value::from(attribute.the().clone())),
                        this.clone(),
                        Term::<Any>::typed_var(name, kind),
                        Term::blank(),
                        Some(attribute.cardinality()),
                    )
                    .into(),
                );
            }
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
        let conclusion = ConceptDescriptor::from(vec![
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
        ]);
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
        let conclusion = ConceptDescriptor::from(vec![
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
        ]);
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
        let conclusion = ConceptDescriptor::from(vec![
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
        ]);

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
        let conclusion = ConceptDescriptor::from(vec![
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
        ]);
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
        let conclusion = ConceptDescriptor::from(vec![
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
        ]);
        assert!(DeductiveRule::new(conclusion, vec![]).is_err());
    }

    #[dialog_common::test]
    fn it_compiles_with_chained_dependencies() {
        let conclusion = ConceptDescriptor::from(vec![
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
        ]);
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
        let conclusion = ConceptDescriptor::from(vec![(
            "key",
            AttributeDescriptor::new(the!("result/key"), "", Cardinality::One, Some(Type::String)),
        )]);

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
        let conclusion = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

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
        let conclusion = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

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

        let concept = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

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

    /// Concept projection emits one premise per `with` attribute
    /// (Required) and one per `maybe` attribute (Optional).
    #[dialog_common::test]
    fn from_concept_with_maybe_emits_optional_resolver() {
        use crate::Premise;
        use crate::attribute::query::Resolution;
        use crate::proposition::Proposition;

        let concept = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .with_maybe(vec![(
            "nickname",
            AttributeDescriptor::new(
                the!("person/nickname"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

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

    /// `with_maybe` builder installs the maybe attributes; an
    /// empty input clears the maybe slot.
    #[dialog_common::test]
    fn with_maybe_installs_and_clears() {
        let concept = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);
        assert!(concept.maybe().is_none(), "no maybe by default");

        let with_maybe = concept.clone().with_maybe(vec![(
            "nickname",
            AttributeDescriptor::new(
                the!("person/nickname"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);
        assert!(with_maybe.maybe().is_some(), "maybe installed");
        assert_eq!(with_maybe.maybe().unwrap().iter().count(), 1);

        let empty: Vec<(&str, AttributeDescriptor)> = Vec::new();
        let cleared = with_maybe.with_maybe(empty);
        assert!(cleared.maybe().is_none(), "empty input clears maybe");
    }

    /// A conclusion variable bound only by an optional attribute
    /// query carries `Nothing` in its meet. Required heads cannot
    /// accept that — the rule could produce an Absent value in a
    /// required slot. Reject.
    #[dialog_common::test]
    fn it_rejects_required_head_from_optional_premise() {
        let conclusion = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);
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
    fn it_accepts_required_head_when_meet_strips_nothing() {
        let conclusion = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);
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
        let conclusion = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);
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
        let conclusion = ConceptDescriptor::from(vec![(
            "mark",
            AttributeDescriptor::new(the!("person/mark"), "", Cardinality::One, Some(Type::Bytes)),
        )]);
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

        let conclusion = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);
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
