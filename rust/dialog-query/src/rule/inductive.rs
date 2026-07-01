//! Inductive rules: assert head facts when the body matches.
//!
//! An inductive rule (a.k.a. *effect*) has the same shape as a
//! [`DeductiveRule`](crate::rule::DeductiveRule) but different
//! evaluation semantics: instead of yielding tuples on query, it
//! commits its head as new facts whenever its body produces
//! bindings. The reactor's fixpoint loop drives evaluation.
//!
//! Compilation reuses the deductive analysis pipeline (planner
//! ordering, unsatisfiable-premise detection, conclusion-variable
//! grounding); the inductive variant adds no extra structural
//! checks today. The two kinds are siblings in the
//! [`Rule`](crate::rule::Rule) enum so the compile-time error
//! types ([`TypeError`](crate::TypeError),
//! [`AnalyzerError`](crate::AnalyzerError)) are uniform.

/// Serializable inductive-rule descriptor.
pub mod descriptor;

use crate::concept::descriptor::ConceptDescriptor;
use crate::error::TypeError;
use crate::negation::Negation;
use crate::planner::{Conjunction, Planner};
use crate::premise::Premise;
use crate::rule::analyzer::AnalyzedRule;
use crate::rule::{Compile, fmt_rule_schema};
use crate::{Environment, Parameters, Proposition};
use descriptor::InductiveRuleDescriptor;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter, Result as FmtResult};

/// An inductive rule that has passed analysis. Assertion-shaped
/// sibling of [`DeductiveRule`](crate::rule::DeductiveRule); holds the
/// analysis and plans per scope.
#[derive(Debug, Clone, PartialEq)]
pub struct InductiveRule {
    /// The narrowed premises, inferred types, and dependency graph
    /// produced by analysis.
    analysis: AnalyzedRule,
}

impl Compile for InductiveRule {
    fn from_analysis(analysis: AnalyzedRule) -> Self {
        InductiveRule { analysis }
    }

    fn in_progress(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Self {
        InductiveRule {
            analysis: AnalyzedRule::in_progress(conclusion, premises),
        }
    }
}

impl InductiveRule {
    /// Analyze a rule from a head concept and body premises into a
    /// verified, plannable rule. Runs the shared analysis pipeline; the
    /// only difference from
    /// [`DeductiveRule::new`](crate::rule::DeductiveRule::new) is what
    /// the evaluator does at runtime.
    pub fn new(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Result<Self, TypeError> {
        <Self as Compile>::compile(conclusion, premises)
    }

    /// The concept this rule asserts when its body matches.
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

    /// Required operand names of the head.
    pub fn required_operands(&self) -> impl Iterator<Item = &str> {
        self.conclusion().required_operands()
    }

    /// Bind concrete parameters into the head and produce the
    /// resulting proposition.
    pub fn apply(&self, parameters: Parameters) -> Result<Proposition, TypeError> {
        self.conclusion().apply(parameters)
    }

    /// Round-trip this rule back to its serializable form.
    pub fn descriptor(&self) -> InductiveRuleDescriptor {
        let mut when = Vec::new();
        let mut unless = Vec::new();

        for premise in &self.analysis.premises {
            match premise {
                Premise::Assert(proposition) => when.push(proposition.clone()),
                Premise::Unless(Negation(proposition)) => unless.push(proposition.clone()),
            }
        }

        InductiveRuleDescriptor {
            description: None,
            assert: self.conclusion().clone(),
            when,
            unless,
        }
    }
}

impl Serialize for InductiveRule {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.descriptor().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for InductiveRule {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let definition = InductiveRuleDescriptor::deserialize(deserializer)?;
        definition.compile().map_err(D::Error::custom)
    }
}

impl Display for InductiveRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        fmt_rule_schema(self.conclusion(), f)
    }
}

#[cfg(test)]
mod tests {
    //! Tests exercise the increment-counter effect: when an
    //! `increment` command targets a counter that already has a
    //! count, the rule asserts a new counter row with `count + 1`.
    //! This mirrors the shape we expect real effects to take:
    //! head differs from any single body premise, formulas
    //! contribute derived values, and the trigger lives on
    //! `effect:system`.

    use super::*;
    use crate::Term;
    use crate::artifact::{Entity, Type, Value};
    use crate::attribute::query::AttributeQuery;
    use crate::attribute::{AttributeDescriptor, Cardinality};
    use crate::formula::Formula;
    use crate::formula::math::Sum;
    use crate::parameters::Parameters;
    use crate::the;

    /// Build the head: a counter row with a `count` field.
    fn counter_head() -> ConceptDescriptor {
        ConceptDescriptor::try_from(vec![(
            "count",
            AttributeDescriptor::new(
                the!("counter/count"),
                "",
                Cardinality::One,
                Some(Type::UnsignedInt),
            ),
        )])
        .unwrap()
    }

    /// Body premises:
    ///   - read the existing counter's count into ?prev
    ///   - derive ?count = ?prev + 1 via math/sum
    fn increment_body() -> Vec<Premise> {
        let this = Term::<Entity>::var("this");
        let mut sum_terms = Parameters::new();
        sum_terms.insert("of".to_string(), Term::var("prev"));
        sum_terms.insert("with".to_string(), Term::constant(1u64));
        sum_terms.insert("is".to_string(), Term::var("count"));
        vec![
            AttributeQuery::new(
                Term::Constant(Value::from(the!("counter/count"))),
                this,
                Term::var("prev"),
                Term::blank(),
                Some(Cardinality::One),
            )
            .into(),
            Sum::apply(sum_terms)
                .expect("Sum::apply should succeed")
                .into(),
        ]
    }

    #[dialog_common::test]
    fn it_compiles_with_valid_premises() {
        let result = InductiveRule::new(counter_head(), increment_body());
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
    }

    #[dialog_common::test]
    fn it_rejects_unbound_head_variable() {
        // Head adds a `name` field that no premise binds.
        let head = ConceptDescriptor::try_from(vec![
            (
                "count",
                AttributeDescriptor::new(
                    the!("counter/count"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
            (
                "name",
                AttributeDescriptor::new(
                    the!("counter/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
        ])
        .unwrap();
        let result = InductiveRule::new(head, increment_body());
        assert!(matches!(result, Err(TypeError::UnboundVariable { .. })));
        if let Err(TypeError::UnboundVariable { variable, .. }) = result {
            assert_eq!(variable, "name");
        }
    }
}
