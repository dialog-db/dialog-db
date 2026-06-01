//! Rule analysis — the phase between parsing and planning.
//!
//! The rule pipeline has three phases:
//!
//! 1. **Parse.** A `DeductiveRuleDescriptor` carries the user's
//!    syntactic form: a conclusion and a list of premises with
//!    user-supplied terms.
//! 2. **Analyze.** Type inference runs against the premises'
//!    slots; type errors (`RequiredHeadFromOptional`, Coalesce
//!    contract violations) surface here. The output is an
//!    `AnalyzedRule` — every premise carries a shared, read-only
//!    view of the rule-wide [`TypeEnv`], plus a dependency graph
//!    describing which premise binds and which reads each variable.
//! 3. **Plan.** Given an `AnalyzedRule` and a scope of already-bound
//!    variables, the planner orders the premises by cost and
//!    produces a `Conjunction` ready for execution.
//!
//! Analysis is rule-scoped; the result is immutable. Planning can
//! run repeatedly against the same analyzed rule with different
//! scopes (the use case is concept-rule replanning when caller
//! bindings change).

use crate::Premise;
use crate::concept::descriptor::ConceptDescriptor;
use crate::constraint::Constraint;
use crate::planner::Plan;
use crate::proposition::Proposition;
use crate::rule::types::TypeEnv;
use crate::schema::Requirement;
use crate::type_system::Type as Kind;
use crate::type_system::unifier::Context;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

/// Variable-usage information for a single premise.
///
/// Computed during analysis by walking the premise's schema and
/// parameters: a variable is in `binds` if the premise will produce
/// a value for it (positive premise; non-blank parameter slot), and
/// in `needs` if the premise reads it without binding (e.g. a
/// required slot the premise can't satisfy on its own, or a value
/// it has to look up). A variable can appear in both sets when the
/// schema lists it under multiple slots with mixed requirements.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PremiseVars {
    /// Variables this premise will bind upon execution.
    pub binds: BTreeSet<String>,
    /// Variables this premise needs already bound to execute.
    pub needs: BTreeSet<String>,
}

/// Per-premise variable usage plus precomputed dependency edges.
///
/// `requires[i]` is the set of premise indices that must execute
/// before premise `i` because they bind a variable in `needs[i]`.
/// The graph respects the user's original premise order: a premise
/// that binds a variable can satisfy any later premise's need for
/// it, regardless of cost. Reordering happens during planning, not
/// here.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DependencyGraph {
    /// Per-premise variable usage in the original order.
    pub usage: Vec<PremiseVars>,
    /// `requires[i]` lists the premise indices `j` such that
    /// premise `j` binds a variable premise `i` needs.
    pub requires: Vec<BTreeSet<usize>>,
}

impl DependencyGraph {
    /// Returns `true` if no premises were analyzed.
    pub fn is_empty(&self) -> bool {
        self.usage.is_empty()
    }

    /// Number of premises in the graph.
    pub fn len(&self) -> usize {
        self.usage.len()
    }

    /// Compute the dependency graph for a sequence of planned steps.
    /// Walks each step's schema once: a non-blank named parameter
    /// goes into `binds` if the schema field is `Optional` or part
    /// of a satisfied choice group, otherwise into `needs`. Choice
    /// groups satisfied by a constant or already-bound parameter
    /// don't contribute to `needs`.
    pub fn from_steps(steps: &[Plan]) -> Self {
        let mut usage = Vec::with_capacity(steps.len());

        for step in steps {
            let is_negation = matches!(step.as_premise(), Premise::Unless(_));
            let schema = step.schema();
            let params = step.parameters();

            // Identify choice groups satisfied by a constant in
            // this step's parameters.
            let mut satisfied_groups = HashSet::new();
            for (slot_name, field) in schema.iter() {
                if let Some(param) = params.get(slot_name)
                    && let Requirement::Required(Some(group)) = &field.requirement
                    && param.is_constant()
                {
                    satisfied_groups.insert(*group);
                }
            }

            let mut vars = PremiseVars::default();
            for (slot_name, field) in schema.iter() {
                let Some(param) = params.get(slot_name) else {
                    continue;
                };
                if param.is_constant() || param.is_blank() {
                    continue;
                }
                let Some(name) = param.name() else {
                    continue;
                };
                match &field.requirement {
                    Requirement::Required(None) => {
                        vars.needs.insert(name.to_string());
                    }
                    Requirement::Required(Some(group)) => {
                        if satisfied_groups.contains(group) {
                            if !is_negation {
                                vars.binds.insert(name.to_string());
                            }
                        } else {
                            vars.needs.insert(name.to_string());
                        }
                    }
                    Requirement::Optional => {
                        if !is_negation {
                            vars.binds.insert(name.to_string());
                        }
                    }
                }
            }
            usage.push(vars);
        }

        // For each premise i, find every other premise j (j != i)
        // that binds something i needs.
        let mut requires = vec![BTreeSet::new(); usage.len()];
        for (i, vars) in usage.iter().enumerate() {
            for need in &vars.needs {
                for (j, other) in usage.iter().enumerate() {
                    if j != i && other.binds.contains(need) {
                        requires[i].insert(j);
                    }
                }
            }
        }

        Self { usage, requires }
    }
}

/// Errors detected by [`analyze`]. These are pre-rule type
/// problems: inference produced a contradiction or a constraint's
/// type contract isn't satisfied. `DeductiveRule::new` wraps these
/// in the full [`TypeError`](crate::error::TypeError) variants
/// after planning, so callers get the rule embedded for display.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum AnalysisError {
    /// Type inference produced a contradiction — see
    /// [`InferenceError`](super::types::InferenceError).
    #[error("type inference failed: {reason}")]
    Inference {
        /// Description of the conflict.
        reason: String,
    },
    /// A conclusion variable's inferred type admits `Nothing` —
    /// the rule could produce `Absent` in a required slot.
    #[error("conclusion variable {variable} is optional")]
    RequiredHeadFromOptional {
        /// Name of the offending head variable.
        variable: String,
    },
    /// A `Coalesce` constraint's type contract is violated.
    #[error("Coalesce type mismatch: {reason}")]
    CoalesceTypeMismatch {
        /// Human-readable reason from the unifier.
        reason: String,
    },
}

/// A rule that has passed analysis. Carries the conclusion, the
/// original premises in their planned order, the rule-wide
/// inferred type environment, and a per-premise dependency graph.
///
/// This is the artifact of analysis. The planner reads it (or its
/// pieces) to build per-step plans; future iterations of the
/// planner can consume the dependency graph directly to avoid
/// re-walking schemas on every iteration.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedRule {
    /// The rule's conclusion.
    pub conclusion: ConceptDescriptor,
    /// The premises in their planned order. Analysis preserves
    /// the planner's ordering; it doesn't re-order on its own.
    pub premises: Vec<Premise>,
    /// The rule-wide inferred type environment. Shared via
    /// [`Arc`] across consumers.
    pub types: Arc<TypeEnv>,
    /// Per-premise variable usage and dependency edges. Indexed
    /// in the same order as `premises`.
    pub graph: DependencyGraph,
}

impl AnalyzedRule {
    /// Iterate over the analyzed premises.
    pub fn premises(&self) -> impl Iterator<Item = &Premise> {
        self.premises.iter()
    }

    /// Look up a variable's inferred type by name.
    pub fn type_of(&self, name: &str) -> Option<&Kind> {
        self.types.get(name)
    }
}

/// Run analysis over the rule's premises:
///
/// 1. Infer the rule-wide type environment from each premise's
///    slot kinds.
/// 2. Check that no conclusion variable's inferred type admits
///    `Nothing` — a required head can't accept `Absent`.
/// 3. Validate every Coalesce constraint's type contract.
///
/// Returns the analyzed rule on success, or an [`AnalysisError`]
/// describing the type problem. `DeductiveRule::new` wraps these
/// in the corresponding [`TypeError`](crate::error::TypeError)
/// variants with the planned rule embedded for display.
pub fn analyze(
    conclusion: ConceptDescriptor,
    steps: &[Plan],
) -> Result<AnalyzedRule, AnalysisError> {
    let types = Arc::new(
        TypeEnv::infer(steps).map_err(|err| AnalysisError::Inference {
            reason: err.to_string(),
        })?,
    );

    // Required heads must not admit `Nothing`.
    if let Some(variable) = conclusion
        .operands()
        .find(|name| types.get(name).is_some_and(Kind::is_optional))
    {
        return Err(AnalysisError::RequiredHeadFromOptional {
            variable: variable.to_string(),
        });
    }

    // Each Coalesce constraint validates against a fresh unifier
    // context. Catches wire-format and raw-builder mismatches
    // where the typed builder isn't the construction path.
    for step in steps {
        let Premise::Assert(Proposition::Constraint(Constraint::Coalesce(coalesce))) =
            step.as_premise()
        else {
            continue;
        };
        let mut ctx = Context::new();
        if let Err(err) = coalesce.validate(&mut ctx) {
            return Err(AnalysisError::CoalesceTypeMismatch {
                reason: err.to_string(),
            });
        }
    }

    let premises = steps.iter().map(|step| step.as_premise()).collect();
    let graph = DependencyGraph::from_steps(steps);
    Ok(AnalyzedRule {
        conclusion,
        premises,
        types,
        graph,
    })
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::{Entity, Type as ValueType};
    use crate::attribute::AttributeDescriptor;
    use crate::attribute::query::AttributeQuery;
    use crate::planner::Planner;
    use crate::the;
    use crate::types::Any;
    use crate::{Cardinality, Environment, Term};

    fn person_with_name() -> ConceptDescriptor {
        ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(ValueType::String),
            ),
        )])
        .unwrap()
    }

    /// Analysis output carries the inferred type environment.
    #[dialog_common::test]
    fn it_analyzes_a_single_typed_premise() {
        let typed_name: Term<Any> = Term::<String>::var("name").into();
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                typed_name,
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();
        let analyzed = analyze(person_with_name(), &plan.steps).unwrap();

        assert_eq!(analyzed.premises.len(), 1);
        let name_kind = analyzed.type_of("name").expect("name has an inferred type");
        assert_eq!(name_kind.as_value_type(), Some(ValueType::String));
    }

    /// Two premises that share a variable `?entity`: one binds it
    /// (the entity slot is a free variable), and one needs it (the
    /// entity slot is the same variable, satisfied by the first
    /// premise binding it). The graph records the dependency edge.
    #[dialog_common::test]
    fn it_records_dependency_edge_between_premises() {
        let q1 = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("entity"),
            Term::<String>::var("name").into(),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let q2 = AttributeQuery::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("entity"),
            Term::<Any>::var("age"),
            Term::var("cause2"),
            Some(Cardinality::One),
        );
        let premises = vec![q1.into(), q2.into()];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();
        let analyzed = analyze(person_with_name(), &plan.steps).unwrap();

        assert_eq!(analyzed.graph.len(), 2);

        // Each step's vars include `entity` (the schema groups
        // the/of/is/cause as one choice; with a constant `the`,
        // the group is satisfied, so all of them are binds).
        for vars in &analyzed.graph.usage {
            assert!(vars.binds.contains("entity"));
        }
    }

    /// A formula premise that needs `?text` and binds `?len`
    /// depends on an earlier attribute premise that binds `?text`.
    /// The dependency graph records an edge from the formula's
    /// `requires[]` to the attribute's index.
    #[dialog_common::test]
    fn it_records_formula_dependency_on_attribute() {
        use crate::formula::Formula;
        use crate::formula::string::Length;
        use crate::{Parameters, Premise};

        // Premise 0: attribute query that binds ?text.
        let attr = AttributeQuery::new(
            Term::from(the!("note/body")),
            Term::<Entity>::var("this"),
            Term::<String>::var("text").into(),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        // Premise 1: Length formula, needs ?text bound, binds ?len.
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::var("text"));
        params.insert("is".to_string(), Term::var("len"));
        let length = Length::apply(params).unwrap();

        let premises: Vec<Premise> = vec![attr.into(), Premise::from(length)];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();
        let analyzed = analyze(person_with_name(), &plan.steps).unwrap();

        assert_eq!(analyzed.graph.len(), 2);

        // The formula step needs `text`; some other step binds it.
        // Concretely: the attribute step's index should appear in
        // the formula step's `requires` set.
        //
        // The planner orders attribute first (cost), so attribute
        // is step 0 and formula is step 1. The formula needs
        // ?text bound by step 0.
        let formula_idx = analyzed
            .premises
            .iter()
            .position(|p| matches!(p, Premise::Assert(crate::Proposition::Formula(_))))
            .expect("formula step present");
        let attr_idx = analyzed
            .premises
            .iter()
            .position(|p| matches!(p, Premise::Assert(crate::Proposition::Attribute(_))))
            .expect("attribute step present");

        assert!(
            analyzed.graph.requires[formula_idx].contains(&attr_idx),
            "formula step should depend on the attribute step that binds ?text"
        );
        assert!(
            analyzed.graph.usage[formula_idx].needs.contains("text"),
            "formula step should record ?text in its needs"
        );
        assert!(
            analyzed.graph.usage[attr_idx].binds.contains("text"),
            "attribute step should record ?text in its binds"
        );
    }

    /// Analysis rejects a conclusion variable whose inferred type
    /// admits `Nothing` — the rule could yield Absent in the head.
    #[dialog_common::test]
    fn it_rejects_required_head_bound_only_by_optional_premises() {
        let optional_name: Term<Any> = Term::<Option<String>>::var("name").into();
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                optional_name,
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();
        let err = analyze(person_with_name(), &plan.steps).unwrap_err();
        match err {
            AnalysisError::RequiredHeadFromOptional { variable } => {
                assert_eq!(variable, "name");
            }
            other => panic!("expected RequiredHeadFromOptional, got {other:?}"),
        }
    }
}
