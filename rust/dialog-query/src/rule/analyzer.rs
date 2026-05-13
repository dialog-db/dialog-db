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
            let is_negation = matches!(step.premise, Premise::Unless(_));
            let schema = step.premise.schema();
            let params = step.premise.parameters();

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

/// A piece of the user's rule with the rule-wide type environment
/// attached. The `source` is whatever the user wrote; `types` is the
/// inferred environment for the rule containing it. Evaluators
/// that want a variable's inferred kind ask the environment by name.
///
/// `Analyzed` is the common shape for premises after analysis. The
/// rule-wide [`TypeEnv`] is shared via [`Arc`] so each premise gets
/// a cheap clone-able view rather than its own projection.
#[derive(Debug, Clone, PartialEq)]
pub struct Analyzed<T> {
    /// The user-supplied form.
    pub source: T,
    /// The rule-wide type environment. Shared across every
    /// `Analyzed` in the same rule.
    pub types: Arc<TypeEnv>,
}

impl<T> Analyzed<T> {
    /// Wrap `source` with a shared type environment.
    pub fn new(source: T, types: Arc<TypeEnv>) -> Self {
        Self { source, types }
    }

    /// Map the wrapped value while preserving the shared environment.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Analyzed<U> {
        Analyzed {
            source: f(self.source),
            types: self.types,
        }
    }

    /// Borrow the wrapped value alongside the shared environment.
    pub fn as_ref(&self) -> Analyzed<&T> {
        Analyzed {
            source: &self.source,
            types: self.types.clone(),
        }
    }
}

/// A rule that has passed analysis. Carries the conclusion, the
/// analyzed premises (each with a shared view of the rule-wide
/// type environment), and the type environment itself.
///
/// This is the artifact the planner consumes. It is immutable: the
/// planner doesn't modify the analyzed rule, it reads from it to
/// build a [`Conjunction`](crate::planner::Conjunction) ordered for
/// a given scope.
///
/// The dependency graph (which premise binds which variable, which
/// premises require what) is computed during analysis and will be
/// added in a follow-up step; the planner currently still derives
/// it on the fly.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedRule {
    /// The rule's conclusion.
    pub conclusion: ConceptDescriptor,
    /// The analyzed premises in their original order. Planning may
    /// reorder them; analysis preserves user order.
    pub premises: Vec<Analyzed<Premise>>,
    /// The rule-wide inferred type environment.
    pub types: Arc<TypeEnv>,
    /// Per-premise variable usage and dependency edges. Indexed in
    /// the same order as `premises`.
    pub graph: DependencyGraph,
}

impl AnalyzedRule {
    /// Iterate over the analyzed premises.
    pub fn premises(&self) -> impl Iterator<Item = &Analyzed<Premise>> {
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
    let types = Arc::new(TypeEnv::infer(steps));

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
            &step.premise
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

    let premises = steps
        .iter()
        .map(|step| Analyzed::new(step.premise.clone(), types.clone()))
        .collect();
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
        ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(ValueType::String),
            ),
        )])
    }

    /// Analysis output carries the inferred type environment and
    /// wraps each premise with a clone of the shared `Arc<TypeEnv>`.
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

        // Every premise in the same rule shares the same Arc.
        for premise in analyzed.premises() {
            assert!(Arc::ptr_eq(&premise.types, &analyzed.types));
        }
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
