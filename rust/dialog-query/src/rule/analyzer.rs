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
use crate::planner::Plan;
use crate::rule::types::TypeEnv;
use crate::type_system::Type as Kind;
use std::sync::Arc;

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
    /// The analyzed premises in their original order. Planning may
    /// reorder them; analysis preserves user order.
    pub premises: Vec<Analyzed<Premise>>,
    /// The rule-wide inferred type environment.
    pub types: Arc<TypeEnv>,
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

/// Run analysis over the rule's premises: infer types, build the
/// shared environment, wrap each premise as an `Analyzed<Premise>`.
///
/// This is currently a thin wrapper around [`TypeEnv::infer`] that
/// runs against a `Vec<Plan>` shape (because that's what `infer`
/// consumes today). It will gain the dependency-graph computation
/// and richer error reporting in follow-up commits.
///
/// Returns the analyzed rule. Type errors detected during analysis
/// (e.g. `RequiredHeadFromOptional`) are not raised here yet —
/// `DeductiveRule::new` still owns those checks. Moving them into
/// the analyzer is a later step.
pub fn analyze(steps: &[Plan]) -> AnalyzedRule {
    let types = Arc::new(TypeEnv::infer(steps));
    let premises = steps
        .iter()
        .map(|step| Analyzed::new(step.premise.clone(), types.clone()))
        .collect();
    AnalyzedRule { premises, types }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::{Entity, Type as ValueType};
    use crate::attribute::query::AttributeQuery;
    use crate::planner::Planner;
    use crate::the;
    use crate::types::Any;
    use crate::{Cardinality, Environment, Term};

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
        let analyzed = analyze(&plan.steps);

        assert_eq!(analyzed.premises.len(), 1);
        let name_kind = analyzed.type_of("name").expect("name has an inferred type");
        assert_eq!(name_kind.as_value_type(), Some(ValueType::String));

        // Every premise in the same rule shares the same Arc.
        for premise in analyzed.premises() {
            assert!(Arc::ptr_eq(&premise.types, &analyzed.types));
        }
    }
}
