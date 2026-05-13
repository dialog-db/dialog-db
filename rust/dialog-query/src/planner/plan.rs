use crate::Term;
use crate::attribute::query::AttributeQuery;
use crate::proposition::Proposition;
use crate::rule::types::TypeEnv;
use crate::selection::Selection;
use crate::source::SelectRules;
use crate::types::Any;
use crate::{Environment, Premise};
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use std::sync::Arc;

/// A finalized, ready-to-execute premise produced by the query planner.
///
/// A `Plan` is the lightweight output of a successful [`Candidate`]. It carries
/// only the information needed at execution time: the premise itself, its
/// estimated cost, the variables it will bind, and the variables already
/// bound in the environment. The cached schema and parameter data used during
/// planning are dropped at this point.
///
/// Plans are assembled into a [`Conjunction`](crate::Conjunction) — an ordered sequence of
/// steps that the query engine evaluates to produce results.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// The premise this plan will execute.
    pub premise: Premise,
    /// Estimated execution cost.
    pub cost: usize,
    /// Variables that this plan will bind upon execution.
    pub binds: Environment,
    /// Variables already bound in the environment when this plan runs.
    pub env: Environment,
    /// Shared view of the rule-wide inferred type environment.
    /// Every step of the same rule points at the same `Arc`.
    /// Standalone queries (no rule context) get an empty
    /// environment.
    pub types: Arc<TypeEnv>,
}

impl Plan {
    /// Returns the estimated execution cost.
    pub fn cost(&self) -> usize {
        self.cost
    }

    /// Returns the set of variables this plan will bind.
    pub fn binds(&self) -> &Environment {
        &self.binds
    }

    /// Returns the environment of already-bound variables for this plan.
    pub fn env(&self) -> &Environment {
        &self.env
    }

    /// Evaluate this plan with the given selection and environment.
    ///
    /// Before evaluation, the plan narrows the premise's variable
    /// slots to the kinds inferred for them at the rule level. The
    /// user-supplied premise (carrying the user's local term kinds)
    /// stays untouched; only the in-flight working copy reflects
    /// the rule-inferred kinds. This is how
    /// [`AttributeQuery::evaluate`]'s `is.is_optional()` check
    /// picks up rule-level narrowing — the term it consults is
    /// already the narrowed one.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        let narrowed = apply_types(self.premise, &self.types);
        narrowed.evaluate(selection, env)
    }
}

/// Replace each premise's variable terms with copies whose kinds
/// match the rule-level inferred environment. Currently only
/// `AttributeQuery::is` is rewritten — that's the slot whose
/// `is_optional()` answer drives the planner-visible behavior
/// difference (Absent-fallback emission).
fn apply_types(premise: Premise, types: &TypeEnv) -> Premise {
    if types.is_empty() {
        return premise;
    }
    match premise {
        Premise::Assert(Proposition::Attribute(boxed)) => {
            let query = *boxed;
            let narrowed = narrow_attribute(query, types);
            Premise::Assert(Proposition::Attribute(Box::new(narrowed)))
        }
        other => other,
    }
}

fn narrow_attribute(query: AttributeQuery, types: &TypeEnv) -> AttributeQuery {
    let is_term = query.is().clone();
    let Some(name) = is_term.name() else {
        return query;
    };
    let Some(kind) = types.get(name) else {
        return query;
    };
    let narrowed = Term::<Any>::typed_var(name.to_string(), kind.clone());
    query.with_is(narrowed)
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::Entity;
    use crate::planner::Planner;
    use crate::the;
    use crate::{Cardinality, Term};

    /// A rule where one premise binds `?name` optionally (so its
    /// local `is` term carries `String | Nothing`) and another binds
    /// it required (kind `String`). Rule-level inference narrows
    /// `?name` to `String`. `Plan::apply_types` should hand the
    /// evaluator an `is` term whose kind no longer admits `Nothing`.
    #[dialog_common::test]
    fn it_narrows_optional_is_term_via_rule_inference() {
        let optional_name: Term<Any> = Term::<Option<String>>::var("name").into();
        let typed_name: Term<Any> = Term::<String>::var("name").into();
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/nickname")),
                Term::<Entity>::var("this"),
                optional_name,
                Term::var("cause1"),
                Some(Cardinality::One),
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                typed_name,
                Term::var("cause2"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let plan = Planner::from(premises)
            .plan(&crate::Environment::new())
            .unwrap();

        // The shared TypeEnv should have narrowed `name` to non-
        // optional. Each step's `types` Arc points at it.
        let name_kind = plan
            .steps
            .first()
            .unwrap()
            .types
            .get("name")
            .expect("inferred kind for name");
        assert!(
            !name_kind.is_optional(),
            "inference should strip Nothing when a required binding also exists"
        );

        // `apply_types` rewrites the optional `is` to the narrowed
        // kind for the optional premise. After rewriting, the
        // attribute query's `is` is no longer optional, so the
        // evaluator won't emit Absent-fallback rows.
        for step in plan.steps {
            let narrowed = apply_types(step.premise.clone(), &step.types);
            if let Premise::Assert(Proposition::Attribute(boxed)) = narrowed {
                assert!(
                    !boxed.is().is_optional(),
                    "rule-level narrowing should leave `is` non-optional"
                );
            }
        }
    }
}
