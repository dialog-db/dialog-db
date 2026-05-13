use crate::Term;
use crate::attribute::query::AttributeQuery;
use crate::negation::Negation;
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
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        self.premise.evaluate(selection, env)
    }
}

/// Rewrite a premise to reflect rule-level inferred types.
///
/// Currently only [`AttributeQuery::is`] is rewritten — that's the
/// slot whose `is_optional()` answer drives the planner-visible
/// behavior difference (Absent-fallback emission). Negated
/// attribute queries are walked too, so a negation over an
/// optional attribute picks up the same narrowing as its positive
/// counterpart.
///
/// Called once when a [`Plan`] is built. The user-supplied premise
/// stays untouched; the plan stores the rewritten working copy.
pub(crate) fn apply_types(premise: Premise, types: &TypeEnv) -> Premise {
    if types.is_empty() {
        return premise;
    }
    match premise {
        Premise::Assert(proposition) => {
            Premise::Assert(apply_types_to_proposition(proposition, types))
        }
        Premise::Unless(Negation(proposition)) => {
            Premise::Unless(Negation(apply_types_to_proposition(proposition, types)))
        }
    }
}

fn apply_types_to_proposition(proposition: Proposition, types: &TypeEnv) -> Proposition {
    match proposition {
        Proposition::Attribute(boxed) => {
            let query = *boxed;
            Proposition::Attribute(Box::new(narrow_attribute(query, types)))
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
    /// local `is` term carries `String | Nothing`) and another
    /// binds it required (kind `String`). Rule-level inference
    /// narrows `?name` to `String`, and the planner stamps the
    /// narrowed term into each plan step's premise so the
    /// evaluator's `is.is_optional()` check returns `false`.
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

        // Every plan step's stored premise already has the
        // narrowed `is` — the rewrite happens at plan time.
        for step in plan.steps {
            if let Premise::Assert(Proposition::Attribute(boxed)) = &step.premise {
                assert!(
                    !boxed.is().is_optional(),
                    "rule-level narrowing should leave `is` non-optional"
                );
            }
        }
    }

    /// `apply_types` reaches into negated propositions too. A
    /// negated attribute query that references an inferred
    /// variable should see its `is` rewritten to match the env.
    #[dialog_common::test]
    fn it_narrows_negated_attribute_via_rule_inference() {
        use crate::artifact::Type as ValueType;
        use crate::negation::Negation;
        use crate::type_system::Type as Kind;

        let env = {
            // Build an env via the public path: a one-premise rule
            // with a typed Term<String>. The single binding
            // dictates the inferred kind.
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
            let plan = Planner::from(premises)
                .plan(&crate::Environment::new())
                .unwrap();
            plan.steps.into_iter().next().unwrap().types
        };

        // Build a negated attribute query that uses `?name` with
        // the still-optional local term kind.
        let optional_name: Term<Any> = Term::<Option<String>>::var("name").into();
        let neg = AttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("this"),
            optional_name,
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let original = Premise::Unless(Negation(Proposition::Attribute(Box::new(neg))));

        let narrowed = apply_types(original, &env);
        if let Premise::Unless(Negation(Proposition::Attribute(boxed))) = narrowed {
            assert!(
                !boxed.is().is_optional(),
                "rule-level narrowing should reach into negated attributes"
            );
            assert_eq!(
                boxed.is().kind().and_then(|k| k.as_value_type()),
                Some(ValueType::String),
                "narrowed `is` should carry the inferred String kind"
            );
            let _ = Kind::primitive(ValueType::String);
        } else {
            panic!("expected negated attribute proposition");
        }
    }
}
