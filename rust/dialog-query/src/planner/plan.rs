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

/// A finalized, ready-to-execute premise produced by the query planner.
///
/// A `Plan` is the lightweight output of a successful [`Candidate`]. It carries
/// only the information needed at execution time: the premise itself, its
/// estimated cost, the variables it will bind, and the variables already
/// bound in the environment. The cached schema and parameter data used during
/// planning are dropped at this point.
///
/// The premise has already been narrowed at plan time via
/// [`apply_types`] — its variable terms reflect the rule-level
/// inferred kinds, so evaluators don't need to consult any external
/// type environment to know which slots are optional.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// The premise this plan will execute. Variable terms have
    /// been narrowed to the rule-level inferred kinds.
    pub premise: Premise,
    /// Estimated execution cost.
    pub cost: usize,
    /// Variables that this plan will bind upon execution.
    pub binds: Environment,
    /// Variables already bound in the environment when this plan runs.
    pub env: Environment,
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

        // Every plan step's stored premise has the narrowed `is` —
        // the rewrite happens at plan time. The first premise's
        // user-supplied `is` was `Option<String>`; after planning
        // it should be `String` only.
        for step in plan.steps {
            if let Premise::Assert(Proposition::Attribute(boxed)) = &step.premise {
                assert!(
                    !boxed.is().is_optional(),
                    "rule-level narrowing should leave `is` non-optional"
                );
            }
        }
    }

    /// End-to-end: with rule-level narrowing applied, an optional
    /// attribute query whose `?nick` is narrowed to non-optional by
    /// a sibling premise no longer emits Absent-fallback rows.
    ///
    /// The test asserts a stronger fact than "rows are filtered":
    /// it walks the plan's stored premises and verifies the
    /// optional attribute's `is` term has been rewritten to a
    /// non-optional kind. Without that rewrite, the attribute's
    /// `evaluate` would emit one Absent fallback row per entity
    /// without a nickname.
    #[dialog_common::test]
    fn it_suppresses_absent_fallback_under_rule_inference() {
        let optional_nick: Term<Any> = Term::<Option<String>>::var("nick").into();
        let typed_nick: Term<Any> = Term::<String>::var("nick").into();
        let nickname_query = AttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("this"),
            optional_nick,
            Term::var("cause1"),
            Some(Cardinality::One),
        );
        let name_query = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("this"),
            typed_nick,
            Term::var("cause2"),
            Some(Cardinality::One),
        );

        // Before planning, the nickname query's `is` is optional.
        assert!(
            nickname_query.is().is_optional(),
            "the local optional kind is preserved on the user's query"
        );

        let plan = Planner::from(vec![nickname_query.into(), name_query.into()])
            .plan(&crate::Environment::new())
            .unwrap();

        // After planning, every attribute step that references
        // `?nick` should have an `is` term whose kind is no
        // longer optional — the narrowing was applied at plan
        // time, not deferred to evaluation.
        let mut narrowed_count = 0;
        for step in &plan.steps {
            if let Premise::Assert(Proposition::Attribute(boxed)) = &step.premise
                && boxed.is().name() == Some("nick")
            {
                assert!(
                    !boxed.is().is_optional(),
                    "rule-level narrowing should have stripped Nothing from ?nick"
                );
                narrowed_count += 1;
            }
        }
        assert_eq!(
            narrowed_count, 2,
            "both attribute steps reference ?nick and should both be checked"
        );
    }

    /// A standalone query (single optional premise, nothing
    /// constraining it further) keeps its `is` term's optional
    /// kind through planning. The rewrite doesn't strip
    /// optionality when inference didn't strip it either.
    #[dialog_common::test]
    fn it_preserves_local_optionality_when_no_other_premise_narrows() {
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
        let plan = Planner::from(premises)
            .plan(&crate::Environment::new())
            .unwrap();

        if let Premise::Assert(Proposition::Attribute(boxed)) = &plan.steps[0].premise {
            assert!(
                boxed.is().is_optional(),
                "single optional premise should keep its optional `is`"
            );
        }
    }

    /// Replanning a `Conjunction` preserves the rule-level
    /// narrowing. The fresh inference pass over already-narrowed
    /// premises is idempotent and yields the same non-optional
    /// `is` kinds.
    #[dialog_common::test]
    fn it_preserves_narrowing_across_replans() {
        use crate::planner::Conjunction;
        let optional_name: Term<Any> = Term::<Option<String>>::var("nick").into();
        let typed_name: Term<Any> = Term::<String>::var("nick").into();
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

        // Replan against a new scope where `this` is bound.
        let mut scope = crate::Environment::new();
        scope.add("this");
        let replanned: Conjunction = plan.plan(&scope).unwrap();

        // The replanned steps still have the narrowed `is`.
        for step in &replanned.steps {
            if let Premise::Assert(Proposition::Attribute(boxed)) = &step.premise
                && boxed.is().name() == Some("nick")
            {
                assert!(
                    !boxed.is().is_optional(),
                    "narrowing must survive replanning"
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
            TypeEnv::infer(&plan.steps).unwrap()
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
