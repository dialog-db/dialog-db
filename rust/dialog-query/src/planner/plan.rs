use super::candidate::categorize;
use crate::attribute::query::{AttributeQuery, DynamicAttributeQuery};
use crate::concept::query::ConceptQuery;
use crate::constraint::Constraint;
use crate::formula::query::FormulaQuery;
use crate::negation::Negation;
use crate::proposition::Proposition;
use crate::query::Application;
use crate::rule::types::TypeEnv;
use crate::selection::Selection;
use crate::source::SelectRules;
use crate::try_stream;
use crate::{Environment, Parameters, Premise, Schema};
use core::pin::Pin;
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use futures_util::TryStreamExt;
use futures_util::future::Either;
use std::collections::BTreeSet;

/// The variables a step will bind once it runs under a given entry
/// adornment.
///
/// This is the SIPS function `f` from the magic-sets literature
/// (Balbin et al.): the set of variables a body literal passes onward
/// once the literals before it have bound their inputs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Binds(pub BTreeSet<String>);

/// Why a step cannot run yet under the current bindings.
///
/// The feasibility verdict ([`Plan::adorn`]) returns this in the
/// `Err` case so the planner — and later demand reification — knows
/// *which* variables a step is still waiting on, not merely that it is
/// blocked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Infeasible {
    /// All of these still-unbound variables must be bound before the
    /// step can run. Mirrors the planner's `requires` set: a choice
    /// group already satisfied (by a constant or a bound variable)
    /// contributes nothing here.
    NeedsAll(BTreeSet<String>),
}

/// Planning metadata shared by every [`Plan`] variant.
///
/// Carries what the planner needs to schedule and re-plan a step, kept
/// alongside the lowered execution payload in each variant. The
/// syntactic premise is *not* stored: it is reconstructed on demand
/// from the variant payload via [`Plan::as_premise`], since the
/// payload is the lowered (and already type-narrowed) form of the same
/// query.
#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    /// Estimated execution cost.
    pub cost: usize,
    /// Variables that this step will bind upon execution.
    pub binds: Environment,
    /// Variables already bound in the environment when this step runs.
    pub env: Environment,
}

/// A finalized, ready-to-execute query step produced by the planner.
///
/// A `Plan` is the lightweight output of a successful [`Candidate`].
/// It is both the compiled operator (the variant selects the kind of
/// work and owns the lowered query) and the carrier of planning
/// metadata (via [`Header`]). Lowering from the syntactic AST happens
/// once, in [`Planner::plan`](crate::Planner), after type inference;
/// `evaluate` then dispatches on the variant rather than walking the
/// AST.
///
/// Leaf variants wrap the concrete query types that own the actual
/// stream logic; [`Negate`](Plan::Negate) wraps a nested `Plan` so
/// negation filters against the lowered inner step.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// Positive attribute lookup — an EAV/AEV/VAE scan with
    /// cardinality-aware winner selection folded into the wrapped
    /// query.
    Scan(Header, Box<DynamicAttributeQuery>),
    /// Pure computation that derives bindings without touching the
    /// fact store.
    Formula(Header, FormulaQuery),
    /// Variable constraint (equality, coalesce) that filters or infers
    /// bindings.
    Constraint(Header, Constraint),
    /// Concept realization. Lowering stops at the concept boundary:
    /// the wrapped [`ConceptQuery`] owns its own planning and
    /// evaluation of the underlying rule bodies.
    Concept(Header, ConceptQuery),
    /// Negation as a filter: a match passes only if evaluating the
    /// inner plan against it produces no rows.
    Negate(Header, Box<Plan>),
}

impl Plan {
    /// Returns the planning metadata header for this step.
    pub fn header(&self) -> &Header {
        match self {
            Plan::Scan(header, _) => header,
            Plan::Formula(header, _) => header,
            Plan::Constraint(header, _) => header,
            Plan::Concept(header, _) => header,
            Plan::Negate(header, _) => header,
        }
    }

    /// Reconstruct the syntactic premise this step was lowered from.
    ///
    /// The payload is the lowered, already type-narrowed form of the
    /// query, so this is a faithful inverse of [`lower`](Plan::lower).
    /// Consumers that analyze a step (rule analyzer, type inference,
    /// descriptor construction) go through this rather than a stored
    /// AST copy.
    pub fn as_premise(&self) -> Premise {
        match self {
            Plan::Scan(_, query) => Premise::Assert(Proposition::Attribute(query.clone())),
            Plan::Concept(_, query) => Premise::Assert(Proposition::Concept(query.clone())),
            Plan::Formula(_, query) => Premise::Assert(Proposition::Formula(query.clone())),
            Plan::Constraint(_, constraint) => {
                Premise::Assert(Proposition::Constraint(constraint.clone()))
            }
            Plan::Negate(_, inner) => match inner.as_premise() {
                Premise::Assert(proposition) => Premise::Unless(Negation(proposition)),
                // The inner plan is always lowered from a positive
                // proposition, so its reconstruction is never itself a
                // negation.
                Premise::Unless(negation) => Premise::Unless(negation),
            },
        }
    }

    /// Returns the schema describing this step's parameters.
    pub fn schema(&self) -> Schema {
        self.as_premise().schema()
    }

    /// Returns the parameter bindings for this step.
    pub fn parameters(&self) -> Parameters {
        self.as_premise().parameters()
    }

    /// Feasibility verdict for this step under the given set of
    /// already-bound variable names.
    ///
    /// Returns `Ok(Binds)` — the variables the step will bind — when
    /// every prerequisite is satisfied by `bound` (or by a constant /
    /// blank / choice-group member), or `Err(Infeasible)` naming the
    /// variables still required.
    ///
    /// This is the SIPS adornment function. It is currently *derived*
    /// from the per-slot [`Requirement`] schema — the same
    /// categorization the planner's `Candidate` performs — exposed as
    /// a per-step function so the planner and demand analysis can share
    /// one definition of feasibility.
    pub fn adorn(&self, bound: &BTreeSet<String>) -> Result<Binds, Infeasible> {
        let premise = self.as_premise();
        let schema = premise.schema();
        let params = premise.parameters();
        let is_negation = matches!(self, Plan::Negate(..));

        // Reuse the planner's single definition of feasibility.
        let mut scope = Environment::new();
        for var in bound {
            scope.add(var.as_str());
        }
        let (binds, requires) = categorize(&schema, &params, is_negation, &scope);

        let needs: BTreeSet<String> = requires.iter().map(String::from).collect();
        if needs.is_empty() {
            Ok(Binds(binds.iter().map(String::from).collect()))
        } else {
            Err(Infeasible::NeedsAll(needs))
        }
    }

    /// Returns the estimated execution cost.
    pub fn cost(&self) -> usize {
        self.header().cost
    }

    /// Returns the set of variables this step will bind.
    pub fn binds(&self) -> &Environment {
        &self.header().binds
    }

    /// Returns the environment of already-bound variables for this step.
    pub fn env(&self) -> &Environment {
        &self.header().env
    }

    /// Lower a syntactic premise into the matching compiled `Plan`
    /// variant, attaching the planning metadata `header`.
    pub(crate) fn lower(premise: Premise, header: Header) -> Self {
        match premise {
            Premise::Assert(proposition) => Self::lower_proposition(proposition, header),
            Premise::Unless(Negation(proposition)) => {
                // Lower the inner proposition under the same metadata so
                // the negation filters against a compiled inner plan.
                let inner = Self::lower_proposition(proposition, header.clone());
                Plan::Negate(header, Box::new(inner))
            }
        }
    }

    fn lower_proposition(proposition: Proposition, header: Header) -> Self {
        match proposition {
            Proposition::Attribute(query) => Plan::Scan(header, query),
            Proposition::Concept(query) => Plan::Concept(header, query),
            Proposition::Formula(query) => Plan::Formula(header, query),
            Proposition::Constraint(constraint) => Plan::Constraint(header, constraint),
        }
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
        match self {
            Plan::Scan(_, query) => Either::Left(Either::Left(Either::Left(
                Application::evaluate(*query, selection, env),
            ))),
            Plan::Concept(_, query) => {
                Either::Left(Either::Left(Either::Right(query.evaluate(selection, env))))
            }
            Plan::Formula(_, query) => Either::Left(Either::Right(query.evaluate(selection))),
            Plan::Constraint(_, constraint) => {
                Either::Right(Either::Left(constraint.evaluate(selection)))
            }
            Plan::Negate(_, inner) => Either::Right(Either::Right(negate(*inner, selection, env))),
        }
    }
}

/// Filter a selection by a negated plan: keep each incoming match only
/// when evaluating `inner` against it yields no rows.
fn negate<'a, Env, M: Selection + 'a>(
    inner: Plan,
    selection: M,
    env: &'a Env,
) -> impl Selection + 'a
where
    Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
{
    try_stream! {
        for await candidate in selection {
            let base = candidate?;
            // Box the recursive evaluation: `Plan::Negate` calls back
            // into `Plan::evaluate`, so the inner stream's type would
            // otherwise be infinitely self-referential. Erasing it to
            // a trait object breaks the recursion.
            let output: Pin<Box<dyn Selection + 'a>> =
                Box::pin(inner.clone().evaluate(base.clone().seed(), env));

            tokio::pin!(output);

            if let Ok(Some(_)) = output.try_next().await {
                continue;
            }

            yield base;
        }
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
    let Some(name) = query.is().name() else {
        return query;
    };
    let Some(kind) = types.get(name) else {
        return query;
    };
    query.with_type(kind.clone())
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::Entity;
    use crate::planner::Planner;
    use crate::the;
    use crate::types::Any;
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
            if let Premise::Assert(Proposition::Attribute(boxed)) = step.as_premise() {
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
            if let Premise::Assert(Proposition::Attribute(boxed)) = step.as_premise()
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

        if let Premise::Assert(Proposition::Attribute(boxed)) = plan.steps[0].as_premise() {
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
            if let Premise::Assert(Proposition::Attribute(boxed)) = step.as_premise()
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
            TypeEnv::infer(&premises).unwrap()
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

    /// `adorn` is a faithful per-step view of the planner's own
    /// feasibility computation: for each planned step, adorning with
    /// the variables bound when that step runs (`step.env()`) must be
    /// `Ok` and bind exactly the step's `binds()` set. This pins the
    /// derived SIPS function to the planner's behavior.
    #[dialog_common::test]
    fn it_adorns_consistently_with_planned_binds() {
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                Term::var("name"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("person/age")),
                Term::<Entity>::var("this"),
                Term::var("age"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let plan = Planner::from(premises)
            .plan(&crate::Environment::new())
            .unwrap();

        for step in &plan.steps {
            let bound: BTreeSet<String> = step.env().iter().map(String::from).collect();
            let expected: BTreeSet<String> = step.binds().iter().map(String::from).collect();
            match step.adorn(&bound) {
                Ok(Binds(binds)) => assert_eq!(
                    binds, expected,
                    "adorn must bind exactly the planner's binds for this step"
                ),
                Err(infeasible) => panic!(
                    "planned step should be feasible at its own env, got {:?}",
                    infeasible
                ),
            }
        }
    }

    /// A premise whose required inputs are unbound is infeasible, and
    /// the verdict names the variables still needed.
    #[dialog_common::test]
    fn it_reports_infeasible_with_needed_variables() {
        use crate::formula::Formula;
        use crate::formula::string::Length;

        let mut params = crate::Parameters::new();
        params.insert("of".to_string(), Term::var("text"));
        params.insert("is".to_string(), Term::var("len"));
        let premise = Premise::from(Length::apply(params).unwrap());
        let plan = Plan::lower(
            premise,
            Header {
                cost: 0,
                binds: crate::Environment::new(),
                env: crate::Environment::new(),
            },
        );

        // `text` unbound → infeasible, naming `text`.
        let empty = BTreeSet::new();
        match plan.adorn(&empty) {
            Err(Infeasible::NeedsAll(needs)) => {
                assert!(needs.contains("text"), "should report `text` as needed");
            }
            other => panic!("expected NeedsAll(text), got {:?}", other),
        }

        // With `text` bound → feasible, binds `len`.
        let bound: BTreeSet<String> = ["text".to_string()].into_iter().collect();
        match plan.adorn(&bound) {
            Ok(Binds(binds)) => assert!(binds.contains("len"), "should bind `len`"),
            Err(infeasible) => panic!("should be feasible, got {:?}", infeasible),
        }
    }
}
