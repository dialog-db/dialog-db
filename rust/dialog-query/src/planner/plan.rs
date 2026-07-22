use crate::attribute::query::{AttributeQuery, DynamicAttributeQuery};
use crate::concept::query::ConceptQuery;
use crate::constraint::Constraint;
use crate::formula::query::FormulaQuery;
use crate::negation::Negation;
use crate::optional::OptionalAttributeQuery;
use crate::proposition::Proposition;
use crate::query::Application;
use crate::rule::types::TypeEnv;
use crate::selection::Selection;
use crate::source::SelectRules;
use crate::try_stream;
use crate::{Environment, Parameters, Premise, Term};
use auto_enums::auto_enum;
use core::pin::Pin;
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use futures_util::TryStreamExt;

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
/// A `Plan` is one ordered step the planner emits for a feasible
/// premise. It is both the compiled operator (the variant selects the
/// kind of work and owns the lowered query) and the carrier of planning
/// metadata (via [`Header`]). Lowering from the syntactic AST happens
/// once, in [`Planner::plan`](crate::Planner); `evaluate` then
/// dispatches on the variant rather than walking the AST.
///
/// Leaf variants wrap the concrete query types that own the actual
/// stream logic; [`Negate`](Plan::Negate) wraps a nested `Plan` so
/// negation filters against the lowered inner step.
#[derive(Debug, Clone, PartialEq)]
// The `Formula` variant inherits `FormulaQuery`'s width (see its allow
// note); plans are transient per-query values, not bulk storage.
#[allow(clippy::large_enum_variant)]
pub enum Plan {
    /// Positive attribute lookup: an EAV/AEV/VAE scan with
    /// cardinality-aware winner selection folded into the wrapped
    /// query.
    Scan(Header, Box<DynamicAttributeQuery>),
    /// Left-join over a scalar attribute lookup: per input row (entity
    /// bound by construction), Present facts extend the row and a miss
    /// yields one row with the value slot bound Absent. The
    /// semantic-layer realization of an optional concept field.
    OptionalScan(Header, Box<OptionalAttributeQuery>),
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
            Plan::OptionalScan(header, _) => header,
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
            Plan::OptionalScan(_, query) => {
                Premise::Assert(Proposition::OptionalAttribute(query.clone()))
            }
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
            Proposition::OptionalAttribute(query) => Plan::OptionalScan(header, query),
            Proposition::Concept(query) => Plan::Concept(header, query),
            Proposition::Formula(query) => Plan::Formula(header, query),
            Proposition::Constraint(constraint) => Plan::Constraint(header, constraint),
        }
    }

    /// Evaluate this plan with the given selection and environment.
    ///
    /// Each variant delegates to its own evaluator, which yields a
    /// stream of matches. They are different concrete stream types,
    /// so `#[auto_enum]` generates an enum that unifies them into one
    /// statically-dispatched `Stream` (no boxing); the blanket impl
    /// makes it `Selection`.
    #[auto_enum(futures03::Stream)]
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        match self {
            Plan::Scan(_, query) => Application::evaluate(*query, selection, env),
            Plan::OptionalScan(_, query) => Application::evaluate(*query, selection, env),
            Plan::Concept(_, query) => query.evaluate(selection, env),
            Plan::Formula(_, query) => query.evaluate(selection),
            Plan::Constraint(_, constraint) => constraint.evaluate(selection),
            Plan::Negate(_, inner) => negate(*inner, selection, env),
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

            match output.try_next().await {
                // The inner query matched: the negation filters the row.
                Ok(Some(_)) => continue,
                // No match: the row passes.
                Ok(None) => yield base,
                // An inner failure is not absence; propagate instead
                // of silently passing the row.
                Err(error) => Err(error)?,
            }
        }
    }
}

/// Rewrite a premise to reflect rule-level inferred types.
///
/// Positive premises only: polarity discipline. The inferred env
/// is an occurrence-typing fact about rows that survive the
/// positive premises ("?x is Present in every surviving row"), so
/// it narrows positive premises: an attribute scan's `is` kind is
/// stamped, and a [`OptionalAttributeQuery`] whose value variable was narrowed
/// to non-optional is demoted to its wrapped scalar scan. A negated
/// premise asks a hypothetical question about rows that do *not*
/// survive it; it is typed in its own context and left untouched.
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
        unless @ Premise::Unless(_) => unless,
    }
}

fn apply_types_to_proposition(proposition: Proposition, types: &TypeEnv) -> Proposition {
    match proposition {
        Proposition::Attribute(boxed) => {
            let query = *boxed;
            Proposition::Attribute(Box::new(narrow_attribute(query, types)))
        }
        Proposition::OptionalAttribute(boxed) => {
            // When rule inference narrowed the value variable to a
            // non-optional kind (a sibling premise guarantees it is
            // Present), the left-join's fallback can never fire:
            // demote to the wrapped scalar scan so evaluation skips
            // the per-row miss handling entirely.
            let maybe = *boxed;
            match maybe.is().name().and_then(|name| types.get(name)) {
                Some(kind) if !kind.is_optional() => {
                    let kind = kind.clone();
                    Proposition::Attribute(Box::new(maybe.into_query().with_type(kind)))
                }
                _ => Proposition::OptionalAttribute(Box::new(maybe)),
            }
        }
        Proposition::Concept(query) => {
            // Project the rule-level kinds onto the concept's
            // parameter terms. This records what the surrounding
            // rule proved about each variable at the boundary: the
            // projection consumed by diagnostics today and by
            // checked execution later. (Row-level enforcement at the
            // boundary is the consuming premise's job: a narrowed
            // variable arriving Absent is filtered by whichever
            // scalar slot demanded it.)
            let mut terms = Parameters::new();
            for (param, term) in query.terms.iter() {
                let projected = match term.name().and_then(|name| types.get(name)) {
                    Some(kind) => {
                        Term::typed_var(term.name().unwrap_or_default().to_string(), kind.clone())
                    }
                    None => term.clone(),
                };
                terms.insert(param.clone(), projected);
            }
            Proposition::Concept(ConceptQuery {
                terms,
                predicate: query.predicate,
            })
        }
        other => other,
    }
}

fn narrow_attribute(query: AttributeQuery, types: &TypeEnv) -> AttributeQuery {
    // Stamp the attribute/entity variables with their rule-level
    // kinds. This is how a prefix refinement proved by a sibling
    // premise reaches the scan boundary, where the selector
    // conversion turns it into index-range bounds.
    let kind_of = |term_name: Option<&str>| term_name.and_then(|name| types.get(name)).cloned();
    let the_kind = kind_of(query.the().name());
    let of_kind = kind_of(query.of().name());
    let query = match (&the_kind, &of_kind) {
        (None, None) => query,
        _ => query.with_subject_kinds(the_kind, of_kind),
    };

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
    use crate::artifact::Type as ValueType;
    use crate::error::TypeError;
    use crate::optional::OptionalAttributeQuery;
    use crate::planner::Planner;
    use crate::the;
    use crate::types::Any;
    use crate::{AttributeDescriptor, Cardinality, ConceptDescriptor, Term};

    fn optional_nickname_premise(var: &str) -> Premise {
        OptionalAttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("this"),
            Term::<String>::var(var).into(),
            Term::blank(),
            Some(Cardinality::One),
        )
        .into()
    }

    fn name_scan(var: &str) -> Premise {
        AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("this"),
            Term::<String>::var(var).into(),
            Term::var("cause2"),
            Some(Cardinality::One),
        )
        .into()
    }

    /// A rule where an optional lookup binds `?name` (set-widened:
    /// `String | Nothing` in its schema) and a sibling scan binds it
    /// required (kind `String`). Rule-level inference narrows
    /// `?name` to `String`, and the planner demotes the optional lookup to
    /// its wrapped scalar scan: the fallback can never fire, so the
    /// plan contains no `OptionalScan` step at all.
    #[dialog_common::test]
    fn it_demotes_optional_to_scan_via_rule_inference() {
        let premises = vec![optional_nickname_premise("name"), name_scan("name")];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();

        assert_eq!(plan.steps.len(), 2);
        for step in plan.steps {
            match step.as_premise() {
                Premise::Assert(Proposition::Attribute(boxed)) => {
                    assert!(
                        !boxed.is().is_optional(),
                        "demoted scan carries the narrowed scalar kind"
                    );
                }
                other => panic!("expected only scalar scans after demotion, got {other}"),
            }
        }
    }

    /// A prefix refinement proved by a sibling `starts-with`
    /// premise is stamped onto the scan's entity term, where the
    /// selector conversion turns it into an index-range bound.
    #[dialog_common::test]
    fn it_stamps_prefix_refinements_onto_scan_subjects() {
        let scan: Premise = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("e"),
            Term::<String>::var("name").into(),
            Term::blank(),
            Some(Cardinality::One),
        )
        .into();
        let predicate = Term::<Any>::var("e").starts_with("did:key:");

        let plan = Planner::from(vec![scan, predicate])
            .plan(&crate::Environment::new())
            .unwrap();

        let stamped = plan.steps.iter().find_map(|step| match step.as_premise() {
            Premise::Assert(Proposition::Attribute(boxed)) => boxed.of().kind(),
            _ => None,
        });
        let kind = stamped.expect("the scan's entity term carries a kind");
        assert_eq!(
            kind.refinement().expect("refined").prefix.as_deref(),
            Some("did:key:"),
            "the proved prefix reaches the scan boundary"
        );
    }

    /// The same prefix pipeline pushes a `starts-with` on the scan's *value*
    /// variable down to the scan's value term, so the VAE value-range bound
    /// (`ArtifactSelector::is_starting_with`) is driven by the constraint.
    #[dialog_common::test]
    fn it_stamps_prefix_refinements_onto_scan_values() {
        let scan: Premise = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("e"),
            Term::<String>::var("name").into(),
            Term::blank(),
            Some(Cardinality::One),
        )
        .into();
        let predicate = Term::<Any>::var("name").starts_with("ali");

        let plan = Planner::from(vec![scan, predicate])
            .plan(&crate::Environment::new())
            .unwrap();

        let stamped = plan.steps.iter().find_map(|step| match step.as_premise() {
            Premise::Assert(Proposition::Attribute(boxed)) => boxed.is().kind(),
            _ => None,
        });
        let kind = stamped.expect("the scan's value term carries a kind");
        assert_eq!(
            kind.refinement().expect("refined").prefix.as_deref(),
            Some("ali"),
            "the proved value prefix reaches the scan boundary"
        );
    }

    /// The same pipeline pushes a comparison's interval down to the
    /// scan's value term: the typed variable narrows the NUMERIC bound
    /// to one type and the constant side's interval rides along, so
    /// the VAE range bound (`ArtifactSelector::is_at_least`) is driven
    /// by the predicate.
    #[dialog_common::test]
    fn it_stamps_interval_refinements_onto_scan_values() {
        let scan: Premise = AttributeQuery::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("e"),
            Term::<u64>::var("age").into(),
            Term::blank(),
            Some(Cardinality::One),
        )
        .into();
        let predicate = Term::<Any>::var("age").at_least(Term::constant(30u64));

        let plan = Planner::from(vec![scan, predicate])
            .plan(&crate::Environment::new())
            .unwrap();

        let stamped = plan.steps.iter().find_map(|step| match step.as_premise() {
            Premise::Assert(Proposition::Attribute(boxed)) => boxed.is().kind(),
            _ => None,
        });
        let kind = stamped.expect("the scan's value term carries a kind");
        assert_eq!(
            kind.primitive_part().required().as_singleton(),
            Some(ValueType::UnsignedInt),
            "the typed variable narrows the comparison's NUMERIC bound"
        );
        let interval = kind
            .refinement()
            .expect("refined")
            .interval
            .clone()
            .expect("the proved interval reaches the scan boundary");
        let lower = interval.lower.expect("lower bound");
        assert!(lower.inclusive, ">= stays inclusive at the boundary");
        assert!(interval.upper.is_none());
    }

    /// A standalone `Maybe` premise cannot be planned at empty
    /// scope: set-widening needs a known entity ("absent for
    /// whom?"), so its schema hard-requires `?this` and the planner
    /// rejects the conjunction naming it. Once the entity is bound
    /// the premise plans, and stays a `Maybe` step: nothing
    /// narrowed it, so the left-join (and its Absent fallback) is
    /// preserved.
    #[dialog_common::test]
    fn it_requires_entity_for_standalone_optional() {
        let premises = vec![optional_nickname_premise("name")];

        match Planner::from(premises.clone()).plan(&Environment::new()) {
            Err(TypeError::RequiredBindings { required }) => {
                assert!(
                    required.contains("this"),
                    "the rejection names the entity the left-join requires"
                );
            }
            other => panic!("expected RequiredBindings, got {other:?}"),
        }

        let mut scope = Environment::new();
        scope.add("this");
        let plan = Planner::from(premises).plan(&scope).unwrap();
        assert!(
            matches!(plan.steps[0], Plan::OptionalScan(..)),
            "an un-narrowed optional premise stays a Maybe step"
        );
    }

    /// Replanning a `Conjunction` preserves the rule-level
    /// narrowing: the demotion of a `Maybe` whose variable a
    /// sibling requires is idempotent across replans at different
    /// scopes.
    #[dialog_common::test]
    fn it_preserves_narrowing_across_replans() {
        let premises = vec![optional_nickname_premise("nick"), name_scan("nick")];
        let plan = Planner::from(premises.clone())
            .plan(&Environment::new())
            .unwrap();
        assert!(
            !plan
                .steps
                .iter()
                .any(|s| matches!(s, Plan::OptionalScan(..))),
            "demotion applies at empty scope"
        );

        // Replan against a new scope where `this` is bound: plan the
        // same premises fresh, the production replan path.
        let mut scope = Environment::new();
        scope.add("this");
        let replanned = Planner::from(premises).plan(&scope).unwrap();
        assert!(
            !replanned
                .steps
                .iter()
                .any(|s| matches!(s, Plan::OptionalScan(..))),
            "demotion must survive replanning"
        );
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

    /// `apply_types` projects the rule-level kinds onto a concept
    /// premise's parameter terms, recording at the boundary what the
    /// surrounding rule proved about each variable.
    #[dialog_common::test]
    fn it_projects_narrowing_onto_concept_parameters() {
        let env = {
            let premises = vec![name_scan("name")];
            TypeEnv::infer(&premises).unwrap()
        };

        let concept = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(the!("person/name"), "", Cardinality::One, None),
        )])
        .unwrap();
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("person"));
        terms.insert("name".to_string(), Term::var("name"));
        let premise = Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: concept,
        }));

        let projected = apply_types(premise, &env);
        let Premise::Assert(Proposition::Concept(query)) = projected else {
            panic!("expected concept premise");
        };
        assert_eq!(
            query
                .terms
                .get("name")
                .and_then(|term| term.kind())
                .and_then(|kind| kind.as_value_type()),
            Some(ValueType::String),
            "the rule-level kind is stamped onto the boundary term"
        );
    }

    /// Polarity discipline: `apply_types` leaves negated premises
    /// untouched. The inferred env describes rows that survive the
    /// positive premises; a negated subquery asks a hypothetical
    /// question and is typed in its own context.
    #[dialog_common::test]
    fn it_leaves_negated_premises_untyped() {
        let env = {
            // Build an env via the public path: a one-premise rule
            // with a typed Term<String>. The single binding
            // dictates the inferred kind.
            let premises = vec![name_scan("name")];
            TypeEnv::infer(&premises).unwrap()
        };

        // The negated attribute query uses `?name` untyped; the
        // rewrite must not stamp the positive env onto it.
        let neg = AttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("this"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let original = Premise::Unless(Negation(Proposition::Attribute(Box::new(neg))));

        let rewritten = apply_types(original.clone(), &env);
        assert_eq!(
            rewritten, original,
            "negated premises are typed in their own context"
        );
    }
}
