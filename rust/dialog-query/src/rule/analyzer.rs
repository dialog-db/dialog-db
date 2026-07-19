//! Rule analysis: the phase between parsing and planning.
//!
//! The rule pipeline has three phases:
//!
//! 1. **Parse.** A `DeductiveRuleDescriptor` carries the user's
//!    syntactic form: a conclusion and a list of premises with
//!    user-supplied terms.
//! 2. **Analyze.** Type inference runs against the premises'
//!    slots; type errors (`RequiredHeadFromOptional`, Coalesce
//!    contract violations) surface here. The output is an
//!    `AnalyzedRule`: every premise carries a shared, read-only
//!    view of the rule-wide [`TypeEnv`], plus a dependency graph
//!    describing which premise binds and which reads each variable.
//! 3. **Plan.** Given an `AnalyzedRule` and a scope of already-bound
//!    variables, the planner orders the premises by cost and
//!    produces a `Conjunction` ready for execution.
//!
//! Analysis is rule-scoped; the result is immutable. Planning, by
//! contrast, runs repeatedly against the same analyzed rule, once
//! per scope. A scope is the set of head variables that arrive
//! *already bound* from the surrounding query (the binding pattern,
//! not the bound values): a rule invoked with `this` bound plans
//! differently from the same rule invoked with `name` bound,
//! because each premise's feasibility and cost depend on what is
//! bound going in. Two callers that bind the same variables to
//! different values share one plan. This is distinct from
//! [`DeductiveRule::apply`](crate::DeductiveRule), which binds
//! concrete values into the head.

use crate::concept::descriptor::ConceptDescriptor;
use crate::constraint::Constraint;
use crate::error::AnalysisError;
use crate::planner::categorize;
use crate::premise::Negation;
use crate::proposition::Proposition;
use crate::rule::RuleKind;
use crate::rule::types::TypeEnv;
use crate::type_system::Type as Kind;
use crate::type_system::unifier::Context;
use crate::{Entity, Environment, Premise, Term};
use std::collections::BTreeSet;
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

/// The dependency structure of a rule body: the partial order `≺`
/// from a SIPS (Sideways Information Passing Strategy), the
/// magic-sets device for deciding how bound values flow between body
/// literals (Alviano Def. 3.1.3). "Sideways information passing"
/// names the idea: once one literal binds a variable, that binding
/// is passed sideways to the other literals that need it.
///
/// Half of a SIPS is the binding function (which variables each
/// premise binds, `feasibility::categorize`); this is the other half,
/// the order/dependency relation. `requires[i]` is the set of premise
/// indices that must execute before premise `i` because they bind a
/// variable in `needs[i]`. Given a binding, the edges name which
/// premises it affects/unblocks: the dependency index the
/// demand-driven incremental work consumes. It is cost-free and
/// order-agnostic: cost-driven reordering happens during planning, not
/// here.
///
/// # Example
///
/// Consider a two-premise body (`?cause` is the attribute query's
/// fourth slot, the causal stamp):
///
/// ```text
/// 0: note/body(?this, ?text, ?cause)  // attribute query
/// 1: text/length(?text, ?len)         // formula: needs ?text, binds ?len
/// ```
///
/// The attribute query's four slots (the constant attribute
/// `note/body` plus `?this`/`?text`/`?cause`) form one choice group.
/// The constant attribute satisfies the group, so every free
/// variable in that premise is a *binding*, not a requirement:
/// premise 0 binds `?this`, `?text`, and `?cause` and needs nothing.
/// Premise 1 reads `?text` without producing it, so it depends on
/// premise 0:
///
/// ```text
/// usage[0].binds = {this, text, cause}   usage[0].needs = {}
/// usage[1].binds = {len}                 usage[1].needs = {text}
///
/// requires[0] = {}     // premise 0 needs nothing
/// requires[1] = {0}    // premise 1 needs ?text, which premise 0 binds
/// ```
///
/// The edge `1 -> 0` (premise 1 requires premise 0) is the only
/// dependency, and it holds regardless of the order the premises were
/// written in: swapping the two lines yields the same edge with the
/// indices relabeled, because `requires` is matched on `needs`/`binds`
/// variable names, not position.
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

    /// Compute the dependency graph for a rule's premises.
    ///
    /// The graph is order-independent (each premise's binds/needs come
    /// from its own schema, and the `requires` edges from matching one
    /// premise's needs to another's binds), so it is computed from the
    /// premises directly, before any execution order is chosen. The
    /// per-premise categorization reuses the planner's single
    /// definition of feasibility ([`categorize`]) at empty scope: a
    /// choice group is satisfied only by a constant member here, since
    /// nothing is bound yet.
    pub fn from_premises(premises: &[Premise]) -> Self {
        let empty = Environment::new();
        let usage: Vec<PremiseVars> = premises
            .iter()
            .map(|premise| {
                let is_negation = matches!(premise, Premise::Unless(_));
                let (binds, requires) = categorize(
                    &premise.schema(),
                    &premise.parameters(),
                    is_negation,
                    &empty,
                );
                PremiseVars {
                    binds: binds.iter().map(String::from).collect(),
                    needs: requires.iter().map(String::from).collect(),
                }
            })
            .collect();

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

/// A rule that has passed analysis. Carries the conclusion, the
/// premises (in their original authored order; analysis never
/// reorders), the rule-wide inferred type environment, and a
/// per-premise dependency graph.
///
/// This is the artifact of analysis. The planner reads it (or its
/// pieces) to build per-step plans; future iterations of the
/// planner can consume the dependency graph directly to avoid
/// re-walking schemas on every iteration.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzedRule {
    /// The rule's conclusion.
    pub conclusion: ConceptDescriptor,
    /// The premises in their original authored order; planning
    /// orders a working copy per scope.
    pub premises: Vec<Premise>,
    /// The rule-wide inferred type environment. Shared via
    /// [`Arc`] across consumers.
    pub types: Arc<TypeEnv>,
    /// Per-premise variable usage and dependency edges. Indexed
    /// in the same order as `premises`.
    pub graph: DependencyGraph,
}

impl AnalyzedRule {
    /// Build a partial, *unanalyzed* rule for display only, used to
    /// embed an in-progress rule in a compile error. Carries the
    /// conclusion and premises but an empty type env and graph; such a
    /// rule is never planned or evaluated.
    pub fn in_progress(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Self {
        AnalyzedRule {
            conclusion,
            premises,
            types: Arc::new(TypeEnv::new()),
            graph: DependencyGraph::default(),
        }
    }

    /// Iterate over the analyzed premises.
    pub fn premises(&self) -> impl Iterator<Item = &Premise> {
        self.premises.iter()
    }

    /// Look up a variable's inferred type by name.
    pub fn type_of(&self, name: &str) -> Option<&Kind> {
        self.types.get(name)
    }

    /// The concepts this rule negates: its *negative IDB edges*.
    /// One entry per `unless` premise over a concept, identified by
    /// the concept's content-addressed URI. Deductive self-negation
    /// is rejected at analysis, so for a deductive rule none of
    /// these is its own conclusion; the global stratification pass
    /// consumes these edges to order (or reject) cycles that span
    /// multiple rules. (An inductive rule may negate its own
    /// conclusion — the idempotence guard — but inductive rules
    /// never feed that pass.)
    pub fn negated_concepts(&self) -> impl Iterator<Item = Entity> + '_ {
        self.premises.iter().filter_map(|premise| match premise {
            Premise::Unless(Negation(Proposition::Concept(query))) => Some(query.predicate.this()),
            _ => None,
        })
    }

    /// Whether every premise reads only the head entity's own facts:
    /// each attribute premise (positive, optional, or negated) is a
    /// lookup `of ?this`, and the remaining premises are row-local
    /// transforms (formulas, constraints) over those bindings.
    ///
    /// For an entity-local rule, a base-fact change for entity `E`
    /// can only affect the rule's rows whose subject is `E` — the
    /// soundness condition for maintaining a subscription by
    /// re-deriving just the touched entities instead of
    /// re-evaluating the whole query. Concept premises are
    /// cross-entity by construction (the target entity is a field
    /// value, not the subject), so any rule carrying one is
    /// non-local.
    pub fn is_entity_local(&self) -> bool {
        fn of_is_this(term: &Term<Entity>) -> bool {
            matches!(term, Term::Variable { name: Some(name), .. } if name == "this")
        }
        self.premises.iter().all(|premise| match premise {
            Premise::Assert(Proposition::Attribute(query)) => of_is_this(query.of()),
            Premise::Assert(Proposition::OptionalAttribute(query)) => of_is_this(query.of()),
            Premise::Assert(Proposition::Constraint(_)) => true,
            Premise::Assert(Proposition::Formula(_)) => true,
            Premise::Unless(Negation(Proposition::Attribute(query))) => of_is_this(query.of()),
            Premise::Unless(Negation(Proposition::Constraint(_))) => true,
            _ => false,
        })
    }
}

/// Run analysis over the rule's premises:
///
/// 1. Infer the rule-wide type environment from each premise's
///    slot kinds.
/// 2. Check that no conclusion variable's inferred type admits
///    `Nothing`: a required head can't accept `Absent`.
/// 3. Validate every Coalesce constraint's type contract.
///
/// Returns the analyzed rule on success, or an [`AnalysisError`]
/// describing the type problem. `DeductiveRule::new` wraps these
/// in the corresponding [`TypeError`](crate::error::TypeError)
/// variants with the planned rule embedded for display.
pub fn analyze(
    conclusion: ConceptDescriptor,
    premises: Vec<Premise>,
    kind: RuleKind,
) -> Result<AnalyzedRule, AnalysisError> {
    let types = Arc::new(
        TypeEnv::infer(&premises).map_err(|err| AnalysisError::Inference {
            reason: err.to_string(),
        })?,
    );

    // Required heads must not admit `Nothing`.
    if let Some(variable) = conclusion
        .required_operands()
        .find(|name| types.get(name).is_some_and(Kind::is_optional))
    {
        return Err(AnalysisError::RequiredHeadFromOptional {
            variable: variable.to_string(),
        });
    }

    // Each Coalesce constraint validates against a fresh unifier
    // context. Catches wire-format and raw-builder mismatches
    // where the typed builder isn't the construction path.
    for premise in &premises {
        let Premise::Assert(Proposition::Constraint(Constraint::Coalesce(coalesce))) = premise
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

    // A left-join under `unless` is rejected: it always yields a row
    // for a bound entity (Present or the Absent fallback), so the
    // negation filters everything. Negate the scalar lookup ("the
    // entity has no such fact") or the concept instead.
    for premise in &premises {
        if let Premise::Unless(Negation(Proposition::OptionalAttribute(_))) = premise {
            return Err(AnalysisError::NegatedOptional);
        }
    }

    // A *deductive* rule that negates its own conclusion is a
    // negative self-loop: it would derive a row exactly when it
    // doesn't, and no stratification can order it. This is the
    // local, always detectable case; negation over *other* derived
    // concepts is a negative IDB edge, surfaced per rule by
    // [`AnalyzedRule::negated_concepts`] for the global
    // stratification pass to consume.
    //
    // An *inductive* rule is exempt: its `unless` reads the
    // pre-transition state while its head asserts into the next one,
    // so "assert P unless P exists" is the standard idempotence
    // guard (Dedalus `P@next :- body, not P@now`), stratified by the
    // time step rather than paradoxical. Inductive rules never join
    // deductive resolution, so the head is base data to the same-
    // instant program and no global negative edge arises either.
    if kind == RuleKind::Deductive {
        for premise in &premises {
            if let Premise::Unless(Negation(Proposition::Concept(query))) = premise
                && query.predicate.this() == conclusion.this()
            {
                return Err(AnalysisError::SelfNegation {
                    concept: conclusion.this().to_string(),
                });
            }
        }
    }

    let graph = DependencyGraph::from_premises(&premises);
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
    use crate::formula::Formula;
    use crate::formula::string::Length;
    use crate::optional::OptionalAttributeQuery;
    use crate::premise::Negation;
    use crate::the;
    use crate::types::Any;
    use crate::{Cardinality, Parameters, Premise, Term};

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
        let analyzed = analyze(person_with_name(), premises, RuleKind::Deductive).unwrap();

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
        let analyzed = analyze(person_with_name(), premises, RuleKind::Deductive).unwrap();

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
        let analyzed = analyze(person_with_name(), premises, RuleKind::Deductive).unwrap();

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
            .position(|p| matches!(p, Premise::Assert(Proposition::Formula(_))))
            .expect("formula step present");
        let attr_idx = analyzed
            .premises
            .iter()
            .position(|p| matches!(p, Premise::Assert(Proposition::Attribute(_))))
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
    /// admits `Nothing`: the rule could yield Absent in the head.
    #[dialog_common::test]
    fn it_rejects_required_head_bound_only_by_optional_premises() {
        let premises = vec![
            OptionalAttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                Term::<String>::var("name").into(),
                Term::blank(),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let err = analyze(person_with_name(), premises, RuleKind::Deductive).unwrap_err();
        match err {
            AnalysisError::RequiredHeadFromOptional { variable } => {
                assert_eq!(variable, "name");
            }
            other => panic!("expected RequiredHeadFromOptional, got {other:?}"),
        }
    }

    /// A left-join under `unless` is rejected at analysis: it always
    /// yields a row for a bound entity (Present or the Absent
    /// fallback), so negating it would filter every row.
    #[dialog_common::test]
    fn it_rejects_negated_optional() {
        let name_scan = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("this"),
            Term::<String>::var("name").into(),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let maybe = OptionalAttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("this"),
            Term::<String>::var("nickname").into(),
            Term::blank(),
            Some(Cardinality::One),
        );
        let premises = vec![
            name_scan.into(),
            Premise::Unless(Negation(Proposition::OptionalAttribute(Box::new(maybe)))),
        ];

        let err = analyze(person_with_name(), premises, RuleKind::Deductive).unwrap_err();
        assert!(
            matches!(err, AnalysisError::NegatedOptional),
            "negating a maybe premise is vacuously false, got {err:?}"
        );
    }
}
