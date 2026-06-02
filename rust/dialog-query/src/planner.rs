mod candidate;
mod conjunction;
mod disjunction;
mod plan;

pub use candidate::*;
pub use conjunction::*;
pub use disjunction::*;
pub use plan::*;

use crate::error::TypeError;
use crate::rule::types::TypeEnv;
use crate::{Environment, Premise};

/// State machine that greedily selects the cheapest viable premise at each
/// step, building an ordered execution plan.
///
/// The planner starts in `Idle` with raw premises. On the first call to
/// [`top`](Planner::top) it analyzes every premise against the current
/// [`Environment`](crate::Environment), picks the viable one with the
/// lowest cost, and transitions to `Active` with the remaining candidates
/// cached as [`Candidate`] values. Subsequent calls incrementally update
/// these candidates as new bindings arrive, potentially unblocking premises
/// that were previously missing prerequisites.
///
/// The planner is consumed by [`Conjunction::try_from`](super::Conjunction) and
/// [`Conjunction::plan`](super::Conjunction) which repeatedly call `top` until all
/// premises have been planned or an error is raised.
pub enum Planner {
    /// Initial state with unprocessed premises.
    Idle {
        /// Premises waiting to be analyzed
        premises: Vec<Premise>,
    },
    /// Processing state with cached candidates and current scope.
    Active {
        /// Candidates being evaluated for selection
        candidates: Vec<Candidate>,
    },
}

impl Planner {
    /// Produce an ordered execution plan ([`Conjunction`]) for the given scope.
    ///
    /// Repeatedly selects the cheapest viable candidate until all premises
    /// have been planned. Returns an error if any premise has unsatisfiable
    /// prerequisites.
    pub fn plan(mut self, scope: &Environment) -> Result<Conjunction, TypeError> {
        // Narrow the premises to their rule-level inferred kinds once,
        // up front, before ordering. Inference is order-independent
        // (per-premise unification), so narrowing here yields the same
        // result as narrowing the planned steps. The planner then only
        // orders and lowers the already-narrowed premises.
        self.narrow()?;

        let env = scope.clone();
        let mut bound = scope.clone();
        let mut steps = vec![];
        let mut cost = 0;

        while !self.done() {
            let plan = self.top(&bound)?;
            cost += plan.cost();
            bound.extend(plan.binds());
            steps.push(plan);
        }

        let mut binds = Environment::new();
        for var_name in bound.iter() {
            if !env.contains(var_name) {
                binds.add(var_name);
            }
        }

        Ok(Conjunction {
            steps,
            cost,
            binds,
            env,
        })
    }

    /// Narrow the planner's premises to their rule-level inferred
    /// kinds, in place. A no-op for the `Active` state (its candidates
    /// were already narrowed when first planned).
    fn narrow(&mut self) -> Result<(), TypeError> {
        let Planner::Idle { premises } = self else {
            return Ok(());
        };
        let types = TypeEnv::infer(premises).map_err(|err| TypeError::TypeInference {
            reason: err.to_string(),
        })?;
        *premises = premises
            .drain(..)
            .map(|premise| plan::apply_types(premise, &types))
            .collect();
        Ok(())
    }

    /// Helper to create a planning error from failed candidates.
    fn fail(candidates: &[Candidate]) -> Result<Plan, TypeError> {
        if candidates.is_empty() {
            return Err(TypeError::RequiredBindings {
                required: Environment::new(),
            });
        }

        for candidate in candidates {
            if let Candidate::Blocked { requires, .. } = candidate
                && !requires.is_empty()
            {
                return Err(TypeError::RequiredBindings {
                    required: requires.clone(),
                });
            }
        }

        unreachable!("Should have had at least one blocked candidate with requirements");
    }

    /// Checks if planning is complete (all premises have been planned).
    fn done(&self) -> bool {
        match self {
            Self::Idle { .. } => false,
            Self::Active { candidates } => candidates.is_empty(),
        }
    }

    /// Selects and returns the best premise to execute next based on cost.
    /// Updates the planner state by removing the selected premise from candidates.
    fn top(&mut self, env: &Environment) -> Result<Plan, TypeError> {
        match self {
            Planner::Idle { premises } => {
                let mut candidates = vec![];
                let mut best: Option<(usize, usize)> = None;

                for (index, premise) in premises.iter().enumerate() {
                    let mut candidate = Candidate::from(premise.clone());
                    candidate.update(env);

                    if candidate.is_viable() {
                        let cost = candidate.cost();

                        if let Some((best_cost, _)) = &best {
                            if cost < *best_cost {
                                best = Some((cost, index));
                            }
                        } else {
                            best = Some((cost, index));
                        }
                    }

                    candidates.push(candidate);
                }

                if let Some((_, best_index)) = best {
                    let candidate = candidates.remove(best_index);
                    *self = Planner::Active { candidates };
                    Plan::try_from(candidate)
                } else {
                    Self::fail(&candidates)
                }
            }
            Planner::Active { candidates } => {
                let mut best: Option<(usize, usize)> = None;

                for (index, candidate) in candidates.iter_mut().enumerate() {
                    candidate.update(env);

                    if candidate.is_viable() {
                        let cost = candidate.cost();

                        if let Some((best_cost, _)) = &best {
                            if cost < *best_cost {
                                best = Some((cost, index));
                            }
                        } else {
                            best = Some((cost, index));
                        }
                    }
                }

                if let Some((_, best_index)) = best {
                    let candidate = candidates.remove(best_index);
                    Plan::try_from(candidate)
                } else {
                    Self::fail(candidates)
                }
            }
        }
    }
}

impl From<Vec<Premise>> for Planner {
    fn from(premises: Vec<Premise>) -> Self {
        Self::Idle { premises }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::selection::Match;
    use crate::the;

    #[dialog_common::test]
    fn it_plans_two_fact_applications() {
        use crate::attribute::query::AttributeQuery;
        use crate::{Cardinality, Proposition, Term};

        let fact1 = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let fact2 = AttributeQuery::new(
            Term::from(the!("person/age")),
            Term::var("person"),
            Term::var("age"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let premises = vec![
            Premise::Assert(Proposition::Attribute(Box::new(fact1))),
            Premise::Assert(Proposition::Attribute(Box::new(fact2))),
        ];

        let plan = Planner::from(premises)
            .plan(&Environment::new())
            .expect("Planning should succeed");

        assert_eq!(plan.steps.len(), 2, "Should have two steps");
        assert!(plan.cost > 0, "Should have non-zero cost");

        assert!(plan.binds.contains("person"), "Should bind person variable");
        assert!(plan.binds.contains("name"), "Should bind name variable");
        assert!(plan.binds.contains("age"), "Should bind age variable");
    }

    #[dialog_common::test]
    fn it_orders_cheaper_premise_first() {
        use crate::attribute::query::AttributeQuery;
        use crate::{Cardinality, Proposition, Term};
        use dialog_artifacts::Entity;

        let fact1 = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::from(Entity::try_from("urn:alice".to_string()).unwrap()),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let fact2 = AttributeQuery::new(
            Term::from(the!("greeting/text")),
            Term::var("name"),
            Term::var("greeting"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let premises = vec![
            Premise::Assert(Proposition::Attribute(Box::new(fact1))),
            Premise::Assert(Proposition::Attribute(Box::new(fact2))),
        ];

        let plan = Planner::from(premises)
            .plan(&Environment::new())
            .expect("Planning should succeed");

        assert_eq!(plan.steps.len(), 2);
        // ?name, ?greeting, and ?cause — the cause slot is now
        // declared in the AttributeQuery schema (bound by the
        // merge step on every Present row), so it counts toward
        // the plan's bind set.
        assert_eq!(plan.binds.len(), 3, "Should bind name, greeting, cause");
    }

    #[dialog_common::test]
    async fn it_executes_planned_query() -> anyhow::Result<()> {
        use crate::attribute::query::AttributeQuery;
        use crate::session::RuleRegistry;
        use crate::source::test::TestEnv;

        use crate::{Cardinality, Proposition, Term, Value, the};
        use dialog_artifacts::Entity;
        use dialog_repository::helpers::{test_operator_with_profile, test_repo};

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        branch
            .transaction()
            .assert(
                the!("person/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            )
            .assert(the!("person/age").of(alice.clone()).is(25u32))
            .assert(the!("person/name").of(bob.clone()).is("Bob".to_string()))
            .assert(the!("person/age").of(bob.clone()).is(30u32))
            .commit()
            .perform(&operator)
            .await?;

        let fact1 = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let fact2 = AttributeQuery::new(
            Term::from(the!("person/age")),
            Term::var("person"),
            Term::var("age"),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let premises = vec![
            Premise::Assert(Proposition::Attribute(Box::new(fact1))),
            Premise::Assert(Proposition::Attribute(Box::new(fact2))),
        ];
        let plan = Planner::from(premises).plan(&Environment::new())?;

        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let selection = futures_util::TryStreamExt::try_collect::<Vec<_>>(
            plan.evaluate(Match::new().seed(), &source),
        )
        .await?;

        assert_eq!(selection.len(), 2, "Should find 2 people");

        let name_param = Term::var("name");
        let age_param = Term::var("age");

        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let name = match_result.lookup(&name_param)?.content()?;
            let age = match_result.lookup(&age_param)?.content()?;

            match name {
                Value::String(n) if n == "Alice" => {
                    assert_eq!(age, Value::UnsignedInt(25), "Alice should be 25");
                    found_alice = true;
                }
                Value::String(n) if n == "Bob" => {
                    assert_eq!(age, Value::UnsignedInt(30), "Bob should be 30");
                    found_bob = true;
                }
                _ => panic!("Unexpected person: {:?}", name),
            }
        }

        assert!(found_alice, "Should find Alice");
        assert!(found_bob, "Should find Bob");

        Ok(())
    }

    #[dialog_common::test]
    fn it_restores_cost_when_replanned_to_empty_scope() {
        use crate::attribute::query::AttributeQuery;
        use crate::schema::{INDEX_SCAN_COST, RANGE_READ_COST};

        use crate::{Cardinality, Proposition, Term};
        use dialog_artifacts::Entity;

        // Cardinality::Many premise:
        //   1/3 constraints (just 'the'): INDEX_SCAN_COST = 5000
        //   2/3 constraints (the + of):   RANGE_READ_COST = 200
        let hobby = AttributeQuery::new(
            Term::from(the!("person/hobbies")),
            Term::<Entity>::var("entity"),
            Term::var("hobby"),
            Term::var("cause"),
            Some(Cardinality::Many),
        );

        let premises = vec![Premise::Assert(Proposition::Attribute(Box::new(hobby)))];
        let plan = Planner::from(premises.clone())
            .plan(&Environment::new())
            .expect("Should compile");

        assert_eq!(plan.steps.len(), 1);
        assert_eq!(
            plan.steps[0].cost(),
            INDEX_SCAN_COST,
            "With 1/3 constraints, cost should be INDEX_SCAN_COST"
        );

        // Replan with entity bound → cheaper
        let mut env_with_entity = Environment::new();
        env_with_entity.add("entity");

        let replanned = Planner::from(premises.clone())
            .plan(&env_with_entity)
            .expect("Should replan with entity");

        assert_eq!(
            replanned.steps[0].cost(),
            RANGE_READ_COST,
            "With 2/3 constraints, cost should be READ"
        );

        // Replan back to empty → cost should return to original
        let replanned_empty = Planner::from(premises)
            .plan(&Environment::new())
            .expect("Should replan back to empty");

        assert_eq!(
            replanned_empty.steps[0].cost(),
            INDEX_SCAN_COST,
            "After replanning back to empty env, cost should return to FULL"
        );
    }
}

/// Coarse plan-ordering characterization: pins the *observable* output
/// of planning — the step order, each step's binds, and the total cost
/// — and that it is stable across replans (grow then shrink-back to the
/// same scope). This is the contract a future refactor of the planner's
/// `Candidate` categorization must preserve; it deliberately does not
/// assert internal candidate state, only the plan that comes out.
#[cfg(test)]
mod plan_ordering {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Conjunction;
    use crate::attribute::The;
    use crate::attribute::query::AttributeQuery;
    use crate::the;
    use crate::{Cardinality, Parameters, Proposition, Term};
    use dialog_artifacts::Entity;
    use std::collections::BTreeSet;

    /// An order signature for a planned conjunction: the per-step
    /// sorted binds, in execution order. Two plans with the same
    /// signature schedule the same work in the same order.
    fn signature(plan: &Conjunction) -> Vec<Vec<String>> {
        plan.steps
            .iter()
            .map(|step| {
                let mut binds: Vec<String> = step.binds().iter().map(String::from).collect();
                binds.sort();
                binds
            })
            .collect()
    }

    fn name_age_premises() -> Vec<Premise> {
        let name = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::<Entity>::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        let age = AttributeQuery::new(
            Term::from(the!("person/age")),
            Term::<Entity>::var("person"),
            Term::var("age"),
            Term::var("cause"),
            Some(Cardinality::One),
        );
        vec![
            Premise::Assert(Proposition::Attribute(Box::new(name))),
            Premise::Assert(Proposition::Attribute(Box::new(age))),
        ]
    }

    /// Planning a two-premise rule yields a stable order and binds.
    /// Pinned so a refactor can be checked against an exact plan.
    #[dialog_common::test]
    fn it_pins_two_premise_plan_order() {
        let plan = Planner::from(name_age_premises())
            .plan(&Environment::new())
            .unwrap();

        // Both premises share `person` and `cause`; the first to run
        // binds them, the second only adds its own value. The plan
        // binds person, name, age, cause across two steps.
        let sig = signature(&plan);
        assert_eq!(sig.len(), 2, "two steps");
        let all_binds: BTreeSet<String> = sig.iter().flatten().cloned().collect();
        assert!(all_binds.contains("person"));
        assert!(all_binds.contains("name"));
        assert!(all_binds.contains("age"));
        assert!(all_binds.contains("cause"));
    }

    /// Replanning the same conjunction at the same scope is
    /// deterministic: same order, same binds, same cost. This is the
    /// idempotence a stateless feasibility function must preserve.
    #[dialog_common::test]
    fn it_replans_deterministically_at_same_scope() {
        let plan = Planner::from(name_age_premises())
            .plan(&Environment::new())
            .unwrap();
        let replanned = Planner::from(name_age_premises())
            .plan(&Environment::new())
            .unwrap();

        assert_eq!(
            signature(&plan),
            signature(&replanned),
            "replanning at the same scope yields the same order and binds"
        );
        assert_eq!(plan.cost, replanned.cost, "same total cost");
    }

    /// Replan grow-then-shrink returns to the original plan. Binding
    /// `person` re-plans (cheaper), and replanning back to the empty
    /// scope restores the original order, binds, and cost — the
    /// behavior the bidirectional-update fix guarantees, pinned at the
    /// plan level rather than via internal candidate state.
    #[dialog_common::test]
    fn it_restores_plan_on_grow_then_shrink() {
        let plan = Planner::from(name_age_premises())
            .plan(&Environment::new())
            .unwrap();
        let original_sig = signature(&plan);
        let original_cost = plan.cost;

        let mut bound = Environment::new();
        bound.add("person");
        let grown = Planner::from(name_age_premises()).plan(&bound).unwrap();
        // With `person` bound, neither step needs to bind it, so the
        // grown plan's binds differ from the empty-scope plan.
        assert!(
            grown.cost <= original_cost,
            "binding a shared variable should not increase cost"
        );

        let shrunk = Planner::from(name_age_premises())
            .plan(&Environment::new())
            .unwrap();
        assert_eq!(
            signature(&shrunk),
            original_sig,
            "replanning back to empty restores the original plan order/binds"
        );
        assert_eq!(
            shrunk.cost, original_cost,
            "replanning back to empty restores the original cost"
        );
    }

    // A discriminant tag per step, so order tests can assert the exact
    // *kind* of each step in sequence (not just its binds).
    fn kinds(plan: &Conjunction) -> Vec<&'static str> {
        plan.steps
            .iter()
            .map(|step| match step {
                Plan::Scan(..) => "scan",
                Plan::Formula(..) => "formula",
                Plan::Constraint(..) => "constraint",
                Plan::Concept(..) => "concept",
                Plan::Negate(..) => "negate",
            })
            .collect()
    }

    fn attribute(the: Term<The>, entity: &str, value: &str) -> Premise {
        AttributeQuery::new(
            the,
            Term::<Entity>::var(entity),
            Term::var(value),
            Term::var("cause"),
            Some(Cardinality::One),
        )
        .into()
    }

    fn length_formula(input: &str, output: &str) -> Premise {
        use crate::formula::Formula;
        use crate::formula::string::Length;
        let mut params = Parameters::new();
        params.insert("of".to_string(), Term::var(input));
        params.insert("is".to_string(), Term::var(output));
        Premise::from(Length::apply(params).unwrap())
    }

    /// A formula consuming an attribute's value must be scheduled
    /// *after* the attribute that binds its input — a formula whose
    /// required input is unbound is not viable, so the scan goes first.
    #[dialog_common::test]
    fn it_orders_formula_after_its_input_scan() {
        let premises = vec![
            // Formula listed first, but it needs `name` which only the
            // scan binds.
            length_formula("name", "len"),
            attribute(Term::from(the!("person/name")), "person", "name"),
        ];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();

        assert_eq!(
            kinds(&plan),
            vec!["scan", "formula"],
            "the scan binding the formula's input must run first"
        );
    }

    /// A negation only filters once its variables are bound, so a
    /// negated constraint over a scan's output is scheduled after the
    /// scan, never first.
    #[dialog_common::test]
    fn it_orders_negation_after_its_variables_are_bound() {
        let x = Term::<String>::var("name");
        let y = Term::<String>::var("forbidden");
        let premises = vec![
            // Negated equality needs both `name` and `forbidden`
            // bound; the scans bind them.
            !x.is(y),
            attribute(Term::from(the!("person/name")), "person", "name"),
            attribute(Term::from(the!("ban/name")), "ban", "forbidden"),
        ];
        let plan = Planner::from(premises).plan(&Environment::new()).unwrap();

        let order = kinds(&plan);
        assert_eq!(order.len(), 3);
        assert_eq!(
            order[2], "negate",
            "negation runs last, after its variables are bound by the scans"
        );
        assert_eq!(order[0], "scan");
        assert_eq!(order[1], "scan");
    }

    /// The greedy planner orders the cheaper premise first: a scan with
    /// a bound entity (LOOKUP) before a fully-unbound scan
    /// (RANGE_SCAN). Pins the cost-driven choice.
    #[dialog_common::test]
    fn it_orders_cheaper_scan_first() {
        // `a` shares `person` with `b`. At empty scope `a` (constant
        // entity via the shared var path) and `b` differ in cost; the
        // planner picks the one that constrains more first. Concretely:
        // a scan that can bind `person` cheaply unlocks the other.
        let alice = Entity::try_from("urn:alice".to_string()).unwrap();
        let bound_entity: Premise = AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::from(alice),
            Term::var("name"),
            Term::var("cause"),
            Some(Cardinality::One),
        )
        .into();
        let open: Premise = attribute(Term::from(the!("greeting/text")), "name", "greeting");

        let plan = Planner::from(vec![open.clone(), bound_entity.clone()])
            .plan(&Environment::new())
            .unwrap();

        // The entity-bound scan is cheaper (LOOKUP) and binds `name`,
        // which the other scan needs as its entity — so it must run
        // first regardless of input order.
        assert_eq!(kinds(&plan), vec!["scan", "scan"]);
        assert!(
            plan.steps[0].binds().contains("name"),
            "the cheaper, entity-bound scan runs first and binds `name`"
        );

        // Same result with the inputs reversed — ordering is by cost,
        // not by position.
        let reversed = Planner::from(vec![bound_entity, open])
            .plan(&Environment::new())
            .unwrap();
        assert_eq!(signature(&plan), signature(&reversed));
    }

    /// A mixed body — scans, a formula, an equality constraint, and a
    /// negation — plans into a deterministic order that is stable
    /// across replans at the same scope. This is the broad guardrail
    /// the stateless `Candidate` refactor must preserve.
    #[dialog_common::test]
    fn it_plans_mixed_body_deterministically() {
        let x = Term::<String>::var("name");
        let y = Term::<String>::var("nick");
        let premises = vec![
            attribute(Term::from(the!("person/name")), "person", "name"),
            length_formula("name", "len"),
            attribute(Term::from(the!("person/nick")), "person", "nick"),
            // equality constraint between two bound values
            x.is(y),
        ];

        let plan = Planner::from(premises.clone())
            .plan(&Environment::new())
            .unwrap();
        let replanned = Planner::from(premises).plan(&Environment::new()).unwrap();

        assert_eq!(
            kinds(&plan),
            kinds(&replanned),
            "mixed-body plan kind-order is deterministic across replans"
        );
        assert_eq!(
            signature(&plan),
            signature(&replanned),
            "mixed-body plan binds-order is deterministic across replans"
        );
        assert_eq!(plan.cost, replanned.cost, "mixed-body cost is stable");

        // Every premise kind that can bind appears; the formula and
        // constraint come after the scans that bind their inputs.
        let order = kinds(&plan);
        let first_scan = order.iter().position(|k| *k == "scan").unwrap();
        let formula_pos = order.iter().position(|k| *k == "formula").unwrap();
        assert!(
            formula_pos > first_scan,
            "formula is scheduled after the scan binding its input"
        );
    }

    /// A concept premise lowers to a `Plan::Concept` step and plans in
    /// a mixed body, scheduled after a scan that binds its `this`.
    #[dialog_common::test]
    fn it_plans_concept_premise() {
        use crate::concept::query::ConceptQuery;
        use crate::{AttributeDescriptor, ConceptDescriptor, Type};

        let concept = ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "person name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::var("person"));
        terms.insert("name".to_string(), Term::var("name"));
        let concept_premise = Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: concept,
        }));

        let premises = vec![
            attribute(Term::from(the!("org/member")), "org", "person"),
            concept_premise,
        ];
        let plan = Planner::from(premises.clone())
            .plan(&Environment::new())
            .unwrap();
        let replanned = Planner::from(premises).plan(&Environment::new()).unwrap();

        assert!(
            kinds(&plan).contains(&"concept"),
            "the concept premise lowers to a Plan::Concept step"
        );
        assert_eq!(
            kinds(&plan),
            kinds(&replanned),
            "concept-in-body plan order is deterministic"
        );
        assert_eq!(plan.cost, replanned.cost);
    }

    /// A coalesce constraint (set-widening unwrap) lowers to a
    /// `Plan::Constraint` step and plans alongside a scan. Because the
    /// coalesce `source` is *optional* (set-widened), the constraint
    /// does not require `nickname` bound to run — it can fall back —
    /// so it is viable at empty scope and, being cheap (cost 1), the
    /// greedy planner schedules it first. This pins the actual
    /// behavior: an optional source makes coalesce orderable before
    /// the scan that would bind it.
    #[dialog_common::test]
    fn it_plans_coalesce_constraint() {
        let nickname: Term<Option<String>> = Term::var("nickname");
        let display: Term<String> = Term::var("display");
        let coalesce = nickname.unwrap_or("Anon".to_string()).is(display);

        let nickname_scan = AttributeQuery::new(
            Term::from(the!("person/nickname")),
            Term::<Entity>::var("person"),
            // optional source term so the scan yields Absent on miss
            Term::<Option<String>>::var("nickname").into(),
            Term::var("cause"),
            Some(Cardinality::One),
        );

        let premises = vec![
            coalesce,
            Premise::Assert(Proposition::Attribute(Box::new(nickname_scan))),
        ];
        let plan = Planner::from(premises.clone())
            .plan(&Environment::new())
            .unwrap();

        let order = kinds(&plan);
        assert_eq!(order.len(), 2);
        assert!(
            order.contains(&"constraint"),
            "coalesce lowers to Constraint"
        );
        assert!(order.contains(&"scan"));
        assert_eq!(
            order,
            vec!["constraint", "scan"],
            "an optional-source coalesce is viable immediately and, being cheap, runs first"
        );

        // Deterministic across replans.
        let replanned = Planner::from(premises).plan(&Environment::new()).unwrap();
        assert_eq!(kinds(&plan), kinds(&replanned));
    }
}
