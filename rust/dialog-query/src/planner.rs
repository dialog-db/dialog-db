mod candidate;
mod conjunction;
mod disjunction;
mod plan;
mod prerequisites;

pub use candidate::*;
pub use conjunction::*;
pub use disjunction::*;
pub use plan::*;
pub use prerequisites::*;

use crate::artifact::Value;
use crate::error::CompileError;
use crate::term::Term;
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
    pub fn plan(mut self, scope: &Environment) -> Result<Conjunction, CompileError> {
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
        for var_name in &bound.variables {
            let var: Term<Value> = Term::var(var_name);
            if !env.contains(&var) {
                binds.add(&var);
            }
        }

        Ok(Conjunction {
            steps,
            cost,
            binds,
            env,
        })
    }

    /// Helper to create a planning error from failed candidates.
    fn fail(candidates: &[Candidate]) -> Result<Plan, CompileError> {
        if candidates.is_empty() {
            return Err(CompileError::RequiredBindings {
                required: Prerequisites::new(),
            });
        }

        for candidate in candidates {
            if let Candidate::Blocked { requires, .. } = candidate
                && !requires.is_empty()
            {
                return Err(CompileError::RequiredBindings {
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
    fn top(&mut self, env: &Environment) -> Result<Plan, CompileError> {
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

impl From<&Vec<Plan>> for Planner {
    fn from(plans: &Vec<Plan>) -> Self {
        Self::Active {
            candidates: plans.iter().map(|plan| plan.into()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::Answer;

    #[dialog_common::test]
    fn test_join_plan_with_two_fact_applications() {
        use crate::relation::descriptor::RelationDescriptor;
        use crate::relation::query::RelationQuery;
        use crate::{Cardinality, Proposition, Term, Value};

        let fact1 = RelationQuery::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let fact2 = RelationQuery::new(
            Term::Constant("person".to_string()),
            Term::Constant("age".to_string()),
            Term::var("person"),
            Term::var("age"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let premises = vec![
            crate::Premise::When(Proposition::Relation(Box::new(fact1))),
            crate::Premise::When(Proposition::Relation(Box::new(fact2))),
        ];

        let plan = Planner::from(premises)
            .plan(&Environment::new())
            .expect("Planning should succeed");

        assert_eq!(plan.steps.len(), 2, "Should have two steps");
        assert!(plan.cost > 0, "Should have non-zero cost");

        let person_var: Term<Value> = Term::var("person");
        let name_var: Term<Value> = Term::var("name");
        let age_var: Term<Value> = Term::var("age");

        assert!(
            plan.binds.contains(&person_var),
            "Should bind person variable"
        );
        assert!(plan.binds.contains(&name_var), "Should bind name variable");
        assert!(plan.binds.contains(&age_var), "Should bind age variable");
    }

    #[dialog_common::test]
    fn test_join_plan_execution_order() {
        use crate::relation::descriptor::RelationDescriptor;
        use crate::relation::query::RelationQuery;
        use crate::{Cardinality, Proposition, Term};
        use dialog_artifacts::Entity;

        let fact1 = RelationQuery::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::Constant(Entity::try_from("urn:alice".to_string()).unwrap()),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let fact2 = RelationQuery::new(
            Term::Constant("greeting".to_string()),
            Term::Constant("text".to_string()),
            Term::var("name"),
            Term::var("greeting"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let premises = vec![
            crate::Premise::When(Proposition::Relation(Box::new(fact1))),
            crate::Premise::When(Proposition::Relation(Box::new(fact2))),
        ];

        let plan = Planner::from(premises)
            .plan(&Environment::new())
            .expect("Planning should succeed");

        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.binds.variables.len(), 2, "Should bind 2 variables");
    }

    #[dialog_common::test]
    async fn test_join_plan_query_execution() -> anyhow::Result<()> {
        use crate::relation::descriptor::RelationDescriptor;
        use crate::relation::query::RelationQuery;
        use crate::session::Session;
        use crate::{Association, Cardinality, Proposition, Term, Value, the};
        use dialog_artifacts::{Artifacts, Entity};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        session
            .transact(vec![
                Association {
                    the: the!("person/name"),
                    of: alice.clone(),
                    is: Value::String("Alice".to_string()),
                },
                Association {
                    the: the!("person/age"),
                    of: alice.clone(),
                    is: Value::UnsignedInt(25),
                },
                Association {
                    the: the!("person/name"),
                    of: bob.clone(),
                    is: Value::String("Bob".to_string()),
                },
                Association {
                    the: the!("person/age"),
                    of: bob.clone(),
                    is: Value::UnsignedInt(30),
                },
            ])
            .await?;

        let fact1 = RelationQuery::new(
            Term::Constant("person".to_string()),
            Term::Constant("name".to_string()),
            Term::var("person"),
            Term::var("name"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let fact2 = RelationQuery::new(
            Term::Constant("person".to_string()),
            Term::Constant("age".to_string()),
            Term::var("person"),
            Term::var("age"),
            Term::var("cause"),
            Some(RelationDescriptor::new(None, Cardinality::One)),
        );

        let premises = vec![
            crate::Premise::When(Proposition::Relation(Box::new(fact1))),
            crate::Premise::When(Proposition::Relation(Box::new(fact2))),
        ];
        let plan = Planner::from(premises).plan(&Environment::new())?;

        let selection = futures_util::TryStreamExt::try_collect::<Vec<_>>(
            plan.evaluate(Answer::new().seed(), &session),
        )
        .await?;

        assert_eq!(selection.len(), 2, "Should find 2 people");

        let name_var: Term<Value> = Term::var("name");
        let age_var: Term<Value> = Term::var("age");

        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let name = match_result.resolve(&name_var)?;
            let age = match_result.resolve(&age_var)?;

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
}
