use crate::analyzer::{Analysis, Plan};
use crate::artifact::Value;
pub use crate::error::{AnalyzerError, PlanError};
use crate::error::{CompileError, QueryResult};
use crate::plan::fresh;
pub use crate::plan::EvaluationPlan;
pub use crate::premise::Premise;
pub use crate::term::Term;
use crate::EvaluationContext;
pub use crate::{try_stream, Selection, Source, VariableScope};

/// Query planner that optimizes the order of premise execution based on cost
/// and dependency analysis. Uses a state machine approach to iteratively
/// select the best premise to execute next.
pub enum Join {
    /// Initial state with unprocessed premises.
    Idle { premises: Vec<Premise> },
    /// Processing state with cached candidates and current scope.
    Active { candidates: Vec<Analysis> },
}

impl Join {
    /// Creates a new planner for the given premises.
    pub fn new(premises: Vec<Premise>) -> Self {
        Self::Idle { premises }
    }

    /// Helper to create a planning error from failed candidates.
    fn fail(analyses: &[Analysis]) -> Result<Plan, CompileError> {
        // If there are no candidates at all, return empty Required
        if analyses.is_empty() {
            return Err(CompileError::RequiredBindings {
                required: crate::analyzer::Required::new(),
            });
        }

        // Return the first required bindings error we find
        for analysis in analyses {
            if let Analysis::Blocked { requires, .. } = analysis {
                if requires.count() > 0 {
                    return Err(CompileError::RequiredBindings {
                        required: requires.clone(),
                    });
                }
            }
        }

        unreachable!("Should have had at least one blocked candidate with requirements");
    }

    /// Checks if planning is complete (all premises have been planned).
    fn done(&self) -> bool {
        match self {
            Self::Idle { .. } => false,
            Self::Active { candidates } => candidates.len() == 0,
        }
    }

    /// Creates an optimized execution plan for all premises.
    /// Returns a JoinPlan with the ordered steps, cost, and variable scopes.
    pub fn plan(&mut self, scope: &VariableScope) -> Result<JoinPlan, CompileError> {
        let env = scope.clone();
        let mut bound = scope.clone();
        let mut steps = vec![];
        let mut cost = 0;

        while !self.done() {
            let plan = self.top(&bound)?;

            cost += plan.cost();
            // Extend the scope with what this premise binds
            bound.extend(plan.binds());

            steps.push(plan);
        }

        // binds is the difference between final scope and initial env
        let mut binds = VariableScope::new();
        for var_name in &bound.variables {
            let var: Term<Value> = Term::var(var_name);
            if !env.contains(&var) {
                binds.add(&var);
            }
        }

        Ok(JoinPlan {
            steps,
            cost,
            binds,
            env,
        })
    }
    /// Selects and returns the best premise to execute next based on cost.
    /// Updates the planner state by removing the selected premise from candidates.
    pub fn top(&mut self, env: &VariableScope) -> Result<Plan, CompileError> {
        match self {
            Join::Idle { premises } => {
                let mut candidates = vec![];
                let mut best: Option<(usize, usize)> = None; // (cost, index)

                // Analyze each premise to create initial candidates
                for (index, premise) in premises.iter().enumerate() {
                    let analysis = premise.analyze(env);

                    // Check if this analysis is viable
                    if analysis.is_viable() {
                        let cost = analysis.cost();

                        if let Some((best_cost, _)) = &best {
                            if cost < *best_cost {
                                best = Some((cost, index));
                            }
                        } else {
                            best = Some((cost, index));
                        }
                    }

                    candidates.push(analysis);
                }

                if let Some((_, best_index)) = best {
                    let analysis = candidates.remove(best_index);
                    *self = Join::Active { candidates };
                    Plan::try_from(analysis)
                } else {
                    Self::fail(&candidates)
                }
            }
            Join::Active { candidates } => {
                let mut best: Option<(usize, usize)> = None; // (cost, index)

                // Update all candidates with new bindings
                for (index, analysis) in candidates.iter_mut().enumerate() {
                    // Update this analysis with the current environment
                    analysis.update(env);

                    // Check if this analysis is now viable
                    if analysis.is_viable() {
                        let cost = analysis.cost();

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
                    let analysis = candidates.remove(best_index);
                    Plan::try_from(analysis)
                } else {
                    Self::fail(&candidates)
                }
            }
        }
    }
}

/// Represents a join plan - the result of planning multiple premises together.
/// Contains the ordered sequence of steps, total cost, and variable scopes.
#[derive(Debug, Clone, PartialEq)]
pub struct JoinPlan {
    /// The ordered steps to execute
    pub steps: Vec<Plan>,

    /// Total execution cost
    pub cost: usize,
    /// Variables provided/bound by this join
    pub binds: VariableScope,
    /// Variables required in the environment to execute this join
    pub env: VariableScope,
}

impl JoinPlan {
    /// Evaluate this join plan by executing all steps in order.
    /// Each step flows results to the next, building up bindings.
    pub fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let steps = self.steps.clone();
        try_stream! {
            match steps.as_slice() {
                [] => {
                    // No steps - just pass through the selection
                    for await each in context.selection {
                            yield each?;
                    }
                }
                [plan, plans @ ..] => {
                    // Single step - evaluate directly without wrapping
                    let source = context.source.clone();
                    let scope = context.scope.clone();
                    let mut selection = plan.evaluate(context);
                    for plan in plans {
                        selection = plan.evaluate(EvaluationContext { selection, source: source.clone(), scope: scope.clone() });
                    }

                    for await each in selection {
                        yield each?;
                    }
                }
            }
        }
    }

    pub fn query<S: Source>(&self, store: &S) -> QueryResult<impl Selection> {
        let store = store.clone();
        let context = fresh(store);
        let selection = self.evaluate(context);
        Ok(selection)
    }
}

#[test]
fn test_join_plan_with_two_fact_applications() {
    use crate::application::FactApplication;
    use crate::{Cardinality, Term, Value};
    use dialog_artifacts::Attribute;

    // Create two fact applications that will be joined
    // First: (person/name, of: ?person, is: ?name) - find person's name
    let fact1 = FactApplication::new(
        Term::Constant(Attribute::try_from("person/name".to_string()).unwrap()),
        Term::var("person"),
        Term::var("name"),
        Cardinality::One,
    );

    // Second: (person/age, of: ?person, is: ?age) - find person's age
    let fact2 = FactApplication::new(
        Term::Constant(Attribute::try_from("person/age".to_string()).unwrap()),
        Term::var("person"),
        Term::var("age"),
        Cardinality::One,
    );

    // Create premises from the applications
    let premises = vec![Premise::from(fact1), Premise::from(fact2)];

    // Create a join planner and plan with empty scope
    let mut join = Join::new(premises);
    let scope = VariableScope::new();
    let plan = join.plan(&scope).expect("Planning should succeed");

    // Verify the plan was created
    assert_eq!(plan.steps.len(), 2, "Should have two steps");
    assert!(plan.cost > 0, "Should have non-zero cost");

    // Verify that the plan binds the expected variables
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

#[test]
fn test_join_plan_execution_order() {
    use crate::application::FactApplication;
    use crate::{Cardinality, Term};
    use dialog_artifacts::{Attribute, Entity};

    // Create two fact applications where one depends on the other
    // First: (person/name, of: urn:alice, is: ?name) - alice's name is bound
    let fact1 = FactApplication::new(
        Term::Constant(Attribute::try_from("person/name".to_string()).unwrap()),
        Term::Constant(Entity::try_from("urn:alice".to_string()).unwrap()),
        Term::var("name"),
        Cardinality::One,
    );

    // Second: (greeting/text, of: ?name, is: ?greeting) - uses ?name from first
    // Note: ?name here refers to the Entity value, not Attribute
    let fact2 = FactApplication::new(
        Term::Constant(Attribute::try_from("greeting/text".to_string()).unwrap()),
        Term::var("name"),
        Term::var("greeting"),
        Cardinality::One,
    );

    let premises = vec![Premise::from(fact1), Premise::from(fact2)];

    let mut join = Join::new(premises);
    let scope = VariableScope::new();
    let plan = join.plan(&scope).expect("Planning should succeed");

    // The planner should execute fact1 first (lower cost - entity is bound)
    // Then fact2 (which now has ?name bound from fact1)
    assert_eq!(plan.steps.len(), 2);

    // After both steps, 2 variables should be bound (name and greeting)
    assert_eq!(plan.binds.variables.len(), 2, "Should bind 2 variables");
}

#[tokio::test]
async fn test_join_plan_query_execution() -> anyhow::Result<()> {
    use crate::application::FactApplication;
    use crate::session::Session;
    use crate::{Cardinality, Fact, SelectionExt, Term, Value};
    use dialog_artifacts::{Artifacts, Attribute, Entity};
    use dialog_storage::MemoryStorageBackend;

    // Create a store and session
    let backend = MemoryStorageBackend::default();
    let store = Artifacts::anonymous(backend).await?;
    let mut session = Session::open(store);

    let alice = Entity::new()?;
    let bob = Entity::new()?;

    session
        .transact(vec![
            Fact::assert(
                "person/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<Attribute>()?,
                alice.clone(),
                Value::UnsignedInt(25),
            ),
            Fact::assert(
                "person/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<Attribute>()?,
                bob.clone(),
                Value::UnsignedInt(30),
            ),
        ])
        .await?;

    // Create a join query: find person's name and age
    let fact1 = FactApplication::new(
        Term::Constant(Attribute::try_from("person/name".to_string()).unwrap()),
        Term::var("person"),
        Term::var("name"),
        Cardinality::One,
    );

    let fact2 = FactApplication::new(
        Term::Constant(Attribute::try_from("person/age".to_string()).unwrap()),
        Term::var("person"),
        Term::var("age"),
        Cardinality::One,
    );

    let premises = vec![Premise::from(fact1), Premise::from(fact2)];
    let mut join = Join::new(premises);
    let scope = VariableScope::new();
    let plan = join.plan(&scope)?;

    // Execute the query
    let selection = plan.query(&session)?.collect_matches().await?;

    // Should find both Alice and Bob with their name and age
    assert_eq!(selection.len(), 2, "Should find 2 people");

    let name_var: Term<Value> = Term::var("name");
    let age_var: Term<Value> = Term::var("age");

    let mut found_alice = false;
    let mut found_bob = false;

    for match_result in selection.iter() {
        let name = match_result.get(&name_var)?;
        let age = match_result.get(&age_var)?;

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
