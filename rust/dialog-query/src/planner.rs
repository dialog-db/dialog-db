use crate::analyzer::{Analysis, Plan};
use crate::artifact::Value;
use crate::context::new_context;
pub use crate::error::{AnalyzerError, PlanError};
use crate::error::{CompileError, QueryResult};
pub use crate::premise::Premise;
use crate::stream::{fork_stream, stream_select};
pub use crate::term::Term;
use crate::EvaluationContext;
pub use crate::{try_stream, Environment, Source};
use core::pin::Pin;

/// Query planner that optimizes the order of premise execution based on cost
/// and dependency analysis. Uses a state machine approach to iteratively
/// select the best premise to execute next.
pub enum Planner {
    /// Initial state with unprocessed premises.
    Idle { premises: Vec<Premise> },
    /// Processing state with cached candidates and current scope.
    Active { candidates: Vec<Analysis> },
}

impl Planner {
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

    /// Selects and returns the best premise to execute next based on cost.
    /// Updates the planner state by removing the selected premise from candidates.
    fn top(&mut self, env: &Environment) -> Result<Plan, CompileError> {
        match self {
            Planner::Idle { premises } => {
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
                    *self = Planner::Active { candidates };
                    Plan::try_from(analysis)
                } else {
                    Self::fail(&candidates)
                }
            }
            Planner::Active { candidates } => {
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

impl From<&Vec<Plan>> for Planner {
    fn from(plans: &Vec<Plan>) -> Self {
        Self::Active {
            candidates: plans.iter().map(|plan| plan.into()).collect(),
        }
    }
}

/// Represents a join plan - the result of planning multiple premises together.
/// Contains the ordered sequence of steps, total cost, and variable scopes.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// The ordered steps to execute
    pub steps: Vec<Plan>,
    /// Total execution cost
    pub cost: usize,
    /// Variables provided/bound by this join
    pub binds: Environment,
    /// Variables required in the environment to execute this join
    pub env: Environment,
}

impl Join {
    /// Replan this join with a different scope by converting existing steps to candidates
    pub fn plan(&self, scope: &Environment) -> Result<Self, CompileError> {
        let env = scope.clone();
        let mut bound = scope.clone();
        let mut steps = vec![];
        let mut cost = 0;

        // Convert existing plans back to analyses for replanning
        // let candidates: Vec<Analysis> = self.steps.iter().map(|plan| plan.into()).collect();

        let mut planner: Planner = (&self.steps).into();

        while !planner.done() {
            let plan = planner.top(&bound)?;

            cost += plan.cost();
            // Extend the scope with what this premise binds
            bound.extend(plan.binds());

            steps.push(plan);
        }

        // binds is the difference between final scope and initial env
        let mut binds = Environment::new();
        for var_name in &bound.variables {
            let var: Term<Value> = Term::var(var_name);
            if !env.contains(&var) {
                binds.add(&var);
            }
        }

        Ok(Join {
            steps,
            cost,
            binds,
            env,
        })
    }
}

impl TryFrom<Vec<Premise>> for Join {
    type Error = CompileError;

    fn try_from(premises: Vec<Premise>) -> Result<Self, Self::Error> {
        let env = Environment::new();
        let mut bound = Environment::new();
        let mut steps = vec![];
        let mut cost = 0;

        let mut planner = Planner::Idle { premises };

        while !planner.done() {
            let plan = planner.top(&bound)?;

            cost += plan.cost();
            // Extend the scope with what this premise binds
            bound.extend(plan.binds());

            steps.push(plan);
        }

        // binds is the difference between final scope and initial env
        let mut binds = Environment::new();
        for var_name in &bound.variables {
            let var: Term<Value> = Term::var(var_name);
            if !env.contains(&var) {
                binds.add(&var);
            }
        }

        Ok(Self {
            steps,
            cost,
            binds,
            env,
        })
    }
}

impl From<&Vec<Plan>> for Join {
    fn from(plans: &Vec<Plan>) -> Self {
        let env = Environment::new();
        let mut bound = Environment::new();
        let mut steps = vec![];
        let mut cost = 0;

        let mut planner: Planner = plans.into();

        while !planner.done() {
            let plan = planner
                .top(&bound)
                .expect("Plan from empty scope can be planned in non-empty scope");

            cost += plan.cost();
            // Extend the scope with what this premise binds
            bound.extend(plan.binds());

            steps.push(plan);
        }

        // binds is the difference between final scope and initial env
        let mut binds = Environment::new();
        for var_name in &bound.variables {
            let var: Term<Value> = Term::var(var_name);
            if !env.contains(&var) {
                binds.add(&var);
            }
        }

        Self {
            steps,
            cost,
            binds,
            env,
        }
    }
}

impl Join {
    /// Evaluate this join plan by executing all steps in order.
    /// Each step flows results to the next, building up bindings.
    pub fn evaluate<S: Source, M: crate::selection::Answers>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl crate::selection::Answers {
        let chain = Chain::from(self.steps.clone());
        chain.evaluate(context)
    }

    pub fn query<S: Source>(&self, store: &S) -> QueryResult<impl crate::selection::Answers> {
        let store = store.clone();
        let context = new_context(store);
        let answers = self.evaluate(context);
        Ok(answers)
    }
}

/// Recursive chain structure for joining 2+ plan steps.
/// This explicit recursion at the value level avoids type-level recursion
/// that would cause compiler stack overflow.
#[derive(Debug, Clone, PartialEq)]
pub enum Chain {
    /// Base case - passes through the selection unchanged.
    Empty,
    /// Recursive case - joins a plan with the rest of the join chain.
    Join(Box<Chain>, Plan),
}

impl Chain {
    /// Creates a new empty join (identity).
    pub fn new() -> Self {
        Chain::Empty
    }

    /// Adds a plan to this join chain.
    pub fn and(self, plan: Plan) -> Self {
        Chain::Join(Box::new(self), plan)
    }

    /// Creates a join from a vector of plans by chaining them together.
    pub fn from(plans: Vec<Plan>) -> Self {
        plans
            .into_iter()
            .fold(Self::Empty, |join, plan| join.and(plan))
    }

    /// Evaluate this chain by executing plans in sequence
    fn evaluate<S: Source, M: crate::selection::Answers>(
        self,
        context: EvaluationContext<S, M>,
    ) -> Pin<Box<dyn crate::selection::Answers>> {
        Box::pin(try_stream! {
            match self {
                Chain::Empty => {
                    for await each in context.selection {
                        yield each?;
                    }
                },
                Chain::Join(left, right) => {
                    let source = context.source.clone();
                    let scope = context.scope.clone();
                    let answers = left.evaluate(context);
                    let output = right.evaluate(EvaluationContext {
                        selection: answers,
                        source,
                        scope,
                    });
                    for await each in output {
                        yield each?;
                    }
                },
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Fork {
    Empty,
    Solo(Join),
    Duet(Join, Join),
    Or(Box<Fork>, Join),
}

impl Fork {
    /// Creates a new empty join (identity).
    pub fn new() -> Self {
        Self::Empty
    }

    /// Creates a new join of two plans.
    pub fn or(self, right: Join) -> Self {
        match self {
            Self::Empty => Self::Solo(right),
            Self::Solo(left) => Self::Duet(left, right),
            _ => Self::Or(Box::new(self), right),
        }
    }

    pub fn evaluate<S: Source, M: crate::selection::Answers>(
        self,
        context: EvaluationContext<S, M>,
    ) -> Pin<Box<dyn crate::selection::Answers>> {
        Box::pin(try_stream! {
            match self {
                Self::Empty => {
                    for await each in context.selection {
                        each?;
                    }
                },
                Self::Solo(left) => {
                    for await each in left.evaluate(context) {
                        yield each?;
                    }
                },
                Self::Duet(left, right) => {
                    let (left_input, right_input) = fork_stream(context.selection);

                    let scope = context.scope.clone();
                    let source = context.source.clone();
                    let left_output = left.evaluate(EvaluationContext { selection:left_input, source: source, scope });
                    let right_output = right.evaluate(EvaluationContext { selection:right_input, source: context.source, scope: context.scope });

                    tokio::pin!(left_output);
                    tokio::pin!(right_output);

                    for await each in stream_select!(left_output, right_output) {
                        yield each?;
                    }
                },
                Self::Or(left, right) => {
                    let (left_input, right_input) = fork_stream(context.selection);

                    let scope = context.scope.clone();
                    let source = context.source.clone();
                    let left_output = left.evaluate(EvaluationContext { selection:left_input, source: source, scope });
                    let right_output = right.evaluate(EvaluationContext { selection:right_input, source: context.source, scope: context.scope });

                    tokio::pin!(left_output);
                    tokio::pin!(right_output);

                    for await each in stream_select!(left_output, right_output) {
                        yield each?;
                    }
                },
            }
        })
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
        Term::var("cause"),
        Cardinality::One,
    );

    // Second: (person/age, of: ?person, is: ?age) - find person's age
    let fact2 = FactApplication::new(
        Term::Constant(Attribute::try_from("person/age".to_string()).unwrap()),
        Term::var("person"),
        Term::var("age"),
        Term::var("cause"),
        Cardinality::One,
    );

    // Create premises from the applications
    let premises = vec![Premise::from(fact1), Premise::from(fact2)];

    // Create a join planner and plan with empty scope
    let plan = Join::try_from(premises).expect("Planning should succeed");

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
        Term::var("cause"),
        Cardinality::One,
    );

    // Second: (greeting/text, of: ?name, is: ?greeting) - uses ?name from first
    // Note: ?name here refers to the Entity value, not Attribute
    let fact2 = FactApplication::new(
        Term::Constant(Attribute::try_from("greeting/text".to_string()).unwrap()),
        Term::var("name"),
        Term::var("greeting"),
        Term::var("cause"),
        Cardinality::One,
    );

    let premises = vec![Premise::from(fact1), Premise::from(fact2)];

    let plan = Join::try_from(premises).expect("Planning should succeed");

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
    use crate::{Cardinality, Fact, Relation, Term, Value};
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
            Relation {
                the: "person/name".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Relation {
                the: "person/age".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::UnsignedInt(25),
            },
            Relation {
                the: "person/name".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Relation {
                the: "person/age".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::UnsignedInt(30),
            },
        ])
        .await?;

    // Create a join query: find person's name and age
    let fact1 = FactApplication::new(
        Term::Constant(Attribute::try_from("person/name".to_string()).unwrap()),
        Term::var("person"),
        Term::var("name"),
        Term::var("cause"),
        Cardinality::One,
    );

    let fact2 = FactApplication::new(
        Term::Constant(Attribute::try_from("person/age".to_string()).unwrap()),
        Term::var("person"),
        Term::var("age"),
        Term::var("cause"),
        Cardinality::One,
    );

    let premises = vec![Premise::from(fact1), Premise::from(fact2)];
    let plan = Join::try_from(premises)?;

    // Execute the query
    let selection =
        futures_util::TryStreamExt::try_collect::<Vec<_>>(plan.query(&session)?).await?;

    // Should find both Alice and Bob with their name and age
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
