use super::Plan;
use crate::{try_stream, EvaluationContext, EvaluationPlan, Selection, Store};
use core::pin::Pin;

/// Represents a join operation that combines multiple query plans.
/// Uses a recursive structure to chain plans together.
#[derive(Debug, Clone)]
pub enum Join {
    /// Base case - passes through the selection unchanged.
    Identity,
    /// Recursive case - joins a plan with the rest of the join chain.
    Join(Box<Join>, Plan),
}

impl Join {
    /// Creates a new empty join (identity).
    pub fn new() -> Self {
        Join::Identity
    }

    /// Creates a join from a vector of plans by chaining them together.
    pub fn from(plans: Vec<Plan>) -> Self {
        plans
            .into_iter()
            .fold(Join::Identity, |join, plan| join.and(plan))
    }

    /// Adds a plan to this join chain.
    pub fn and(self, plan: Plan) -> Self {
        Join::Join(Box::new(self), plan)
    }

    /// Evaluates the join by executing each plan in sequence,
    /// feeding the output of one plan as input to the next.
    pub fn evaluate<S: Store, M: Selection>(
        self,
        context: EvaluationContext<S, M>,
    ) -> Pin<Box<dyn Selection>> {
        Box::pin(try_stream! {
            match self {
                Join::Identity => {
                    for await each in context.selection {
                        yield each?;
                    }
                },
                Join::Join(left, right) => {
                    let store = context.store.clone();
                    let selection = left.evaluate(context);
                    let output = right.evaluate(EvaluationContext { selection, store });
                    for await each in output {
                        yield each?;
                    }
                },
            }
        })
    }
}

#[test]
fn test_join_operations() {
    let join = Join::new();
    match join {
        Join::Identity => {
            // Expected initial state
        }
        _ => panic!("Expected Identity variant"),
    }

    // Test building joins
    let plans = vec![];
    let join_from_plans = Join::from(plans);
    match join_from_plans {
        Join::Identity => {
            // Expected for empty vec
        }
        _ => panic!("Expected Identity for empty plans"),
    }
}
