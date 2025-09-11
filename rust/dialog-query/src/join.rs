use crate::deductive_rule::Plan;
use crate::{try_stream, EvaluationContext, EvaluationPlan, Match, QueryError, Selection, Store};

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
    ) -> impl Selection {
        use futures_util::stream::BoxStream;

        fn evaluate_recursive<S: Store, M: Selection>(
            join: Join,
            context: EvaluationContext<S, M>,
        ) -> BoxStream<'static, Result<Match, QueryError>> {
            match join {
                Join::Identity => Box::pin(try_stream! {
                    for await each in context.selection {
                        yield each?;
                    }
                }),
                Join::Join(left, right) => Box::pin(try_stream! {
                    let store = context.store.clone();
                    let selection = evaluate_recursive(*left, context);
                    let output = right.evaluate(EvaluationContext { selection, store });
                    for await each in output {
                        yield each?;
                    }
                }),
            }
        }

        evaluate_recursive(self, context)
    }
}
