//! And combinator for combining multiple evaluation plans
//!
//! This module provides an `And` struct that combines two evaluation plans
//! by threading the selection from the first through the second.

use crate::plan::{EvaluationContext, EvaluationPlan};
use crate::syntax::VariableScope;
use crate::query::Store;
use crate::Selection;
use async_stream::try_stream;
/// Combines two evaluation plans by threading selections through them
#[derive(Debug, Clone)]
pub struct And<Left, Right> {
    pub left: Left,
    pub right: Right,
    cost: usize,
}

impl<Left, Right> And<Left, Right>
where
    Left: EvaluationPlan,
    Right: EvaluationPlan,
{
    /// Create a new And combinator from two plans
    pub fn new(left: Left, right: Right) -> Self {
        let cost = left.cost() + right.cost();

        Self { left, right, cost }
    }
}

impl<Left, Right> EvaluationPlan for And<Left, Right>
where
    Left: EvaluationPlan + 'static,
    Right: EvaluationPlan + 'static,
{
    fn cost(&self) -> usize {
        self.cost
    }

    fn provides(&self) -> VariableScope {
        let mut scope = VariableScope::new();
        scope.extend(self.left.provides());
        scope.extend(self.right.provides());
        scope
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let store = context.store.clone();

        let left = self.left.evaluate(context);
        let right = self.right.evaluate(EvaluationContext {
            selection: left,
            store,
        });

        right
    }
}

pub fn join<S: Store + 'static, M: Selection, P: EvaluationPlan + 'static>(
    conjuncts: Vec<P>,
    context: EvaluationContext<S, M>,
) -> impl Selection {
    try_stream! {
        match conjuncts.as_slice() {
            [] => {
                // Empty conjuncts - return input selection unchanged
                for await frame in context.selection {
                    yield frame?;
                }
            }
            [single] => {
                // Single conjunct - evaluate it directly
                let selection = single.evaluate(context);
                for await frame in selection {
                    yield frame?;
                }
            }
            plans => {
                // Multiple conjuncts - fold through them sequentially
                let store = context.store.clone();
                let mut current_selection: std::pin::Pin<Box<dyn Selection>> = Box::pin(context.selection);

                for plan in plans {
                    let plan_context = EvaluationContext {
                        store: store.clone(),
                        selection: current_selection,
                    };
                    current_selection = Box::pin(plan.evaluate(plan_context));
                }

                for await frame in current_selection {
                    yield frame?;
                }
            }
        }
    }
}
