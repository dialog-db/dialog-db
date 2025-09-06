use crate::plan::{EvaluationContext, EvaluationPlan};
use crate::query::Store;
use crate::syntax::VariableScope;
use crate::Selection;
use async_stream::try_stream;

/// A composable join structure that can represent zero, one, or two evaluation plans
#[derive(Debug, Clone)]
pub struct And<L: EvaluationPlan + 'static, R: EvaluationPlan + 'static> {
    left: L,
    right: R,
}

impl<L: EvaluationPlan + 'static, R: EvaluationPlan + 'static> And<L, R> {
    /// Create a new `ComposableJoin` with two evaluation plans
    pub fn new(left: L, right: R) -> Self {
        And { left, right }
    }

    /// Create a new `ComposableJoin` with two evaluation plans
    pub fn and<P: EvaluationPlan + 'static>(self, right: P) -> And<Self, P> {
        And { left: self, right }
    }
}

#[derive(Debug, Clone)]
pub struct Through;
impl Through {
    pub fn new() -> impl EvaluationPlan + Sized {
        Through
    }

    pub fn and<P: EvaluationPlan + 'static>(self, right: P) -> impl EvaluationPlan {
        and(self, right)
    }
}
impl EvaluationPlan for Through {
    fn cost(&self) -> usize {
        0
    }

    fn provides(&self) -> VariableScope {
        VariableScope::new()
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        context.selection
    }
}

pub fn empty() -> impl EvaluationPlan {
    Through::new()
}

pub fn and<L: EvaluationPlan + 'static, R: EvaluationPlan + 'static>(
    left: L,
    right: R,
) -> impl EvaluationPlan {
    And::new(left, right)
}

impl<L: EvaluationPlan + 'static, R: EvaluationPlan + 'static> EvaluationPlan for And<L, R> {
    fn cost(&self) -> usize {
        self.left.cost() + self.right.cost()
    }

    fn provides(&self) -> VariableScope {
        self.left.provides().extend(self.right.provides())
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let left = self.left.clone();
        let right = self.right.clone();

        try_stream! {
          let store = context.store.clone();
          let left_selection = left.evaluate(context);
          let right_selection = right.evaluate(EvaluationContext {
              selection: left_selection,
              store
          });
          for await frame in right_selection {
              yield frame?;
          }
        }
    }
}

trait PlanChain: EvaluationPlan + Sized + 'static {
    fn chain<Other: EvaluationPlan + 'static>(self, other: Other) -> impl EvaluationPlan + Sized {
        And::new(self, other)
    }
}
// Implement for all EvaluationPlan types
impl<T: EvaluationPlan + 'static> PlanChain for T {}

// Now you can chain fluently with zero cost:
// fn test_building_join<L: EvaluationPlan, M: EvaluationPlan, R: EvaluationPlan>(
//     left: L,
//     middle: M,
//     right: R,
// ) -> impl EvaluationPlan {
//     // let mut chain = Through::new();
//     // chain = chain.chain(left);
//     // // chain = chain.chain(middle);
//     // // chain = chain.chain(right);
//     // chain
// }
