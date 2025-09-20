use super::{Plan, Selection, Source, VariableScope};
use crate::plan::Join;
use crate::predicate::DeductiveRule;
use crate::{try_stream, EvaluationContext, EvaluationPlan, Parameters};

/// Execution plan for a rule application.
/// Contains all information needed to execute the rule and produce results.
#[derive(Debug, Clone, PartialEq)]
pub struct RuleApplicationPlan {
    /// Total estimated execution cost.
    pub cost: usize,
    /// Term bindings for the rule parameters.
    pub terms: Parameters,
    /// Ordered list of sub-plans to execute.
    pub conjuncts: Vec<Plan>,
    /// Variables that will be provided by this plan.
    pub provides: VariableScope,
    /// The rule being executed.
    pub rule: DeductiveRule,
}

impl RuleApplicationPlan {
    /// Evaluates this rule application plan against the provided context.
    pub fn eval<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        Self::eval_helper(context.store, context.selection, self.conjuncts.clone())
    }

    /// Helper function that recursively evaluates conjuncts in order.
    pub fn eval_helper<S: Source, M: Selection>(
        store: S,
        source: M,
        conjuncts: Vec<Plan>,
    ) -> impl Selection {
        try_stream! {
            match conjuncts.as_slice() {
                [] => {
                    for await each in source {
                        yield each?;
                    }
                }
                [plan, rest @ ..] => {
                    let selection = plan.evaluate(EvaluationContext {
                        store: store.clone(),
                        selection: source
                    });



                    let output = Self::eval_helper(
                        store,
                        selection,
                        rest.to_vec()
                    );

                    for await each in output {
                        yield each?;
                    }
                }
            }
        }
    }
}

impl EvaluationPlan for RuleApplicationPlan {
    fn cost(&self) -> usize {
        self.cost
    }
    fn provides(&self) -> &VariableScope {
        &self.provides
    }
    fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let join = Join::from(self.conjuncts.clone());
        join.evaluate(context)
    }
}
