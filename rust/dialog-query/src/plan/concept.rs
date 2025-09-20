use super::{EvaluationContext, EvaluationPlan, Join, Plan, Selection, Source, VariableScope};

/// Execution plan for a concept application.
/// Contains the cost estimate, variables that will be provided by execution,
/// and the individual sub-plans that need to be executed and joined.
#[derive(Debug, Clone, PartialEq)]
pub struct ConceptPlan {
    /// Estimated execution cost for this plan.
    pub cost: usize,
    /// Variables that will be bound by executing this plan.
    pub provides: VariableScope,
    /// Individual sub-plans that must all succeed for the concept to match.
    pub conjuncts: Vec<Plan>,
}
impl EvaluationPlan for ConceptPlan {
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
