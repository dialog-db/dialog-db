pub use super::{
    application, try_stream, ConceptPlan, EvaluationContext, EvaluationPlan, FactApplicationPlan,
    FormulaApplicationPlan, NegationPlan, Ordering, RuleApplicationPlan, Selection, Store,
    VariableScope,
};

/// Execution plan for different types of applications.
/// Contains the optimized execution strategy for each application type.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplicationPlan {
    /// Plan for fact selection operations
    Select(FactApplicationPlan),
    /// Plan for concept realization operations
    Concept(ConceptPlan),
    /// Plan for rule application operations
    Rule(RuleApplicationPlan),
    /// Plan for formula application operations
    Formula(FormulaApplicationPlan),
}

impl ApplicationPlan {
    /// Converts this application plan into a negated plan.
    pub fn not(self) -> NegationPlan {
        NegationPlan::not(self)
    }
    // evaluate method is now part of the EvaluationPlan trait implementation
}

impl EvaluationPlan for ApplicationPlan {
    fn cost(&self) -> usize {
        match self {
            ApplicationPlan::Select(plan) => plan.cost(),
            ApplicationPlan::Concept(plan) => plan.cost(),
            ApplicationPlan::Formula(plan) => plan.cost(),
            ApplicationPlan::Rule(plan) => plan.cost(),
        }
    }
    fn provides(&self) -> &VariableScope {
        match self {
            ApplicationPlan::Select(plan) => plan.provides(),
            ApplicationPlan::Concept(plan) => plan.provides(),
            ApplicationPlan::Formula(plan) => plan.provides(),
            ApplicationPlan::Rule(plan) => plan.provides(),
        }
    }

    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection {
        let source = self.clone();
        try_stream! {
            match source {
                ApplicationPlan::Select(plan) => {
                    for await each in plan.evaluate(context) {
                        yield each?;
                    }
                }
                ApplicationPlan::Concept(plan) => {
                    for await each in plan.evaluate(context) {
                        yield each?;
                    }
                },
                ApplicationPlan::Formula(plan) => {
                    for await each in plan.evaluate(context) {
                        yield each?;
                    }
                }
                ApplicationPlan::Rule(plan) => {
                    for await each in plan.evaluate(context) {
                        yield each?;
                    }
                }
            }
        }
    }
}

impl PartialOrd for ApplicationPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.cost().partial_cmp(&other.cost())
    }
}
