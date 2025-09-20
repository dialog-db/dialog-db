pub use super::{
    stream, try_stream, ApplicationPlan, EvaluationContext, EvaluationPlan, Ordering, Selection,
    Source, TryStreamExt, VariableScope,
};

/// Execution plan for a negated application.
/// Does not provide any variables since negation only filters matches.
#[derive(Debug, Clone, PartialEq)]
pub struct NegationPlan {
    /// The underlying application plan that will be negated
    pub application: ApplicationPlan,
    /// Variables provided by this plan (always empty for negation)
    pub provides: VariableScope,
}

impl NegationPlan {
    /// Creates a new negation plan from an application plan.
    pub fn not(application: ApplicationPlan) -> Self {
        Self {
            application,
            provides: VariableScope::new(),
        }
    }
    // evaluate method is now part of the EvaluationPlan trait implementation
}

// evaluate method is now part of the EvaluationPlan trait implementation

impl PartialOrd for NegationPlan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.cost().partial_cmp(&other.cost())
    }
}

impl EvaluationPlan for NegationPlan {
    fn cost(&self) -> usize {
        let Self { application, .. } = self;
        application.cost()
    }
    fn provides(&self) -> &VariableScope {
        &self.provides
    }

    fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let plan = self.application.clone();
        try_stream! {
            for await each in context.selection {
                let frame = each?;
                let not = frame.clone();
                let output = plan.evaluate(EvaluationContext {
                    selection: stream::once(async move { Ok(not)}),
                    store: context.store.clone()
                });

                tokio::pin!(output);

                if let Ok(Some(_)) = output.try_next().await {
                    continue;
                }

                yield frame;
            }
        }
    }
}
