use super::Environment;
use super::application::Application;
use crate::{EvaluationContext, Parameters, Schema, Source, try_stream};
pub use futures_util::{TryStreamExt, stream};
use std::fmt::Display;

// FactSelectorPlan's EvaluationPlan implementation is in fact_selector.rs

/// Cost overhead added for negation operations (checking non-existence)
pub const NEGATION_OVERHEAD: usize = 100;

/// Represents a negated application that excludes matching results.
/// Used in rules to specify conditions that must NOT hold.
#[derive(Debug, Clone, PartialEq)]
pub struct Negation(pub Application);

impl Negation {
    pub fn not(application: Application) -> Self {
        Negation(application)
    }

    /// Estimate the cost of this negation given the current environment.
    /// Negation adds overhead to the underlying application's cost.
    /// Returns None if the underlying application cannot be executed.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        let Negation(application) = self;
        application
            .estimate(env)
            .map(|cost| cost + NEGATION_OVERHEAD)
    }

    pub fn parameters(&self) -> Parameters {
        let Negation(application) = self;
        application.parameters()
    }

    /// Returns schema for negation - all parameters become required
    /// because negation can't run until all terms are bound.
    /// Exception: blank terms don't need to be bound.
    pub fn schema(&self) -> Schema {
        let Negation(application) = self;
        let mut schema = application.schema();
        let params = application.parameters();

        // Convert all parameters: non-blank become required, blank become optional
        for (name, constraint) in schema.iter_mut() {
            if let Some(term) = params.get(name) {
                constraint.requirement = if term.is_blank() {
                    // Blank terms are wildcards - mark as optional so they don't block planning
                    crate::Requirement::Optional
                } else {
                    // Non-blank terms must be bound before negation can run
                    crate::Requirement::Required(None)
                };
            }
        }

        schema
    }

    pub fn evaluate<S: Source, M: crate::selection::Answers>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl crate::selection::Answers {
        let application = self.0.clone();
        try_stream! {
            for await each in context.selection {
                let answer = each?;
                let not = answer.clone();
                let output = application.evaluate(EvaluationContext {
                    selection: stream::once(async move { Ok(not)}),
                    source: context.source.clone(),
                    scope: context.scope.clone(),
                });

                tokio::pin!(output);

                if let Ok(Some(_)) = output.try_next().await {
                    continue;
                }

                yield answer;
            }
        }
    }
}

impl Display for Negation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Negation(application) = self;
        write!(f, "! {}", application)
    }
}
