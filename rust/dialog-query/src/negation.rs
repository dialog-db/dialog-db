use super::application::Application;
use super::VariableScope;
use crate::{try_stream, EvaluationContext, Parameters, Schema, Selection, Source};
pub use futures_util::{stream, TryStreamExt};
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
    pub fn estimate(&self, env: &VariableScope) -> Option<usize> {
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
    /// because negation can't run until all terms are bound
    pub fn schema(&self) -> Schema {
        let Negation(application) = self;
        let mut schema = application.schema();

        // Convert all desired parameters to required
        for (_, constraint) in schema.iter_mut() {
            constraint.requirement = crate::Requirement::Required(None);
        }

        schema
    }

    pub fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let application = self.0.clone();
        try_stream! {
            for await each in context.selection {
                let frame = each?;
                let not = frame.clone();
                let output = application.evaluate(EvaluationContext {
                    selection: stream::once(async move { Ok(not)}),
                    source: context.source.clone(),
                    scope: context.scope.clone(),
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

impl Display for Negation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Negation(application) = self;
        write!(f, "! {}", application)
    }
}
