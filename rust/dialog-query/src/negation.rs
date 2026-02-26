use super::proposition::Proposition;
use crate::selection::Answers;
use crate::{Environment, Parameters, Requirement, Schema, Source, try_stream};
pub use futures_util::{TryStreamExt, stream};
use std::fmt::Display;

/// Cost overhead added for negation operations (checking non-existence)
pub const NEGATION_OVERHEAD: usize = 100;

/// A negated proposition used inside [`Premise::Unless`](crate::Premise::Unless).
///
/// During query evaluation a `Negation` acts as a filter: for each incoming
/// [`Answer`](crate::selection::Answer), the wrapped [`Proposition`] is
/// evaluated. If it produces *any* results the answer is discarded; if it
/// produces *no* results the answer passes through unchanged.
///
/// Negations never bind new variables — they only constrain existing ones.
/// The planner accounts for this by never adding a negation's parameters to
/// the `binds` set of an [`Candidate`](crate::planner::Candidate).
#[derive(Debug, Clone, PartialEq)]
pub struct Negation(pub Proposition);

impl Negation {
    /// Create a negation wrapping the given application
    pub fn not(application: Proposition) -> Self {
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

    /// Returns the parameters of the underlying application
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
                    Requirement::Optional
                } else {
                    // Non-blank terms must be bound before negation can run
                    Requirement::Required(None)
                };
            }
        }

        schema
    }

    /// Evaluate this negation, yielding answers that do NOT match the inner application
    pub fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        let application = self.0;
        let source = source.clone();
        try_stream! {
            for await each in answers {
                let answer = each?;
                let not = answer.clone();
                let output = application.clone().evaluate(not.seed(), &source);

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
