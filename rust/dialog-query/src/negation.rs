use std::fmt::{self, Display};

use super::proposition::Proposition;
use crate::selection::Selection;
use crate::{Environment, Parameters, Requirement, Schema, Source, try_stream};
pub use futures_util::{TryStreamExt, stream};

/// Cost overhead added for negation operations (checking non-existence)
pub const NEGATION_OVERHEAD: usize = 100;

/// A negated proposition used inside [`Premise::Unless`](crate::Premise::Unless).
///
/// During query evaluation a `Negation` acts as a filter: for each incoming
/// [`Match`](crate::selection::Match), the wrapped [`Proposition`] is
/// evaluated. If it produces *any* results the match is discarded; if it
/// produces *no* results the match passes through unchanged.
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

    /// Evaluate this negation, yielding matches that do NOT match the inner application
    pub fn evaluate<S: Source, M: Selection>(self, selection: M, source: &S) -> impl Selection {
        let application = self.0;
        let source = source.clone();
        try_stream! {
            for await candidate in selection {
                let base = candidate?;
                let output = application.clone().evaluate(base.clone().seed(), &source);

                tokio::pin!(output);

                if let Ok(Some(_)) = output.try_next().await {
                    continue;
                }

                yield base;
            }
        }
    }
}

impl Display for Negation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Negation(application) = self;
        write!(f, "! {}", application)
    }
}

#[cfg(test)]
mod tests {
    use crate::Session;
    use crate::artifact::Artifacts;
    use crate::error::EvaluationError;
    use crate::selection::Match;
    use crate::types::Any;
    use crate::{Term, Value};
    use dialog_storage::MemoryStorageBackend;
    use futures_util::TryStreamExt;

    #[dialog_common::test]
    async fn it_passes_match_when_negated_equality_not_satisfied() -> Result<(), EvaluationError> {
        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await.unwrap();
        let session = Session::open(store);

        // a=1, b=2 → equality finds no match → negation keeps the match
        let a = Term::<String>::var("a");
        let b = Term::<String>::var("b");
        let premise = !a.clone().is(b.clone());

        let mut input = Match::new();
        input.bind(&Term::<Any>::from(&a), Value::from(1))?;
        input.bind(&Term::<Any>::from(&b), Value::from(2))?;

        let results: Vec<Match> = premise
            .evaluate(input.seed(), &session)
            .try_collect()
            .await?;

        assert_eq!(
            results.len(),
            1,
            "Match where a != b should pass through negated equality"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_filters_match_when_negated_equality_satisfied() -> Result<(), EvaluationError> {
        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await.unwrap();
        let session = Session::open(store);

        let a = Term::<String>::var("a");
        let b = Term::<String>::var("b");
        let premise = !a.clone().is(b.clone());

        let mut input = Match::new();
        input.bind(&Term::<Any>::from(&a), Value::from(1))?;
        input.bind(&Term::<Any>::from(&b), Value::from(1))?;

        let results: Vec<Match> = premise
            .evaluate(input.seed(), &session)
            .try_collect()
            .await?;

        assert_eq!(
            results.len(),
            0,
            "Match where a == b should be filtered out by negated equality"
        );

        Ok(())
    }
}
