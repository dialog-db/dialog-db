use async_stream::try_stream;
use dialog_common::ConditionalSend;

use crate::error::QueryError;
use crate::selection;
use crate::selection::Answer;

use super::{Output, Source};

/// A query pattern that can be evaluated against a [`Source`] to produce
/// typed results.
///
/// Every `#[derive(Concept)]` and `#[derive(Formula)]` query struct
/// implements `Application`. The trait has two core methods:
/// - [`evaluate`](Application::evaluate) — takes an answer stream and a
///   source, and returns a new answer stream with additional bindings.
/// - [`realize`](Application::realize) — converts a fully-bound
///   [`Answer`](crate::selection::Answer) into the concrete `Conclusion` type.
///
/// The convenience method [`perform`](Application::perform) chains them
/// together into a single `Output<Conclusion>` stream ready for consumption.
pub trait Application: Clone + ConditionalSend + 'static {
    /// The concrete result type produced by this query.
    type Conclusion: ConditionalSend + 'static;

    /// Evaluate this query, producing a stream of answers.
    fn evaluate<S: Source, M: selection::Answers>(
        self,
        answers: M,
        source: &S,
    ) -> impl selection::Answers;

    /// Convert an answer into a concrete result value.
    fn realize(&self, input: selection::Answer) -> Result<Self::Conclusion, QueryError>;

    /// Execute this query against a source, returning a stream of typed results.
    fn perform<S: Source>(self, source: &S) -> impl Output<Self::Conclusion>
    where
        Self: Sized,
    {
        let query = self.clone();
        let results = self.evaluate(Answer::new().seed(), source);
        try_stream! {
            for await each in results {
                yield query.realize(each?)?;
            }
        }
    }
}
