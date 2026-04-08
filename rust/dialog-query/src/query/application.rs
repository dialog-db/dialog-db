use async_stream::try_stream;
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::{ConditionalSend, ConditionalSync};

use crate::error::EvaluationError;
use crate::selection;
use crate::selection::Match;
use crate::source::SelectRules;

use super::Output;

/// A query pattern that can be evaluated against a provider to produce
/// typed results.
///
/// Every `#[derive(Concept)]` and `#[derive(Formula)]` query struct
/// implements `Application`. The trait has two core methods:
/// - [`evaluate`](Application::evaluate) — takes a selection stream and an
///   environment, and returns a new selection stream with additional bindings.
/// - [`realize`](Application::realize) — converts a fully-bound
///   [`Match`](crate::selection::Match) into the concrete `Conclusion` type.
///
/// The convenience method [`perform`](Application::perform) chains them
/// together into a single `Output<Conclusion>` stream ready for consumption.
pub trait Application: Clone + ConditionalSend + 'static {
    /// The concrete result type produced by this query.
    type Conclusion: ConditionalSend + 'static;

    /// Evaluate this query, producing a selection stream.
    fn evaluate<'a, Env, M: selection::Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> impl selection::Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync;

    /// Convert a match into a concrete result value.
    fn realize(&self, input: selection::Match) -> Result<Self::Conclusion, EvaluationError>;

    /// Execute this query against an environment, returning a stream of typed results.
    fn perform<'a, Env>(self, env: &'a Env) -> impl Output<Self::Conclusion> + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
        Self: Sized,
    {
        let query = self.clone();
        let results = Box::pin(self.evaluate(Match::new().seed(), env));
        try_stream! {
            for await each in results {
                yield query.realize(each?)?;
            }
        }
    }
}
