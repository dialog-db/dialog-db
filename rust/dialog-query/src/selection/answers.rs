use async_stream::try_stream;
use dialog_common::ConditionalSend;

use super::Answer;
use crate::error::QueryError;

/// Re-exported stream traits for working with answer streams.
pub use futures_util::stream::{Stream, TryStream};

/// A fallible, asynchronous stream of [`Answer`] values.
///
/// This is the primary data-flow abstraction during query evaluation. Each
/// premise in a plan receives an `impl Answers` from the previous step and
/// produces a new `impl Answers` that may contain more, fewer, or
/// differently-bound answers. The final stream is what the caller collects
/// or iterates over.
///
/// Combinators like [`try_flat_map`](Answers::try_flat_map),
/// [`expand`](Answers::expand), and [`try_expand`](Answers::try_expand)
/// make it easy to transform answer streams within premise implementations.
pub trait Answers: Stream<Item = Result<Answer, QueryError>> + 'static + ConditionalSend {
    /// Collect all answers into a Vec, propagating any errors
    #[allow(async_fn_in_trait)]
    fn try_vec(
        self,
    ) -> impl std::future::Future<Output = Result<Vec<Answer>, QueryError>> + ConditionalSend
    where
        Self: Sized,
    {
        async move { futures_util::TryStreamExt::try_collect(self).await }
    }

    /// Flat-map each answer into a stream of answers, propagating errors.
    ///
    /// Like `StreamExt::flat_map` but for fallible streams: errors from
    /// the outer stream are forwarded directly, `Ok` values are passed
    /// to `f` which returns a new answer stream that gets flattened in.
    fn try_flat_map<S, F>(self, mut f: F) -> impl Answers
    where
        Self: Sized,
        S: Answers,
        F: FnMut(Answer) -> S + ConditionalSend + 'static,
    {
        use futures_util::future::Either;
        futures_util::StreamExt::flat_map(self, move |result| match result {
            Ok(answer) => Either::Left(f(answer)),
            Err(e) => Either::Right(futures_util::stream::once(async move { Err(e) })),
        })
    }

    /// Expand each answer into zero or more answers using an infallible expander.
    fn expand<M: AnswersExpand>(self, expander: M) -> impl Answers
    where
        Self: Sized,
    {
        try_stream! {
            for await each in self {
                for expanded in expander.expand(each?) {
                    yield expanded;
                }
            }
        }
    }

    /// Expand each answer into zero or more answers using a fallible expander.
    fn try_expand<M: AnswersTryExpand>(self, expander: M) -> impl Answers
    where
        Self: Sized,
    {
        try_stream! {
            for await each in self {
                for expanded in expander.try_expand(each?)? {
                    yield expanded;
                }
            }
        }
    }
}

impl<S> Answers for S where S: Stream<Item = Result<Answer, QueryError>> + 'static + ConditionalSend {}

/// Expands an answer into multiple answers, potentially returning an error.
pub trait AnswersTryExpand: ConditionalSend + 'static {
    /// Attempt to expand a single answer into zero or more answers.
    fn try_expand(&self, item: Answer) -> Result<Vec<Answer>, QueryError>;
}

/// Expands an answer into multiple answers infallibly.
pub trait AnswersExpand: ConditionalSend + 'static {
    /// Expand a single answer into zero or more answers.
    fn expand(&self, item: Answer) -> Vec<Answer>;
}

impl<F: Fn(Answer) -> Result<Vec<Answer>, QueryError> + ConditionalSend + 'static> AnswersTryExpand
    for F
{
    fn try_expand(&self, answer: Answer) -> Result<Vec<Answer>, QueryError> {
        self(answer)
    }
}

impl<F: Fn(Answer) -> Vec<Answer> + ConditionalSend + 'static> AnswersExpand for F {
    fn expand(&self, answer: Answer) -> Vec<Answer> {
        self(answer)
    }
}
