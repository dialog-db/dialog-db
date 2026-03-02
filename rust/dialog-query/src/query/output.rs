use crate::error::QueryError;
use dialog_common::ConditionalSend;
pub use futures_util::stream::{Stream, StreamExt, TryStream};

/// A fallible, asynchronous stream of typed query results.
///
/// This is the consumer-facing counterpart to [`Answers`](crate::selection::Answers).
/// Where `Answers` carries raw [`Answer`](crate::selection::Answer) rows,
/// `Output<T>` carries fully realized `T` values (e.g. a concept conclusion
/// struct). It is produced by [`Application::perform`](crate::query::Application::perform).
pub trait Output<T: ConditionalSend>:
    Stream<Item = Result<T, QueryError>> + 'static + ConditionalSend
{
    /// Collect all items into a Vec, propagating any errors
    #[allow(async_fn_in_trait)]
    fn try_vec(
        self,
    ) -> impl std::future::Future<Output = Result<Vec<T>, QueryError>> + ConditionalSend
    where
        Self: Sized,
    {
        async move { futures_util::TryStreamExt::try_collect(self).await }
    }
}

impl<S, T: ConditionalSend> Output<T> for S where
    S: Stream<Item = Result<T, QueryError>> + 'static + ConditionalSend
{
}
