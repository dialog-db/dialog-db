mod r#match;

pub use r#match::*;

use async_stream::try_stream;
use dialog_common::ConditionalSend;

use crate::error::EvaluationError;

pub use futures_util::stream::{Stream, TryStream};

/// A fallible, asynchronous stream of [`Match`] values.
///
/// This is the primary data-flow abstraction during query evaluation. Each
/// premise in a plan receives an `impl Selection` from the previous step and
/// produces a new `impl Selection` that may contain more, fewer, or
/// differently-bound matches. The final stream is what the caller collects
/// or iterates over.
///
/// Combinators like [`try_flat_map`](Selection::try_flat_map),
/// [`expand`](Selection::expand), and [`try_expand`](Selection::try_expand)
/// make it easy to transform selection streams within premise implementations.
pub trait Selection:
    Stream<Item = Result<Match, EvaluationError>> + 'static + ConditionalSend
{
    /// Collect all matches into a Vec, propagating any errors
    #[allow(async_fn_in_trait)]
    fn try_vec(
        self,
    ) -> impl std::future::Future<Output = Result<Vec<Match>, EvaluationError>> + ConditionalSend
    where
        Self: Sized,
    {
        async move { futures_util::TryStreamExt::try_collect(self).await }
    }

    /// Flat-map each match into a stream of matches, propagating errors.
    ///
    /// Like `StreamExt::flat_map` but for fallible streams: errors from
    /// the outer stream are forwarded directly, `Ok` values are passed
    /// to `f` which returns a new selection stream that gets flattened in.
    fn try_flat_map<S, F>(self, mut f: F) -> impl Selection
    where
        Self: Sized,
        S: Selection,
        F: FnMut(Match) -> S + ConditionalSend + 'static,
    {
        use futures_util::future::Either;
        futures_util::StreamExt::flat_map(self, move |result| match result {
            Ok(matched) => Either::Left(f(matched)),
            Err(e) => Either::Right(futures_util::stream::once(async move { Err(e) })),
        })
    }

    /// Expand each match into zero or more matches using an infallible expander.
    fn expand<M: SelectionExpand>(self, expander: M) -> impl Selection
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

    /// Expand each match into zero or more matches using a fallible expander.
    fn try_expand<M: SelectionTryExpand>(self, expander: M) -> impl Selection
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

impl<S> Selection for S where
    S: Stream<Item = Result<Match, EvaluationError>> + 'static + ConditionalSend
{
}

/// Expands a match into multiple matches, potentially returning an error.
pub trait SelectionTryExpand: ConditionalSend + 'static {
    /// Attempt to expand a single match into zero or more matches.
    fn try_expand(&self, item: Match) -> Result<Vec<Match>, EvaluationError>;
}

/// Expands a match into multiple matches infallibly.
pub trait SelectionExpand: ConditionalSend + 'static {
    /// Expand a single match into zero or more matches.
    fn expand(&self, item: Match) -> Vec<Match>;
}

impl<F: Fn(Match) -> Result<Vec<Match>, EvaluationError> + ConditionalSend + 'static>
    SelectionTryExpand for F
{
    fn try_expand(&self, matched: Match) -> Result<Vec<Match>, EvaluationError> {
        self(matched)
    }
}

impl<F: Fn(Match) -> Vec<Match> + ConditionalSend + 'static> SelectionExpand for F {
    fn expand(&self, matched: Match) -> Vec<Match> {
        self(matched)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;
    use crate::artifact::{Entity, Value};
    use crate::error::EvaluationError;

    #[dialog_common::test]
    async fn it_seeds_one_empty_match() {
        use futures_util::TryStreamExt;
        let results: Vec<Match> = Match::new().seed().try_collect().await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[dialog_common::test]
    fn it_contains_bound_variable() {
        let mut candidate = Match::new();
        let name_term = Term::var("name");

        assert!(!candidate.contains(&name_term));

        candidate
            .bind(&name_term, Value::String("Alice".to_string()))
            .unwrap();
        assert!(candidate.contains(&name_term));
    }

    #[dialog_common::test]
    fn it_excludes_unbound_variable() {
        let candidate = Match::new();
        let name_term = Term::var("name");
        assert!(!candidate.contains(&name_term));
    }

    #[dialog_common::test]
    fn it_contains_constant() {
        let candidate = Match::new();
        let constant_param = Term::constant("constant_value".to_string());
        assert!(candidate.contains(&constant_param));
    }

    #[dialog_common::test]
    fn it_excludes_blank_variable() {
        let candidate = Match::new();
        let blank_param = Term::blank();
        assert!(!candidate.contains(&blank_param));
    }

    #[dialog_common::test]
    fn it_resolves_string() {
        let mut candidate = Match::new();
        let name_param = Term::var("name");

        candidate
            .bind(&name_param, Value::String("Alice".to_string()))
            .unwrap();

        let result = candidate
            .lookup(&name_param)
            .and_then(|v| String::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Alice");
    }

    #[dialog_common::test]
    fn it_resolves_u32() {
        let mut candidate = Match::new();
        let age_param = Term::var("age");

        candidate.bind(&age_param, Value::UnsignedInt(25)).unwrap();

        let result = candidate
            .lookup(&age_param)
            .and_then(|v| u32::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 25);
    }

    #[dialog_common::test]
    fn it_resolves_i32() {
        let mut candidate = Match::new();
        let score_param = Term::var("score");

        candidate.bind(&score_param, Value::SignedInt(-10)).unwrap();

        let result = candidate
            .lookup(&score_param)
            .and_then(|v| i32::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -10);
    }

    #[dialog_common::test]
    fn it_resolves_bool() {
        let mut candidate = Match::new();
        let active_param = Term::var("active");

        candidate.bind(&active_param, Value::Boolean(true)).unwrap();

        let result = candidate
            .lookup(&active_param)
            .and_then(|v| bool::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[dialog_common::test]
    fn it_resolves_entity() {
        let entity_value = Entity::new().unwrap();
        let mut candidate = Match::new();
        let entity_param = Term::var("entity_id");

        candidate
            .bind(&entity_param, Value::Entity(entity_value.clone()))
            .unwrap();

        let result = candidate
            .lookup(&entity_param)
            .and_then(|v| Entity::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), entity_value);
    }

    #[dialog_common::test]
    fn it_resolves_constant() {
        let candidate = Match::new();
        let constant_param = Term::constant("constant_value".to_string());

        let result = candidate
            .lookup(&constant_param)
            .and_then(|v| String::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "constant_value");
    }

    #[dialog_common::test]
    fn it_errors_on_unbound_variable() {
        let candidate = Match::new();
        let name_param = Term::var("name");

        let result = candidate
            .lookup(&name_param)
            .and_then(|v| String::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_err());
        match result.unwrap_err() {
            EvaluationError::UnboundVariable { variable_name } => {
                assert_eq!(variable_name, "name");
            }
            _ => panic!("Expected UnboundVariable"),
        }
    }

    #[dialog_common::test]
    fn it_errors_on_blank_variable() {
        let candidate = Match::new();
        let blank_param = Term::blank();

        let result = candidate
            .lookup(&blank_param)
            .and_then(|v| String::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_err());
        match result.unwrap_err() {
            EvaluationError::UnboundVariable { .. } => {}
            _ => panic!("Expected UnboundVariable"),
        }
    }

    #[dialog_common::test]
    fn it_errors_on_type_mismatch() {
        let mut candidate = Match::new();
        let name_param = Term::var("name");

        candidate
            .bind(&name_param, Value::String("Alice".to_string()))
            .unwrap();

        let result = candidate
            .lookup(&name_param)
            .and_then(|v| u32::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_err());
        match result.unwrap_err() {
            EvaluationError::TypeMismatch { .. } => {}
            _ => panic!("Expected TypeMismatch error"),
        }
    }

    #[dialog_common::test]
    fn it_allows_consistent_rebinding() {
        let mut candidate = Match::new();
        let name_term = Term::var("name");
        let value = Value::String("Alice".to_string());

        // Bind same value twice - should succeed
        candidate.bind(&name_term, value.clone()).unwrap();
        candidate.bind(&name_term, value.clone()).unwrap();

        assert_eq!(candidate.lookup(&name_term).unwrap(), value);
    }

    #[dialog_common::test]
    fn it_rejects_inconsistent_rebinding() {
        let mut candidate = Match::new();
        let name_term = Term::var("name");

        candidate
            .bind(&name_term, Value::String("Alice".to_string()))
            .unwrap();
        let result = candidate.bind(&name_term, Value::String("Bob".to_string()));
        assert!(result.is_err());
    }

    #[dialog_common::test]
    fn it_resolves_multiple_types() {
        let mut candidate = Match::new();

        candidate
            .bind(&Term::var("name"), Value::String("Bob".to_string()))
            .unwrap();
        candidate
            .bind(&Term::var("age"), Value::UnsignedInt(30))
            .unwrap();
        candidate
            .bind(&Term::var("active"), Value::Boolean(true))
            .unwrap();

        let name_result = String::try_from(candidate.lookup(&Term::var("name")).unwrap()).unwrap();
        let age_result = u32::try_from(candidate.lookup(&Term::var("age")).unwrap()).unwrap();
        let active_result =
            bool::try_from(candidate.lookup(&Term::var("active")).unwrap()).unwrap();

        assert_eq!(name_result, "Bob");
        assert_eq!(age_result, 30);
        assert!(active_result);
    }
}
