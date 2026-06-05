mod r#match;

pub use r#match::*;

use dialog_common::ConditionalSend;

use crate::error::EvaluationError;

pub use futures_util::stream::{Stream, TryStream};
pub use std::future::Future;

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
pub trait Selection: Stream<Item = Result<Match, EvaluationError>> + ConditionalSend {
    /// Collect all matches into a Vec, propagating any errors.
    #[allow(async_fn_in_trait)]
    fn try_vec(self) -> impl Future<Output = Result<Vec<Match>, EvaluationError>> + ConditionalSend
    where
        Self: Sized,
    {
        async move { futures_util::TryStreamExt::try_collect(self).await }
    }
}

impl<S> Selection for S where S: Stream<Item = Result<Match, EvaluationError>> + ConditionalSend {}

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
