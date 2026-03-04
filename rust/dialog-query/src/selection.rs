mod answer;
mod answers;

pub use answer::*;
pub use answers::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;
    use crate::artifact::{Entity, Value};
    use crate::error::EvaluationError;

    #[dialog_common::test]
    async fn it_seeds_one_empty_answer() {
        use futures_util::TryStreamExt;
        let results: Vec<Answer> = Answer::new().seed().try_collect().await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[dialog_common::test]
    fn it_contains_bound_variable() {
        let mut answer = Answer::new();
        let name_term = Term::var("name");

        assert!(!answer.contains(&name_term));

        answer
            .bind(&name_term, Value::String("Alice".to_string()))
            .unwrap();
        assert!(answer.contains(&name_term));
    }

    #[dialog_common::test]
    fn it_excludes_unbound_variable() {
        let answer = Answer::new();
        let name_term = Term::var("name");
        assert!(!answer.contains(&name_term));
    }

    #[dialog_common::test]
    fn it_contains_constant() {
        let answer = Answer::new();
        let constant_param = Term::constant("constant_value".to_string());
        assert!(answer.contains(&constant_param));
    }

    #[dialog_common::test]
    fn it_excludes_blank_variable() {
        let answer = Answer::new();
        let blank_param = Term::blank();
        assert!(!answer.contains(&blank_param));
    }

    #[dialog_common::test]
    fn it_resolves_string() {
        let mut answer = Answer::new();
        let name_param = Term::var("name");

        answer
            .bind(&name_param, Value::String("Alice".to_string()))
            .unwrap();

        let result = answer
            .resolve(&name_param)
            .and_then(|v| String::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Alice");
    }

    #[dialog_common::test]
    fn it_resolves_u32() {
        let mut answer = Answer::new();
        let age_param = Term::var("age");

        answer.bind(&age_param, Value::UnsignedInt(25)).unwrap();

        let result = answer
            .resolve(&age_param)
            .and_then(|v| u32::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 25);
    }

    #[dialog_common::test]
    fn it_resolves_i32() {
        let mut answer = Answer::new();
        let score_param = Term::var("score");

        answer.bind(&score_param, Value::SignedInt(-10)).unwrap();

        let result = answer
            .resolve(&score_param)
            .and_then(|v| i32::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -10);
    }

    #[dialog_common::test]
    fn it_resolves_bool() {
        let mut answer = Answer::new();
        let active_param = Term::var("active");

        answer.bind(&active_param, Value::Boolean(true)).unwrap();

        let result = answer
            .resolve(&active_param)
            .and_then(|v| bool::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[dialog_common::test]
    fn it_resolves_entity() {
        let entity_value = Entity::new().unwrap();
        let mut answer = Answer::new();
        let entity_param = Term::var("entity_id");

        answer
            .bind(&entity_param, Value::Entity(entity_value.clone()))
            .unwrap();

        let result = answer
            .resolve(&entity_param)
            .and_then(|v| Entity::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), entity_value);
    }

    #[dialog_common::test]
    fn it_resolves_constant() {
        let answer = Answer::new();
        let constant_param = Term::constant("constant_value".to_string());

        let result = answer
            .resolve(&constant_param)
            .and_then(|v| String::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "constant_value");
    }

    #[dialog_common::test]
    fn it_errors_on_unbound_variable() {
        let answer = Answer::new();
        let name_param = Term::var("name");

        let result = answer
            .resolve(&name_param)
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
        let answer = Answer::new();
        let blank_param = Term::blank();

        let result = answer
            .resolve(&blank_param)
            .and_then(|v| String::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_err());
        match result.unwrap_err() {
            EvaluationError::UnboundVariable { .. } => {}
            _ => panic!("Expected UnboundVariable"),
        }
    }

    #[dialog_common::test]
    fn it_errors_on_type_mismatch() {
        let mut answer = Answer::new();
        let name_param = Term::var("name");

        answer
            .bind(&name_param, Value::String("Alice".to_string()))
            .unwrap();

        let result = answer
            .resolve(&name_param)
            .and_then(|v| u32::try_from(v).map_err(EvaluationError::from));
        assert!(result.is_err());
        match result.unwrap_err() {
            EvaluationError::TypeMismatch { .. } => {}
            _ => panic!("Expected TypeMismatch error"),
        }
    }

    #[dialog_common::test]
    fn it_allows_consistent_rebinding() {
        let mut answer = Answer::new();
        let name_term = Term::var("name");
        let value = Value::String("Alice".to_string());

        // Bind same value twice - should succeed
        answer.bind(&name_term, value.clone()).unwrap();
        answer.bind(&name_term, value.clone()).unwrap();

        assert_eq!(answer.resolve(&name_term).unwrap(), value);
    }

    #[dialog_common::test]
    fn it_rejects_inconsistent_rebinding() {
        let mut answer = Answer::new();
        let name_term = Term::var("name");

        answer
            .bind(&name_term, Value::String("Alice".to_string()))
            .unwrap();
        let result = answer.bind(&name_term, Value::String("Bob".to_string()));
        assert!(result.is_err());
    }

    #[dialog_common::test]
    fn it_resolves_multiple_types() {
        let mut answer = Answer::new();

        answer
            .bind(&Term::var("name"), Value::String("Bob".to_string()))
            .unwrap();
        answer
            .bind(&Term::var("age"), Value::UnsignedInt(30))
            .unwrap();
        answer
            .bind(&Term::var("active"), Value::Boolean(true))
            .unwrap();

        let name_result = String::try_from(answer.resolve(&Term::var("name")).unwrap()).unwrap();
        let age_result = u32::try_from(answer.resolve(&Term::var("age")).unwrap()).unwrap();
        let active_result = bool::try_from(answer.resolve(&Term::var("active")).unwrap()).unwrap();

        assert_eq!(name_result, "Bob");
        assert_eq!(age_result, 30);
        assert!(active_result);
    }

}
