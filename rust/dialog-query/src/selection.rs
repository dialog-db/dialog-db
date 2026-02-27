mod answer;
mod answers;
mod evidence;
mod factor;
mod factors;
mod selector;

pub use answer::*;
pub use answers::*;
pub use evidence::*;
pub use factor::*;
pub use factors::*;
pub use selector::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Relation;
    use crate::Term;
    use crate::artifact::{Attribute, Entity, Value};
    use crate::error::InconsistencyError;
    use std::sync::Arc;

    #[dialog_common::test]
    async fn test_answer_seed_produces_one_empty_answer() {
        use futures_util::TryStreamExt;
        let results: Vec<Answer> = Answer::new().seed().try_collect().await.unwrap();
        assert_eq!(results.len(), 1);
    }
    use std::str::FromStr;

    // Helper function to create a test relation for Answer tests
    fn create_test_relation(entity: Entity, attr: Attribute, value: Value) -> Relation {
        use crate::artifact::Cause;
        use crate::attribute::Cardinality;

        let attr_str = attr.to_string();
        let (domain, name) = attr_str
            .split_once('/')
            .map(|(ns, n)| (ns.to_string(), n.to_string()))
            .unwrap_or_else(|| (String::new(), attr_str));

        Relation {
            domain,
            name,
            of: entity,
            is: value,
            cause: Cause([0u8; 32]),
            cardinality: Cardinality::Many,
        }
    }

    // Helper to create a Factor::Selected for testing
    fn create_test_factor(selector: Selector, fact: Arc<Relation>) -> Factor {
        use crate::relation::query::RelationQuery;

        // Create a minimal RelationQuery for testing
        let application = Arc::new(RelationQuery::new(
            Term::var("the_ns"),
            Term::var("the_name"),
            Term::var("of"),
            Term::var("is"),
            Term::var("cause"),
            None,
        ));

        Factor::Selected {
            selector,
            application,
            fact,
        }
    }

    #[dialog_common::test]
    fn test_answer_contains_bound_variable() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());
        let fact = Arc::new(create_test_relation(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let name_term = Term::<Value>::var("name");

        // Initially should not contain the variable
        assert!(!answer.contains(&name_term));

        // After assignment, should contain the variable
        answer.assign(&name_term, &factor).unwrap();
        assert!(answer.contains(&name_term));
    }

    #[dialog_common::test]
    fn test_answer_contains_unbound_variable() {
        let answer = Answer::new();
        let name_term = Term::<Value>::var("name");

        // Should not contain unbound variable
        assert!(!answer.contains(&name_term));
    }

    #[dialog_common::test]
    fn test_answer_contains_constant() {
        let answer = Answer::new();
        let constant_term = Term::Constant(Value::String("constant_value".to_string()));

        // Constants are always "bound"
        assert!(answer.contains(&constant_term));
    }

    #[dialog_common::test]
    fn test_answer_contains_blank_variable() {
        let answer = Answer::new();
        let blank_term = Term::<Value>::blank();

        // Blank variables (Any) are never "bound"
        assert!(!answer.contains(&blank_term));
    }

    #[dialog_common::test]
    fn test_answer_resolve_string() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());
        let fact = Arc::new(create_test_relation(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let name_term = Term::<String>::var("name");
        let name_term_value = Term::<Value>::var("name");

        // Assign the value
        answer.assign(&name_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&name_term)
            .and_then(|v| String::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Alice");
    }

    #[dialog_common::test]
    fn test_answer_resolve_u32() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/age").unwrap();
        let value = Value::UnsignedInt(25);
        let fact = Arc::new(create_test_relation(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let age_term = Term::<u32>::var("age");
        let age_term_value = Term::<Value>::var("age");

        // Assign the value
        answer.assign(&age_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&age_term)
            .and_then(|v| u32::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 25);
    }

    #[dialog_common::test]
    fn test_answer_resolve_i32() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/score").unwrap();
        let value = Value::SignedInt(-10);
        let fact = Arc::new(create_test_relation(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let score_term = Term::<i32>::var("score");
        let score_term_value = Term::<Value>::var("score");

        // Assign the value
        answer.assign(&score_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&score_term)
            .and_then(|v| i32::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -10);
    }

    #[dialog_common::test]
    fn test_answer_resolve_bool() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/active").unwrap();
        let value = Value::Boolean(true);
        let fact = Arc::new(create_test_relation(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let active_term = Term::<bool>::var("active");
        let active_term_value = Term::<Value>::var("active");

        // Assign the value
        answer.assign(&active_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&active_term)
            .and_then(|v| bool::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[dialog_common::test]
    fn test_answer_resolve_entity() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/id").unwrap();
        let entity_value = Entity::new().unwrap();
        let value = Value::Entity(entity_value.clone());
        let fact = Arc::new(create_test_relation(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let entity_term = Term::<Entity>::var("entity_id");
        let entity_term_value = Term::<Value>::var("entity_id");

        // Assign the value
        answer.assign(&entity_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&entity_term)
            .and_then(|v| Entity::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), entity_value);
    }

    #[dialog_common::test]
    fn test_answer_resolve_constant() {
        let answer = Answer::new();
        let constant_term = Term::Constant("constant_value".to_string());

        // Resolve constant directly
        let result = answer
            .resolve(&constant_term)
            .and_then(|v| String::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "constant_value");
    }

    #[dialog_common::test]
    fn test_answer_resolve_unbound_variable() {
        let answer = Answer::new();
        let name_term = Term::<String>::var("name");

        // Try to resolve unbound variable (should fail)
        let result = answer
            .resolve(&name_term)
            .and_then(|v| String::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::UnboundVariableError(var) => {
                assert_eq!(var, "name");
            }
            _ => panic!("Expected UnboundVariableError"),
        }
    }

    #[dialog_common::test]
    fn test_answer_resolve_blank_variable() {
        let answer = Answer::new();
        let blank_term = Term::<String>::blank();

        // Try to resolve blank variable (should fail)
        let result = answer
            .resolve(&blank_term)
            .and_then(|v| String::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::UnboundVariableError(_) => {} // Expected
            _ => panic!("Expected UnboundVariableError"),
        }
    }

    #[dialog_common::test]
    fn test_answer_resolve_type_mismatch() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());
        let fact = Arc::new(create_test_relation(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let name_term_value = Term::<Value>::var("name");

        // Assign a string value
        answer.assign(&name_term_value, &factor).unwrap();

        // Try to resolve it as a u32 (should fail)
        let age_term = Term::<u32>::var("name");
        let result = answer
            .resolve(&age_term)
            .and_then(|v| u32::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::TypeConversion(_) => {} // Expected
            _ => panic!("Expected TypeConversion error"),
        }
    }

    #[dialog_common::test]
    fn test_answer_factors_evidence() {
        let entity1 = Entity::new().unwrap();
        let entity2 = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());

        // Create two different facts with the same value but different entities
        let fact1 = Arc::new(create_test_relation(
            entity1.clone(),
            attr.clone(),
            value.clone(),
        ));
        let fact2 = Arc::new(create_test_relation(
            entity2.clone(),
            attr.clone(),
            value.clone(),
        ));

        let factor1 = create_test_factor(Selector::Is, Arc::clone(&fact1));
        let factor2 = create_test_factor(Selector::Is, Arc::clone(&fact2));

        let mut answer = Answer::new();
        let name_term = Term::<Value>::var("name");

        // Assign the same value from two different facts
        answer.assign(&name_term, &factor1).unwrap();
        answer.assign(&name_term, &factor2).unwrap();

        // Get the factors and check evidence
        let factors = answer.resolve_factors(&name_term).unwrap();

        // The content should be the same
        assert_eq!(factors.content(), value);

        // Collect evidence
        let evidence: Vec<_> = factors.evidence().collect();

        // Should have both factors since they come from different facts
        // (even though they have the same value)
        assert_eq!(
            evidence.len(),
            2,
            "Should have 2 factors from different facts"
        );
        assert!(evidence.contains(&&factor1));
        assert!(evidence.contains(&&factor2));
    }

    #[dialog_common::test]
    fn test_answer_resolve_multiple_types() {
        let entity = Entity::new().unwrap();

        // Create multiple facts
        let name_attr = Attribute::from_str("user/name").unwrap();
        let name_value = Value::String("Bob".to_string());
        let name_fact = Arc::new(create_test_relation(
            entity.clone(),
            name_attr.clone(),
            name_value.clone(),
        ));
        let name_factor = create_test_factor(Selector::Is, Arc::clone(&name_fact));

        let age_attr = Attribute::from_str("user/age").unwrap();
        let age_value = Value::UnsignedInt(30);
        let age_fact = Arc::new(create_test_relation(
            entity.clone(),
            age_attr.clone(),
            age_value.clone(),
        ));
        let age_factor = create_test_factor(Selector::Is, Arc::clone(&age_fact));

        let active_attr = Attribute::from_str("user/active").unwrap();
        let active_value = Value::Boolean(true);
        let active_fact = Arc::new(create_test_relation(
            entity.clone(),
            active_attr.clone(),
            active_value.clone(),
        ));
        let active_factor = create_test_factor(Selector::Is, Arc::clone(&active_fact));

        let mut answer = Answer::new();

        // Assign all values using chaining
        answer
            .assign(&Term::<Value>::var("name"), &name_factor)
            .unwrap();
        answer
            .assign(&Term::<Value>::var("age"), &age_factor)
            .unwrap();
        answer
            .assign(&Term::<Value>::var("active"), &active_factor)
            .unwrap();

        // Resolve all values with correct types
        let name_result =
            String::try_from(answer.resolve::<String>(&Term::var("name")).unwrap()).unwrap();
        let age_result = u32::try_from(answer.resolve::<u32>(&Term::var("age")).unwrap()).unwrap();
        let active_result =
            bool::try_from(answer.resolve::<bool>(&Term::var("active")).unwrap()).unwrap();

        assert_eq!(name_result, "Bob");
        assert_eq!(age_result, 30);
        assert!(active_result);
    }

    #[dialog_common::test]
    fn test_answer_extend() {
        let entity = Entity::new().unwrap();

        // Create multiple facts
        let name_attr = Attribute::from_str("user/name").unwrap();
        let name_value = Value::String("Charlie".to_string());
        let name_fact = Arc::new(create_test_relation(
            entity.clone(),
            name_attr.clone(),
            name_value.clone(),
        ));
        let name_factor = create_test_factor(Selector::Is, Arc::clone(&name_fact));

        let age_attr = Attribute::from_str("user/age").unwrap();
        let age_value = Value::UnsignedInt(35);
        let age_fact = Arc::new(create_test_relation(
            entity.clone(),
            age_attr.clone(),
            age_value.clone(),
        ));
        let age_factor = create_test_factor(Selector::Is, Arc::clone(&age_fact));

        // Use extend to assign multiple values at once
        let assignments = vec![
            (Term::<Value>::var("name"), name_factor),
            (Term::<Value>::var("age"), age_factor),
        ];

        let mut answer = Answer::new();
        answer.extend(assignments).unwrap();

        // Verify all values were assigned
        let name_result =
            String::try_from(answer.resolve::<String>(&Term::var("name")).unwrap()).unwrap();
        let age_result = u32::try_from(answer.resolve::<u32>(&Term::var("age")).unwrap()).unwrap();

        assert_eq!(name_result, "Charlie");
        assert_eq!(age_result, 35);
    }
}
