use std::{collections::BTreeMap, sync::Arc};

use dialog_artifacts::Value;
use dialog_common::ConditionalSend;
use futures_core::Stream;

use crate::{fact::Scalar, InconsistencyError, QueryError, Term};

pub trait Selection: Stream<Item = Result<Match, QueryError>> + 'static + ConditionalSend {}

impl<S> Selection for S where S: Stream<Item = Result<Match, QueryError>> + 'static + ConditionalSend
{}

#[derive(Clone, Debug)]
pub struct Match {
    variables: Arc<BTreeMap<String, Value>>,
}

impl Match {
    pub fn new() -> Self {
        Self {
            variables: Arc::new(BTreeMap::new()),
        }
    }

    // Type-safe methods using Term<T>
    pub fn get<T>(&self, term: &Term<T>) -> Result<T, InconsistencyError>
    where
        T: Scalar + std::convert::TryFrom<Value>,
        InconsistencyError: From<<T as std::convert::TryFrom<Value>>::Error>,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(value) = self.variables.get(key) {
                        T::try_from(value.clone()).map_err(Into::into)
                    } else {
                        Err(InconsistencyError::UnboundVariableError(key.clone()))
                    }
                } else {
                    Err(InconsistencyError::UnboundVariableError("".to_string()))
                }
            }
            Term::Constant(constant) => Ok(constant.clone()),
        }
    }

    pub fn set<T>(&self, term: Term<T>, value: T) -> Result<Self, InconsistencyError>
    where
        T: crate::types::IntoValueDataType
            + Clone
            + Into<Value>
            + PartialEq
            + std::convert::TryFrom<Value>
            + std::fmt::Debug,
        InconsistencyError: From<<T as std::convert::TryFrom<Value>>::Error>,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    // Check if variable is already bound
                    if let Some(existing_value) = self.variables.get(&key) {
                        let existing_as_t_result = T::try_from(existing_value.clone());

                        match existing_as_t_result {
                            Ok(existing_as_t) => {
                                if existing_as_t == value {
                                    Ok(self.clone())
                                } else {
                                    Err(InconsistencyError::AssignmentError(format!(
                                    "Can not set {:?} to {:?} because it is already set to {:?}.",
                                    key,
                                    value.into(),
                                    existing_value
                                )))
                                }
                            }
                            Err(conversion_error) => {
                                // Type mismatch with existing value
                                Err(conversion_error.into())
                            }
                        }
                    } else {
                        // New binding
                        let mut variables = (*self.variables).clone();
                        variables.insert(key, value.into());
                        Ok(Self {
                            variables: Arc::new(variables),
                        })
                    }
                } else {
                    // TODO: We should still check the type here
                    Ok(self.clone())
                }
            }
            Term::Constant(constant) => {
                // For constants, we check if the value matches
                if constant == value {
                    Ok(self.clone())
                } else {
                    Err(InconsistencyError::AssignmentError(format!(
                        "Cannot set constant {:?} to different value {:?}",
                        constant, value
                    )))
                }
            }
        }
    }

    pub fn has<T>(&self, term: &Term<T>) -> bool
    where
        T: crate::types::IntoValueDataType + Clone,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    self.variables.contains_key(key)
                } else {
                    // We don't capture values for Any
                    false
                }
            }
            Term::Constant(_) => true, // Constants are always "bound"
        }
    }

    pub fn unify<T>(&self, term: Term<T>, value: Value) -> Result<Self, InconsistencyError>
    where
        T: crate::types::IntoValueDataType + Clone + Into<Value> + PartialEq<Value>,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    let mut variables = (*self.variables).clone();
                    variables.insert(key, value);

                    Ok(Self {
                        variables: Arc::new(variables),
                    })
                } else {
                    Ok(self.clone())
                }
            }
            Term::Constant(constant) => {
                let constant_value: Value = constant.into();
                if constant_value == value {
                    Ok(self.clone())
                } else {
                    Err(InconsistencyError::TypeMismatch {
                        expected: constant_value,
                        actual: value,
                    })
                }
            }
        }
    }

    pub fn unify_value<T>(&self, term: Term<T>, value: Value) -> Result<Self, InconsistencyError>
    where
        T: Scalar,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    let mut variables = (*self.variables).clone();
                    variables.insert(key, value);

                    Ok(Self {
                        variables: Arc::new(variables),
                    })
                } else {
                    Ok(self.clone())
                }
            }
            Term::Constant(constant) => {
                let constant_value = constant.as_value();
                if constant_value == value {
                    Ok(self.clone())
                } else {
                    Err(InconsistencyError::TypeMismatch {
                        expected: constant_value,
                        actual: value,
                    })
                }
            }
        }
    }

    pub fn resolve<T>(&self, term: &Term<T>) -> Result<T, InconsistencyError>
    where
        T: Scalar + From<Value>,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(value) = self.variables.get(key) {
                        Ok(T::from(value.clone()))
                    } else {
                        Err(InconsistencyError::UnboundVariableError(key.clone()))
                    }
                } else {
                    Err(InconsistencyError::UnboundVariableError("Any".to_string()))
                }
            }
            Term::Constant(constant) => Ok(constant.clone().into()),
        }
    }

    pub fn resolve_value<T>(&self, term: &Term<T>) -> Result<Value, InconsistencyError>
    where
        T: Scalar,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(value) = self.variables.get(key) {
                        Ok(value.clone())
                    } else {
                        Err(InconsistencyError::UnboundVariableError(key.clone()))
                    }
                } else {
                    Err(InconsistencyError::UnboundVariableError("Any".to_string()))
                }
            }
            Term::Constant(constant) => Ok(constant.as_value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;
    use dialog_artifacts::{Attribute, Entity};
    use std::str::FromStr;

    #[test]
    fn test_type_safe_get_string() {
        let mut match_frame = Match::new();

        // Set a string value using the internal method
        match_frame = match_frame
            .set(Term::var("name"), "Alice".to_string())
            .unwrap();

        // Get it using the type-safe method
        let name_term = Term::var("name");
        let result = match_frame.get::<String>(&name_term);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Alice");
    }

    #[test]
    fn test_type_safe_get_type_mismatch() {
        let mut match_frame = Match::new();

        // Set a string value
        match_frame = match_frame
            .set(Term::<String>::var("age"), "not_a_number".to_string())
            .unwrap();

        // Try to get it as a u32 (should fail)
        let age_term = Term::var("age");
        let result = match_frame.get::<u32>(&age_term);

        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::TypeConversion(_) => {} // Expected
            _ => panic!("Expected TypeConversion error"),
        }
    }

    #[test]
    fn test_type_safe_set_string() {
        let match_frame = Match::new();

        let name_term = Term::<String>::var("name");
        let result = match_frame.set(name_term, "Bob".to_string());

        assert!(result.is_ok());
        let new_frame = result.unwrap();

        // Verify the value was set correctly
        let verify_term = Term::<String>::var("name");
        let stored_value: String = new_frame.get(&verify_term).unwrap();
        assert_eq!(stored_value, "Bob");
    }

    #[test]
    fn test_type_safe_set_term_integer() {
        let match_frame = Match::new();

        let age_term = Term::<u32>::var("age");
        let result = match_frame.set(age_term, 25u32);

        assert!(result.is_ok());
        let new_frame = result.unwrap();

        // Verify the value was set correctly
        let verify_term = Term::<u32>::var("age");
        let stored_value: u32 = new_frame.get(&verify_term).unwrap();
        assert_eq!(stored_value, 25u32);
    }

    #[test]
    fn test_type_safe_set_term_consistent_assignment() {
        let match_frame = Match::new();

        // Set initial value
        let name_term = Term::<String>::var("name");
        let frame1 = match_frame
            .set(name_term.clone(), "Charlie".to_string())
            .unwrap();

        // Set the same value again (should succeed)
        let result = frame1.set(name_term, "Charlie".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_safe_set_term_inconsistent_assignment() {
        let match_frame = Match::new();

        // Set initial value
        let name_term = Term::<String>::var("name");
        let frame1 = match_frame
            .set(name_term.clone(), "Diana".to_string())
            .unwrap();

        // Try to set a different value (should fail)
        let result = frame1.set(name_term, "Eve".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::AssignmentError(_) => {} // Expected
            _ => panic!("Expected AssignmentError"),
        }
    }

    #[test]
    fn test_type_safe_set_term_type_mismatch() {
        let mut match_frame = Match::new();

        // Set a string value using new API
        match_frame = match_frame
            .set(Term::<String>::var("value"), "text".to_string())
            .unwrap();

        // Try to set it as a u32 using type-safe method (should fail due to type mismatch)
        let value_term = Term::<u32>::var("value");
        let result = match_frame.set(value_term, 42u32);

        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::TypeConversion(_) => {} // Expected
            _ => panic!("Expected TypeConversion error"),
        }
    }

    #[test]
    fn test_type_safe_set_term_constant() {
        let match_frame = Match::new();

        // Set a constant term with matching value (should succeed)
        let constant_term = Term::Constant("fixed_value".to_string());
        let result = match_frame.set(constant_term, "fixed_value".to_string());
        assert!(result.is_ok());

        // Set a constant term with different value (should fail)
        let constant_term2 = Term::Constant("fixed_value".to_string());
        let result2 = match_frame.set(constant_term2, "different_value".to_string());
        assert!(result2.is_err());
        match result2.unwrap_err() {
            InconsistencyError::AssignmentError(_) => {} // Expected
            _ => panic!("Expected AssignmentError for constant mismatch"),
        }
    }

    #[test]
    fn test_type_safe_set_term_any() {
        let match_frame = Match::new();

        // Setting Any term should always succeed
        let any_term = Term::<String>::blank();
        let result = match_frame.set(any_term, "anything".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_safe_has_term() {
        let mut match_frame = Match::new();

        // Initially should not have the variable
        let name_term: Term<String> = Term::<String>::var("name");
        assert!(!match_frame.has(&name_term));

        // Set the variable
        match_frame = match_frame
            .set(Term::var("name"), "Frank".to_string())
            .unwrap();

        // Now should have the variable
        assert!(match_frame.has(&name_term));

        // Constants are always "bound"
        let constant_term = Term::Constant("value".to_string());
        assert!(match_frame.has(&constant_term));

        // Any is always "bound"
        let any_term = Term::<String>::blank();
        assert!(!match_frame.has(&any_term));
    }

    #[test]
    fn test_type_safe_entity_operations() {
        let match_frame = Match::new();
        let entity = Entity::new().unwrap();

        // Set entity using type-safe method
        let entity_term = Term::<Entity>::var("entity");
        let frame = match_frame
            .set(entity_term.clone(), entity.clone())
            .unwrap();

        // Get entity using type-safe method
        let result: Result<Entity, _> = frame.get(&entity_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), entity);
    }

    #[test]
    fn test_type_safe_attribute_operations() {
        let match_frame = Match::new();
        let attr = Attribute::from_str("user/name").unwrap();

        // Set attribute using type-safe method
        let attr_term = Term::<Attribute>::var("attr");
        let frame = match_frame.set(attr_term.clone(), attr.clone()).unwrap();

        // Get attribute using type-safe method
        let result: Result<Attribute, _> = frame.get(&attr_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), attr);
    }

    #[test]
    fn test_type_safe_mixed_types() {
        let match_frame = Match::new();

        // Set multiple types
        let name_term = Term::var("name");
        let age_term = Term::var("age");
        let active_term = Term::var("active");

        let frame1 = match_frame
            .set(name_term.clone(), "Grace".to_string())
            .unwrap();
        let frame2 = frame1.set(age_term.clone(), 30u32).unwrap();
        let frame3 = frame2.set(active_term.clone(), true).unwrap();

        // Get all values back with correct types
        let name_result: String = frame3.get(&name_term).unwrap();
        let age_result: u32 = frame3.get(&age_term).unwrap();
        let active_result: bool = frame3.get(&active_term).unwrap();

        assert_eq!(name_result, "Grace");
        assert_eq!(age_result, 30u32);
        assert_eq!(active_result, true);
    }

    #[test]
    fn test_backward_compatibility() {
        let mut match_frame = Match::new();

        // Use new API methods
        match_frame = match_frame
            .set(Term::var("name"), "Henry".to_string())
            .unwrap();
        let name_term = Term::var("name");
        assert!(match_frame.has(&name_term));
        let value: String = match_frame.get(&name_term).unwrap();
        assert_eq!(value, "Henry");

        // Mix with type-safe methods
        let typed_value: String = match_frame.get(&name_term).unwrap();
        assert_eq!(typed_value, "Henry");
    }
}
