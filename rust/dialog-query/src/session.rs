use dialog_common::ConditionalSend;

use crate::artifact::{ArtifactStoreMutExt, DialogArtifactsError};
use dialog_artifacts::Instruction;

/// A trait for collections of instruction-producing items
pub trait Changes: ConditionalSend {
    fn collect_instructions(self) -> Vec<Instruction>;
}

// Implement Changes for Vec<Claim<T>>
impl<T> Changes for Vec<crate::fact::Claim<T>>
where
    T: crate::types::Scalar,
{
    fn collect_instructions(self) -> Vec<Instruction> {
        self.into_iter()
            .flat_map(|claim| -> Vec<Instruction> { claim.into() })
            .collect()
    }
}

// Implement Changes for single Claim<T>
impl<T> Changes for crate::fact::Claim<T>
where
    T: crate::types::Scalar,
{
    fn collect_instructions(self) -> Vec<Instruction> {
        let vec: Vec<Instruction> = self.into();
        vec
    }
}

// Implement Changes for Vec<Instruction>
impl Changes for Vec<Instruction> {
    fn collect_instructions(self) -> Vec<Instruction> {
        self
    }
}

// Implement Changes for single Instruction
impl Changes for Instruction {
    fn collect_instructions(self) -> Vec<Instruction> {
        vec![self]
    }
}

pub struct Session<S>
where
    S: ArtifactStoreMutExt + ConditionalSend,
{
    store: S,
}

impl<S: ArtifactStoreMutExt + ConditionalSend> Session<S> {
    pub fn open(store: S) -> Self {
        Session { store }
    }

    pub async fn commit<C: Changes>(&mut self, changes: C) -> Result<(), DialogArtifactsError> {
        let instructions = changes.collect_instructions();
        ArtifactStoreMutExt::commit(&mut self.store, instructions).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, marker::PhantomData};

    use dialog_artifacts::{ArtifactStore, ValueDataType};
    use futures_util::{task::UnsafeFutureObj, StreamExt};

    use crate::{
        predicate, query::PlannedQuery, Attribute, Parameters, SelectionExt, VariableScope,
    };

    use super::*;

    #[tokio::test]
    async fn test_session() -> anyhow::Result<()> {
        use crate::artifact::{Artifacts, Attribute as ArtifactAttribute, Entity, Value};
        use crate::{Fact, Term};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let mallory = Entity::new()?;

        session
            .commit(vec![
                Fact::assert(
                    "person/name".parse::<ArtifactAttribute>()?,
                    alice.clone(),
                    Value::String("Alice".to_string()),
                ),
                Fact::assert(
                    "person/age".parse::<ArtifactAttribute>()?,
                    alice.clone(),
                    Value::UnsignedInt(25),
                ),
                Fact::assert(
                    "person/name".parse::<ArtifactAttribute>()?,
                    bob.clone(),
                    Value::String("Bob".to_string()),
                ),
                Fact::assert(
                    "person/age".parse::<ArtifactAttribute>()?,
                    bob.clone(),
                    Value::UnsignedInt(30),
                ),
                Fact::assert(
                    "person/name".parse::<ArtifactAttribute>()?,
                    mallory.clone(),
                    Value::String("Mallory".to_string()),
                ),
            ])
            .await?;

        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        let name = Term::var("name");
        let age = Term::var("age");
        let mut params = Parameters::new();
        params.insert("name".into(), name.clone());
        params.insert("age".into(), age.clone());

        // Let's test with empty parameters first to see the exact error
        let application = person.apply(params);

        let plan = application.plan(&VariableScope::new())?;

        let selection = plan.query(&session.store)?.collect_matches().await?;
        assert_eq!(selection.len(), 2); // Should find just Alice and Bob

        // Check that we have both Alice and Bob (order may vary)
        let mut found_alice = false;
        let mut found_bob = false;

        for match_result in selection.iter() {
            let person_name = match_result.get(&name)?;
            let person_age = match_result.get(&age)?;

            match person_name {
                Value::String(name_str) if name_str == "Alice" => {
                    assert_eq!(person_age, Value::UnsignedInt(25));
                    found_alice = true;
                }
                Value::String(name_str) if name_str == "Bob" => {
                    assert_eq!(person_age, Value::UnsignedInt(30));
                    found_bob = true;
                }
                _ => panic!("Unexpected person: {:?}", person_name),
            }
        }

        assert!(found_alice, "Should find Alice");
        assert!(found_bob, "Should find Bob");

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_planning_errors() -> anyhow::Result<()> {
        use crate::artifact::{
            Artifacts, Attribute as ArtifactAttribute, Entity, Value, ValueDataType,
        };
        use crate::{error::PlanError, Fact, Term};
        use dialog_storage::MemoryStorageBackend;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        // Set up concept with attributes
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );
        attributes.insert(
            "age".into(),
            Attribute::new(&"person", &"age", &"person age", ValueDataType::UnsignedInt),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Test 1: Empty parameters should return UnparameterizedApplication error
        let empty_params = Parameters::new();
        let application = person.apply(empty_params);
        let result = application.plan(&VariableScope::new());

        assert!(result.is_err());
        if let Err(PlanError::UnparameterizedApplication) = result {
            // Expected error
        } else {
            panic!(
                "Expected UnparameterizedApplication error, got: {:?}",
                result
            );
        }

        // Test 2: All blank parameters should return UnparameterizedApplication error
        let mut all_blank_params = Parameters::new();
        all_blank_params.insert("name".into(), Term::blank());
        all_blank_params.insert("age".into(), Term::blank());

        let application = person.apply(all_blank_params);
        let result = application.plan(&VariableScope::new());

        assert!(result.is_err());
        if let Err(PlanError::UnparameterizedApplication) = result {
            // Expected error
        } else {
            panic!(
                "Expected UnparameterizedApplication error, got: {:?}",
                result
            );
        }

        // Test 3: Parameters that don't match concept attributes should return UnparameterizedApplication
        let mut no_match_params = Parameters::new();
        no_match_params.insert("this".into(), Term::var("entity")); // Non-blank "this"
        no_match_params.insert("unknown_param".into(), Term::var("x"));

        let application = person.apply(no_match_params);
        let result = application.plan(&VariableScope::new());

        assert!(result.is_err());
        if let Err(PlanError::UnparameterizedApplication) = result {
            // Expected error - even though "this" is non-blank, no premises can be generated
        } else {
            panic!(
                "Expected UnparameterizedApplication error, got: {:?}",
                result
            );
        }

        // Test 4: Mixed case - some parameters match, some don't, but at least one matches (should succeed)
        let mut mixed_params = Parameters::new();
        mixed_params.insert("name".into(), Term::var("person_name")); // This matches
        mixed_params.insert("unknown_param".into(), Term::var("x")); // This doesn't match

        let application = person.apply(mixed_params);
        let result = application.plan(&VariableScope::new());

        // Should succeed because at least one parameter matches
        assert!(
            result.is_ok(),
            "Mixed parameters with at least one match should succeed, got: {:?}",
            result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_improved_error_messages() -> anyhow::Result<()> {
        use crate::artifact::ValueDataType;
        use crate::{error::PlanError, Term};

        // Set up concept
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".into(),
            Attribute::new(&"person", &"name", &"person name", ValueDataType::String),
        );

        let person = predicate::Concept {
            operator: "person".into(),
            attributes,
        };

        // Test that error messages are now helpful instead of "UnexpectedError"
        let empty_application = person.apply(Parameters::new());
        let error = empty_application.plan(&VariableScope::new()).unwrap_err();

        let error_message = error.to_string();
        println!("Empty parameters error: {}", error_message);
        assert!(error_message.contains("requires at least one non-blank parameter"));
        assert!(!error_message.contains("Unexpected error"));

        Ok(())
    }
}
