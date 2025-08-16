//! Fact, Assertion, and Retraction types for the dialog-query system

use serde::{Deserialize, Serialize};
use dialog_artifacts::{Value, Entity, Attribute, Artifact, Instruction, Cause};

/// An assertion represents a fact to be asserted in the database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assertion {
    /// The attribute (predicate)
    pub the: Attribute,
    /// The entity (subject)  
    pub of: Entity,
    /// The value (object)
    pub is: Value,
}

/// A retraction represents a fact to be retracted from the database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Retraction {
    /// The attribute (predicate)
    pub the: Attribute,
    /// The entity (subject)
    pub of: Entity,
    /// The value (object)
    pub is: Value,
}

/// A fact is an assertion with a cause - represents persisted data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fact {
    /// The attribute (predicate)
    pub the: Attribute,
    /// The entity (subject)
    pub of: Entity,
    /// The value (object)
    pub is: Value,
    /// The cause of this fact
    pub cause: Cause,
}

impl Fact {
    /// Create an assertion from individual components
    pub fn assert(
        the: impl Into<Attribute>, 
        of: impl Into<Entity>, 
        is: impl Into<Value>
    ) -> Assertion {
        Assertion {
            the: the.into(),
            of: of.into(),
            is: is.into(),
        }
    }
    
    /// Create a retraction from individual components
    pub fn retract(
        the: impl Into<Attribute>,
        of: impl Into<Entity>, 
        is: impl Into<Value>
    ) -> Retraction {
        Retraction {
            the: the.into(),
            of: of.into(),
            is: is.into(),
        }
    }
    
    /// Start building a fact selector for queries
    pub fn select() -> crate::FactSelector {
        crate::FactSelector::new()
    }
}

/// Convert Assertion to Instruction for committing
impl From<Assertion> for Instruction {
    fn from(assertion: Assertion) -> Self {
        let artifact = Artifact {
            the: assertion.the,
            of: assertion.of,
            is: assertion.is,
            cause: None, // Assertions start without a cause
        };
        Instruction::Assert(artifact)
    }
}

/// Convert Retraction to Instruction for committing
impl From<Retraction> for Instruction {
    fn from(retraction: Retraction) -> Self {
        let artifact = Artifact {
            the: retraction.the,
            of: retraction.of,
            is: retraction.is,
            cause: None, // Retractions specify what to retract, cause is not relevant
        };
        Instruction::Retract(artifact)
    }
}

// Note: From implementations for external types (Attribute, Value) cannot be defined here
// due to Rust's orphan rules. Users should use .parse() or explicit constructors instead.

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_fact_assert() {
        let entity = Entity::new().unwrap();
        let assertion = Fact::assert(
            "user/name".parse::<Attribute>().unwrap(), 
            entity.clone(), 
            Value::String("Alice".to_string())
        );
        
        assert_eq!(assertion.the.to_string(), "user/name");
        assert_eq!(assertion.of, entity);
        assert_eq!(assertion.is, Value::String("Alice".to_string()));
    }
    
    #[test]
    fn test_fact_retract() {
        let entity = Entity::new().unwrap();
        let retraction = Fact::retract(
            "user/name".parse::<Attribute>().unwrap(), 
            entity.clone(), 
            Value::String("Alice".to_string())
        );
        
        assert_eq!(retraction.the.to_string(), "user/name");
        assert_eq!(retraction.of, entity);
        assert_eq!(retraction.is, Value::String("Alice".to_string()));
    }
    
    #[test]
    fn test_assertion_to_instruction() {
        let entity = Entity::new().unwrap();
        let assertion = Fact::assert(
            "user/name".parse::<Attribute>().unwrap(), 
            entity.clone(), 
            Value::String("Alice".to_string())
        );
        let instruction: Instruction = assertion.into();
        
        match instruction {
            Instruction::Assert(artifact) => {
                assert_eq!(artifact.the.to_string(), "user/name");
                assert_eq!(artifact.of, entity);
                assert_eq!(artifact.is, Value::String("Alice".to_string()));
                assert_eq!(artifact.cause, None);
            }
            _ => panic!("Expected Instruction::Assert"),
        }
    }
    
    #[test]
    fn test_retraction_to_instruction() {
        let entity = Entity::new().unwrap();
        let retraction = Fact::retract(
            "user/name".parse::<Attribute>().unwrap(), 
            entity.clone(), 
            Value::String("Alice".to_string())
        );
        let instruction: Instruction = retraction.into();
        
        match instruction {
            Instruction::Retract(artifact) => {
                assert_eq!(artifact.the.to_string(), "user/name");
                assert_eq!(artifact.of, entity);
                assert_eq!(artifact.is, Value::String("Alice".to_string()));
                assert_eq!(artifact.cause, None);
            }
            _ => panic!("Expected Instruction::Retract"),
        }
    }
    
    #[test]
    fn test_ergonomic_usage() {
        let entity = Entity::new().unwrap();
        
        // This is the clean API we want:
        let assertion = Fact::assert(
            "user/name".parse::<Attribute>().unwrap(), 
            entity.clone(), 
            Value::String("Alice".to_string())
        );
        let retraction = Fact::retract(
            "user/email".parse::<Attribute>().unwrap(), 
            entity.clone(), 
            Value::String("alice@example.com".to_string())
        );
        
        // Convert to instructions for committing
        let assert_instruction: Instruction = assertion.into();
        let retract_instruction: Instruction = retraction.into();
        
        // Verify they're the right types
        assert!(matches!(assert_instruction, Instruction::Assert(_)));
        assert!(matches!(retract_instruction, Instruction::Retract(_)));
    }
}

#[cfg(test)]
mod integration_tests {
    //! Integration tests for the complete Fact workflow:
    //! Fact::assert/retract → commit → Fact::select → query

    use super::*;
    use anyhow::Result;
    use dialog_artifacts::{
        Artifacts, ArtifactStoreMut, Entity, Value, Attribute, Instruction
    };
    use dialog_storage::MemoryStorageBackend;
    use crate::{Variable, Query};
    use futures_util::{stream, StreamExt};

    #[tokio::test]
    async fn test_fact_assert_retract_and_query_constants() -> Result<()> {
        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;
        
        // Step 1: Create entities for testing
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        
        // Step 2: Create facts using our Fact DSL
        let alice_name = Fact::assert(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string())
        );
        
        let alice_email = Fact::assert(
            "user/email".parse::<Attribute>()?,
            alice.clone(),
            Value::String("alice@example.com".to_string())
        );
        
        let bob_name = Fact::assert(
            "user/name".parse::<Attribute>()?,
            bob.clone(),
            Value::String("Bob".to_string())
        );
        
        // Step 3: Convert to instructions and commit to artifacts store
        let instructions = vec![
            Instruction::from(alice_name),
            Instruction::from(alice_email), 
            Instruction::from(bob_name),
        ];
        
        artifacts.commit(stream::iter(instructions)).await?;
        
        // Step 4: Query using Fact::select DSL with Query trait
        
        // Query 1: Find Alice specifically by name using Query trait
        let alice_query = Fact::select()
            .the("user/name")
            .of(alice.clone())
            .is(Value::String("Alice".to_string()));
        
        let alice_results = alice_query
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(alice_results.len(), 1);
        assert_eq!(alice_results[0].of, alice);
        assert_eq!(alice_results[0].is, Value::String("Alice".to_string()));
        
        // Query 2: Find Alice's email specifically using Query trait
        let alice_email_query = Fact::select()
            .the("user/email")
            .of(alice.clone())
            .is(Value::String("alice@example.com".to_string()));
        
        let alice_email_results = alice_email_query
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(alice_email_results.len(), 1);
        assert_eq!(alice_email_results[0].is, Value::String("alice@example.com".to_string()));
        
        // Query 3: Find all facts with user/name attribute using Query trait
        let all_names_query = Fact::select()
            .the("user/name");
        
        let all_names_results = all_names_query
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(all_names_results.len(), 2); // Alice and Bob
        
        // Verify we have both names
        let names: Vec<String> = all_names_results
            .iter()
            .map(|artifact| match &artifact.is {
                Value::String(s) => s.clone(),
                _ => panic!("Expected string value"),
            })
            .collect();
        
        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Bob".to_string()));
        
        Ok(())
    }

    #[tokio::test]
    async fn test_retraction_workflow() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;
        
        let alice = Entity::new()?;
        
        // Step 1: Assert a fact
        let alice_name = Fact::assert(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string())
        );
        
        artifacts.commit(stream::iter(vec![Instruction::from(alice_name)])).await?;
        
        // Step 2: Verify fact exists using Query trait
        let query = Fact::select()
            .the("user/name")
            .of(alice.clone());
        
        let results = query
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, Value::String("Alice".to_string()));
        
        // Step 3: Retract the fact
        let retraction = Fact::retract(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string())
        );
        
        artifacts.commit(stream::iter(vec![Instruction::from(retraction)])).await?;
        
        // Step 4: Verify fact is gone using Query trait
        let query2 = Fact::select()
            .the("user/name")
            .of(alice.clone());
        
        let results2 = query2
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(results2.len(), 0); // Fact should be retracted
        
        Ok(())
    }

    #[tokio::test]
    async fn test_complex_queries_with_constants() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;
        
        // Create test data: users with different roles
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let charlie = Entity::new()?;
        
        let facts = vec![
            // Users and roles
            Fact::assert("user/name".parse::<Attribute>()?, alice.clone(), Value::String("Alice".to_string())),
            Fact::assert("user/role".parse::<Attribute>()?, alice.clone(), Value::String("admin".to_string())),
            Fact::assert("user/name".parse::<Attribute>()?, bob.clone(), Value::String("Bob".to_string())),
            Fact::assert("user/role".parse::<Attribute>()?, bob.clone(), Value::String("user".to_string())),
            Fact::assert("user/name".parse::<Attribute>()?, charlie.clone(), Value::String("Charlie".to_string())),
            Fact::assert("user/role".parse::<Attribute>()?, charlie.clone(), Value::String("admin".to_string())),
        ];
        
        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;
        
        // Query 1: Find all admins by role using Query trait
        let admin_query = Fact::select()
            .the("user/role")
            .is(Value::String("admin".to_string()));
        
        let admin_results = admin_query
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(admin_results.len(), 2); // Alice and Charlie
        
        // Query 2: Find all user/role facts using Query trait
        let role_query = Fact::select()
            .the("user/role");
        
        let role_results = role_query
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(role_results.len(), 3); // Alice=admin, Bob=user, Charlie=admin
        
        let roles: Vec<String> = role_results
            .iter()
            .map(|artifact| match &artifact.is {
                Value::String(s) => s.clone(),
                _ => panic!("Expected string value"),
            })
            .collect();
        
        assert!(roles.contains(&"admin".to_string()));
        assert!(roles.contains(&"user".to_string()));
        
        // Query 3: Find Bob specifically using Query trait
        let bob_query = Fact::select()
            .the("user/name")
            .of(bob.clone())
            .is(Value::String("Bob".to_string()));
        
        let bob_results = bob_query
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(bob_results.len(), 1);
        assert_eq!(bob_results[0].of, bob);
        
        Ok(())
    }

    #[tokio::test] 
    async fn test_variable_queries_fail_with_helpful_error() -> Result<()> {
        // This test demonstrates that queries with variables cannot be used with Query trait
        // This is expected behavior since ArtifactSelector only works with concrete values
        
        let query_with_variables = Fact::select()
            .the("user/name")
            .of(Variable::Entity("user"))  // This is a variable
            .is(Variable::String("name")); // This is also a variable
        
        // Setup store for completeness
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;
        
        // Attempting to use Query trait should fail
        let result = query_with_variables.query(&artifacts);
        
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(error.to_string().contains("Variable not supported"));
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_constants_and_variables_fail() -> Result<()> {
        // Test that queries with mixed constants and variables fail appropriately
        
        let alice = Entity::new()?;
        
        let mixed_query = Fact::select()
            .the("user/name")                    // Constant - OK
            .of(alice)                           // Constant - OK  
            .is(Variable::String("name"));       // Variable - should fail
        
        // Setup store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;
        
        let result = mixed_query.query(&artifacts);
        
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(error.to_string().contains("Variable not supported"));
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_fluent_query_building_and_execution() -> Result<()> {
        // This test shows how the Query trait enables fluent query building and execution
        
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;
        
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        
        let facts = vec![
            Fact::assert("user/name".parse::<Attribute>()?, alice.clone(), Value::String("Alice".to_string())),
            Fact::assert("user/role".parse::<Attribute>()?, alice.clone(), Value::String("admin".to_string())),
            Fact::assert("user/name".parse::<Attribute>()?, bob.clone(), Value::String("Bob".to_string())),
            Fact::assert("user/role".parse::<Attribute>()?, bob.clone(), Value::String("user".to_string())),
        ];
        
        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;
        
        // Test fluent query building with immediate execution using Query trait
        let admin_count = Fact::select()
            .the("user/role")
            .is(Value::String("admin".to_string()))
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await
            .len();
        
        assert_eq!(admin_count, 1); // Only Alice is admin
        
        // Test another fluent query using Query trait
        let user_names: Vec<String> = Fact::select()
            .the("user/name")
            .query(&artifacts)?
            .filter_map(|result| async move { 
                result.ok().and_then(|artifact| match artifact.is {
                    Value::String(name) => Some(name),
                    _ => None,
                })
            })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(user_names.len(), 2);
        assert!(user_names.contains(&"Alice".to_string()));
        assert!(user_names.contains(&"Bob".to_string()));
        
        Ok(())
    }
}