//! Fact, Assertion, Retraction, and Claim types for the dialog-query system

use dialog_artifacts::{Artifact, Attribute, Cause, Instruction, Value};
use serde::{Deserialize, Serialize};

pub use dialog_artifacts::Entity;

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

/// A claim represents an assertion or retraction before it becomes a fact
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Claim<T = Value> {
    /// An assertion claim
    Assertion {
        /// The attribute (predicate)
        the: Attribute,
        /// The entity (subject)
        of: Entity,
        /// The value (object)
        is: T,
    },
    /// A retraction claim
    Retraction {
        /// The attribute (predicate)
        the: Attribute,
        /// The entity (subject)
        of: Entity,
        /// The value (object)
        is: T,
    },
}

/// A fact represents persisted data with a cause - can be an assertion or retraction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Fact<T = Value> {
    /// An assertion fact with cause
    Assertion {
        /// The attribute (predicate)
        the: Attribute,
        /// The entity (subject)
        of: Entity,
        /// The value (object)
        is: T,
        /// The cause of this fact
        cause: Cause,
    },
    /// A retraction fact with cause
    Retraction {
        /// The attribute (predicate)
        the: Attribute,
        /// The entity (subject)
        of: Entity,
        /// The value (object)
        is: T,
        /// The cause of this fact
        cause: Cause,
    },
}

impl Fact {
    /// Start building a fact selector for queries
    pub fn select() -> crate::FactSelector<Value> {
        crate::FactSelector::new()
    }
}

impl<T> Fact<T> {
    /// Create an assertion claim from individual components
    pub fn assert<The: Into<Attribute>, Of: Into<Entity>>(the: The, of: Of, is: T) -> Claim<T> {
        Claim::Assertion {
            the: the.into(),
            of: of.into(),
            is,
        }
    }

    /// Create a retraction claim from individual components
    pub fn retract(the: impl Into<Attribute>, of: impl Into<Entity>, is: T) -> Claim<T> {
        Claim::Retraction {
            the: the.into(),
            of: of.into(),
            is,
        }
    }
}

/// Create a generic assertion claim from individual components
pub fn assert<T, The: Into<Attribute>, Of: Into<Entity>, Is: Into<T>>(
    the: The,
    of: Of,
    is: Is,
) -> Claim<T> {
    Claim::Assertion {
        the: the.into(),
        of: of.into(),
        is: is.into(),
    }
}

/// Create a generic retraction claim from individual components
pub fn retract<T, The: Into<Attribute>, Of: Into<Entity>, Is: Into<T>>(
    the: The,
    of: Of,
    is: Is,
) -> Claim<T> {
    Claim::Retraction {
        the: the.into(),
        of: of.into(),
        is: is.into(),
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

/// Convert Claim to Instruction for committing
impl<T> From<Claim<T>> for Instruction
where
    T: Into<Value>,
{
    fn from(claim: Claim<T>) -> Self {
        match claim {
            Claim::Assertion { the, of, is } => {
                let artifact = Artifact {
                    the,
                    of,
                    is: is.into(),
                    cause: None,
                };
                Instruction::Assert(artifact)
            }
            Claim::Retraction { the, of, is } => {
                let artifact = Artifact {
                    the,
                    of,
                    is: is.into(),
                    cause: None,
                };
                Instruction::Retract(artifact)
            }
        }
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
        let claim = Fact::assert(
            "user/name".parse::<Attribute>().unwrap(),
            entity.clone(),
            Value::String("Alice".to_string()),
        );

        match claim {
            Claim::Assertion { the, of, is } => {
                assert_eq!(the.to_string(), "user/name");
                assert_eq!(of, entity);
                assert_eq!(is, Value::String("Alice".to_string()));
            }
            _ => panic!("Expected Claim::Assertion"),
        }
    }

    #[test]
    fn test_fact_retract() {
        let entity = Entity::new().unwrap();
        let claim = Fact::retract(
            "user/name".parse::<Attribute>().unwrap(),
            entity.clone(),
            Value::String("Alice".to_string()),
        );

        match claim {
            Claim::Retraction { the, of, is } => {
                assert_eq!(the.to_string(), "user/name");
                assert_eq!(of, entity);
                assert_eq!(is, Value::String("Alice".to_string()));
            }
            _ => panic!("Expected Claim::Retraction"),
        }
    }

    #[test]
    fn test_assertion_to_instruction() {
        let entity = Entity::new().unwrap();
        let claim = Fact::assert(
            "user/name".parse::<Attribute>().unwrap(),
            entity.clone(),
            Value::String("Alice".to_string()),
        );
        let instruction: Instruction = claim.into();

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
        let claim = Fact::retract(
            "user/name".parse::<Attribute>().unwrap(),
            entity.clone(),
            Value::String("Alice".to_string()),
        );
        let instruction: Instruction = claim.into();

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
        let assertion_claim = Fact::assert(
            "user/name".parse::<Attribute>().unwrap(),
            entity.clone(),
            Value::String("Alice".to_string()),
        );
        let retraction_claim = Fact::retract(
            "user/email".parse::<Attribute>().unwrap(),
            entity.clone(),
            Value::String("alice@example.com".to_string()),
        );

        // Convert to instructions for committing
        let assert_instruction: Instruction = assertion_claim.into();
        let retract_instruction: Instruction = retraction_claim.into();

        // Verify they're the right types
        assert!(matches!(assert_instruction, Instruction::Assert(_)));
        assert!(matches!(retract_instruction, Instruction::Retract(_)));
    }

    #[test]
    fn test_generic_static_functions() {
        let entity = Entity::new().unwrap();

        // Test generic static assert function with String type
        let string_claim: Claim<String> = assert(
            "user/name".parse::<Attribute>().unwrap(),
            entity.clone(),
            "Alice".to_string(),
        );

        match string_claim {
            Claim::Assertion { the, of, is } => {
                assert_eq!(the.to_string(), "user/name");
                assert_eq!(of, entity);
                assert_eq!(is, "Alice".to_string());
            }
            _ => panic!("Expected Claim::Assertion"),
        }

        // Test generic static retract function with u32 type
        let number_claim: Claim<u32> = retract(
            "user/age".parse::<Attribute>().unwrap(),
            entity.clone(),
            25u32,
        );

        match number_claim {
            Claim::Retraction { the, of, is } => {
                assert_eq!(the.to_string(), "user/age");
                assert_eq!(of, entity);
                assert_eq!(is, 25u32);
            }
            _ => panic!("Expected Claim::Retraction"),
        }
    }

    #[test]
    fn test_string_literal_support() {
        let entity = Entity::new().unwrap();

        // Test with Value type (need to construct Value explicitly)
        let claim = Fact::<Value>::assert(
            "user/name".parse::<Attribute>().unwrap(),
            entity.clone(),
            Value::String("Alice".to_string()), // Direct Value construction
        );

        match claim {
            Claim::Assertion { the, of, is } => {
                assert_eq!(the.to_string(), "user/name");
                assert_eq!(of, entity);
                assert_eq!(is, Value::String("Alice".to_string()));
            }
            _ => panic!("Expected Claim::Assertion"),
        }

        // Test with String type directly
        let string_claim = Fact::<String>::assert(
            "user/email".parse::<Attribute>().unwrap(),
            entity.clone(),
            "alice@example.com".to_string(),
        );

        match string_claim {
            Claim::Assertion { the, of, is } => {
                assert_eq!(the.to_string(), "user/email");
                assert_eq!(of, entity);
                assert_eq!(is, "alice@example.com".to_string());
            }
            _ => panic!("Expected Claim::Assertion"),
        }

        // Test that both types work with FactSelector and Query trait
        let value_selector: crate::FactSelector<Value> = crate::FactSelector::new();
        let string_selector: crate::FactSelector<String> = crate::FactSelector::new();

        // Both should compile and work
        assert!(value_selector.the.is_none());
        assert!(string_selector.the.is_none());
    }
}

#[cfg(test)]
mod integration_tests {
    //! Integration tests for the complete Fact workflow:
    //! Fact::assert/retract → commit → Fact::select → query

    use super::*;
    use crate::variable::TypedVariable;
    use crate::Query;
    use anyhow::Result;
    use dialog_artifacts::{ArtifactStoreMut, Artifacts, Attribute, Entity, Instruction, Value};
    use dialog_storage::MemoryStorageBackend;
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
            Value::String("Alice".to_string()),
        );

        let alice_email = Fact::assert(
            "user/email".parse::<Attribute>()?,
            alice.clone(),
            Value::String("alice@example.com".to_string()),
        );

        let bob_name = Fact::assert(
            "user/name".parse::<Attribute>()?,
            bob.clone(),
            Value::String("Bob".to_string()),
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
        assert_eq!(
            alice_email_results[0].is,
            Value::String("alice@example.com".to_string())
        );

        // Query 3: Find all facts with user/name attribute using Query trait
        let all_names_query = Fact::select().the("user/name");

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
            Value::String("Alice".to_string()),
        );

        artifacts
            .commit(stream::iter(vec![Instruction::from(alice_name)]))
            .await?;

        // Step 2: Verify fact exists using Query trait
        let query = Fact::select().the("user/name").of(alice.clone());

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
            Value::String("Alice".to_string()),
        );

        artifacts
            .commit(stream::iter(vec![Instruction::from(retraction)]))
            .await?;

        // Step 4: Verify fact is gone using Query trait
        let query2 = Fact::select().the("user/name").of(alice.clone());

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
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "user/role".parse::<Attribute>()?,
                alice.clone(),
                Value::String("admin".to_string()),
            ),
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "user/role".parse::<Attribute>()?,
                bob.clone(),
                Value::String("user".to_string()),
            ),
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                charlie.clone(),
                Value::String("Charlie".to_string()),
            ),
            Fact::assert(
                "user/role".parse::<Attribute>()?,
                charlie.clone(),
                Value::String("admin".to_string()),
            ),
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
        let role_query = Fact::select().the("user/role");

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
            .of(TypedVariable::<Entity>::new("user")) // This is a variable
            .is(TypedVariable::<String>::new("name")); // This is also a variable

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
            .the("user/name") // Constant - OK
            .of(alice) // Constant - OK
            .is(TypedVariable::<String>::new("name")); // Variable - should fail

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
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "user/role".parse::<Attribute>()?,
                alice.clone(),
                Value::String("admin".to_string()),
            ),
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "user/role".parse::<Attribute>()?,
                bob.clone(),
                Value::String("user".to_string()),
            ),
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
