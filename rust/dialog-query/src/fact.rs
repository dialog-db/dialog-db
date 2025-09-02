//! Fact, Assertion, Retraction, and Claim types for the dialog-query system

pub use crate::artifact::{Artifact, Attribute, Cause, Entity, Instruction, Value};
pub use crate::types::Scalar;
use serde::{Deserialize, Serialize};

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

impl<T> Fact<T>
where
    T: Scalar,
{
    /// Start building a fact selector for queries
    pub fn select() -> crate::FactSelector<T> {
        crate::FactSelector::new()
    }

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
    use crate::artifact::{ArtifactStoreMut, Artifacts, Attribute, Entity, Instruction, Value};
    use crate::error::QueryResult;
    use crate::{Query, Term};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;
    use futures_util::{stream, StreamExt};

    #[tokio::test]
    async fn test_fact_assert_retract_and_query_with_variables() -> Result<()> {
        use crate::selection::SelectionExt;
        
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

        // Step 4: Test 1 - Named variables should get bound in matches
        let query_with_named_vars = Fact::<Value>::select()
            .the("user/name")
            .of(Term::var("user"))  // Named variable - should be bound
            .is(Term::var("name")); // Named variable - should be bound

        let matches = query_with_named_vars.query(&artifacts)?.collect_set().await?;
        
        assert_eq!(matches.len(), 2, "Should find both Alice and Bob");
        
        // Test .contains() style assertions with set semantics
        assert!(matches.contains_binding("name", &Value::String("Alice".to_string())));
        assert!(matches.contains_binding("name", &Value::String("Bob".to_string())));
        assert!(matches.contains_binding("user", &Value::Entity(alice.clone())));
        assert!(matches.contains_binding("user", &Value::Entity(bob.clone())));
        
        // Test values_for() to get all values and contains_value_for()
        let names = matches.values_for("name");
        assert_eq!(names.len(), 2);
        assert!(matches.contains_value_for("name", &Value::String("Alice".to_string())));
        assert!(matches.contains_value_for("name", &Value::String("Bob".to_string())));

        // Step 5: Test 2 - Unnamed variables should not get bound
        let query_with_wildcards = Fact::<Value>::select()
            .the("user/email") 
            .of(Term::blank())  // Unnamed variable (wildcard) - should not be bound
            .is(Term::blank()); // Unnamed variable (wildcard) - should not be bound

        let wildcard_matches = query_with_wildcards.query(&artifacts)?.collect_set().await?;
        
        assert_eq!(wildcard_matches.len(), 1, "Should find Alice's email");
        
        // Verify no variables are bound (set should have empty variable maps)
        for match_frame in wildcard_matches.iter() {
            assert!(match_frame.variables.is_empty(), "Wildcards should not bind variables");
        }

        // Step 6: Test 3 - Mixed named and unnamed variables
        let mixed_query = Fact::<Value>::select()
            .the("user/name")
            .of(Term::var("person"))  // Named - should be bound
            .is(Term::blank());       // Unnamed - should not be bound

        let mixed_matches = mixed_query.query(&artifacts)?.collect_set().await?;
        
        assert_eq!(mixed_matches.len(), 2, "Should find both users");
        
        // Test that only named variable is bound
        assert!(mixed_matches.contains_binding("person", &Value::Entity(alice.clone())));
        assert!(mixed_matches.contains_binding("person", &Value::Entity(bob.clone())));
        
        // Verify unnamed variable didn't get bound - all matches should only have "person" key
        for match_frame in mixed_matches.iter() {
            let variables: Vec<String> = match_frame.variables.keys().cloned().collect();
            assert_eq!(variables, vec!["person"], "Only named variable should be bound");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_retraction_workflow() -> Result<()> {
        use crate::selection::SelectionExt;
        
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

        // Step 2: Verify fact exists using constant entity (no variables should be bound)
        let query_constant = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())  // Constant entity
            .is(Term::var("name")); // Variable value - should be bound

        let results = query_constant.query(&artifacts)?.collect_set().await?;

        assert_eq!(results.len(), 1);
        // Only the variable should be bound, not the constant
        assert!(results.contains_binding("name", &Value::String("Alice".to_string())));
        // Verify the constant entity is not bound (no "of" variable)
        assert!(!results.iter().any(|m| m.variables.contains_key("of")));

        // Step 3: Retract the fact
        let retraction = Fact::retract(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        );

        artifacts
            .commit(stream::iter(vec![Instruction::from(retraction)]))
            .await?;

        // Step 4: Verify fact is gone using the same constant query
        let query2 = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())  // Same constant entity
            .is(Term::var("name"));

        let results2 = query2.query(&artifacts)?.collect_set().await?;

        assert_eq!(results2.len(), 0, "Fact should be retracted");

        Ok(())
    }

    #[tokio::test]
    async fn test_constants_vs_variables_binding() -> Result<()> {
        use crate::selection::SelectionExt;
        
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Create facts
        let facts = vec![
            Fact::assert("user/name".parse::<Attribute>()?, alice.clone(), Value::String("Alice".to_string())),
            Fact::assert("user/name".parse::<Attribute>()?, bob.clone(), Value::String("Bob".to_string())),
            Fact::assert("user/age".parse::<Attribute>()?, alice.clone(), Value::UnsignedInt(30)),
        ];

        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;

        // Test 1: All constants - no variables should be bound
        let all_constants_query = Fact::<Value>::select()
            .the("user/name")                                    // Constant attribute
            .of(alice.clone())                                   // Constant entity  
            .is(Value::String("Alice".to_string()));            // Constant value

        let constant_results = all_constants_query.query(&artifacts)?.collect_set().await?;
        
        assert_eq!(constant_results.len(), 1, "Should find Alice's name fact");
        // No variables should be bound since all terms are constants
        for match_frame in constant_results.iter() {
            assert!(match_frame.variables.is_empty(), "Constants should not create variable bindings");
        }

        // Test 2: Mixed constants and variables - only variables should be bound
        let mixed_query = Fact::<Value>::select()
            .the("user/name")                                    // Constant attribute
            .of(Term::var("person"))                             // Variable entity - should bind
            .is(Value::String("Alice".to_string()));            // Constant value

        let mixed_results = mixed_query.query(&artifacts)?.collect_set().await?;
        
        assert_eq!(mixed_results.len(), 1, "Should find Alice specifically");
        // Only the entity variable should be bound
        assert!(mixed_results.contains_binding("person", &Value::Entity(alice.clone())));
        // Verify only one variable is bound
        for match_frame in mixed_results.iter() {
            assert_eq!(match_frame.variables.len(), 1, "Only one variable should be bound");
            assert!(match_frame.variables.contains_key("person"), "Should bind the entity variable");
        }

        // Test 3: Constant that finds multiple facts via variable
        let find_all_names = Fact::<Value>::select()
            .the("user/name")                                    // Constant attribute
            .of(Term::var("person"))                             // Variable entity
            .is(Term::var("name"));                              // Variable value

        let all_name_results = find_all_names.query(&artifacts)?.collect_set().await?;
        
        assert_eq!(all_name_results.len(), 2, "Should find both Alice and Bob");
        assert!(all_name_results.contains_binding("person", &Value::Entity(alice.clone())));
        assert!(all_name_results.contains_binding("person", &Value::Entity(bob.clone())));
        assert!(all_name_results.contains_binding("name", &Value::String("Alice".to_string())));
        assert!(all_name_results.contains_binding("name", &Value::String("Bob".to_string())));

        Ok(())
    }

    #[cfg(disabled)] // Disabled - needs update for new Query API returning Match instead of Artifact
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
        let admin_query = Fact::select().the("user/role").is("admin");

        let admin_results = admin_query
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;

        assert_eq!(admin_results.len(), 2); // Alice and Charlie

        // Query 2: Find all user/role facts using Query trait
        let role_query = Fact::<Value>::select().the("user/role");

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
        let bob_query = Fact::<Value>::select()
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

    #[cfg(disabled)] // Disabled - needs update for new Query API returning Match instead of Artifact
    #[tokio::test]
    async fn test_variable_queries_succeed_with_constants() -> Result<()> {
        // This test demonstrates that queries with variables succeed if there are constants
        // Variables are silently skipped, and the query uses only the constant parts

        let query_with_variables = Fact::<String>::select()
            .the("user/name") // This is a constant
            .of(Term::var("user")) // This is a variable (skipped)
            .is(Term::<String>::var("name")); // This now works with String type

        // Setup store for completeness
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Query trait should succeed, using only the constant "user/name"
        let result = query_with_variables.query(&artifacts);
        assert!(result.is_ok());

        Ok(())
    }

    #[cfg(disabled)] // Disabled - needs update for new Query API returning Match instead of Artifact
    #[tokio::test]
    async fn test_typed_fact_selector_patterns() -> Result<()> {
        // This test demonstrates the new generic Fact<T>::select() patterns

        // Pattern 1: String-typed FactSelector
        let string_selector = Fact::<String>::select()
            .the("user/name")
            .is(Term::<String>::var("name")); // String type is preserved

        // Pattern 2: Value-typed FactSelector (backward compatible)
        let value_selector = Fact::<Value>::select()
            .the("user/name")
            .is(Term::<Value>::var("name"));

        // Pattern 3: Entity-typed FactSelector
        let entity_selector = Fact::<Entity>::select()
            .the("user/friend")
            .is(Term::<Entity>::var("friend"));

        // All should compile and create appropriate FactSelector types
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Test that all patterns work with the Query trait
        let _string_result = string_selector.query(&artifacts);
        let _value_result = value_selector.query(&artifacts);
        let _entity_result = entity_selector.query(&artifacts);

        Ok(())
    }

    #[cfg(disabled)] // Disabled - needs update for new Query API returning Match instead of Artifact
    #[tokio::test]
    async fn test_type_inference_with_string_literals() -> Result<()> {
        // This test demonstrates how type inference works with string literals

        // Pattern 1: When using Fact::select() without type annotation,
        // string literals infer to String type
        let inferred_string_query = Fact::select().the("user/name").is("Bob"); // This infers Fact<String> because "Bob" -> Term<String>

        // Pattern 2: When you need Value type, be explicit
        let value_query = Fact::select()
            .the("user/email")
            .is(Value::String("alice@example.com".to_string())); // Explicit Value

        // Pattern 3: String-typed selectors work naturally
        let string_query = Fact::select().the("user/name").is("Bob"); // Type is already String, works naturally

        // Pattern 4: The specific case from line 602 now works with inference!
        let admin_query = Fact::select().the("user/role").is("admin"); // Infers as Fact<String>

        // All should compile without ambiguity
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Verify they work - the inferred query is actually FactSelector<String>
        let _inferred_result = inferred_string_query.query(&artifacts);
        let _value_result = value_query.query(&artifacts);
        let _string_result = string_query.query(&artifacts);
        let _admin_result = admin_query.query(&artifacts);

        Ok(())
    }

    #[cfg(disabled)] // Disabled - needs update for new Query API returning Match instead of Artifact
    #[tokio::test]
    async fn test_mixed_constants_and_variables_succeed() -> Result<()> {
        // Test that queries with mixed constants and variables succeed
        // Variables are skipped, constants are used

        let alice = Entity::new()?;

        let mixed_query = Fact::<Value>::select()
            .the("user/name") // Constant - used
            .of(alice) // Constant - used
            .is(Term::<Value>::var("name")); // Variable - skipped

        // Setup store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let result = mixed_query.query(&artifacts);

        // Should succeed because we have constants (the and of)
        assert!(result.is_ok());

        Ok(())
    }

    #[cfg(disabled)] // Disabled - needs update for new Query API returning Match instead of Artifact
    #[tokio::test]
    async fn test_only_variables_query_fails() -> Result<()> {
        // Test that queries with ONLY variables and NO constants fail

        let query_only_vars = Fact::<Value>::select()
            .the(Term::<Attribute>::var("attr")) // Variable
            .of(Term::<Entity>::var("entity")) // Variable
            .is(Term::<Value>::var("value")); // Variable

        // Setup store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let result = query_only_vars.query(&artifacts);

        // Should fail because there are no constants at all
        assert!(result.is_err());
        if let Err(error) = result {
            assert!(error.to_string().contains("Variable not supported"));
        }

        Ok(())
    }

    #[cfg(disabled)] // Disabled - needs update for new Query API returning Match instead of Artifact
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
        let admin_count = Fact::<Value>::select()
            .the("user/role")
            .is(Value::String("admin".to_string()))
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await
            .len();

        assert_eq!(admin_count, 1); // Only Alice is admin

        // Test another fluent query using Query trait
        let user_names: Vec<String> = Fact::<Value>::select()
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
