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
    use crate::{Query, Term};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;
    use futures_util::stream;

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
            .of(Term::var("user")) // Named variable - should be bound
            .is(Term::var("name")); // Named variable - should be bound

        let matches = query_with_named_vars
            .query(&artifacts)?
            .collect_set()
            .await?;

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
            .of(Term::blank()) // Unnamed variable (wildcard) - should not be bound
            .is(Term::blank()); // Unnamed variable (wildcard) - should not be bound

        let wildcard_matches = query_with_wildcards
            .query(&artifacts)?
            .collect_set()
            .await?;

        assert_eq!(wildcard_matches.len(), 1, "Should find Alice's email");

        // Verify no variables are bound (set should have empty variable maps)
        for match_frame in wildcard_matches.iter() {
            assert!(
                match_frame.variables.is_empty(),
                "Wildcards should not bind variables"
            );
        }

        // Step 6: Test 3 - Mixed named and unnamed variables
        let mixed_query = Fact::<Value>::select()
            .the("user/name")
            .of(Term::var("person")) // Named - should be bound
            .is(Term::blank()); // Unnamed - should not be bound

        let mixed_matches = mixed_query.query(&artifacts)?.collect_set().await?;

        assert_eq!(mixed_matches.len(), 2, "Should find both users");

        // Test that only named variable is bound
        assert!(mixed_matches.contains_binding("person", &Value::Entity(alice.clone())));
        assert!(mixed_matches.contains_binding("person", &Value::Entity(bob.clone())));

        // Verify unnamed variable didn't get bound - all matches should only have "person" key
        for match_frame in mixed_matches.iter() {
            let variables: Vec<String> = match_frame.variables.keys().cloned().collect();
            assert_eq!(
                variables,
                vec!["person"],
                "Only named variable should be bound"
            );
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
            .of(alice.clone()) // Constant entity
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
            .of(alice.clone()) // Same constant entity
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
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "user/age".parse::<Attribute>()?,
                alice.clone(),
                Value::UnsignedInt(30),
            ),
        ];

        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;

        // Test 1: All constants - no variables should be bound
        let all_constants_query = Fact::<Value>::select()
            .the("user/name") // Constant attribute
            .of(alice.clone()) // Constant entity
            .is(Value::String("Alice".to_string())); // Constant value

        let constant_results = all_constants_query.query(&artifacts)?.collect_set().await?;

        assert_eq!(constant_results.len(), 1, "Should find Alice's name fact");
        // No variables should be bound since all terms are constants
        for match_frame in constant_results.iter() {
            assert!(
                match_frame.variables.is_empty(),
                "Constants should not create variable bindings"
            );
        }

        // Test 2: Mixed constants and variables - only variables should be bound
        let mixed_query = Fact::<Value>::select()
            .the("user/name") // Constant attribute
            .of(Term::var("person")) // Variable entity - should bind
            .is(Value::String("Alice".to_string())); // Constant value

        let mixed_results = mixed_query.query(&artifacts)?.collect_set().await?;

        assert_eq!(mixed_results.len(), 1, "Should find Alice specifically");
        // Only the entity variable should be bound
        assert!(mixed_results.contains_binding("person", &Value::Entity(alice.clone())));
        // Verify only one variable is bound
        for match_frame in mixed_results.iter() {
            assert_eq!(
                match_frame.variables.len(),
                1,
                "Only one variable should be bound"
            );
            assert!(
                match_frame.variables.contains_key("person"),
                "Should bind the entity variable"
            );
        }

        // Test 3: Constant that finds multiple facts via variable
        let find_all_names = Fact::<Value>::select()
            .the("user/name") // Constant attribute
            .of(Term::var("person")) // Variable entity
            .is(Term::var("name")); // Variable value

        let all_name_results = find_all_names.query(&artifacts)?.collect_set().await?;

        assert_eq!(all_name_results.len(), 2, "Should find both Alice and Bob");
        assert!(all_name_results.contains_binding("person", &Value::Entity(alice.clone())));
        assert!(all_name_results.contains_binding("person", &Value::Entity(bob.clone())));
        assert!(all_name_results.contains_binding("name", &Value::String("Alice".to_string())));
        assert!(all_name_results.contains_binding("name", &Value::String("Bob".to_string())));

        Ok(())
    }

    #[tokio::test]
    async fn test_complex_queries_with_constants() -> Result<()> {
        use crate::selection::SelectionExt;

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

        // Query 1: Find all admins by role - using constants with variable entity
        let admin_query = Fact::select()
            .the("user/role") // Constant attribute
            .of(Term::var("admin_user")) // Variable entity - should bind
            .is("admin"); // Constant value

        let admin_results = admin_query.query(&artifacts)?.collect_set().await?;

        assert_eq!(
            admin_results.len(),
            2,
            "Should find Alice and Charlie as admins"
        );

        // Verify that entity variable is bound but constants are not
        assert!(admin_results.contains_binding("admin_user", &Value::Entity(alice.clone())));
        assert!(admin_results.contains_binding("admin_user", &Value::Entity(charlie.clone())));

        // Verify no bindings for constants (role value shouldn't be bound)
        for match_frame in admin_results.iter() {
            assert_eq!(
                match_frame.variables.len(),
                1,
                "Only entity variable should be bound"
            );
            assert!(
                match_frame.variables.contains_key("admin_user"),
                "Entity variable should be bound"
            );
        }

        // Query 2: Find all user roles with variable entity and value
        let role_query = Fact::<Value>::select()
            .the("user/role") // Constant attribute
            .of(Term::var("user")) // Variable entity
            .is(Term::var("role")); // Variable value

        let role_results = role_query.query(&artifacts)?.collect_set().await?;

        assert_eq!(role_results.len(), 3, "Should find all 3 role assignments");

        // Test set-based contains for both variables
        assert!(role_results.contains_binding("role", &Value::String("admin".to_string())));
        assert!(role_results.contains_binding("role", &Value::String("user".to_string())));
        assert!(role_results.contains_binding("user", &Value::Entity(alice.clone())));
        assert!(role_results.contains_binding("user", &Value::Entity(bob.clone())));
        assert!(role_results.contains_binding("user", &Value::Entity(charlie.clone())));

        // Query 3: Find Bob specifically using all constants (no variables)
        let bob_query = Fact::<Value>::select()
            .the("user/name") // Constant attribute
            .of(bob.clone()) // Constant entity
            .is(Value::String("Bob".to_string())); // Constant value

        let bob_results = bob_query.query(&artifacts)?.collect_set().await?;

        assert_eq!(bob_results.len(), 1, "Should find exactly Bob's name fact");

        // Verify no variables are bound since all terms are constants
        for match_frame in bob_results.iter() {
            assert!(
                match_frame.variables.is_empty(),
                "No variables should be bound for all-constant query"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_variable_queries_succeed_with_constants() -> Result<()> {
        use crate::selection::SelectionExt;

        // This test demonstrates that queries with variables work properly -
        // constants are used for matching, variables get bound in results

        // Setup store with test data
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
                "user/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
        ];

        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;

        let query_with_variables = Fact::<Value>::select()
            .the("user/name") // Constant - used for matching
            .of(Term::var("user")) // Variable - gets bound to entities
            .is(Term::var("name")); // Variable - gets bound to names

        // Query should succeed and return matches with variable bindings
        let results = query_with_variables
            .query(&artifacts)?
            .collect_set()
            .await?;

        assert_eq!(results.len(), 2, "Should find both Alice and Bob");

        // Verify variable bindings
        assert!(results.contains_binding("user", &Value::Entity(alice.clone())));
        assert!(results.contains_binding("user", &Value::Entity(bob.clone())));
        assert!(results.contains_binding("name", &Value::String("Alice".to_string())));
        assert!(results.contains_binding("name", &Value::String("Bob".to_string())));

        Ok(())
    }

    #[tokio::test]
    async fn test_typed_fact_selector_patterns() -> Result<()> {
        use crate::selection::SelectionExt;

        // This test demonstrates that different typed fact selectors work with the new Query API

        // Setup test data
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
                "user/friend".parse::<Attribute>()?,
                alice.clone(),
                Value::Entity(bob.clone()),
            ),
        ];

        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;

        // Pattern 1: String-typed FactSelector (most common, backward compatible)
        let value_selector = Fact::select()
            .the("user/name")
            .of(Term::var("user"))
            .is(Term::<String>::var("name"));

        let value_results = value_selector.query(&artifacts)?.collect_set().await?;
        assert_eq!(value_results.len(), 1);
        assert!(value_results.contains_binding("user", &Value::Entity(alice.clone())));
        assert!(value_results.contains_binding("name", &Value::String("Alice".to_string())));

        // Pattern 2: Entity-typed FactSelector for entity values
        let entity_selector = Fact::<Value>::select()
            .the("user/friend")
            .of(alice.clone()) // Constant entity
            .is(Term::<Value>::var("friend")); // Variable - should bind to Bob

        let entity_results = entity_selector.query(&artifacts)?.collect_set().await?;
        assert_eq!(entity_results.len(), 1);
        assert!(entity_results.contains_binding("friend", &Value::Entity(bob.clone())));

        // Verify only the variable is bound, not the constant
        for match_frame in entity_results.iter() {
            assert_eq!(
                match_frame.variables.len(),
                1,
                "Only variable should be bound"
            );
        }

        // Pattern 3: Test with all constants (no variables)
        let constant_selector = Fact::<Value>::select()
            .the("user/name") // Constant
            .of(alice.clone()) // Constant
            .is(Value::String("Alice".to_string())); // Constant

        let constant_results = constant_selector.query(&artifacts)?.collect_set().await?;
        assert_eq!(constant_results.len(), 1);

        // No variables should be bound
        for match_frame in constant_results.iter() {
            assert!(
                match_frame.variables.is_empty(),
                "No variables should be bound for all-constant query"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_type_inference_with_string_literals() -> Result<()> {
        use crate::selection::SelectionExt;

        // This test demonstrates that queries work with different value types and string literals

        // Setup test data
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
                "user/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "user/role".parse::<Attribute>()?,
                alice.clone(),
                Value::String("admin".to_string()),
            ),
        ];

        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;

        // Pattern 1: Find Bob by name using string constant
        let bob_query = Fact::<Value>::select()
            .the("user/name")
            .of(Term::var("user")) // Variable - should bind
            .is(Value::String("Bob".to_string())); // String constant

        let bob_results = bob_query.query(&artifacts)?.collect_set().await?;
        assert_eq!(bob_results.len(), 1);
        assert!(bob_results.contains_binding("user", &Value::Entity(bob.clone())));

        // Pattern 2: Find admin using string constant
        let admin_query = Fact::<Value>::select()
            .the("user/role")
            .of(Term::var("admin_user")) // Variable - should bind to Alice
            .is(Value::String("admin".to_string())); // String constant

        let admin_results = admin_query.query(&artifacts)?.collect_set().await?;
        assert_eq!(admin_results.len(), 1);
        assert!(admin_results.contains_binding("admin_user", &Value::Entity(alice.clone())));

        // Pattern 3: Find all names using variable
        let names_query = Fact::<Value>::select()
            .the("user/name") // Constant attribute
            .of(Term::var("user")) // Variable entity
            .is(Term::var("name")); // Variable value

        let name_results = names_query.query(&artifacts)?.collect_set().await?;
        assert_eq!(name_results.len(), 2);
        assert!(name_results.contains_binding("name", &Value::String("Alice".to_string())));
        assert!(name_results.contains_binding("name", &Value::String("Bob".to_string())));

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_constants_and_variables_succeed() -> Result<()> {
        use crate::selection::SelectionExt;

        // Test that queries with mixed constants and variables work correctly
        // Constants are used for matching, variables get bound

        let alice = Entity::new()?;

        // Setup store with test data
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        let facts = vec![Fact::assert(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        )];

        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;

        let mixed_query = Fact::<Value>::select()
            .the("user/name") // Constant - used for matching
            .of(alice.clone()) // Constant - used for matching
            .is(Term::<Value>::var("name")); // Variable - should bind to "Alice"

        let results = mixed_query.query(&artifacts)?.collect_set().await?;

        assert_eq!(results.len(), 1, "Should find Alice's name fact");

        // Variable should be bound, constants should not create bindings
        assert!(results.contains_binding("name", &Value::String("Alice".to_string())));

        // Verify only the variable is bound
        for match_frame in results.iter() {
            assert_eq!(
                match_frame.variables.len(),
                1,
                "Only variable should be bound"
            );
            assert!(
                match_frame.variables.contains_key("name"),
                "Name variable should be bound"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_only_variables_query_fails() -> Result<()> {
        // Test that queries with ONLY variables and NO constants fail during planning

        let query_only_vars = Fact::<Value>::select()
            .the(Term::<Attribute>::var("attr")) // Variable
            .of(Term::<Entity>::var("entity")) // Variable
            .is(Term::<Value>::var("value")); // Variable

        // Setup store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let result = query_only_vars.query(&artifacts);

        // Should fail because there are no constants at all - this fails during planning
        assert!(result.is_err(), "Query with only variables should fail");

        if let Err(error) = result {
            // The error should mention that the selector needs constraints
            let error_msg = error.to_string();
            assert!(
                error_msg.contains("bound parameter"),
                "Error should mention constraint requirements: {}",
                error_msg
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_fluent_query_building_and_execution() -> Result<()> {
        use crate::selection::SelectionExt;

        // This test shows how the Query trait enables fluent query building and execution with Match API

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

        // Test 1: Find admin users using fluent query building
        let admin_results = Fact::<Value>::select()
            .the("user/role")
            .of(Term::var("admin_user")) // Variable - binds to admin users
            .is(Value::String("admin".to_string())) // Constant role
            .query(&artifacts)?
            .collect_set()
            .await?;

        assert_eq!(admin_results.len(), 1, "Should find one admin (Alice)");
        assert!(admin_results.contains_binding("admin_user", &Value::Entity(alice.clone())));

        // Test 2: Find all user names with set-based collection
        let name_results = Fact::<Value>::select()
            .the("user/name") // Constant attribute
            .of(Term::var("user")) // Variable entity
            .is(Term::var("name")) // Variable name
            .query(&artifacts)?
            .collect_set()
            .await?;

        assert_eq!(name_results.len(), 2, "Should find both Alice and Bob");

        // Extract names using values_for convenience method
        let user_names: Vec<&Value> = name_results.values_for("name");
        assert_eq!(user_names.len(), 2);

        // Test contains_value_for convenience method
        assert!(name_results.contains_value_for("name", &Value::String("Alice".to_string())));
        assert!(name_results.contains_value_for("name", &Value::String("Bob".to_string())));

        // Test that both users are bound
        assert!(name_results.contains_binding("user", &Value::Entity(alice.clone())));
        assert!(name_results.contains_binding("user", &Value::Entity(bob.clone())));

        Ok(())
    }
}
