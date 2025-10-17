//! Fact, Assertion, Retraction, and Claim types for the dialog-query system

use std::hash::Hash;

pub use super::claim::{fact, Claim};
pub use super::predicate::fact::Fact as PredicateFact;
pub use crate::application::FactApplication;
pub use crate::artifact::{Artifact, Attribute, Cause, Entity, Instruction, Value};
use crate::claim::fact::Relation;
pub use crate::dsl::Quarriable;
pub use crate::error::SchemaError;
pub use crate::query::Output;
pub use crate::types::Scalar;
pub use crate::Term;
use dialog_artifacts::{Blake3Hash, CborEncoder, DialogArtifactsError, Encoder};
use dialog_common::{ConditionalSend, ConditionalSync};
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

/// A fact represents persisted data with a cause - can be an assertion or retraction
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum Fact<T: Scalar + ConditionalSend = Value> {
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

impl Quarriable for Fact {
    type Query = PredicateFact;
}

impl<T: Scalar + ConditionalSend> Fact<T> {
    /// Create an assertion claim from individual components
    pub fn assert<The: Into<Attribute>, Of: Into<Entity>>(the: The, of: Of, is: T) -> Claim {
        let relation = Relation::new(the.into(), of.into(), is.as_value());
        Claim::Fact(fact::Claim::Assert(relation))
    }

    /// Create a retraction claim from individual components
    pub fn retract<The: Into<Attribute>, Of: Into<Entity>>(the: The, of: Of, is: T) -> Claim {
        let relation = Relation::new(the.into(), of.into(), is.as_value());
        Claim::Fact(fact::Claim::Retract(relation))
    }

    pub fn select() -> PredicateFact {
        PredicateFact::new()
    }

    pub fn the(&self) -> &Attribute {
        match self {
            Fact::Assertion { the, .. } => the,
            Fact::Retraction { the, .. } => the,
        }
    }
    pub fn of(&self) -> &Entity {
        match self {
            Fact::Assertion { of, .. } => of,
            Fact::Retraction { of, .. } => of,
        }
    }
    pub fn is(&self) -> &T {
        match self {
            Fact::Assertion { is, .. } => is,
            Fact::Retraction { is, .. } => is,
        }
    }
    pub fn cause(&self) -> &Cause {
        match self {
            Fact::Assertion { cause, .. } => cause,
            Fact::Retraction { cause, .. } => cause,
        }
    }
}

impl<T: Scalar + ConditionalSend + ConditionalSync + Serialize> Fact<T> {
    pub async fn as_bytes(&self) -> Result<Vec<u8>, DialogArtifactsError> {
        let (_, bytes) = CborEncoder.encode(self).await?;
        Ok(bytes)
    }

    pub async fn hash(&self) -> Result<Blake3Hash, DialogArtifactsError> {
        let (hash, _) = CborEncoder.encode(self).await?;
        Ok(hash)
    }
}

/// Create a generic assertion claim from individual components
pub fn assert<The: Into<Attribute>, Of: Into<Entity>, Is: Scalar>(
    the: The,
    of: Of,
    is: Is,
) -> Claim {
    let relation = Relation::new(the.into(), of.into(), is.as_value());
    Claim::Fact(fact::Claim::Assert(relation))
}

/// Create a generic retraction claim from individual components
pub fn retract<The: Into<Attribute>, Of: Into<Entity>, Is: Scalar>(
    the: The,
    of: Of,
    is: Is,
) -> Claim {
    let relation = Relation::new(the.into(), of.into(), is.as_value());
    Claim::Fact(fact::Claim::Retract(relation))
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

/// Convert Claim to Instruction for committing (legacy API)
///
/// **Deprecated**: Use the `Edit` trait with `claim.merge(&mut transaction)` instead.
impl From<fact::Claim> for Instruction {
    fn from(claim: fact::Claim) -> Self {
        match claim {
            fact::Claim::Assert(relation) => {
                let artifact = Artifact {
                    the: relation.the,
                    of: relation.of,
                    is: relation.is.into(),
                    cause: None,
                };
                Instruction::Assert(artifact)
            }
            fact::Claim::Retract(relation) => {
                let artifact = Artifact {
                    the: relation.the,
                    of: relation.of,
                    is: relation.is.into(),
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
    use dialog_query::Match;

    #[test]
    fn test_fact_assert() {
        let entity = Entity::new().unwrap();
        let claim = Fact::assert(
            "user/name".parse::<Attribute>().unwrap(),
            entity.clone(),
            Value::String("Alice".to_string()),
        );

        match claim {
            Claim::Fact(fact::Claim::Assert(relation)) => {
                assert_eq!(relation.the.to_string(), "user/name");
                assert_eq!(relation.of, entity);
                assert_eq!(relation.is, Value::String("Alice".to_string()));
            }
            _ => panic!("Expected Claim::Fact(Assertion)"),
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
            Claim::Fact(fact::Claim::Retract(relation)) => {
                assert_eq!(relation.the.to_string(), "user/name");
                assert_eq!(relation.of, entity);
                assert_eq!(relation.is, Value::String("Alice".to_string()));
            }
            _ => panic!("Expected Claim::Fact(Retraction)"),
        }
    }

    #[test]
    fn test_generic_static_functions() {
        let entity = Entity::new().unwrap();

        // Test generic static assert function with String type
        let string_claim = assert(
            "user/name".parse::<Attribute>().unwrap(),
            entity.clone(),
            "Alice".to_string(),
        );

        match string_claim {
            Claim::Fact(fact::Claim::Assert(relation)) => {
                assert_eq!(relation.the.to_string(), "user/name");
                assert_eq!(relation.of, entity);
                assert_eq!(relation.is, Value::String("Alice".to_string()));
            }
            _ => panic!("Expected Claim::Fact(Assertion)"),
        }

        // Test generic static retract function with u32 type
        let number_claim = retract(
            "user/age".parse::<Attribute>().unwrap(),
            entity.clone(),
            25u32,
        );

        match number_claim {
            Claim::Fact(fact::Claim::Retract(relation)) => {
                assert_eq!(relation.the.to_string(), "user/age");
                assert_eq!(relation.of, entity);
                assert_eq!(relation.is, Value::UnsignedInt(25u128));
            }
            _ => panic!("Expected Claim::Fact(Retraction)"),
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
            Claim::Fact(fact::Claim::Assert(relation)) => {
                assert_eq!(relation.the.to_string(), "user/name");
                assert_eq!(relation.of, entity);
                assert_eq!(relation.is, Value::String("Alice".to_string()));
            }
            _ => panic!("Expected Claim::Fact(Assertion)"),
        }

        // Test with String type directly
        let string_claim = Fact::<String>::assert(
            "user/email".parse::<Attribute>().unwrap(),
            entity.clone(),
            "alice@example.com".to_string(),
        );

        match string_claim {
            Claim::Fact(fact::Claim::Assert(relation)) => {
                assert_eq!(relation.the.to_string(), "user/email");
                assert_eq!(relation.of, entity);
                assert_eq!(relation.is, Value::String("alice@example.com".to_string()));
            }
            _ => panic!("Expected Claim::Fact(Assertion)"),
        }
    }
}

#[cfg(test)]
mod integration_tests {
    //! Integration tests for the complete Fact workflow:
    //! Fact::assert/retract → commit → Fact::select → query

    use super::*;
    use crate::artifact::{Artifacts, Attribute, Entity, Value};
    use crate::Session;
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    #[tokio::test]
    async fn test_fact_assert_retract_and_query_with_variables() -> Result<()> {
        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

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

        // Step 3: Commit using session API
        let claims = vec![alice_name, alice_email, bob_name];
        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Step 4: Test 1 - Query for user names
        let query_names = Fact::<Value>::select().the("user/name").compile()?;

        let facts = query_names
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(facts.len(), 2, "Should find both Alice and Bob");

        // Check that we got the right facts
        let has_alice = facts.iter().any(|f| match f {
            Fact::Assertion { the, of, is, .. } => {
                the.to_string() == "user/name"
                    && *of == alice
                    && *is == Value::String("Alice".to_string())
            }
            _ => false,
        });
        assert!(has_alice, "Should find Alice's name fact");

        let has_bob = facts.iter().any(|f| match f {
            Fact::Assertion { the, of, is, .. } => {
                the.to_string() == "user/name"
                    && *of == bob
                    && *is == Value::String("Bob".to_string())
            }
            _ => false,
        });
        assert!(has_bob, "Should find Bob's name fact");

        // Step 5: Test 2 - Query for email
        let query_email = Fact::<Value>::select().the("user/email").compile()?;

        let email_facts = query_email
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(email_facts.len(), 1, "Should find Alice's email");

        let has_email = email_facts.iter().any(|f| match f {
            Fact::Assertion { the, of, is, .. } => {
                the.to_string() == "user/email"
                    && *of == alice
                    && *is == Value::String("alice@example.com".to_string())
            }
            _ => false,
        });
        assert!(has_email, "Should find Alice's email fact");

        // Step 6: Test 3 - Query for specific user
        let query_alice = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())
            .compile()?;

        let alice_facts = query_alice
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(alice_facts.len(), 1, "Should find Alice's name");

        Ok(())
    }

    #[tokio::test]
    async fn test_retraction_workflow() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        // Step 1: Assert a fact
        let alice_name = Fact::assert(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        );

        let mut session = Session::open(artifacts.clone());
        session.transact(vec![alice_name]).await?;

        // Step 2: Verify fact exists
        let query_constant = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())
            .compile()?;

        let results = query_constant
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1);

        // Verify the fact content
        let has_alice = results.iter().any(|f| match f {
            Fact::Assertion { the, of, is, .. } => {
                the.to_string() == "user/name"
                    && *of == alice
                    && *is == Value::String("Alice".to_string())
            }
            _ => false,
        });
        assert!(has_alice, "Should find Alice's name fact");

        // Step 3: Retract the fact
        let retraction = Fact::retract(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        );

        let mut session = Session::open(artifacts.clone());
        session.transact(vec![retraction]).await?;

        // Step 4: Verify fact is gone
        let query2 = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())
            .compile()?;

        let results2 = query2
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results2.len(), 0, "Fact should be retracted");

        Ok(())
    }

    #[tokio::test]
    async fn test_constants_vs_variables_binding() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

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

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Test 1: All constants
        let all_constants_query = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())
            .is(Value::String("Alice".to_string()))
            .compile()?;

        let constant_results = all_constants_query
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(constant_results.len(), 1, "Should find Alice's name fact");

        // Test 2: Find Alice specifically
        let mixed_query = Fact::<Value>::select()
            .the("user/name")
            .is(Value::String("Alice".to_string()))
            .compile()?;

        let mixed_results = mixed_query
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(mixed_results.len(), 1, "Should find Alice specifically");

        // Test 3: Find all names
        let find_all_names = Fact::<Value>::select().the("user/name").compile()?;

        let all_name_results = find_all_names
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(all_name_results.len(), 2, "Should find both Alice and Bob");

        // Verify we have both users' facts
        let has_alice = all_name_results.iter().any(|f| match f {
            Fact::Assertion { of, is, .. } => {
                *of == alice && *is == Value::String("Alice".to_string())
            }
            _ => false,
        });
        let has_bob = all_name_results.iter().any(|f| match f {
            Fact::Assertion { of, is, .. } => *of == bob && *is == Value::String("Bob".to_string()),
            _ => false,
        });
        assert!(
            has_alice && has_bob,
            "Should find both Alice and Bob's facts"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_complex_queries_with_constants() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

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

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Query 1: Find all admins by role
        let admin_query = Fact::<Value>::select()
            .the("user/role")
            .is(Value::String("admin".to_string()))
            .compile()?;

        let admin_results = admin_query
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(
            admin_results.len(),
            2,
            "Should find Alice and Charlie as admins"
        );

        // Verify we have admin facts for alice and charlie
        let has_alice_admin = admin_results.iter().any(|f| match f {
            Fact::Assertion { of, is, .. } => {
                *of == alice && *is == Value::String("admin".to_string())
            }
            _ => false,
        });
        let has_charlie_admin = admin_results.iter().any(|f| match f {
            Fact::Assertion { of, is, .. } => {
                *of == charlie && *is == Value::String("admin".to_string())
            }
            _ => false,
        });
        assert!(has_alice_admin && has_charlie_admin);

        // Query 2: Find all user roles
        let role_query = Fact::<Value>::select().the("user/role").compile()?;

        let role_results = role_query
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(role_results.len(), 3, "Should find all 3 role assignments");

        // Query 3: Find Bob specifically using all constants
        let bob_query = Fact::<Value>::select()
            .the("user/name")
            .of(bob.clone())
            .is(Value::String("Bob".to_string()))
            .compile()?;

        let bob_results = bob_query
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(bob_results.len(), 1, "Should find exactly Bob's name fact");

        Ok(())
    }

    #[tokio::test]
    async fn test_variable_queries_succeed_with_constants() -> Result<()> {
        // Setup store with test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

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

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        let query_with_variables = Fact::<Value>::select().the("user/name").compile()?;

        let results = query_with_variables
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 2, "Should find both Alice and Bob");

        // Verify we have both facts
        let has_alice = results.iter().any(|f| match f {
            Fact::Assertion { of, is, .. } => {
                *of == alice && *is == Value::String("Alice".to_string())
            }
            _ => false,
        });
        let has_bob = results.iter().any(|f| match f {
            Fact::Assertion { of, is, .. } => *of == bob && *is == Value::String("Bob".to_string()),
            _ => false,
        });
        assert!(has_alice && has_bob);

        Ok(())
    }

    #[tokio::test]
    async fn test_typed_fact_selector_patterns() -> Result<()> {
        // Setup test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

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

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Pattern 1: Query for user names
        let value_selector = Fact::<Value>::select().the("user/name").compile()?;

        let value_results = value_selector
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(value_results.len(), 1);

        let has_alice = value_results.iter().any(|f| match f {
            Fact::Assertion { of, is, .. } => {
                *of == alice && *is == Value::String("Alice".to_string())
            }
            _ => false,
        });
        assert!(has_alice);

        // Pattern 2: Query for entity values (friends)
        let entity_selector = Fact::<Value>::select()
            .the("user/friend")
            .of(alice.clone())
            .compile()?;

        let entity_results = entity_selector
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(entity_results.len(), 1);

        let has_bob = entity_results.iter().any(|f| match f {
            Fact::Assertion { is, .. } => *is == Value::Entity(bob.clone()),
            _ => false,
        });
        assert!(has_bob);

        // Pattern 3: Test with all constants
        let constant_selector = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())
            .is(Value::String("Alice".to_string()))
            .compile()?;

        let constant_results = constant_selector
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(constant_results.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_type_inference_with_string_literals() -> Result<()> {
        // Setup test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

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

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Pattern 1: Find Bob by name using string constant
        let bob_query = Fact::<Value>::select()
            .the("user/name")
            .is(Value::String("Bob".to_string()))
            .compile()?;

        let bob_results = bob_query
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(bob_results.len(), 1);

        let has_bob = bob_results.iter().any(|f| match f {
            Fact::Assertion { of, .. } => *of == bob,
            _ => false,
        });
        assert!(has_bob);

        // Pattern 2: Find admin using string constant
        let admin_query = Fact::<Value>::select()
            .the("user/role")
            .is(Value::String("admin".to_string()))
            .compile()?;

        let admin_results = admin_query
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(admin_results.len(), 1);

        let has_alice = admin_results.iter().any(|f| match f {
            Fact::Assertion { of, .. } => *of == alice,
            _ => false,
        });
        assert!(has_alice);

        // Pattern 3: Find all names
        let names_query = Fact::<Value>::select().the("user/name").compile()?;

        let name_results = names_query
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(name_results.len(), 2);

        let has_alice_name = name_results.iter().any(|f| match f {
            Fact::Assertion { is, .. } => *is == Value::String("Alice".to_string()),
            _ => false,
        });
        let has_bob_name = name_results.iter().any(|f| match f {
            Fact::Assertion { is, .. } => *is == Value::String("Bob".to_string()),
            _ => false,
        });
        assert!(has_alice_name && has_bob_name);

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_constants_and_variables_succeed() -> Result<()> {
        let alice = Entity::new()?;

        // Setup store with test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let facts = vec![Fact::assert(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        )];

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        let mixed_query = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())
            .compile()?;

        let results = mixed_query
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1, "Should find Alice's name fact");

        // Verify we got the right fact
        let has_alice = results.iter().any(|f| match f {
            Fact::Assertion { the, of, is, .. } => {
                the.to_string() == "user/name"
                    && *of == alice
                    && *is == Value::String("Alice".to_string())
            }
            _ => false,
        });
        assert!(has_alice);

        Ok(())
    }

    #[tokio::test]
    async fn test_only_variables_query_fails() -> Result<()> {
        // Test that queries with ONLY variables and NO constants fail during building
        // This test verifies error handling for completely unconstrained queries

        // Try to build a query with no constants - this should fail at build time
        // since the ArtifactSelector conversion requires at least one constrained field
        let result = Fact::<Value>::select().compile();

        // Should fail because there are no constraints at all
        assert!(result.is_err(), "Query with no constraints should fail");

        if let Err(error) = result {
            // The error should mention that at least one field must be constrained
            let error_msg = error.to_string();
            assert!(
                error_msg.contains("At least one field must be constrained")
                    || error_msg.contains("Unconstrained"),
                "Error should mention constraint requirements: {}",
                error_msg
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_fluent_query_building_and_execution() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

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

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Test 1: Find admin users using fluent query building
        let admin_search = Fact::<Value>::select()
            .the("user/role")
            .is(Value::String("admin".to_string()))
            .compile()?;

        let admin_results = admin_search
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(admin_results.len(), 1, "Should find one admin (Alice)");

        let has_alice_admin = admin_results.iter().any(|f| match f {
            Fact::Assertion { of, .. } => *of == alice,
            _ => false,
        });
        assert!(has_alice_admin);

        // Test 2: Find all user names
        let name_search = Fact::<Value>::select().the("user/name").compile()?;

        let name_results = name_search
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(name_results.len(), 2, "Should find both Alice and Bob");

        // Verify we have both names
        let has_alice_name = name_results.iter().any(|f| match f {
            Fact::Assertion { is, .. } => *is == Value::String("Alice".to_string()),
            _ => false,
        });
        let has_bob_name = name_results.iter().any(|f| match f {
            Fact::Assertion { is, .. } => *is == Value::String("Bob".to_string()),
            _ => false,
        });
        assert!(has_alice_name && has_bob_name);

        // Verify we have both users
        let has_alice_entity = name_results.iter().any(|f| match f {
            Fact::Assertion { of, .. } => *of == alice,
            _ => false,
        });
        let has_bob_entity = name_results.iter().any(|f| match f {
            Fact::Assertion { of, .. } => *of == bob,
            _ => false,
        });
        assert!(has_alice_entity && has_bob_entity);

        Ok(())
    }

    #[tokio::test]
    async fn match_fact() -> Result<()> {
        use dialog_query::Match;
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

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

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Pattern 1: Query for user names
        let value_selector = Match::<Fact> {
            the: "user/name".try_into()?,
            of: Term::blank(),
            is: Term::blank(),
            cause: Term::blank(),
        }
        .compile()?;

        let value_results = value_selector.query(&session).try_vec().await?;
        assert_eq!(value_results.len(), 1);

        let has_alice = value_results.iter().any(|f| match f {
            Fact::Assertion { of, is, .. } => {
                *of == alice && *is == Value::String("Alice".to_string())
            }
            _ => false,
        });
        assert!(has_alice);

        // Pattern 2: Query for entity values (friends)
        let entity_selector = Match::<Fact> {
            the: "user/friend".try_into()?,
            of: alice.clone().into(),
            is: Term::blank(),
            cause: Term::blank(),
        }
        .compile()?;

        let entity_results = entity_selector.query(&session).try_vec().await?;
        assert_eq!(entity_results.len(), 1);

        let has_bob = entity_results.iter().any(|f| match f {
            Fact::Assertion { is, .. } => *is == Value::Entity(bob.clone()),
            _ => false,
        });
        assert!(has_bob);

        // Pattern 3: Test with all constants
        let constant_selector = Match::<Fact> {
            the: "user/name".try_into()?,
            of: alice.clone().into(),
            is: "Alice".into(),
            cause: Term::blank(),
        }
        .compile()?;

        let constant_results = constant_selector
            .query(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(constant_results.len(), 1);

        Ok(())
    }
}
