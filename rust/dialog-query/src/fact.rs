//! Fact and Claim types for the dialog-query system

pub use crate::Term;
pub use crate::artifact::{Artifact, Attribute, Cause, Entity, Instruction, Value};
pub use crate::error::SchemaError;
pub use crate::query::Output;
pub use crate::types::Scalar;
use dialog_common::ConditionalSend;
use serde::{Deserialize, Serialize};

/// A stored EAV datum tagged with its provenance ([`Cause`]).
///
/// Facts are the atomic unit of the knowledge base. Each fact records an
/// `(attribute, entity, value)` triple together with a content-addressed
/// `cause` that identifies the write operation that produced it. Facts come
/// in two flavours:
/// - `Assertion` — states that the triple holds.
/// - `Retraction` — states that a previously asserted triple no longer holds.
///
/// The generic parameter `T` defaults to [`Value`] (dynamically typed) but
/// can be narrowed to a specific scalar type (e.g. `Fact<String>`) when the
/// attribute's type is known at compile time.
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

impl From<&Artifact> for Fact {
    fn from(artifact: &Artifact) -> Self {
        Fact::Assertion {
            the: artifact.the.clone(),
            of: artifact.of.clone(),
            is: artifact.is.clone(),
            cause: artifact.cause.clone().unwrap_or(Cause([0; 32])),
        }
    }
}

impl<T: Scalar + ConditionalSend> Fact<T> {
    /// Get the attribute of this fact
    pub fn the(&self) -> &Attribute {
        match self {
            Fact::Assertion { the, .. } => the,
            Fact::Retraction { the, .. } => the,
        }
    }
    /// Get the entity of this fact
    pub fn of(&self) -> &Entity {
        match self {
            Fact::Assertion { of, .. } => of,
            Fact::Retraction { of, .. } => of,
        }
    }
    /// Get the value of this fact
    pub fn is(&self) -> &T {
        match self {
            Fact::Assertion { is, .. } => is,
            Fact::Retraction { is, .. } => is,
        }
    }
    /// Get the cause (provenance hash) of this fact
    pub fn cause(&self) -> &Cause {
        match self {
            Fact::Assertion { cause, .. } => cause,
            Fact::Retraction { cause, .. } => cause,
        }
    }
}

#[cfg(test)]
mod integration_tests {
    //! Integration tests for the complete Fact workflow:
    //! assert/retract → commit → RelationApplication → query

    use super::*;
    use crate::artifact::{Artifacts, Attribute, Entity, Value};
    use crate::relation::application::RelationApplication;
    use crate::{Assertion, Session};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    #[dialog_common::test]
    async fn test_fact_assert_retract_and_query_with_variables() -> Result<()> {
        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Step 1: Create entities for testing
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Step 2: Create facts using our Fact DSL
        let alice_name = Assertion {
            the: "user/name".parse::<Attribute>()?,
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        };

        let alice_email = Assertion {
            the: "user/email".parse::<Attribute>()?,
            of: alice.clone(),
            is: Value::String("alice@example.com".to_string()),
        };

        let bob_name = Assertion {
            the: "user/name".parse::<Attribute>()?,
            of: bob.clone(),
            is: Value::String("Bob".to_string()),
        };

        // Step 3: Commit using session API
        let claims = vec![alice_name, alice_email, bob_name];
        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Step 4: Test 1 - Query for user names
        let query_names = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let facts = query_names
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(facts.len(), 2, "Should find both Alice and Bob");

        // Check that we got the right facts
        let has_alice = facts.iter().any(|f| {
            f.namespace == "user"
                && f.name == "name"
                && f.of == alice
                && f.is == Value::String("Alice".to_string())
        });
        assert!(has_alice, "Should find Alice's name fact");

        let has_bob = facts.iter().any(|f| {
            f.namespace == "user"
                && f.name == "name"
                && f.of == bob
                && f.is == Value::String("Bob".to_string())
        });
        assert!(has_bob, "Should find Bob's name fact");

        // Step 5: Test 2 - Query for email
        let query_email = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("email".into()),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let email_facts = query_email
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(email_facts.len(), 1, "Should find Alice's email");

        let has_email = email_facts.iter().any(|f| {
            f.namespace == "user"
                && f.name == "email"
                && f.of == alice
                && f.is == Value::String("alice@example.com".to_string())
        });
        assert!(has_email, "Should find Alice's email fact");

        // Step 6: Test 3 - Query for specific user
        let query_alice = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let alice_facts = query_alice
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(alice_facts.len(), 1, "Should find Alice's name");

        Ok(())
    }

    #[dialog_common::test]
    async fn test_retraction_workflow() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        // Step 1: Assert a fact
        let alice_name = Assertion {
            the: "user/name".parse::<Attribute>()?,
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        };

        let mut session = Session::open(artifacts.clone());
        session.transact(vec![alice_name.clone()]).await?;

        // Step 2: Verify fact exists
        let query_constant = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results = query_constant
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1);

        // Verify the fact content
        let has_alice = results.iter().any(|f| {
            f.namespace == "user"
                && f.name == "name"
                && f.of == alice
                && f.is == Value::String("Alice".to_string())
        });
        assert!(has_alice, "Should find Alice's name fact");

        let mut session = Session::open(artifacts.clone());
        session.transact([!alice_name]).await?;

        // Step 4: Verify fact is gone
        let query2 = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results2 = query2
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results2.len(), 0, "Fact should be retracted");

        Ok(())
    }

    #[dialog_common::test]
    async fn test_constants_vs_variables_binding() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Create facts
        let claims = vec![
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Assertion {
                the: "user/age".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::UnsignedInt(30),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Test 1: All constants
        let all_constants_query = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            alice.clone().into(),
            Term::Constant(Value::String("Alice".to_string())),
            Term::blank(),
            None,
        );

        let constant_results = all_constants_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(constant_results.len(), 1, "Should find Alice's name fact");

        // Test 2: Find Alice specifically
        let mixed_query = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            Term::blank(),
            Term::Constant(Value::String("Alice".to_string())),
            Term::blank(),
            None,
        );

        let mixed_results = mixed_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(mixed_results.len(), 1, "Should find Alice specifically");

        // Test 3: Find all names
        let find_all_names = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let all_name_results = find_all_names
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(all_name_results.len(), 2, "Should find both Alice and Bob");

        // Verify we have both users' facts
        let has_alice = all_name_results
            .iter()
            .any(|f| f.of == alice && f.is == Value::String("Alice".to_string()));
        let has_bob = all_name_results
            .iter()
            .any(|f| f.of == bob && f.is == Value::String("Bob".to_string()));
        assert!(
            has_alice && has_bob,
            "Should find both Alice and Bob's facts"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn test_complex_queries_with_constants() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Create test data: users with different roles
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let charlie = Entity::new()?;

        let claims = vec![
            // Users and roles
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Assertion {
                the: "user/role".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("admin".to_string()),
            },
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Assertion {
                the: "user/role".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::String("user".to_string()),
            },
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: charlie.clone(),
                is: Value::String("Charlie".to_string()),
            },
            Assertion {
                the: "user/role".parse::<Attribute>()?,
                of: charlie.clone(),
                is: Value::String("admin".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Query 1: Find all admins by role
        let admin_query = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("role".into()),
            Term::blank(),
            Term::Constant(Value::String("admin".to_string())),
            Term::blank(),
            None,
        );

        let admin_results = admin_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(
            admin_results.len(),
            2,
            "Should find Alice and Charlie as admins"
        );

        // Verify we have admin facts for alice and charlie
        let has_alice_admin = admin_results
            .iter()
            .any(|f| f.of == alice && f.is == Value::String("admin".to_string()));
        let has_charlie_admin = admin_results
            .iter()
            .any(|f| f.of == charlie && f.is == Value::String("admin".to_string()));
        assert!(has_alice_admin && has_charlie_admin);

        // Query 2: Find all user roles
        let role_query = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("role".into()),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let role_results = role_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(role_results.len(), 3, "Should find all 3 role assignments");

        // Query 3: Find Bob specifically using all constants
        let bob_query = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            bob.clone().into(),
            Term::Constant(Value::String("Bob".to_string())),
            Term::blank(),
            None,
        );

        let bob_results = bob_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(bob_results.len(), 1, "Should find exactly Bob's name fact");

        Ok(())
    }

    #[dialog_common::test]
    async fn test_variable_queries_succeed_with_constants() -> Result<()> {
        // Setup store with test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query_with_variables = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results = query_with_variables
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 2, "Should find both Alice and Bob");

        // Verify we have both facts
        let has_alice = results
            .iter()
            .any(|f| f.of == alice && f.is == Value::String("Alice".to_string()));
        let has_bob = results
            .iter()
            .any(|f| f.of == bob && f.is == Value::String("Bob".to_string()));
        assert!(has_alice && has_bob);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_typed_fact_selector_patterns() -> Result<()> {
        // Setup test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Assertion {
                the: "user/friend".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::Entity(bob.clone()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Pattern 1: Query for user names
        let value_selector = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let value_results = value_selector
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(value_results.len(), 1);

        let has_alice = value_results
            .iter()
            .any(|f| f.of == alice && f.is == Value::String("Alice".to_string()));
        assert!(has_alice);

        // Pattern 2: Query for entity values (friends)
        let entity_selector = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("friend".into()),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let entity_results = entity_selector
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(entity_results.len(), 1);

        let has_bob = entity_results
            .iter()
            .any(|f| f.is == Value::Entity(bob.clone()));
        assert!(has_bob);

        // Pattern 3: Test with all constants
        let constant_selector = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            alice.clone().into(),
            Term::Constant(Value::String("Alice".to_string())),
            Term::blank(),
            None,
        );

        let constant_results = constant_selector
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(constant_results.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_type_inference_with_string_literals() -> Result<()> {
        // Setup test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Assertion {
                the: "user/role".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("admin".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Pattern 1: Find Bob by name using string constant
        let bob_query = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            Term::blank(),
            Term::Constant(Value::String("Bob".to_string())),
            Term::blank(),
            None,
        );

        let bob_results = bob_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(bob_results.len(), 1);

        let has_bob = bob_results.iter().any(|f| f.of == bob);
        assert!(has_bob);

        // Pattern 2: Find admin using string constant
        let admin_query = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("role".into()),
            Term::blank(),
            Term::Constant(Value::String("admin".to_string())),
            Term::blank(),
            None,
        );

        let admin_results = admin_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(admin_results.len(), 1);

        let has_alice = admin_results.iter().any(|f| f.of == alice);
        assert!(has_alice);

        // Pattern 3: Find all names
        let names_query = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let name_results = names_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(name_results.len(), 2);

        let has_alice_name = name_results
            .iter()
            .any(|f| f.is == Value::String("Alice".to_string()));
        let has_bob_name = name_results
            .iter()
            .any(|f| f.is == Value::String("Bob".to_string()));
        assert!(has_alice_name && has_bob_name);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_mixed_constants_and_variables_succeed() -> Result<()> {
        let alice = Entity::new()?;

        // Setup store with test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let claims = vec![Assertion {
            the: "user/name".parse::<Attribute>()?,
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let mixed_query = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let results = mixed_query
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1, "Should find Alice's name fact");

        // Verify we got the right fact
        let has_alice = results.iter().any(|f| {
            f.namespace == "user"
                && f.name == "name"
                && f.of == alice
                && f.is == Value::String("Alice".to_string())
        });
        assert!(has_alice);

        Ok(())
    }

    #[dialog_common::test]
    async fn test_fluent_query_building_and_execution() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Assertion {
                the: "user/role".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("admin".to_string()),
            },
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Assertion {
                the: "user/role".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::String("user".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Test 1: Find admin users
        let admin_search = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("role".into()),
            Term::blank(),
            Term::Constant(Value::String("admin".to_string())),
            Term::blank(),
            None,
        );

        let admin_results = admin_search
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(admin_results.len(), 1, "Should find one admin (Alice)");

        let has_alice_admin = admin_results.iter().any(|f| f.of == alice);
        assert!(has_alice_admin);

        // Test 2: Find all user names
        let name_search = RelationApplication::new(
            Term::Constant("user".into()),
            Term::Constant("name".into()),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let name_results = name_search
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;

        assert_eq!(name_results.len(), 2, "Should find both Alice and Bob");

        // Verify we have both names
        let has_alice_name = name_results
            .iter()
            .any(|f| f.is == Value::String("Alice".to_string()));
        let has_bob_name = name_results
            .iter()
            .any(|f| f.is == Value::String("Bob".to_string()));
        assert!(has_alice_name && has_bob_name);

        // Verify we have both users
        let has_alice_entity = name_results.iter().any(|f| f.of == alice);
        let has_bob_entity = name_results.iter().any(|f| f.of == bob);
        assert!(has_alice_entity && has_bob_entity);

        Ok(())
    }

    #[dialog_common::test]
    async fn match_fact() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Assertion {
                the: "user/friend".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::Entity(bob.clone()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Pattern 1: Query for user names
        let value_selector = RelationApplication::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let value_results = value_selector.perform(&session).try_vec().await?;
        assert_eq!(value_results.len(), 1);

        let has_alice = value_results
            .iter()
            .any(|f| f.of == alice && f.is == Value::String("Alice".to_string()));
        assert!(has_alice);

        // Pattern 2: Query for entity values (friends)
        let entity_selector = RelationApplication::new(
            Term::Constant("user".to_string()),
            Term::Constant("friend".to_string()),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let entity_results = entity_selector.perform(&session).try_vec().await?;
        assert_eq!(entity_results.len(), 1);

        let has_bob = entity_results
            .iter()
            .any(|f| f.is == Value::Entity(bob.clone()));
        assert!(has_bob);

        // Pattern 3: Test with all constants
        let constant_selector = RelationApplication::new(
            Term::Constant("user".to_string()),
            Term::Constant("name".to_string()),
            alice.clone().into(),
            "Alice".into(),
            Term::blank(),
            None,
        );

        let constant_results = constant_selector
            .perform(&Session::open(artifacts.clone()))
            .try_vec()
            .await?;
        assert_eq!(constant_results.len(), 1);

        Ok(())
    }
}
