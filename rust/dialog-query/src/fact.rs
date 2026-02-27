//! Claim types for the dialog-query system

pub use crate::Term;
pub use crate::artifact::{Artifact, Attribute, Cause, Entity, Instruction, Value};
pub use crate::error::SchemaError;
pub use crate::query::Output;
pub use crate::types::Scalar;
use dialog_common::ConditionalSend;
use serde::{Deserialize, Serialize};

/// A stored EAV datum tagged with its provenance ([`Cause`]).
///
/// Claims are the atomic unit of the knowledge base. Each claim records an
/// `(attribute, entity, value)` triple together with a content-addressed
/// `cause` that identifies the write operation that produced it. Claims come
/// in two flavours:
/// - `Assertion` — states that the triple holds.
/// - `Retraction` — states that a previously asserted triple no longer holds.
///
/// The generic parameter `T` defaults to [`Value`] (dynamically typed) but
/// can be narrowed to a specific scalar type (e.g. `Claim<String>`) when the
/// attribute's type is known at compile time.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Serialize, Deserialize)]
pub enum Claim<T: Scalar + ConditionalSend = Value> {
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

impl From<&Artifact> for Claim {
    fn from(artifact: &Artifact) -> Self {
        Claim::Assertion {
            the: artifact.the.clone(),
            of: artifact.of.clone(),
            is: artifact.is.clone(),
            cause: artifact.cause.clone().unwrap_or(Cause([0; 32])),
        }
    }
}

impl<T: Scalar + ConditionalSend> Claim<T> {
    /// Get the attribute of this claim
    pub fn the(&self) -> &Attribute {
        match self {
            Claim::Assertion { the, .. } => the,
            Claim::Retraction { the, .. } => the,
        }
    }
    /// Get the entity of this claim
    pub fn of(&self) -> &Entity {
        match self {
            Claim::Assertion { of, .. } => of,
            Claim::Retraction { of, .. } => of,
        }
    }
    /// Get the value of this claim
    pub fn is(&self) -> &T {
        match self {
            Claim::Assertion { is, .. } => is,
            Claim::Retraction { is, .. } => is,
        }
    }
    /// Get the cause (provenance hash) of this claim
    pub fn cause(&self) -> &Cause {
        match self {
            Claim::Assertion { cause, .. } => cause,
            Claim::Retraction { cause, .. } => cause,
        }
    }
}

#[cfg(test)]
mod integration_tests {
    //! Integration tests for the complete Claim workflow:
    //! assert/retract → commit → RelationQuery → query

    use super::*;
    use crate::artifact::{Artifacts, Entity, Value};
    use crate::relation::query::RelationQuery;
    use crate::the;
    use crate::{Association, Session};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    #[dialog_common::test]
    async fn it_asserts_retracts_and_queries() -> Result<()> {
        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Step 1: Create entities for testing
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Step 2: Create facts using our Fact DSL
        let alice_name = Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        };

        let alice_email = Association {
            the: the!("user/email"),
            of: alice.clone(),
            is: Value::String("alice@example.com".to_string()),
        };

        let bob_name = Association {
            the: the!("user/name"),
            of: bob.clone(),
            is: Value::String("Bob".to_string()),
        };

        // Step 3: Commit using session API
        let claims = vec![alice_name, alice_email, bob_name];
        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Step 4: Test 1 - Query for user names
        let query_names = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
            f.domain() == "user"
                && f.name() == "name"
                && f.of == alice
                && f.is == Value::String("Alice".to_string())
        });
        assert!(has_alice, "Should find Alice's name fact");

        let has_bob = facts.iter().any(|f| {
            f.domain() == "user"
                && f.name() == "name"
                && f.of == bob
                && f.is == Value::String("Bob".to_string())
        });
        assert!(has_bob, "Should find Bob's name fact");

        // Step 5: Test 2 - Query for email
        let query_email = RelationQuery::new(
            Term::Constant(the!("user/email")),
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
            f.domain() == "user"
                && f.name() == "email"
                && f.of == alice
                && f.is == Value::String("alice@example.com".to_string())
        });
        assert!(has_email, "Should find Alice's email fact");

        // Step 6: Test 3 - Query for specific user
        let query_alice = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
    async fn it_retracts_facts() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        // Step 1: Assert a fact
        let alice_name = Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        };

        let mut session = Session::open(artifacts.clone());
        session.transact(vec![alice_name.clone()]).await?;

        // Step 2: Verify fact exists
        let query_constant = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
            f.domain() == "user"
                && f.name() == "name"
                && f.of == alice
                && f.is == Value::String("Alice".to_string())
        });
        assert!(has_alice, "Should find Alice's name fact");

        let mut session = Session::open(artifacts.clone());
        session.transact([!alice_name]).await?;

        // Step 4: Verify fact is gone
        let query2 = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
    async fn it_binds_constants_and_variables() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        // Create facts
        let claims = vec![
            Association {
                the: the!("user/name"),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Association {
                the: the!("user/name"),
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Association {
                the: the!("user/age"),
                of: alice.clone(),
                is: Value::UnsignedInt(30),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Test 1: All constants
        let all_constants_query = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
        let mixed_query = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
        let find_all_names = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
    async fn it_queries_with_constant_constraints() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Create test data: users with different roles
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let charlie = Entity::new()?;

        let claims = vec![
            // Users and roles
            Association {
                the: the!("user/name"),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Association {
                the: the!("user/role"),
                of: alice.clone(),
                is: Value::String("admin".to_string()),
            },
            Association {
                the: the!("user/name"),
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Association {
                the: the!("user/role"),
                of: bob.clone(),
                is: Value::String("user".to_string()),
            },
            Association {
                the: the!("user/name"),
                of: charlie.clone(),
                is: Value::String("Charlie".to_string()),
            },
            Association {
                the: the!("user/role"),
                of: charlie.clone(),
                is: Value::String("admin".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Query 1: Find all admins by role
        let admin_query = RelationQuery::new(
            Term::Constant(the!("user/role")),
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
        let role_query = RelationQuery::new(
            Term::Constant(the!("user/role")),
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
        let bob_query = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
    async fn it_succeeds_variable_queries_with_constants() -> Result<()> {
        // Setup store with test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Association {
                the: the!("user/name"),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Association {
                the: the!("user/name"),
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let query_with_variables = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
    async fn it_uses_typed_fact_selectors() -> Result<()> {
        // Setup test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Association {
                the: the!("user/name"),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Association {
                the: the!("user/friend"),
                of: alice.clone(),
                is: Value::Entity(bob.clone()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Pattern 1: Query for user names
        let value_selector = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
        let entity_selector = RelationQuery::new(
            Term::Constant(the!("user/friend")),
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
        let constant_selector = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
    async fn it_infers_types_from_string_literals() -> Result<()> {
        // Setup test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Association {
                the: the!("user/name"),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Association {
                the: the!("user/name"),
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Association {
                the: the!("user/role"),
                of: alice.clone(),
                is: Value::String("admin".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Pattern 1: Find Bob by name using string constant
        let bob_query = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
        let admin_query = RelationQuery::new(
            Term::Constant(the!("user/role")),
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
        let names_query = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
    async fn it_mixes_constants_and_variables() -> Result<()> {
        let alice = Entity::new()?;

        // Setup store with test data
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let claims = vec![Association {
            the: the!("user/name"),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let mixed_query = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
            f.domain() == "user"
                && f.name() == "name"
                && f.of == alice
                && f.is == Value::String("Alice".to_string())
        });
        assert!(has_alice);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_builds_and_executes_fluent_query() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Association {
                the: the!("user/name"),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Association {
                the: the!("user/role"),
                of: alice.clone(),
                is: Value::String("admin".to_string()),
            },
            Association {
                the: the!("user/name"),
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Association {
                the: the!("user/role"),
                of: bob.clone(),
                is: Value::String("user".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Test 1: Find admin users
        let admin_search = RelationQuery::new(
            Term::Constant(the!("user/role")),
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
        let name_search = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
            Association {
                the: the!("user/name"),
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Association {
                the: the!("user/friend"),
                of: alice.clone(),
                is: Value::Entity(bob.clone()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Pattern 1: Query for user names
        let value_selector = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
        let entity_selector = RelationQuery::new(
            Term::Constant(the!("user/friend")),
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
        let constant_selector = RelationQuery::new(
            Term::Constant(the!("user/name")),
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
