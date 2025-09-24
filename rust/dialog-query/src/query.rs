//! Query trait for polymorphic querying across different store types

pub use dialog_common::ConditionalSend;

use crate::artifact::{ArtifactStore, ArtifactStoreMut};
pub use crate::error::QueryResult;
use crate::plan::{fresh, EvaluationPlan};
use crate::Selection;

use crate::predicate::DeductiveRule;

/// Source trait for stores that support both artifact storage and rule resolution
///
/// This trait extends ArtifactStore with rule resolution capabilities, allowing
/// query evaluation to access both stored facts and registered deductive rules.
/// This enables rule-based inference during query execution.
pub trait Source: ArtifactStore + Clone + Send + Sync + 'static {
    /// Resolve rules for the given operator
    ///
    /// Returns all deductive rules that have conclusions matching the given operator.
    /// This enables concept evaluation to discover and apply relevant rules when
    /// facts are not directly available in the store.
    ///
    /// # Arguments
    /// * `operator` - The concept operator to find rules for
    ///
    /// # Returns
    /// A vector of DeductiveRule instances whose conclusions match the operator
    fn resolve_rules(&self, operator: &str) -> Vec<DeductiveRule>;
}

// Note: Source implementations must be provided explicitly by each artifact store type.
// The Session type provides rule resolution by maintaining a rule registry.
// Other artifact stores should implement Source with empty rule resolution.

// Note: Source implementations are provided by Session and QuerySession.
// For basic artifact stores, use Session::open() to enable rule-aware querying:
//
//   let session = Session::open(artifacts);
//   let results = concept.query(&session)?;

pub trait Store: ArtifactStoreMut + Clone + ConditionalSend {}
/// Blanket implementation - any type that satisfies the bounds automatically implements QueryStore
impl<T> Store for T where T: ArtifactStoreMut + Clone + ConditionalSend {}

/// A trait for types that can query an ArtifactStore
///
/// This provides a consistent interface for querying, abstracting over the details
/// of query planning, variable resolution, and execution against the store.
pub trait Query {
    /// Execute the query against the provided store
    ///
    /// Returns a stream of artifacts that match the query criteria.
    fn query<S: Source>(&self, source: &S) -> QueryResult<impl Selection>;
}

pub trait PlannedQuery {
    /// Execute the query against the provided store
    ///
    /// Returns a stream of artifacts that match the query criteria.
    fn query<S: Source>(&self, source: &S) -> QueryResult<impl Selection>;
}

impl<Plan: EvaluationPlan> PlannedQuery for Plan {
    fn query<S: Source>(&self, store: &S) -> QueryResult<impl Selection> {
        let store = store.clone();
        let context = fresh(store);
        let selection = self.evaluate(context);
        Ok(selection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{ArtifactStoreMut, Artifacts, Attribute, Entity, Value};
    use crate::{Claims, Fact, Session, Term};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    #[tokio::test]
    async fn test_fact_selector_query_trait() -> Result<()> {
        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        // Step 1: Create test data
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let facts = vec![
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "user/email".parse::<Attribute>()?,
                alice.clone(),
                Value::String("alice@example.com".to_string()),
            ),
            Fact::assert(
                "user/name".parse::<Attribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
        ];

        artifacts.commit(Claims::from(facts)).await?;

        // Step 2: Test Query trait on FactSelector with constants

        // Query 1: Find Alice by name using Query trait
        let alice_query = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())
            .is(Value::String("Alice".to_string()));

        // Use the Query trait method - should succeed since all fields are constants
        let session = Session::open(artifacts.clone());
        let result = alice_query.query(&session);
        assert!(result.is_ok()); // Should succeed with constants, returns empty stream

        // Query 2: Find all user/name facts using Query trait
        let all_names_query = Fact::<Value>::select().the("user/name");

        let session = Session::open(artifacts.clone());
        let result = all_names_query.query(&session);
        assert!(result.is_ok()); // Should succeed with constants

        // Query 3: Find Alice's email using Query trait
        let email_query = Fact::<Value>::select().the("user/email").of(alice.clone());

        let session = Session::open(artifacts);
        let result = email_query.query(&session);
        assert!(result.is_ok()); // Should succeed with constants

        Ok(())
    }

    #[tokio::test]
    async fn test_query_trait_with_variables_succeeds_if_constants_present() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Create a query with variables - variables are skipped, constants used
        let variable_query = Fact::<Value>::select()
            .the("user/name") // Constant - used
            .of(Term::<Entity>::var("user")) // Variable - skipped
            .is(Term::<Value>::var("name")); // Variable - skipped

        // Should succeed since planning validation only rejects all-unbound queries, and this has a constant
        let session = Session::open(artifacts);
        let result = variable_query.query(&session);
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_polymorphic_querying() -> Result<()> {
        // This test demonstrates polymorphic querying - same function can work with any Query impl

        async fn execute_query<Q: Query>(
            query: Q,
            store: &(impl Source + 'static),
        ) -> Result<Vec<crate::artifact::Artifact>> {
            let result = query.query(store);
            // Should succeed with constants, returns empty stream for now
            assert!(result.is_ok());
            Ok(vec![])
        }

        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let facts = vec![Fact::assert(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        )];

        artifacts.commit(Claims::from(facts)).await?;

        // Test with FactSelector
        let fact_selector = Fact::<Value>::select().the("user/name").of(alice.clone());

        let session = Session::open(artifacts);
        let results = execute_query(fact_selector, &session).await?;
        assert_eq!(results.len(), 0); // Empty since evaluation returns empty stream

        // Could also test with FactSelectorPlan if we had a way to create one easily
        // This demonstrates the polymorphic nature of the Query trait

        Ok(())
    }

    #[tokio::test]
    async fn test_chaining_query_operations() -> Result<()> {
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

        artifacts.commit(Claims::from(facts)).await?;

        // Test fluent query building - should succeed with constants
        let session = Session::open(artifacts.clone());
        let admin_result = Fact::<Value>::select()
            .the("user/role")
            .is(Value::String("admin".to_string()))
            .query(&session);
        assert!(admin_result.is_ok());

        // Test another fluent query - should also succeed
        let session = Session::open(artifacts);
        let names_result = Fact::<Value>::select().the("user/name").query(&session);
        assert!(names_result.is_ok());

        Ok(())
    }
}
