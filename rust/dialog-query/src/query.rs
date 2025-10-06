//! Query trait for polymorphic querying across different store types

use async_stream::try_stream;
pub use dialog_common::ConditionalSend;

use crate::artifact::{ArtifactStore, ArtifactStoreMut};
pub use crate::context::new_context;
pub use crate::error::{QueryError, QueryResult};
pub use crate::fact::Fact;
use crate::{selection, EvaluationContext, Selection};
pub use futures_util::stream::{Stream, StreamExt, TryStream};

use crate::predicate::DeductiveRule;

pub trait Output<T: ConditionalSend>:
    Stream<Item = Result<T, QueryError>> + 'static + ConditionalSend
{
    /// Collect all items into a Vec, propagating any errors
    #[allow(async_fn_in_trait)]
    fn try_vec(
        self,
    ) -> impl std::future::Future<Output = Result<Vec<T>, QueryError>> + ConditionalSend
    where
        Self: Sized,
    {
        async move { futures_util::TryStreamExt::try_collect(self).await }
    }
}

impl<S, T: ConditionalSend> Output<T> for S where
    S: Stream<Item = Result<T, QueryError>> + 'static + ConditionalSend
{
}

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

pub trait Circuit: ConditionalSend + 'static {
    fn evaluate<S: Source, M: Selection>(&self, context: EvaluationContext<S, M>)
        -> impl Selection;
}

pub trait Query<T: ConditionalSend + 'static>: Circuit + Clone + ConditionalSend {
    fn realize(&self, input: selection::Match) -> Result<T, QueryError>;
    fn execute<S: Source>(&self, source: &S) -> impl Output<T>
    where
        Self: Sized,
    {
        let context = new_context(source.clone());
        let selection = self.evaluate(context);
        let query = self.clone();
        try_stream! {
            for await each in selection {
                yield query.realize(each?)?;
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Artifacts, Attribute, Entity, Value};
    use crate::{Session, Term};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    #[tokio::test]
    async fn test_fact_selector_query_trait() -> Result<()> {
        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

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

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Step 2: Test Query trait on FactSelector with constants

        // Query 1: Find Alice by name using Query trait
        let alice_query = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())
            .is(Value::String("Alice".to_string()))
            .build()?;

        // Use the Query trait method - should succeed since all fields are constants
        let session = Session::open(artifacts.clone());
        let result = alice_query.query(&session).try_vec().await;
        assert!(result.is_ok()); // Should succeed with constants, returns empty stream

        // Query 2: Find all user/name facts using Query trait
        let all_names_query = Fact::<Value>::select().the("user/name").build()?;

        let session = Session::open(artifacts.clone());
        let result = all_names_query.query(&session).try_vec().await;
        assert!(result.is_ok()); // Should succeed with constants

        // Query 3: Find Alice's email using Query trait
        let email_query = Fact::<Value>::select()
            .the("user/email")
            .of(alice.clone())
            .build()?;

        let session = Session::open(artifacts);
        let result = email_query.query(&session).try_vec().await;
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
            .is(Term::<Value>::var("name")) // Variable - skippeda
            .build()?;

        // Should succeed since planning validation only rejects all-unbound queries, and this has a constant
        let session = Session::open(artifacts);
        let result = variable_query.query(&session).try_vec().await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_polymorphic_querying() -> Result<()> {
        // This test demonstrates polymorphic querying - same function can work with any Query impl

        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let facts = vec![Fact::assert(
            "user/name".parse::<Attribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        )];

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Test with FactSelector
        let fact_selector = Fact::<Value>::select()
            .the("user/name")
            .of(alice.clone())
            .build()?;

        let session = Session::open(artifacts);
        let results = fact_selector.query(&session).try_vec().await?;
        assert_eq!(results.len(), 1); // Should find the Alice fact

        // Could also test with FactSelectorPlan if we had a way to create one easily
        // This demonstrates the polymorphic nature of the Query trait

        Ok(())
    }

    #[tokio::test]
    async fn test_chaining_query_operations() -> Result<()> {
        // This test shows how the Query trait enables fluent query building and execution

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

        // Test fluent query building - should succeed with constants
        let session = Session::open(artifacts.clone());
        let admin_result = Fact::<Value>::select()
            .the("user/role")
            .is(Value::String("admin".to_string()))
            .build()?
            .query(&session)
            .try_vec()
            .await;
        assert!(admin_result.is_ok());

        // Test another fluent query - should also succeed
        let session = Session::open(artifacts);
        let names_result = Fact::<Value>::select()
            .the("user/name")
            .build()?
            .query(&session)
            .try_vec()
            .await;
        assert!(names_result.is_ok());

        Ok(())
    }
}
