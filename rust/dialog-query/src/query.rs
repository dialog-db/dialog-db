//! Query trait for polymorphic querying across different store types

use async_stream::try_stream;
pub use dialog_common::{ConditionalSend, ConditionalSync};

use crate::artifact::{ArtifactStore, ArtifactStoreMut};
pub use crate::context::new_context;
pub use crate::error::{QueryError, QueryResult};
pub use crate::relation::Relation;
use crate::{EvaluationContext, selection};
pub use futures_util::stream::{Stream, StreamExt, TryStream};

/// A stream of query results that can be collected or iterated
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
pub trait Source: ArtifactStore + Clone + ConditionalSend + ConditionalSync + 'static {
    /// Acquire rules for the given concept predicate.
    ///
    /// Returns a `ConceptRules` that owns the default rule, any installed rules,
    /// and a per-adornment plan cache. Always returns a value. If no rules were
    /// explicitly registered, an implicit rule (derived from the predicate's
    /// attributes) is used.
    fn acquire(
        &self,
        predicate: &crate::predicate::ConceptPredicate,
    ) -> Result<crate::proposition::concept::ConceptRules, QueryError>;
}

/// A query type that can be evaluated against a source to produce concrete results.
///
/// This is the unified query trait that all query types implement. It replaces
/// the previous `Query<T>` and `ConceptQuery` traits with a single interface.
pub trait Application: Clone + ConditionalSend + 'static {
    /// The concrete result type produced by this query.
    type Proof: ConditionalSend + 'static;

    /// Evaluate this query, producing a stream of answers.
    fn evaluate<S: Source, M: selection::Answers>(
        self,
        context: EvaluationContext<S, M>,
    ) -> impl selection::Answers;

    /// Convert an answer into a concrete result value.
    fn realize(&self, input: selection::Answer) -> Result<Self::Proof, QueryError>;

    /// Execute this query against a source, returning a stream of typed results.
    fn perform<S: Source>(self, source: &S) -> impl Output<Self::Proof>
    where
        Self: Sized,
    {
        let context = new_context(source.clone());
        let query = self.clone();
        let answers = self.evaluate(context);
        try_stream! {
            for await each in answers {
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
//   let results = concept.perform(&session)?;

/// A mutable store that can be used for writes
pub trait Store: ArtifactStoreMut + Clone + ConditionalSend {}
/// Blanket implementation - any type that satisfies the bounds automatically implements QueryStore
impl<T> Store for T where T: ArtifactStoreMut + Clone + ConditionalSend {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Artifacts, Attribute, Entity, Value};
    use crate::proposition::relation::RelationApplication;
    use crate::{Assertion, Session, Term};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    #[dialog_common::test]
    async fn test_fact_selector_query_trait() -> Result<()> {
        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Step 1: Create test data
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let claims = vec![
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Assertion {
                the: "user/email".parse::<Attribute>()?,
                of: alice.clone(),
                is: Value::String("alice@example.com".to_string()),
            },
            Assertion {
                the: "user/name".parse::<Attribute>()?,
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Query 1: Find Alice by name using Query trait
        let alice_query = RelationApplication::new(
            Term::from("user"),
            Term::from("name"),
            alice.clone().into(),
            Term::from(Value::String("Alice".to_string())),
            Term::blank(),
            None,
        );

        // Use the Query trait method - should succeed since all fields are constants
        let session = Session::open(artifacts.clone());
        let result = alice_query.perform(&session).try_vec().await;
        assert!(result.is_ok()); // Should succeed with constants, returns empty stream

        // Query 2: Find all user/name facts using Query trait
        let all_names_query = RelationApplication::new(
            Term::from("user"),
            Term::from("name"),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let session = Session::open(artifacts.clone());
        let result = all_names_query.perform(&session).try_vec().await;
        assert!(result.is_ok()); // Should succeed with constants

        // Query 3: Find Alice's email using Query trait
        let email_query = RelationApplication::new(
            Term::from("user"),
            Term::from("email"),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let session = Session::open(artifacts);
        let result = email_query.perform(&session).try_vec().await;
        assert!(result.is_ok()); // Should succeed with constants

        Ok(())
    }

    #[dialog_common::test]
    async fn test_query_trait_with_variables_succeeds_if_constants_present() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        // Create a query with variables - variables are skipped, constants used
        let variable_query = RelationApplication::new(
            Term::from("user"),
            Term::from("name"),
            Term::<Entity>::var("user"),
            Term::<Value>::var("name"),
            Term::blank(),
            None,
        );

        // Should succeed since planning validation only rejects all-unbound queries, and this has a constant
        let session = Session::open(artifacts);
        let result = variable_query.perform(&session).try_vec().await;
        assert!(result.is_ok());

        Ok(())
    }

    #[dialog_common::test]
    async fn test_polymorphic_querying() -> Result<()> {
        // This test demonstrates polymorphic querying - same function can work with any Query impl

        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let claims = vec![Assertion {
            the: "user/name".parse::<Attribute>()?,
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        let fact_selector = RelationApplication::new(
            Term::from("user"),
            Term::from("name"),
            alice.clone().into(),
            Term::blank(),
            Term::blank(),
            None,
        );

        let session = Session::open(artifacts);
        let results = fact_selector.perform(&session).try_vec().await?;
        assert_eq!(results.len(), 1); // Should find the Alice fact

        Ok(())
    }

    #[dialog_common::test]
    async fn test_chaining_query_operations() -> Result<()> {
        // This test shows how the Query trait enables fluent query building and execution

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

        // Test query building - should succeed with constants
        let session = Session::open(artifacts.clone());
        let admin_result = RelationApplication::new(
            Term::from("user"),
            Term::from("role"),
            Term::blank(),
            Term::from(Value::String("admin".to_string())),
            Term::blank(),
            None,
        )
        .perform(&session)
        .try_vec()
        .await;
        assert!(admin_result.is_ok());

        // Test another query - should also succeed
        let session = Session::open(artifacts);
        let names_result = RelationApplication::new(
            Term::from("user"),
            Term::from("name"),
            Term::blank(),
            Term::blank(),
            Term::blank(),
            None,
        )
        .perform(&session)
        .try_vec()
        .await;
        assert!(names_result.is_ok());

        Ok(())
    }
}
