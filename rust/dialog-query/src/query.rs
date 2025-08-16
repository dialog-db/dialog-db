//! Query trait for polymorphic querying across different store types

use dialog_artifacts::{ArtifactStore, Artifact, DialogArtifactsError};
use futures_util::Stream;
use crate::error::QueryResult;

/// A trait for types that can query an ArtifactStore
/// 
/// This provides a consistent interface for querying, abstracting over the details
/// of query planning, variable resolution, and execution against the store.
/// 
/// ## Architecture
/// 
/// The proper implementation should follow this flow:
/// 1. **Plan**: Create an execution plan from the query pattern
/// 2. **Evaluate**: Execute the plan against the store with variable bindings
/// 3. **Stream**: Return results as a stream of artifacts
/// 
/// For queries with only constants, this can optimize by converting directly to
/// `ArtifactSelector`. For queries with variables, it should:
/// 1. Create an `EvaluationContext` with empty variable bindings
/// 2. Call `evaluate()` on the plan to get `MatchFrame`s with variable bindings
/// 3. Convert match frames back to artifacts using the resolved bindings
/// 
/// ## Current Implementation Status
/// 
/// - ✅ **FactSelector**: Implements `Query` with constants-only support
/// - ✅ **FactSelectorPlan**: Implements `Query` with constants-only support  
/// - ❌ **Variable Resolution**: Not yet implemented in `evaluate()` methods
/// 
/// The variable resolution in `FactSelectorPlan::evaluate()` needs to:
/// 1. Query the store using patterns that can be partially bound
/// 2. Unify results with the query pattern to resolve variables
/// 3. Return match frames with variable bindings
pub trait Query {
    /// Execute the query against the provided store
    /// 
    /// Returns a stream of artifacts that match the query criteria.
    fn query<S>(
        &self,
        store: &S,
    ) -> QueryResult<impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 'static>
    where
        S: ArtifactStore;
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use dialog_artifacts::{
        Artifacts, ArtifactStore, ArtifactStoreMut, Entity, Value, Attribute, Instruction
    };
    use dialog_storage::MemoryStorageBackend;
    use crate::{Fact, Variable};
    use futures_util::{stream, StreamExt};

    #[tokio::test]
    async fn test_fact_selector_query_trait() -> Result<()> {
        // Setup: Create in-memory storage and artifacts store
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;
        
        // Step 1: Create test data
        let alice = Entity::new()?;
        let bob = Entity::new()?;
        
        let facts = vec![
            Fact::assert("user/name".parse::<Attribute>()?, alice.clone(), Value::String("Alice".to_string())),
            Fact::assert("user/email".parse::<Attribute>()?, alice.clone(), Value::String("alice@example.com".to_string())),
            Fact::assert("user/name".parse::<Attribute>()?, bob.clone(), Value::String("Bob".to_string())),
        ];
        
        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;
        
        // Step 2: Test Query trait on FactSelector with constants
        
        // Query 1: Find Alice by name using Query trait
        let alice_query = Fact::select()
            .the("user/name")
            .of(alice.clone())
            .is(Value::String("Alice".to_string()));
        
        // Use the Query trait method
        let alice_stream = alice_query.query(&artifacts)?;
        let alice_results = alice_stream
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(alice_results.len(), 1);
        assert_eq!(alice_results[0].of, alice);
        assert_eq!(alice_results[0].is, Value::String("Alice".to_string()));
        
        // Query 2: Find all user/name facts using Query trait
        let all_names_query = Fact::select().the("user/name");
        
        let all_names_stream = all_names_query.query(&artifacts)?;
        let all_names_results = all_names_stream
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(all_names_results.len(), 2); // Alice and Bob
        
        // Query 3: Find Alice's email using Query trait
        let email_query = Fact::select()
            .the("user/email")
            .of(alice.clone());
        
        let email_stream = email_query.query(&artifacts)?;
        let email_results = email_stream
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await;
        
        assert_eq!(email_results.len(), 1);
        assert_eq!(email_results[0].is, Value::String("alice@example.com".to_string()));
        
        Ok(())
    }

    #[tokio::test]
    async fn test_query_trait_with_variables_fails() -> Result<()> {
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;
        
        // Create a query with variables - this should fail when using Query trait
        let variable_query = Fact::select()
            .the("user/name")
            .of(Variable::<Entity>::new("user"))    // Variable!
            .is(Variable::<String>::new("name"));   // Variable!
        
        // Attempt to query - should fail with helpful error
        let result = variable_query.query(&artifacts);
        assert!(result.is_err());
        
        if let Err(error) = result {
            assert!(error.to_string().contains("Variable not supported"));
        }
        
        Ok(())
    }

    #[tokio::test]
    async fn test_polymorphic_querying() -> Result<()> {
        // This test demonstrates polymorphic querying - same function can work with any Query impl
        
        async fn execute_query<Q: Query>(
            query: Q,
            store: &impl ArtifactStore,
        ) -> Result<Vec<dialog_artifacts::Artifact>> {
            let stream = query.query(store)?;
            let results = stream
                .filter_map(|result| async move { result.ok() })
                .collect::<Vec<_>>()
                .await;
            Ok(results)
        }
        
        // Setup
        let storage_backend = MemoryStorageBackend::default();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;
        
        let alice = Entity::new()?;
        let facts = vec![
            Fact::assert("user/name".parse::<Attribute>()?, alice.clone(), Value::String("Alice".to_string())),
        ];
        
        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;
        
        // Test with FactSelector
        let fact_selector = Fact::select()
            .the("user/name")
            .of(alice.clone());
        
        let results = execute_query(fact_selector, &artifacts).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, Value::String("Alice".to_string()));
        
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
            Fact::assert("user/name".parse::<Attribute>()?, alice.clone(), Value::String("Alice".to_string())),
            Fact::assert("user/role".parse::<Attribute>()?, alice.clone(), Value::String("admin".to_string())),
            Fact::assert("user/name".parse::<Attribute>()?, bob.clone(), Value::String("Bob".to_string())),
            Fact::assert("user/role".parse::<Attribute>()?, bob.clone(), Value::String("user".to_string())),
        ];
        
        let instructions: Vec<Instruction> = facts.into_iter().map(Instruction::from).collect();
        artifacts.commit(stream::iter(instructions)).await?;
        
        // Test fluent query building with immediate execution
        let admin_count = Fact::select()
            .the("user/role")
            .is(Value::String("admin".to_string()))
            .query(&artifacts)?
            .filter_map(|result| async move { result.ok() })
            .collect::<Vec<_>>()
            .await
            .len();
        
        assert_eq!(admin_count, 1); // Only Alice is admin
        
        // Test another fluent query
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