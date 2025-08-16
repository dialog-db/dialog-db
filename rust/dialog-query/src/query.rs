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