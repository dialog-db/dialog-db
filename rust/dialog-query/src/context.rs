//! Query execution plans - traits and context for evaluation

pub use crate::query::Source;
pub use crate::{try_stream, Match, Selection, Value};
pub use dialog_common::ConditionalSend;
pub use futures_util::stream::once;
use std::collections::BTreeMap;

pub use futures_util::{stream, TryStreamExt};

pub use super::parameters::Parameters;
pub use super::syntax::VariableScope;

pub fn new_context<S: Source>(store: S) -> EvaluationContext<S, impl Selection> {
    let selection = once(async move { Ok(Match::new()) });
    EvaluationContext {
        source: store,
        selection,
        scope: VariableScope::new(),
    }
}

/// A single result frame with variable bindings
/// Equivalent to MatchFrame in TypeScript: Map<Variable, Scalar>
pub type MatchFrame = BTreeMap<String, Value>;

/// Evaluation context passed to plans during execution
/// Based on TypeScript EvaluationContext in @query/src/api.ts
pub struct EvaluationContext<S, M>
where
    S: Source,
    M: Selection,
{
    /// Current selection of frames being processed (equivalent to frames in familiar-query)
    pub selection: M,
    /// Artifact store for querying facts (equivalent to source/Querier in TypeScript)
    pub source: S,
    /// Variables that are bound at this evaluation point
    pub scope: VariableScope,
}

impl<S, M> EvaluationContext<S, M>
where
    S: Source,
    M: Selection,
{
    /// Create a new evaluation context with given scope
    pub fn single(store: S, selection: M, scope: VariableScope) -> Self {
        Self {
            source: store,
            selection,
            scope,
        }
    }

    pub fn new(store: S) -> EvaluationContext<S, impl Selection> {
        let selection = once(async move { Ok(Match::new()) });

        EvaluationContext {
            source: store,
            selection,
            scope: VariableScope::new(),
        }
    }

    /// Create a new context with updated scope
    pub fn with_scope(&self, scope: VariableScope) -> EvaluationContext<S, M>
    where
        M: Clone,
    {
        EvaluationContext {
            source: self.source.clone(),
            selection: self.selection.clone(),
            scope,
        }
    }
}

/// Trait implemented by execution plans
/// Following the familiar-query pattern: process selection of frames and return new frames
pub trait EvaluationPlan: Clone + std::fmt::Debug + ConditionalSend {
    /// Get the estimated cost of executing this plan
    fn cost(&self) -> usize;
    /// Set of variables that this plan will bind
    fn provides(&self) -> &VariableScope;
    /// Execute this plan with the given context and return result frames
    /// This follows the familiar-query pattern where frames flow through the evaluation
    fn evaluate<S: Source, M: Selection>(&self, context: EvaluationContext<S, M>)
        -> impl Selection;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Artifacts;
    use crate::{Session, Term, Value};
    use dialog_storage::MemoryStorageBackend;

    #[tokio::test]
    async fn test_fresh_context_has_empty_scope() {
        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();
        let session = Session::open(artifacts);

        let context = new_context(session);

        // Fresh context should have empty scope
        assert_eq!(context.scope.size(), 0);
    }

    #[tokio::test]
    async fn test_context_single_with_scope() {
        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();
        let session = Session::open(artifacts);

        let selection = once(async move { Ok(Match::new()) });
        let mut scope = VariableScope::new();
        scope.add(&Term::<Value>::var("z"));

        let context = EvaluationContext::single(session, selection, scope.clone());

        // Context should have the provided scope
        assert_eq!(context.scope.size(), 1);
        assert!(context.scope.contains(&Term::<Value>::var("z")));
    }
}
