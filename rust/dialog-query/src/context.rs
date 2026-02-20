//! Query execution context for evaluation

pub use crate::query::Source;
pub use crate::{selection::Answer, selection::Answers, try_stream};
pub use futures_util::stream::once;

pub use futures_util::{TryStreamExt, stream};

pub use super::environment::Environment;
pub use super::parameters::Parameters;

/// Create a fresh evaluation context with an empty scope and a single empty answer
pub fn new_context<S: Source>(store: S) -> EvaluationContext<S, impl Answers> {
    let answers = once(async move { Ok(Answer::new()) });
    EvaluationContext {
        source: store,
        selection: answers,
        scope: Environment::new(),
    }
}

/// Evaluation context passed to plans during execution
/// Based on TypeScript EvaluationContext in @query/src/api.ts
pub struct EvaluationContext<S, M>
where
    S: Source,
    M: Answers,
{
    /// Current selection of answers being processed (with provenance tracking)
    pub selection: M,
    /// Artifact store for querying facts (equivalent to source/Querier in TypeScript)
    pub source: S,
    /// Variables that are bound at this evaluation point
    pub scope: Environment,
}

impl<S, M> EvaluationContext<S, M>
where
    S: Source,
    M: Answers,
{
    /// Create a new evaluation context with given scope
    pub fn single(store: S, selection: M, scope: Environment) -> Self {
        Self {
            source: store,
            selection,
            scope,
        }
    }

    /// Create a new context with updated scope
    pub fn with_scope(&self, scope: Environment) -> EvaluationContext<S, M>
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Artifacts;
    use crate::{Session, Term, Value};
    use dialog_storage::MemoryStorageBackend;

    #[dialog_common::test]
    async fn test_fresh_context_has_empty_scope() {
        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();
        let session = Session::open(artifacts);

        let context = new_context(session);

        // Fresh context should have empty scope
        assert_eq!(context.scope.size(), 0);
    }

    #[dialog_common::test]
    async fn test_context_single_with_scope() {
        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();
        let session = Session::open(artifacts);

        let answers = once(async move { Ok(Answer::new()) });
        let mut scope = Environment::new();
        scope.add(&Term::<Value>::var("z"));

        let context = EvaluationContext::single(session, answers, scope.clone());

        // Context should have the provided scope
        assert_eq!(context.scope.size(), 1);
        assert!(context.scope.contains(&Term::<Value>::var("z")));
    }
}
