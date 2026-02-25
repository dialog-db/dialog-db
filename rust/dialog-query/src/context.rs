//! Query execution context for evaluation

pub use crate::query::Source;
pub use crate::{selection::Answer, selection::Answers, try_stream};
pub use futures_util::stream::once;

pub use futures_util::{TryStreamExt, stream};

pub use super::parameters::Parameters;

/// Create a fresh evaluation context with a single empty answer
pub fn new_context<S: Source>(store: S) -> EvaluationContext<S, impl Answers> {
    let answers = once(async move { Ok(Answer::new()) });
    EvaluationContext {
        source: store,
        selection: answers,
    }
}

/// Evaluation context passed to plans during execution
pub struct EvaluationContext<S, M>
where
    S: Source,
    M: Answers,
{
    /// Current selection of answers being processed (with provenance tracking)
    pub selection: M,
    /// Artifact store for querying facts (equivalent to source/Querier in TypeScript)
    pub source: S,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Session;
    use crate::artifact::Artifacts;
    use dialog_storage::MemoryStorageBackend;

    #[dialog_common::test]
    async fn test_fresh_context_creates_successfully() {
        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();
        let session = Session::open(artifacts);

        let _context = new_context(session);
    }
}
