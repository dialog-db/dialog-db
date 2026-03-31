use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{Artifact, ArtifactSelector, Branch, DialogArtifactsError};
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive;
use futures_util::Stream;

use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::ConceptRules;
use crate::error::EvaluationError;
use crate::session::RuleRegistry;

/// A read-only data source for query evaluation.
///
/// Combines a branch (for artifact storage), an env (for capability
/// dispatch), and a rule registry (for deductive inference).
pub struct Source<'a, Env> {
    branch: &'a Branch,
    env: &'a Env,
    rules: RuleRegistry,
}

impl<Env> Clone for Source<'_, Env> {
    fn clone(&self) -> Self {
        Self {
            branch: self.branch,
            env: self.env,
            rules: self.rules.clone(),
        }
    }
}

impl<'a, Env> Source<'a, Env> {
    /// Create a new source.
    pub fn new(branch: &'a Branch, env: &'a Env, rules: RuleRegistry) -> Self {
        Self { branch, env, rules }
    }

    /// Acquire rules for the given concept predicate.
    pub fn acquire(&self, predicate: &ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        self.rules.acquire(predicate)
    }
}

impl<'a, Env> Source<'a, Env>
where
    Env: Provider<archive::Get> + Provider<archive::Put> + ConditionalSync + 'static,
{
    /// Select artifacts matching the selector.
    pub async fn select(
        &self,
        selector: ArtifactSelector<Constrained>,
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>> + '_, DialogArtifactsError>
    {
        self.branch.select(selector).perform(self.env).await
    }
}
