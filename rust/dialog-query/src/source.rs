use dialog_capability::Command;

use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::ConceptRules;
use crate::error::EvaluationError;

/// Command for acquiring deductive rules for a concept.
///
/// Given a `ConceptDescriptor`, returns the `ConceptRules` bundle
/// containing default and installed rules with a plan cache.
pub struct SelectRules;

impl Command for SelectRules {
    type Input = ConceptDescriptor;
    type Output = Result<ConceptRules, EvaluationError>;
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::session::RuleRegistry;
    use dialog_artifacts::selector::Constrained;
    use dialog_artifacts::{ArtifactSelector, ArtifactStream, DialogArtifactsError, Select};
    use dialog_capability::Provider;
    use dialog_repository::{Branch, Operator};

    /// Test environment that implements both `Provider<Select<'a>>` and
    /// `Provider<SelectRules>`, bridging a Branch + Operator with a RuleRegistry.
    pub struct TestEnv<'b> {
        branch: &'b Branch,
        operator: &'b Operator,
        rules: RuleRegistry,
    }

    impl<'b> TestEnv<'b> {
        pub fn new(branch: &'b Branch, operator: &'b Operator, rules: RuleRegistry) -> Self {
            Self {
                branch,
                operator,
                rules,
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl<'a> Provider<Select<'a>> for TestEnv<'a> {
        async fn execute(
            &self,
            input: ArtifactSelector<Constrained>,
        ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
            let stream = self
                .branch
                .claims()
                .select(input)
                .perform(self.operator)
                .await?;
            Ok(Box::pin(stream))
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<SelectRules> for TestEnv<'_> {
        async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
            self.rules.acquire(&input)
        }
    }
}
