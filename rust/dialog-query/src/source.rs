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
    use crate::recall::{BodyMemo, Memo};
    use crate::session::RuleRegistry;
    use dialog_artifacts::selector::Constrained;
    use dialog_artifacts::{
        ArtifactSelector, ArtifactStream, DialogArtifactsError, LoadBlob, Select,
    };
    use dialog_capability::Provider;
    use dialog_peer::Peer as DialogOperator;
    use dialog_repository::{Branch, NetworkedIndex};
    use dialog_search_tree::{DialogSearchTreeError, LoadBlock};
    use dialog_storage::provider::storage::VolatileSpace;

    type Operator = DialogOperator<VolatileSpace, dialog_peer::Session>;

    /// Test environment that implements both `Provider<Select<'a>>` and
    /// `Provider<SelectRules>`, bridging a Branch + Operator with a RuleRegistry.
    pub struct TestEnv<'b> {
        branch: &'b Branch,
        operator: &'b Operator,
        rules: RuleRegistry,
        memo: Memo,
    }

    impl<'b> TestEnv<'b> {
        pub fn new(branch: &'b Branch, operator: &'b Operator, rules: RuleRegistry) -> Self {
            Self {
                branch,
                operator,
                rules,
                memo: Memo::default(),
            }
        }
    }

    impl BodyMemo for TestEnv<'_> {
        fn memo(&self) -> Option<&Memo> {
            Some(&self.memo)
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

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<dialog_artifacts::Estimate> for TestEnv<'_> {
        async fn execute(
            &self,
            input: ArtifactSelector<Constrained>,
        ) -> Result<Option<u64>, DialogArtifactsError> {
            self.branch
                .claims()
                .select(input)
                .estimate_perform(self.operator)
                .await
        }
    }

    // Preload hints have no listener in the test env: refuse them so
    // evaluation stops composing hints after the first.
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<dialog_artifacts::Preload> for TestEnv<'_> {
        async fn execute(&self, _input: dialog_artifacts::PreloadRequest) -> bool {
            false
        }
    }

    // Raw node loads for resolver premises, local archive only.
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<LoadBlock> for TestEnv<'_> {
        async fn execute(
            &self,
            load: LoadBlock,
        ) -> Result<Option<dialog_common::Buffer>, DialogSearchTreeError> {
            let store = NetworkedIndex::new(self.operator, self.branch.archive().index(), None);
            load.perform(&store).await
        }
    }

    // Spilled-value loads for `tree/value`, local archive only.
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<LoadBlob> for TestEnv<'_> {
        async fn execute(
            &self,
            load: LoadBlob,
        ) -> Result<Option<dialog_common::Buffer>, DialogArtifactsError> {
            let store = NetworkedIndex::new(self.operator, self.branch.archive().index(), None);
            load.perform(&store).await
        }
    }
}
