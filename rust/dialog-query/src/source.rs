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
    use dialog_artifacts::tree::{ArtifactNode, ArtifactPreload, ArtifactRead};
    use dialog_artifacts::{ArtifactSelector, ArtifactStream, DialogArtifactsError, Select};
    use dialog_capability::Provider;
    use dialog_capability::Resume;
    use dialog_effects::archive::{self, ArchiveError};
    use dialog_effects::blob;
    use dialog_peer::Peer as DialogOperator;
    use dialog_repository::Branch;
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

    /// What evaluation keeps between queries is the operator's to hold.
    impl dialog_common::Holds for TestEnv<'_> {
        fn held(&self, key: &str) -> Option<dialog_common::Held> {
            self.operator.held(key)
        }

        fn hold(&self, key: String, handle: dialog_common::Held) {
            self.operator.hold(key, handle)
        }

        fn held_or(
            &self,
            key: &str,
            make: &dyn Fn() -> dialog_common::Held,
        ) -> dialog_common::Held {
            self.operator.held_or(key, make)
        }
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

    #[dialog_capability::provider]
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

    #[dialog_capability::provider]
    impl Provider<SelectRules> for TestEnv<'_> {
        async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
            self.rules.acquire(&input)
        }
    }

    #[dialog_capability::provider]
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
    #[dialog_capability::provider]
    impl Provider<dialog_artifacts::Preload> for TestEnv<'_> {
        async fn execute(&self, _input: dialog_artifacts::PreloadRequest) -> bool {
            false
        }
    }

    // Raw node loads for resolver premises, which ask for a block anywhere
    // the query reads: the branch's archive catalog, through the operator.
    #[dialog_capability::provider]
    impl Provider<archive::Load> for TestEnv<'_> {
        async fn execute(
            &self,
            load: archive::Load,
        ) -> Result<Option<dialog_common::Buffer>, ArchiveError> {
            archive::Load {
                catalog: self.branch.archive().index(),
                ..load
            }
            .perform(self.operator)
            .await
        }
    }

    // Node reads for the scans, in the branch's catalog: what the operator
    /// answers at once is at once here too.
    impl Provider<ArtifactRead> for TestEnv<'_> {
        fn execute(
            &self,
            read: ArtifactRead,
        ) -> Resume<'_, Result<Option<ArtifactNode>, dialog_search_tree::DialogSearchTreeError>>
        {
            let mut line = read;
            line.catalog = self.branch.archive().index();
            line.perform(self.operator)
        }
    }

    #[dialog_capability::provider]
    impl Provider<ArtifactPreload> for TestEnv<'_> {
        async fn execute(&self, preload: ArtifactPreload) {
            let mut line = preload;
            line.catalog = self.branch.archive().index();
            line.perform(self.operator).await
        }
    }

    #[dialog_capability::provider]
    impl Provider<blob::Load> for TestEnv<'_> {
        async fn execute(
            &self,
            load: blob::Load,
        ) -> Result<Option<dialog_common::Buffer>, ArchiveError> {
            blob::Load {
                archive: self.branch.archive(),
                ..load
            }
            .perform(self.operator)
            .await
        }
    }
}
