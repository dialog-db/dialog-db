use dialog_artifacts::Importer;
use dialog_artifacts::Instruction;
use dialog_capability::Provider;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive as archive_fx;
use dialog_effects::authority;
use dialog_effects::memory as memory_fx;
use dialog_storage::Blake3Hash;
use futures_util::StreamExt;

use super::Branch;
use crate::DialogArtifactsError;

/// Command struct for importing artifacts into a branch.
pub struct Import<'a, I> {
    branch: &'a Branch,
    importer: I,
}

impl<'a, I> Import<'a, I> {
    pub(super) fn new(branch: &'a Branch, importer: I) -> Self {
        Self { branch, importer }
    }
}

impl<I: Importer + Unpin + ConditionalSend> Import<'_, I> {
    /// Execute the import, reading artifacts and committing them as assertions.
    pub async fn perform<Env>(self, env: &Env) -> Result<Blake3Hash, DialogArtifactsError>
    where
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<memory_fx::Publish>
            + Provider<authority::Identify>
            + ConditionalSync
            + 'static,
    {
        let instructions = self.importer.filter_map(|result| async {
            match result {
                Ok(artifact) => Some(Instruction::Assert(artifact)),
                Err(error) => {
                    eprintln!("Skipping incompatible datum: {error}");
                    None
                }
            }
        });

        self.branch.commit(instructions).perform(env).await
    }
}
