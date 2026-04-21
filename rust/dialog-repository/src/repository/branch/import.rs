use dialog_artifacts::{Importer, Instruction};
use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::{Publish, Resolve};
use futures_util::StreamExt;

use crate::{Branch, CommitError, RemoteSite, Revision};

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
    pub async fn perform<Env>(self, env: &Env) -> Result<Revision, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
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
