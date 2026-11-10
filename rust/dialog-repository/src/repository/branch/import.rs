use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Importer, Instruction};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Import as ArchiveImport, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::Import as BlobImport;
use dialog_effects::blob::Load as BlobLoad;
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::nodes;
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
        Env: Provider<BlobImport>
            + Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<ArchiveImport>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<ArchiveLoad>
            + Provider<nodes::Write<CorpusNode>>
            + ConditionalSync
            + Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + ConditionalSync
            + Provider<BlobLoad>
            + Provider<Fork<RemoteSite, Resolve>>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let instructions = self.importer.filter_map(|result| async {
            match result {
                Ok(artifact) => Some(Instruction::Assert(artifact, dialog_corpus::Pick::All)),
                Err(error) => {
                    eprintln!("Skipping incompatible datum: {error}");
                    None
                }
            }
        });

        self.branch.commit(instructions).perform(env).await
    }
}
