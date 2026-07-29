//! Lossless installation of an exact branch revision into another storage environment.

use dialog_artifacts::BlobIndexExt as _;
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_capability::{Fork, Provider};
use dialog_common::{Blake3Hash as NodeHash, ConditionalSync};
use dialog_effects::archive::prelude::{ArchiveSubjectExt as _, CatalogExt as _};
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::prelude::{ArchiveBlobExt as _, BlobExt as _};
use dialog_effects::blob::{BlobError, Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::Resolve;
use dialog_search_tree::{ContentAddressedStorage as TreeStorage, TreeDifference};
use futures_util::StreamExt as _;

use crate::{
    Branch, Index, InstallRevisionError, NetworkedIndex, RemoteRepository, RemoteSite,
    RepositoryArchiveExt as _, RepositoryMemoryExt as _, Revision, Upstream,
};

/// Install an exact branch revision into another storage environment.
///
/// Created by [`Branch::install`]. The command copies the revision's complete
/// reachable tree and referenced blobs. It does not publish a branch head,
/// create a commit, or otherwise make the destination branch visible.
pub struct InstallRevision<'a> {
    branch: &'a Branch,
    revision: Revision,
}

impl Branch {
    /// Prepare to install `revision` from this branch's storage into another
    /// storage environment.
    ///
    /// The destination must already have the same repository DID mounted. The
    /// caller may publish the returned revision only after this operation
    /// succeeds; installation itself deliberately leaves branch memory cells
    /// untouched.
    pub fn install(&self, revision: Revision) -> InstallRevision<'_> {
        InstallRevision {
            branch: self,
            revision,
        }
    }
}

impl InstallRevision<'_> {
    /// Copy the exact revision and all content it references from `source` to
    /// `destination` without minting a new revision.
    pub async fn perform<Source, Destination>(
        self,
        source: &Source,
        destination: &Destination,
    ) -> Result<Revision, InstallRevisionError>
    where
        Source: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<BlobRead>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + ConditionalSync
            + 'static,
        Destination: Provider<Put> + Provider<BlobImport> + ConditionalSync + 'static,
    {
        let branch = self.branch;
        let remote = match branch.upstream() {
            Some(Upstream::Remote { remote, .. }) => Some(
                branch
                    .subject()
                    .remote(remote)
                    .load()
                    .perform(source)
                    .await?,
            ),
            _ => None,
        };

        let catalog = branch.archive().index();
        let source_index = NetworkedIndex::new(source, catalog.clone(), remote.clone());
        let destination_index = catalog.clone();
        let current = Index::from_hash(NodeHash::from(*self.revision.tree.hash()));
        let empty = Index::empty();
        let tree_storage = TreeStorage::new(TreeStorageBridge(source_index.clone()));
        let difference =
            TreeDifference::compute(&empty, &current, &tree_storage, &tree_storage).await?;

        let nodes = difference.novel_nodes();
        futures_util::pin_mut!(nodes);
        while let Some(node) = nodes.next().await {
            destination_index
                .clone()
                .put(node?.buffer().clone())
                .perform(destination)
                .await?;
        }

        let blobs = current.list_blobs(source_index);
        futures_util::pin_mut!(blobs);
        while let Some(blob) = blobs.next().await {
            let (hash, record) = blob?;
            install_blob(
                branch,
                remote.as_ref(),
                hash,
                record.size,
                source,
                destination,
            )
            .await?;
        }

        Ok(self.revision)
    }
}

async fn install_blob<Source, Destination>(
    branch: &Branch,
    remote: Option<&RemoteRepository>,
    hash: [u8; 32],
    size: u64,
    source: &Source,
    destination: &Destination,
) -> Result<(), InstallRevisionError>
where
    Source: Provider<BlobRead> + Provider<Fork<RemoteSite, BlobRead>> + ConditionalSync,
    Destination: Provider<BlobImport> + ConditionalSync,
{
    let digest = NodeHash::from(hash);
    let local = branch
        .archive()
        .blob()
        .read(digest.clone())
        .perform(source)
        .await;
    let mut reader = match (local, remote) {
        (Ok(reader), _) => reader,
        (Err(BlobError::NotFound(_)), Some(remote)) => {
            let address = remote.address();
            address
                .subject
                .clone()
                .archive()
                .blob()
                .read(digest.clone())
                .fork(address.site())
                .perform(source)
                .await?
        }
        (Err(error), _) => return Err(error.into()),
    };

    let mut writer = branch
        .archive()
        .blob()
        .import(digest.clone(), size)
        .perform(destination)
        .await?;
    while let Some(chunk) = reader.next().await? {
        writer.write_all(&chunk).await?;
    }
    let installed = writer.finish().await?;
    if installed != digest {
        return Err(InstallRevisionError::BlobDigestMismatch {
            expected: digest,
            actual: installed,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_artifacts::{Artifact, ArtifactSelector, Entity, Instruction, Value};
    use dialog_capability::{Fork, Provider, Subject};
    use dialog_common::ConditionalSync;
    use dialog_credentials::Credential;
    use dialog_effects::archive::{Get, Put};
    use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
    use dialog_effects::memory::{Publish, Resolve};
    use dialog_effects::storage::{LocationExt as _, Storage as StorageFx};
    use dialog_network::Network;
    use dialog_operator::{Operator, Profile};
    #[cfg(not(target_arch = "wasm32"))]
    use dialog_storage::NativeTempSpace;
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use futures_util::{StreamExt as _, stream};

    use crate::helpers::{generate_data, test_operator_with_profile, test_repo, unique_name};
    use crate::{Blob, Branch, RemoteSite, Repository, Revision};

    struct Stage {
        source: Operator<VolatileSpace>,
        profile: Profile,
        repository: Repository,
        branch: Branch,
        revision: Revision,
        blob: Entity,
        blob_bytes: Vec<u8>,
        large: Value,
    }

    async fn prepare_stage() -> Result<Stage> {
        let (source, profile) = test_operator_with_profile().await;
        let repository = test_repo(&source, &profile).await;
        let branch = repository.branch("main").open().perform(&source).await?;

        let mut facts: Vec<Instruction> = generate_data(80)?
            .into_iter()
            .map(Instruction::Assert)
            .collect();
        let large = Value::String("staged".repeat(1024));
        facts.push(Instruction::Assert(Artifact {
            the: "document/body".parse()?,
            of: "document:large".parse()?,
            is: large.clone(),
            cause: None,
        }));
        branch.commit(stream::iter(facts)).perform(&source).await?;

        let blob_bytes = b"lossless staged blob".repeat(1024);
        let blob = Blob::import(stream::iter(vec![Ok(blob_bytes.clone())]))
            .write(branch.blobs())
            .perform(&source)
            .await?;
        let revision = branch.revision().expect("staged branch has a revision");

        Ok(Stage {
            source,
            profile,
            repository,
            branch,
            revision,
            blob,
            blob_bytes,
            large,
        })
    }

    async fn verify_install<Destination>(stage: Stage, destination: &Destination) -> Result<()>
    where
        Destination: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<BlobRead>
            + Provider<BlobImport>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + ConditionalSync
            + 'static,
    {
        let destination_repository: Repository = stage.repository.credential().clone().into();
        let destination_branch = destination_repository
            .branch("main")
            .open()
            .perform(destination)
            .await?;
        assert!(
            destination_branch.revision().is_none(),
            "the destination starts unindexed"
        );

        let installed = stage
            .branch
            .install(stage.revision.clone())
            .perform(&stage.source, destination)
            .await?;
        assert_eq!(
            installed, stage.revision,
            "installation preserves the exact head"
        );

        let still_hidden = destination_repository
            .branch("main")
            .load()
            .perform(destination)
            .await;
        assert!(
            still_hidden.is_err(),
            "installing blocks does not publish the branch head"
        );

        destination_branch
            .reset(installed.clone())
            .perform(destination)
            .await?;
        let destination_branch = destination_repository
            .branch("main")
            .load()
            .perform(destination)
            .await?;
        assert_eq!(destination_branch.revision(), Some(installed));

        let large_facts: Vec<_> = destination_branch
            .claims()
            .select(ArtifactSelector::new().the("document/body".parse()?))
            .perform(destination)
            .await?
            .collect()
            .await;
        assert_eq!(large_facts.len(), 1);
        assert_eq!(large_facts[0].as_ref().expect("valid fact").is, stage.large);

        let mut reader = Blob::from(stage.blob)
            .read(destination_branch.blobs())
            .perform(destination)
            .await?;
        let mut copied_blob = Vec::new();
        while let Some(chunk) = reader.next().await? {
            copied_blob.extend_from_slice(&chunk);
        }
        assert_eq!(copied_blob, stage.blob_bytes);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_installs_an_exact_revision_between_volatile_spaces() -> Result<()> {
        let stage = prepare_stage().await?;
        let destination = Storage::<VolatileSpace>::volatile();
        StorageFx::profile(unique_name("install-profile"))
            .create(Credential::Signer(stage.profile.signer().clone()))
            .perform(&destination)
            .await?;
        StorageFx::profile(unique_name("install-repository"))
            .create(stage.repository.credential().clone())
            .perform(&destination)
            .await?;
        let operator = stage
            .profile
            .derive(b"install-volatile")
            .allow(Subject::any())
            .network(Network::default())
            .build(destination)
            .await?;
        verify_install(stage, &operator).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_installs_an_exact_revision_into_durable_storage() -> Result<()> {
        let stage = prepare_stage().await?;
        let destination = Storage::<NativeTempSpace>::temp();
        StorageFx::profile(unique_name("install-profile"))
            .create(Credential::Signer(stage.profile.signer().clone()))
            .perform(&destination)
            .await?;
        StorageFx::profile(unique_name("install-repository"))
            .create(stage.repository.credential().clone())
            .perform(&destination)
            .await?;
        let operator = stage
            .profile
            .derive(b"install-durable")
            .allow(Subject::any())
            .network(Network::default())
            .build(destination)
            .await?;
        verify_install(stage, &operator).await
    }
}
