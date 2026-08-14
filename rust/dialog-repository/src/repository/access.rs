//! Synced access: the operator's delegations served from a branch.
//!
//! [`SyncedAccess`] implements the operator's
//! [`AccessProvider`](dialog_operator::AccessProvider) over a branch's
//! retained delegations, so proofs resolve through the tree walk
//! ([`Delegations::prove`](crate::Delegations::prove)) and retains land as
//! synced `dialog.ucan/*` facts — replicated by ordinary push/pull instead
//! of living in a per-device certificate store.
//!
//! [`synced_access`] is the installer: it opens the profile's own
//! repository branch (the store that is always locally present — proving
//! access to it needs only the profile-to-operator delegation the builder
//! mints locally, so a fresh device can pull it before it holds anything
//! else), migrates every certificate the legacy store retains into the
//! branch, and returns an operator with the override installed:
//!
//! ```no_run
//! # use dialog_operator::{Operator, Profile};
//! # use dialog_repository::synced_access;
//! # use dialog_storage::provider::storage::VolatileSpace;
//! # async fn example(
//! #     profile: &Profile,
//! #     operator: Operator<VolatileSpace>,
//! # ) -> anyhow::Result<()> {
//! let operator = synced_access(profile, operator).await?;
//! // proofs now resolve from the profile branch's delegation records
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use crate::Branch;
use dialog_capability::access::{Access, AuthorizeError, Export, Prove};
use dialog_capability::{Provider, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead, Write as BlobWrite};
use dialog_effects::memory::{Publish, Resolve};
use dialog_operator::{AccessProvider, Operator, Profile};
use dialog_storage::provider::space::SpaceProvider;
use dialog_storage::provider::storage::Storage;
use dialog_ucan::{Ucan, UcanDelegation, UcanProof};
use dialog_ucan_core::DelegationChain;

use crate::RemoteSite;

/// The branch name delegations are retained under in the profile's own
/// repository.
pub const ACCESS_BRANCH: &str = "main";

/// An [`AccessProvider`] serving proofs and retains from a branch's
/// delegation records.
pub struct SyncedAccess<Env> {
    branch: Branch,
    env: Env,
}

impl<Env> SyncedAccess<Env> {
    /// Serve access from `branch`'s delegation records, performing the
    /// walk's effects against `env`.
    pub fn new(branch: Branch, env: Env) -> Self {
        Self { branch, env }
    }

    /// The branch this access provider serves from.
    pub fn branch(&self) -> &Branch {
        &self.branch
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env> AccessProvider for SyncedAccess<Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<BlobRead>
        + Provider<BlobWrite>
        + Provider<BlobImport>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + Provider<Fork<RemoteSite, BlobRead>>
        + ConditionalSend
        + ConditionalSync
        + 'static,
{
    async fn prove(&self, claim: Prove<Ucan>) -> Result<UcanProof, AuthorizeError> {
        Box::pin(
            self.branch
                .delegations()
                .prove(claim.principal, claim.access)
                .during(claim.duration)
                .perform(&self.env),
        )
        .await
    }

    async fn retain(&self, delegation: UcanDelegation) -> Result<(), AuthorizeError> {
        self.branch
            .delegations()
            .retain(delegation)
            .perform(&self.env)
            .await
            .map(|_| ())
            .map_err(|error| AuthorizeError::Malformed {
                detail: format!("failed to retain delegation: {error}"),
            })
    }
}

use dialog_capability::Fork;

/// Serve the operator's access from the profile's own repository branch.
///
/// Opens the profile repository's [`ACCESS_BRANCH`], migrates every
/// certificate the legacy store retains into it (idempotent by content
/// address, so repeated installs re-import nothing), and returns the
/// operator with a [`SyncedAccess`] override installed. From then on
/// proofs resolve through the tree walk over the branch's `dialog.ucan/*`
/// facts and retained delegations land as synced facts.
pub async fn synced_access<S>(
    profile: &Profile,
    operator: Operator<S>,
) -> Result<Operator<S>, AuthorizeError>
where
    S: SpaceProvider
        + Provider<BlobRead>
        + Provider<BlobWrite>
        + Provider<BlobImport>
        + Clone
        + 'static,
    Operator<S>: ConditionalSend + ConditionalSync,
    Storage<S>: Provider<Prove<Ucan>> + Provider<Export<Ucan>>,
{
    let repository = crate::Repository::from(profile);
    let branch = repository
        .branch(ACCESS_BRANCH)
        .open()
        .perform(&operator)
        .await
        .map_err(|error| AuthorizeError::Malformed {
            detail: format!("failed to open the profile access branch: {error}"),
        })?;

    // Migrate the legacy certificate store: enumerate everything it holds
    // and retain it into the branch as one commit. Content-addressed
    // idempotence makes this a no-op when nothing is new.
    let certificates = Subject::from(profile.did())
        .attenuate(Access)
        .invoke(Export::<Ucan>::new())
        .perform(&operator)
        .await?;
    if !certificates.is_empty() {
        let chains = certificates
            .into_iter()
            .map(|certificate| UcanDelegation::new(DelegationChain::new(certificate.0)))
            .collect();
        branch
            .delegations()
            .retain_all(chains)
            .perform(&operator)
            .await
            .map_err(|error| AuthorizeError::Malformed {
                detail: format!("failed to migrate legacy certificates: {error}"),
            })?;
    }

    let access = SyncedAccess::new(branch, operator.clone());
    Ok(operator.with_access(Arc::new(access)))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::DELEGATION_AUDIENCE;
    use crate::RepositoryExt as _;
    use anyhow::Result;
    use dialog_artifacts::{ArtifactSelector, Value};
    use dialog_capability::access::{Proof as _, Retain, TimeRange};
    use dialog_network::Network;
    use dialog_storage::provider::storage::Storage;
    use dialog_ucan::Scope;
    use dialog_ucan_core::DelegationBuilder;
    use dialog_ucan_core::command::Command;
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use dialog_varsig::Principal as _;
    use futures_util::StreamExt as _;

    use crate::helpers::unique_name;

    fn scope(subject: dialog_capability::Did) -> Scope {
        Scope {
            subject: UcanSubject::Specific(subject),
            command: Command(vec![]),
            parameters: dialog_ucan::Parameters::default(),
        }
    }

    #[dialog_common::test]
    async fn it_migrates_legacy_certificates_and_serves_proofs_from_the_branch() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("synced-access"))
            .perform(&storage)
            .await?;
        // `allow` retains a profile-to-operator delegation through the
        // LEGACY certificate store at build time.
        let operator = profile
            .derive(b"test")
            .allow(dialog_capability::Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;

        let operator = synced_access(&profile, operator).await?;

        // The migrated delegation stands as facts in the profile branch.
        let repository = crate::Repository::from(&profile);
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await?;
        let facts: Vec<_> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .the(DELEGATION_AUDIENCE.parse()?)
                    .is(Value::String(operator.did().to_string())),
            )
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(facts.len(), 1, "the legacy delegation migrated: {facts:?}");

        // Proofs resolve through the branch: the operator proves the
        // migrated powerline against the profile's own space (the
        // powerline's issuer IS that subject, so the chain is one direct
        // grant; a subject the profile holds no grant for would rightly
        // refuse on either store).
        let mut claim = Prove::<Ucan>::new(operator.did(), scope(profile.did()));
        claim.duration = TimeRange::unbounded();
        let proof = Subject::from(profile.did())
            .attenuate(Access)
            .invoke(claim)
            .perform(&operator)
            .await?;
        assert_eq!(proof.proofs().len(), 1, "proved via the migrated grant");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retains_through_the_branch_not_the_legacy_store() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("synced-retain"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .network(Network::default())
            .build(storage)
            .await?;
        let operator = synced_access(&profile, operator).await?;

        // Retain a fresh delegation through the operator's Retain effect.
        let space = dialog_credentials::Ed25519Signer::generate().await?;
        let holder = dialog_credentials::Ed25519Signer::generate().await?;
        let delegation = DelegationBuilder::new()
            .issuer(space.clone())
            .audience(&holder)
            .subject(UcanSubject::Specific(space.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await?;
        let chain = UcanDelegation::new(DelegationChain::new(delegation));
        Subject::from(profile.did())
            .attenuate(Access)
            .invoke(Retain::<Ucan>::new(chain))
            .perform(&operator)
            .await?;

        // The legacy store never saw it: enumeration is empty.
        let exported = Subject::from(profile.did())
            .attenuate(Access)
            .invoke(Export::<Ucan>::new())
            .perform(&operator)
            .await?;
        assert!(
            exported.is_empty(),
            "retains route to the branch, not the legacy store"
        );

        // The branch proves it.
        let mut claim = Prove::<Ucan>::new(
            holder.did(),
            Scope {
                subject: UcanSubject::Specific(space.did()),
                command: Command(vec!["storage".to_string()]),
                parameters: dialog_ucan::Parameters::default(),
            },
        );
        claim.duration = TimeRange::unbounded();
        let proof = Subject::from(profile.did())
            .attenuate(Access)
            .invoke(claim)
            .perform(&operator)
            .await?;
        assert_eq!(proof.proofs().len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_installs_idempotently() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("synced-idempotent"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .allow(dialog_capability::Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;

        let operator = synced_access(&profile, operator).await?;
        let repository = crate::Repository::from(&profile);
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await?;
        let head = branch.revision().map(|revision| revision.version());

        // Installing again re-migrates nothing: the branch head is
        // unchanged.
        let operator = synced_access(&profile, operator).await?;
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&operator)
            .await?;
        assert_eq!(
            branch.revision().map(|revision| revision.version()),
            head,
            "a second install migrates nothing new"
        );

        Ok(())
    }

    /// The full repository flow works with synced access installed: create
    /// a repository (whose delegations now land in the profile branch),
    /// open a branch, commit and read back — every authorize along the way
    /// resolves through the tree walk.
    #[dialog_common::test]
    async fn it_serves_the_repository_flow() -> Result<()> {
        use dialog_artifacts::{Artifact, Instruction};
        use futures_util::stream;

        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("synced-repo-flow"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"test")
            .allow(dialog_capability::Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        let operator = synced_access(&profile, operator).await?;

        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: Value::String("Alice".into()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let facts: Vec<_> = branch
            .claims()
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(facts.len(), 1);

        Ok(())
    }
}
