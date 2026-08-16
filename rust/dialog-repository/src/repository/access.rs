//! The profile's access branch, and migration into it.
//!
//! A profile's retained delegations live in the profile repository's own
//! branch, named [`ACCESS_BRANCH`]. The operator (in `dialog-operator`,
//! above this crate) opens it at build time and serves every proof from
//! its `dialog.ucan/*` facts; cross-party delegations retained there
//! replicate across the profile's replicas by ordinary push/pull.
//!
//! [`MigrateAccess::migrate`] moves a profile's legacy certificate store
//! into the branch, once, explicitly:
//!
//! ```no_run
//! # use dialog_identity::Profile;
//! # use dialog_repository::MigrateAccess as _;
//! # use dialog_storage::provider::storage::{Storage, VolatileSpace};
//! # async fn example(profile: &Profile, storage: &Storage<VolatileSpace>) -> anyhow::Result<()> {
//! profile.access().migrate().perform(storage).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Three rules govern it:
//!
//! - **Self-issued certificates are skipped.** Anything the profile signed
//!   it can re-sign on demand — the operator's in-memory session grants
//!   are exactly these — and a self-issued certificate in one's own store
//!   never helps one prove anything. This is what keeps the field-observed
//!   accumulation (one persisted session grant per build, forever) out of
//!   the synced store.
//! - **Interchangeable certificates compact.** Two certificates differing
//!   only by nonce are interchangeable as proofs, so one representative
//!   per semantic payload (issuer, audience, subject, command, policy,
//!   validity window) is retained.
//! - **What migrated is drained; what was skipped stays.** The migrated
//!   certificates (representatives and their duplicates) are removed from
//!   the legacy store on success; skipped self-issued ones are left for
//!   any not-yet-upgraded code path. A rerun after a partial failure
//!   re-skips by content address and drains the remainder.

use std::collections::BTreeMap;

use crate::RemoteSite;
use dialog_artifacts::Entity;
use dialog_capability::access::{
    Access as AccessAttenuation, AuthorizeError, Certificate as _, Export, Forget,
};
use dialog_capability::{Command, Fork, Provider, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::Write as BlobWrite;
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::{Publish, Resolve};
use dialog_identity::Authority;
use dialog_identity::access::Access;
use dialog_storage::provider::space::SpaceProvider;
use dialog_storage::provider::storage::Storage;
use dialog_ucan::{Ucan, UcanCertificate, UcanDelegation};
use dialog_ucan_core::DelegationChain;
use dialog_varsig::Principal as _;

/// The branch of the profile's own repository that retains delegations.
pub const ACCESS_BRANCH: &str = "main";

/// Migrate a profile's legacy certificate store into its access branch.
///
/// An extension trait rather than an inherent method because [`Access`]
/// lives in `dialog-identity`, below this crate, and cannot name the
/// branch machinery.
pub trait MigrateAccess {
    /// Build the migration command; execute with
    /// [`perform`](MigrateCertificates::perform) against the storage.
    fn migrate(&self) -> MigrateCertificates;
}

impl MigrateAccess for Access<'_> {
    fn migrate(&self) -> MigrateCertificates {
        MigrateCertificates {
            credential: self.signer().signer().clone(),
        }
    }
}

/// The migration command. Created by [`MigrateAccess::migrate`].
pub struct MigrateCertificates {
    credential: dialog_credentials::Signer,
}

/// The environment migration commits with: the profile identifies and
/// attests for itself (the migration revision's issuer is the profile
/// key), storage serves the local effects, and the remote fork providers
/// are stubs — migration is a local operation.
struct MigrateEnv<S: Clone> {
    authority: Authority,
    storage: Storage<S>,
}

macro_rules! migrate_storage {
    ($($effect:ty),+ $(,)?) => {$(
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<S> Provider<$effect> for MigrateEnv<S>
        where
            S: Clone + ConditionalSend + ConditionalSync + 'static,
            <$effect as Command>::Input: ConditionalSend,
            Storage<S>: Provider<$effect>,
            Self: ConditionalSync,
        {
            async fn execute(
                &self,
                input: <$effect as Command>::Input,
            ) -> <$effect as Command>::Output {
                Provider::<$effect>::execute(&self.storage, input).await
            }
        }
    )+};
}

macro_rules! migrate_authority {
    ($($effect:ty),+ $(,)?) => {$(
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl<S> Provider<$effect> for MigrateEnv<S>
        where
            S: Clone + ConditionalSend + ConditionalSync + 'static,
            <$effect as Command>::Input: ConditionalSend,
            Authority: Provider<$effect>,
            Self: ConditionalSync,
        {
            async fn execute(
                &self,
                input: <$effect as Command>::Input,
            ) -> <$effect as Command>::Output {
                Provider::<$effect>::execute(&self.authority, input).await
            }
        }
    )+};
}

migrate_storage!(
    Get, Put, Import, Resolve, Publish, BlobRead, BlobWrite, BlobImport
);
migrate_authority!(Identify, Attest);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Fork<RemoteSite, Get>> for MigrateEnv<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: ConditionalSync,
{
    async fn execute(
        &self,
        _input: <Fork<RemoteSite, Get> as Command>::Input,
    ) -> <Fork<RemoteSite, Get> as Command>::Output {
        Ok(None)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Fork<RemoteSite, Resolve>> for MigrateEnv<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: ConditionalSync,
{
    async fn execute(
        &self,
        _input: <Fork<RemoteSite, Resolve> as Command>::Input,
    ) -> <Fork<RemoteSite, Resolve> as Command>::Output {
        Ok(None)
    }
}

/// The compaction key: everything a certificate states except its nonce.
/// Two certificates sharing it are interchangeable as proofs.
fn semantic_key(certificate: &UcanCertificate) -> Result<Vec<u8>, AuthorizeError> {
    let policy = serde_ipld_dagcbor::to_vec(certificate.0.policy()).map_err(|error| {
        AuthorizeError::Malformed {
            detail: format!("unencodable policy: {error}"),
        }
    })?;
    let mut key = format!(
        "{}|{}|{}|{}|{:?}|{:?}|",
        certificate.issuer(),
        certificate.audience(),
        certificate
            .subject()
            .map(|did| did.to_string())
            .unwrap_or_else(|| "_".to_string()),
        certificate.0.command(),
        certificate.0.not_before().map(|t| t.to_unix()),
        certificate.0.expiration().map(|t| t.to_unix()),
    )
    .into_bytes();
    key.extend(policy);
    Ok(key)
}

impl MigrateCertificates {
    /// Execute the migration, returning the entity of every delegation
    /// record it retained (empty when the legacy store held nothing to
    /// migrate).
    pub async fn perform<S>(self, storage: &Storage<S>) -> Result<Vec<Entity>, AuthorizeError>
    where
        S: SpaceProvider
            + Provider<BlobRead>
            + Provider<BlobWrite>
            + Provider<BlobImport>
            + Clone
            + 'static,
        Storage<S>: Provider<Export<Ucan>> + Provider<Forget<Ucan>> + ConditionalSync,
    {
        let profile_did = self.credential.did();

        // Everything the legacy store retains.
        let certificates = Subject::from(profile_did.clone())
            .attenuate(AccessAttenuation)
            .invoke(Export::<Ucan>::new())
            .perform(storage)
            .await?;

        // Skip self-issued certificates: regenerable on demand, never
        // useful in one's own store.
        let migratable: Vec<UcanCertificate> = certificates
            .into_iter()
            .filter(|certificate| certificate.issuer() != &profile_did)
            .collect();
        if migratable.is_empty() {
            return Ok(Vec::new());
        }

        // Compact: one representative per semantic payload.
        let mut classes: BTreeMap<Vec<u8>, UcanCertificate> = BTreeMap::new();
        for certificate in &migratable {
            classes
                .entry(semantic_key(certificate)?)
                .or_insert_with(|| certificate.clone());
        }

        // Retain the representatives into the access branch as one commit,
        // with the profile itself as the revision's issuer.
        let env = MigrateEnv {
            authority: Authority::new("profile", self.credential.clone(), self.credential.clone()),
            storage: storage.clone(),
        };
        let repository = crate::Repository::from(self.credential.clone());
        let branch = repository
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&env)
            .await
            .map_err(|error| AuthorizeError::Malformed {
                detail: format!("failed to open the access branch: {error}"),
            })?;
        let chains = classes
            .into_values()
            .map(|certificate| UcanDelegation::new(DelegationChain::new(certificate.0)))
            .collect();
        let retained = branch
            .delegations()
            .retain_all(chains)
            .perform(&env)
            .await
            .map_err(|error| AuthorizeError::Malformed {
                detail: format!("failed to retain migrated certificates: {error}"),
            })?;

        // Drain exactly what migrated (representatives and duplicates
        // alike; every migratable certificate's content is represented in
        // the branch). Skipped self-issued certificates stay behind.
        Subject::from(profile_did)
            .attenuate(AccessAttenuation)
            .invoke(Forget::<Ucan>::new(migratable))
            .perform(storage)
            .await?;

        Ok(retained)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::DELEGATION_AUDIENCE;
    use anyhow::Result;
    use dialog_artifacts::{ArtifactSelector, Value};
    use dialog_capability::access::Retain;
    use dialog_credentials::Ed25519Signer;
    use dialog_identity::Profile;
    use dialog_operator::helpers::unique_name;
    use dialog_storage::provider::storage::{Storage, VolatileSpace};
    use dialog_ucan_core::DelegationBuilder;
    use dialog_ucan_core::subject::Subject as UcanSubject;
    use futures_util::StreamExt as _;

    async fn signer() -> Ed25519Signer {
        Ed25519Signer::generate().await.unwrap()
    }

    /// Save a delegation into the LEGACY certificate store, storage-routed.
    async fn seed_legacy(
        storage: &Storage<VolatileSpace>,
        profile: &Profile,
        issuer: &Ed25519Signer,
        audience: dialog_capability::Did,
        subject: UcanSubject,
    ) -> UcanCertificate {
        let delegation = DelegationBuilder::new()
            .issuer(issuer.clone())
            .audience(&audience)
            .subject(subject)
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();
        let chain = UcanDelegation::new(DelegationChain::new(delegation.clone()));
        Subject::from(profile.did())
            .attenuate(AccessAttenuation)
            .invoke(Retain::<Ucan>::new(chain))
            .perform(storage)
            .await
            .unwrap();
        UcanCertificate(delegation)
    }

    async fn export(storage: &Storage<VolatileSpace>, profile: &Profile) -> Vec<UcanCertificate> {
        Subject::from(profile.did())
            .attenuate(AccessAttenuation)
            .invoke(Export::<Ucan>::new())
            .perform(storage)
            .await
            .unwrap()
    }

    #[dialog_common::test]
    async fn it_migrates_compacts_and_drains() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("migrate"))
            .perform(&storage)
            .await?;
        let space = signer().await;
        let other_space = signer().await;
        let profile_signer = profile.signer().signer().as_ed25519().unwrap().clone();

        // Legacy store: two interchangeable space->profile grants (same
        // payload, different nonce), one distinct grant, and one
        // self-issued session-style grant.
        seed_legacy(
            &storage,
            &profile,
            &space,
            profile.did(),
            UcanSubject::Specific(space.did()),
        )
        .await;
        seed_legacy(
            &storage,
            &profile,
            &space,
            profile.did(),
            UcanSubject::Specific(space.did()),
        )
        .await;
        seed_legacy(
            &storage,
            &profile,
            &other_space,
            profile.did(),
            UcanSubject::Specific(other_space.did()),
        )
        .await;
        let session_like = seed_legacy(
            &storage,
            &profile,
            &profile_signer,
            space.did(),
            UcanSubject::Specific(profile.did()),
        )
        .await;
        assert_eq!(export(&storage, &profile).await.len(), 4);

        let retained = profile.access().migrate().perform(&storage).await?;
        assert_eq!(
            retained.len(),
            2,
            "two semantic classes migrate; the duplicate compacts away"
        );

        // The legacy store keeps only the skipped self-issued certificate.
        let remaining = export(&storage, &profile).await;
        assert_eq!(remaining.len(), 1, "migrated certificates drained");
        assert_eq!(remaining[0].issuer(), session_like.issuer());

        // The branch holds the migrated delegation records.
        let env = MigrateEnv {
            authority: Authority::new("profile", profile_signer.clone(), profile_signer.clone()),
            storage: storage.clone(),
        };
        let branch = crate::Repository::from(profile_signer.clone())
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&env)
            .await?;
        let facts: Vec<_> = branch
            .claims()
            .select(
                ArtifactSelector::new()
                    .the(DELEGATION_AUDIENCE.parse()?)
                    .is(Value::String(profile.did().to_string())),
            )
            .perform(&env)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(facts.len(), 2, "one record per semantic class");

        // A rerun migrates nothing and leaves the survivor in place.
        let again = profile.access().migrate().perform(&storage).await?;
        assert!(again.is_empty(), "rerun is a no-op");
        assert_eq!(export(&storage, &profile).await.len(), 1);

        Ok(())
    }

    /// The acceptance criterion: a delegation that lived in the legacy
    /// store proves through an operator built AFTER migration — the
    /// operator's walk finds it in the access branch and composes its own
    /// session link on top.
    #[dialog_common::test]
    async fn it_proves_migrated_delegations_through_a_fresh_operator() -> Result<()> {
        use dialog_capability::access::{Proof as _, Prove, TimeRange};
        use dialog_operator::DeriveOperator as _;
        use dialog_ucan::Scope;
        use dialog_ucan_core::command::Command as UcanCommand;

        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("migrate-prove"))
            .perform(&storage)
            .await?;
        let space = signer().await;

        // Legacy: space grants the profile.
        seed_legacy(
            &storage,
            &profile,
            &space,
            profile.did(),
            UcanSubject::Specific(space.did()),
        )
        .await;

        profile.access().migrate().perform(&storage).await?;

        let operator = profile
            .derive(b"test")
            .allow(dialog_capability::Subject::any())
            .network(dialog_network::Network::default())
            .build(storage)
            .await?;

        let mut claim = Prove::<Ucan>::new(
            operator.did(),
            Scope {
                subject: UcanSubject::Specific(space.did()),
                command: UcanCommand(vec!["storage".to_string()]),
                parameters: dialog_ucan::Parameters::default(),
            },
        );
        claim.duration = TimeRange::unbounded();
        let proof = Subject::from(profile.did())
            .attenuate(AccessAttenuation)
            .invoke(claim)
            .perform(&operator)
            .await?;
        assert_eq!(
            proof.proofs().len(),
            2,
            "migrated space->profile chain composes with the session link"
        );
        Ok(())
    }

    /// Migration over the FILESYSTEM certificate store: the layout native
    /// users actually hold. Exercises the fs `export` walk and `forget`
    /// deletion end to end.
    // Native only: `Storage::temp()` needs a real temp directory and does
    // not exist on wasm. Both gates are needed here (unlike the tests in
    // the feature-gated integration module): the target gate keeps the
    // test out of plain wasm builds, and the feature gate keeps the
    // macro's native wrapper from launching a test the wasm side lacks.
    #[cfg(all(not(target_arch = "wasm32"), not(feature = "web-integration-tests")))]
    #[dialog_common::test]
    async fn it_migrates_the_filesystem_store() -> Result<()> {
        let storage = Storage::temp();
        let profile = Profile::open(unique_name("migrate-fs"))
            .perform(&storage)
            .await?;
        let space = signer().await;
        let profile_signer = profile.signer().signer().as_ed25519().unwrap().clone();

        // One migratable grant, one self-issued survivor.
        let seed = |issuer: Ed25519Signer, audience: dialog_capability::Did, subject| {
            let storage = &storage;
            let profile = &profile;
            async move {
                let delegation = DelegationBuilder::new()
                    .issuer(issuer)
                    .audience(&audience)
                    .subject(subject)
                    .command(vec!["storage".to_string()])
                    .try_build()
                    .await
                    .unwrap();
                let chain = UcanDelegation::new(DelegationChain::new(delegation));
                Subject::from(profile.did())
                    .attenuate(AccessAttenuation)
                    .invoke(Retain::<Ucan>::new(chain))
                    .perform(storage)
                    .await
                    .unwrap();
            }
        };
        seed(
            space.clone(),
            profile.did(),
            UcanSubject::Specific(space.did()),
        )
        .await;
        seed(
            profile_signer.clone(),
            space.did(),
            UcanSubject::Specific(profile.did()),
        )
        .await;

        let retained = profile.access().migrate().perform(&storage).await?;
        assert_eq!(retained.len(), 1, "the cross-party grant migrates");

        let remaining = Subject::from(profile.did())
            .attenuate(AccessAttenuation)
            .invoke(Export::<Ucan>::new())
            .perform(&storage)
            .await?;
        assert_eq!(remaining.len(), 1, "the fs files drained on migration");
        assert_eq!(remaining[0].issuer(), &profile.did());
        Ok(())
    }

    /// The crash window between retain and drain: the branch already holds
    /// the delegation but the legacy store was never drained. A rerun
    /// retains nothing new (content-addressed) and still completes the
    /// drain.
    #[dialog_common::test]
    async fn it_completes_the_drain_on_rerun() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("migrate-rerun"))
            .perform(&storage)
            .await?;
        let space = signer().await;
        let profile_signer = profile.signer().signer().as_ed25519().unwrap().clone();

        let certificate = seed_legacy(
            &storage,
            &profile,
            &space,
            profile.did(),
            UcanSubject::Specific(space.did()),
        )
        .await;

        // Simulate the crash: the branch already retained the delegation
        // (as a completed first attempt would have), but the legacy store
        // still holds it.
        let env = MigrateEnv {
            authority: Authority::new("profile", profile_signer.clone(), profile_signer.clone()),
            storage: storage.clone(),
        };
        let branch = crate::Repository::from(profile_signer.clone())
            .branch(ACCESS_BRANCH)
            .open()
            .perform(&env)
            .await?;
        branch
            .delegations()
            .retain(UcanDelegation::new(DelegationChain::new(
                certificate.0.clone(),
            )))
            .perform(&env)
            .await?;
        assert_eq!(export(&storage, &profile).await.len(), 1);

        // The rerun retains nothing new but must still drain.
        let retained = profile.access().migrate().perform(&storage).await?;
        assert!(retained.is_empty(), "already retained: nothing new");
        assert!(
            export(&storage, &profile).await.is_empty(),
            "the rerun completes the drain"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_is_a_noop_on_an_empty_store() -> Result<()> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("migrate-empty"))
            .perform(&storage)
            .await?;
        let retained = profile.access().migrate().perform(&storage).await?;
        assert!(retained.is_empty());
        Ok(())
    }
}
