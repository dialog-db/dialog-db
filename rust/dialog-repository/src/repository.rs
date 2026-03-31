//! Capability-based repository system.
//!
//! This module provides a repository abstraction built on top of the
//! capability-based effect system (`dialog-capability` / `dialog-effects`).
//!
//! - [`archive`] — CAS adapter bridging capabilities with prolly tree storage
//! - [`branch`] — Branch operations (open, load, commit, select, reset, pull)
//! - [`cell`] — Transactional memory cells with edition tracking
//! - [`revision`] — Revision tracking and logical timestamps

/// Capability access and delegation for repositories.
pub mod access;
/// Archive capabilities and CAS adapters.
pub mod archive;
/// Capability-based branch operations (command pattern).
pub mod branch;
/// Cell descriptor for typed memory cell operations.
pub mod cell;
/// Repository error types.
pub mod error;
/// Memory capability wrapper (`Subject → Memory → Space → Cell`).
pub mod memory;
/// Node reference type for tree root hashes.
pub mod node_reference;
/// Occurence logical timestamp type.
pub mod occurence;
/// Remote site / repository / branch cursor hierarchy.
pub mod remote;
/// Revision type and edition tracking.
pub mod revision;

use crate::storage::LocationExt;
use dialog_capability::storage::{self as cap_storage, Location};
use dialog_capability::{Capability, Did, Policy, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_storage::provider::Address;
use dialog_varsig::Principal;

use self::archive::Archive;
use self::memory::Memory;

pub use branch::*;
pub use error::*;
pub use remote::*;

/// A repository scoped to a specific subject.
///
/// The credential type parameter determines access level:
/// - `Repository<SignerCredential>` — owns the keypair, can delegate
/// - `Repository<Credential>` — either signer or verifier, determined at runtime
pub struct Repository<C: Principal = Credential> {
    credential: C,
    memory: Memory,
    archive: Archive,
}

impl<C: Principal> Repository<C> {
    fn new(credential: C) -> Self {
        let subject = Subject::from(credential.did());
        Self {
            memory: Memory::new(subject.clone()),
            archive: Archive::new(subject),
            credential,
        }
    }

    /// Get the credential.
    pub fn credential(&self) -> &C {
        &self.credential
    }

    /// The subject DID.
    pub fn did(&self) -> Did {
        self.credential.did()
    }

    /// The subject as a `Subject`.
    pub fn subject(&self) -> Subject {
        self.did().into()
    }

    /// Pre-attenuated memory capability (`Subject → Memory`).
    pub fn memory(&self) -> &Memory {
        &self.memory
    }

    /// Pre-attenuated archive capability (`Subject → Archive`).
    pub fn archive(&self) -> &Archive {
        &self.archive
    }

    /// Delegation scope for this repository's capabilities.
    ///
    /// From here you can delegate full ownership or narrow to specific
    /// domains (archive, memory) before delegating.
    pub fn ownership(&self) -> access::Access<'_, C, Subject> {
        access::Access::new(self.subject().into(), &self.credential)
    }
}

impl From<Credential> for Repository {
    fn from(credential: Credential) -> Self {
        Self::new(credential)
    }
}

impl From<SignerCredential> for Repository<SignerCredential> {
    fn from(credential: SignerCredential) -> Self {
        Self::new(credential)
    }
}

impl From<Ed25519Signer> for Repository<SignerCredential> {
    fn from(signer: Ed25519Signer) -> Self {
        SignerCredential::from(signer).into()
    }
}

enum OpenMode {
    OpenOrCreate,
    Load,
    Create,
}

/// Command to open, load, or create a repository.
pub struct OpenRepository {
    location: Capability<Location<Address>>,
    mode: OpenMode,
}

impl Repository {
    /// Open a repository — loads existing or creates new.
    ///
    /// Use `Storage::current("name")` to get the location.
    pub fn open(location: Capability<Location<Address>>) -> OpenRepository {
        OpenRepository {
            location,
            mode: OpenMode::OpenOrCreate,
        }
    }

    /// Load an existing repository — fails if not found.
    pub fn load(location: Capability<Location<Address>>) -> OpenRepository {
        OpenRepository {
            location,
            mode: OpenMode::Load,
        }
    }

    /// Create a new repository — fails if one already exists.
    pub fn create(location: Capability<Location<Address>>) -> OpenRepository {
        OpenRepository {
            location,
            mode: OpenMode::Create,
        }
    }
}

impl OpenRepository {
    /// Execute against a storage provider.
    ///
    /// Reads credentials from `{location}/credential/space`.
    /// Mounts the repository DID at `{location}` in the storage store table.
    pub async fn perform<S>(self, storage: &S) -> Result<Repository, RepositoryError>
    where
        S: Provider<cap_storage::Load<Credential, Address>>
            + Provider<cap_storage::Save<Credential, Address>>
            + Provider<cap_storage::Mount<Address>>
            + ConditionalSync,
    {
        let location = self.location;
        let cred_location = location
            .resolve("credential/space")
            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

        let credential = match self.mode {
            OpenMode::Load => cred_location
                .load::<Credential>()
                .perform(storage)
                .await
                .map_err(|e| RepositoryError::StorageError(e.to_string()))?,
            OpenMode::Create => {
                let existing = cred_location
                    .clone()
                    .load::<Credential>()
                    .perform(storage)
                    .await;

                if existing.is_ok() {
                    return Err(RepositoryError::AlreadyExists(String::new()));
                }

                let signer = Ed25519Signer::generate()
                    .await
                    .map_err(|e| RepositoryError::StorageError(e.to_string()))?;
                let credential = Credential::Signer(SignerCredential::from(signer));

                cred_location
                    .save(credential.clone())
                    .perform(storage)
                    .await
                    .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

                credential
            }
            OpenMode::OpenOrCreate => {
                let load = cred_location
                    .clone()
                    .load::<Credential>()
                    .perform(storage)
                    .await;

                match load {
                    Ok(cred) => cred,
                    Err(_) => {
                        let signer = Ed25519Signer::generate()
                            .await
                            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;
                        let credential = Credential::Signer(SignerCredential::from(signer));

                        cred_location
                            .save(credential.clone())
                            .perform(storage)
                            .await
                            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

                        credential
                    }
                }
            }
        };

        // Mount the repository DID at the root location
        let address = Location::of(&location).address().clone();
        cap_storage::Storage::mount(credential.did(), address)
            .perform(storage)
            .await
            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

        Ok(credential.into())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::{test_operator, unique_location};
    use crate::storage::Storage;
    use crate::{Artifact, Instruction};
    use dialog_remote_s3::Address as S3Address;
    use futures_util::stream;

    fn test_remote_address() -> RemoteAddress {
        let s3_addr = S3Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "bucket");
        RemoteAddress::new(
            SiteAddress::S3(s3_addr),
            "did:key:z6MkTest".parse().unwrap(),
        )
    }

    /// Extract the Ed25519Signer from a Credential that is known to be a Signer variant.
    fn extract_signer(credential: &Credential) -> Ed25519Signer {
        match credential {
            Credential::Signer(s) => s.clone().into(),
            Credential::Verifier(_) => panic!("expected Signer credential"),
        }
    }

    #[dialog_common::test]
    async fn open_creates_and_mounts() {
        let storage = Storage::temp_storage();
        let repo = Repository::open(unique_location("open-mount"))
            .perform(&storage)
            .await
            .unwrap();

        assert!(storage.stores().contains(&repo.did()));
    }

    #[dialog_common::test]
    async fn create_then_load_mounts() {
        let storage = Storage::temp_storage();
        let location = unique_location("create-load-mount");

        let created = Repository::create(location.clone())
            .perform(&storage)
            .await
            .unwrap();
        assert!(storage.stores().contains(&created.did()));

        let loaded = Repository::load(location).perform(&storage).await.unwrap();
        assert_eq!(created.did(), loaded.did());
        assert!(storage.stores().contains(&loaded.did()));
    }

    #[dialog_common::test]
    async fn it_opens_branch_via_repository() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = Repository::open(unique_location("branch"))
            .perform(&operator)
            .await?;

        let branch = repo.branch("main").open().perform(&operator).await?;

        assert_eq!(branch.name().as_str(), "main");
        assert!(
            branch.revision().is_none(),
            "New branch should have no revision"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_loads_branch_via_repository() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = Repository::open(unique_location("load-branch"))
            .perform(&operator)
            .await?;

        let _branch = repo.branch("main").open().perform(&operator).await?;
        let branch = repo.branch("main").load().perform(&operator).await?;
        assert_eq!(branch.name().as_str(), "main");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_via_repository() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = Repository::open(unique_location("commit"))
            .perform(&operator)
            .await?;

        let branch = repo.branch("main").open().perform(&operator).await?;
        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };
        let _hash = branch
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&operator)
            .await?;

        assert!(
            branch.revision().is_some(),
            "Branch should have a revision after commit"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_adds_and_loads_remote_via_repository() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let space = Repository::open(unique_location("remote"))
            .perform(&operator)
            .await?;

        let site = space
            .remote("origin")
            .create(test_remote_address())
            .perform(&operator)
            .await?;
        assert_eq!(site.name(), "origin");

        let loaded = space.remote("origin").load().perform(&operator).await?;
        assert_eq!(loaded.name(), "origin");
        assert_eq!(loaded.address(), test_remote_address());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_opens_repository_by_name() -> anyhow::Result<()> {
        let storage = Storage::temp_storage();

        let repo = Repository::open(unique_location("home"))
            .perform(&storage)
            .await?;
        assert!(
            !repo.subject().to_string().is_empty(),
            "should produce a valid subject DID"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reopens_same_repository() -> anyhow::Result<()> {
        let storage = Storage::temp_storage();
        let location = unique_location("home");

        let did1 = Repository::open(location.clone())
            .perform(&storage)
            .await?
            .subject();
        let did2 = Repository::open(location)
            .perform(&storage)
            .await?
            .subject();

        assert_eq!(did1, did2, "reopening should return same subject DID");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_repositories_by_name() -> anyhow::Result<()> {
        let storage = Storage::temp_storage();

        let repo1 = Repository::open(unique_location("home"))
            .perform(&storage)
            .await?;
        let repo2 = Repository::open(unique_location("work"))
            .perform(&storage)
            .await?;

        assert_ne!(
            repo1.subject(),
            repo2.subject(),
            "different names should produce different subjects"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_and_selects_by_attribute() -> anyhow::Result<()> {
        use crate::ArtifactSelector;
        use futures_util::StreamExt;

        let operator = test_operator().await;
        let repo = Repository::open(unique_location("select-attr"))
            .perform(&operator)
            .await?;

        let branch = repo.branch("main").open().perform(&operator).await?;

        let artifacts = vec![
            Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: crate::Value::String("Alice".into()),
                cause: None,
            }),
            Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:1".parse()?,
                is: crate::Value::String("alice@example.com".into()),
                cause: None,
            }),
            Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:2".parse()?,
                is: crate::Value::String("Bob".into()),
                cause: None,
            }),
        ];

        branch
            .commit(stream::iter(artifacts))
            .perform(&operator)
            .await?;

        let results: Vec<_> = branch
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(results.len(), 2, "should find 2 user/name artifacts");
        let names: Vec<_> = results.iter().map(|a| &a.is).collect();
        assert!(
            names.contains(&&crate::Value::String("Alice".into())),
            "should contain Alice"
        );
        assert!(
            names.contains(&&crate::Value::String("Bob".into())),
            "should contain Bob"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_and_selects_by_entity() -> anyhow::Result<()> {
        use crate::ArtifactSelector;
        use futures_util::StreamExt;

        let operator = test_operator().await;
        let repo = Repository::open(unique_location("select-entity"))
            .perform(&operator)
            .await?;

        let branch = repo.branch("main").open().perform(&operator).await?;

        let artifacts = vec![
            Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:alice".parse()?,
                is: crate::Value::String("Alice".into()),
                cause: None,
            }),
            Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:bob".parse()?,
                is: crate::Value::String("Bob".into()),
                cause: None,
            }),
            Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:alice".parse()?,
                is: crate::Value::String("alice@example.com".into()),
                cause: None,
            }),
        ];

        branch
            .commit(stream::iter(artifacts))
            .perform(&operator)
            .await?;

        let results: Vec<_> = branch
            .select(ArtifactSelector::new().of("user:alice".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(results.len(), 2, "should find 2 artifacts for user:alice");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_selects_empty_branch() -> anyhow::Result<()> {
        use crate::ArtifactSelector;
        use futures_util::StreamExt;

        let operator = test_operator().await;
        let repo = Repository::open(unique_location("select-empty"))
            .perform(&operator)
            .await?;

        let branch = repo.branch("main").open().perform(&operator).await?;

        let results: Vec<_> = branch
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(results.len(), 0, "empty branch should have no artifacts");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_artifact() -> anyhow::Result<()> {
        use crate::ArtifactSelector;
        use futures_util::StreamExt;

        let operator = test_operator().await;
        let repo = Repository::open(unique_location("retract"))
            .perform(&operator)
            .await?;

        let branch = repo.branch("main").open().perform(&operator).await?;

        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:1".parse()?,
            is: crate::Value::String("Alice".into()),
            cause: None,
        };

        branch
            .commit(stream::iter(vec![Instruction::Assert(artifact.clone())]))
            .perform(&operator)
            .await?;

        // Verify it's there
        let before: Vec<_> = branch
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(before.len(), 1, "should have 1 artifact before retract");

        // Retract it
        branch
            .commit(stream::iter(vec![Instruction::Retract(artifact)]))
            .perform(&operator)
            .await?;

        let after: Vec<_> = branch
            .select(ArtifactSelector::new().the("user/name".parse()?))
            .perform(&operator)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(after.len(), 0, "should have 0 artifacts after retract");

        Ok(())
    }

    #[cfg(feature = "ucan")]
    mod delegation_tests {
        use super::*;
        use crate::helpers::test_operator;
        use dialog_capability::ucan::{Issuer, Ucan, claim, import_delegation_chain};
        use dialog_effects::storage as fx_storage;

        #[dialog_common::test]
        async fn it_delegates_repo_to_profile_and_claims() -> anyhow::Result<()> {
            let operator = test_operator().await;
            let repo = Repository::create(unique_location("home"))
                .perform(&operator)
                .await?;

            let signer = extract_signer(repo.credential());
            let chain = Ucan::delegate(&repo.subject())
                .issuer(signer)
                .audience(operator.profile_did())
                .perform(&operator)
                .await?;
            import_delegation_chain(&operator, &operator.profile_did(), &chain).await?;

            let capability = repo
                .subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("data"));

            let authority = operator.build_authority(repo.did());
            let result = claim(&operator, Issuer::new(&operator, authority), &capability).await;
            assert!(
                result.is_ok(),
                "should find delegation chain: {:?}",
                result.err()
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_enforces_scoped_delegation_policy() -> anyhow::Result<()> {
            let operator = test_operator().await;
            let repo = Repository::create(unique_location("home"))
                .perform(&operator)
                .await?;

            let signer = extract_signer(repo.credential());
            let scoped_cap = repo
                .subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("data"));
            let chain = Ucan::delegate(&scoped_cap)
                .audience(operator.profile_did())
                .issuer(signer)
                .perform(&operator)
                .await?;
            import_delegation_chain(&operator, &operator.profile_did(), &chain).await?;

            let data_cap = repo
                .subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("data"));
            let authority = operator.build_authority(repo.did());
            let result = claim(&operator, Issuer::new(&operator, authority), &data_cap).await;
            assert!(
                result.is_ok(),
                "claim on delegated store 'data' should succeed: {:?}",
                result.err()
            );

            let secret_cap = repo
                .subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("secret"));
            let authority = operator.build_authority(repo.did());
            let result = claim(&operator, Issuer::new(&operator, authority), &secret_cap).await;
            assert!(
                result.is_err(),
                "claim on non-delegated store 'secret' should be denied"
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_validates_delegation_against_policy() -> anyhow::Result<()> {
            let operator = test_operator().await;
            let repo = Repository::create(unique_location("home"))
                .perform(&operator)
                .await?;

            let signer = extract_signer(repo.credential());
            let scoped_cap = repo
                .subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("data"));
            let chain = Ucan::delegate(&scoped_cap)
                .audience(operator.profile_did())
                .issuer(signer)
                .perform(&operator)
                .await?;
            import_delegation_chain(&operator, &operator.profile_did(), &chain).await?;

            // Delegating for 'data' store should succeed (chain exists)
            let data_cap = repo
                .subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("data"));
            let result = Ucan::delegate(&data_cap)
                .audience(operator.did())
                .perform(&operator)
                .await;
            assert!(
                result.is_ok(),
                "delegation for store 'data' should succeed: {:?}",
                result.err()
            );

            // Delegating for 'secret' store should fail (no chain)
            let secret_cap = repo
                .subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("secret"));
            let result = Ucan::delegate(&secret_cap)
                .audience(operator.did())
                .perform(&operator)
                .await;
            assert!(
                result.is_err(),
                "delegation for non-delegated store 'secret' should fail"
            );

            Ok(())
        }
    }
}
