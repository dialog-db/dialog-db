//! Capability-based repository system.
//!
//! This module provides a repository abstraction built on top of the
//! capability-based effect system (`dialog-capability` / `dialog-effects`).
//!
//! - [`archive`] — CAS adapter bridging capabilities with prolly tree storage
//! - [`branch`] — Branch operations (open, load, commit, select, reset, pull)
//! - [`cell`] — Transactional memory cells with edition tracking
//! - [`revision`] — Revision tracking and logical timestamps

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
/// Provider impls for repository Load/Save capabilities.
pub mod provider;
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
pub use node_reference::*;
pub use occurence::*;
pub use remote::*;
pub use revision::*;

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
    /// The subject DID.
    pub fn did(&self) -> Did {
        self.credential.did()
    }

    /// The subject as a `Subject`.
    pub fn subject(&self) -> Subject {
        Subject::from(self.did())
    }

    /// Pre-attenuated memory capability (`Subject → Memory`).
    pub fn memory(&self) -> &Memory {
        &self.memory
    }

    /// Pre-attenuated archive capability (`Subject → Archive`).
    pub fn archive(&self) -> &Archive {
        &self.archive
    }

    /// Add a new remote site to this repository.
    pub fn add_remote(
        &self,
        name: impl Into<remote::SiteName>,
        address: RemoteAddress,
    ) -> remote::site::Open {
        remote::site::Open::new(name, address, self.memory.space("site"))
    }

    /// Load an existing remote site from this repository.
    pub fn load_remote(&self, name: impl Into<SiteName>) -> remote::site::Load {
        remote::site::Load::new(name, self.memory.space("site"))
    }

    /// Open (load or create) a branch.
    pub fn open_branch(&self, name: impl Into<branch::BranchName>) -> branch::Open {
        let trace = self.memory.trace(name);
        branch::Open::new(self.credential.did(), self.memory.clone(), trace)
    }

    /// Load an existing branch (error if not found).
    pub fn load_branch(&self, name: impl Into<branch::BranchName>) -> branch::Load {
        let trace = self.memory.trace(name);
        branch::Load::new(self.credential.did(), self.memory.clone(), trace)
    }
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

impl<C: Principal> Repository<C> {
    /// Get the credential.
    pub fn credential(&self) -> &C {
        &self.credential
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
    use super::*;
    use crate::artifacts::{Artifact, Instruction};
    use crate::environment::{Builder, Ucan};
    use crate::storage::Storage;
    use dialog_capability::ucan::{Issuer, claim};
    use dialog_effects::storage as fx_storage;
    use dialog_remote_s3::Address as S3Address;
    use futures_util::stream;

    async fn test_signer() -> Ed25519Signer {
        Ed25519Signer::import(&[42; 32]).await.unwrap()
    }

    fn test_address() -> RemoteAddress {
        let s3_addr = S3Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "bucket");
        RemoteAddress::S3(s3_addr)
    }

    /// Extract the Ed25519Signer from a Credential that is known to be a Signer variant.
    fn extract_signer(credential: &Credential) -> Ed25519Signer {
        match credential {
            Credential::Signer(s) => s.clone().into(),
            Credential::Verifier(_) => panic!("expected Signer credential"),
        }
    }

    /// Generate a unique location for test isolation.
    fn unique_location(prefix: &str) -> Capability<Location<Address>> {
        use dialog_common::time;
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = time::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        Storage::temp(&format!("{prefix}-{id}-{seq}"))
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
        let env = Builder::volatile().build().await?;
        let repo = Repository::from(test_signer().await);

        let branch = repo.open_branch("main").perform(&env).await?;

        assert_eq!(branch.name().as_str(), "main");
        assert!(
            branch.revision().is_none(),
            "New branch should have no revision"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_loads_branch_via_repository() -> anyhow::Result<()> {
        let env = Builder::volatile().build().await?;
        let repo = Repository::from(test_signer().await);

        let _branch = repo.open_branch("main").perform(&env).await?;
        let branch = repo.load_branch("main").perform(&env).await?;
        assert_eq!(branch.name().as_str(), "main");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_via_repository() -> anyhow::Result<()> {
        let env = Builder::volatile().build().await?;
        let repo = Repository::from(test_signer().await);

        let branch = repo.open_branch("main").perform(&env).await?;
        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };
        let _hash = branch
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&env)
            .await?;

        assert!(
            branch.revision().is_some(),
            "Branch should have a revision after commit"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_adds_and_loads_remote_via_repository() -> anyhow::Result<()> {
        let env = Builder::volatile().build().await?;
        let repo = Repository::from(test_signer().await);

        let site = repo
            .add_remote("origin", test_address())
            .perform(&env)
            .await?;
        assert_eq!(site.name(), "origin");

        let loaded = repo.load_remote("origin").perform(&env).await?;
        assert_eq!(loaded.name(), "origin");
        assert_eq!(loaded.address(), &test_address());

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
    async fn it_delegates_repo_to_profile_and_claims() -> anyhow::Result<()> {
        let storage = Storage::temp_storage();
        let env = Builder::temp().grant(Ucan::unrestricted()).build().await?;
        let repo = Repository::create(unique_location("home"))
            .perform(&storage)
            .await?;

        let signer = extract_signer(repo.credential());
        Ucan::delegate(repo.subject())
            .issuer(signer)
            .audience(env.authority.profile_did())
            .perform(&env)
            .await?;

        let capability = repo
            .subject()
            .attenuate(fx_storage::Storage)
            .attenuate(fx_storage::Store::new("data"));

        let authority = env.authority.build_authority(repo.did());
        let result = claim(&env, Issuer::new(&env, authority), &capability).await;
        assert!(
            result.is_ok(),
            "should find delegation chain: {:?}",
            result.err()
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_enforces_scoped_delegation_policy() -> anyhow::Result<()> {
        let storage = Storage::temp_storage();
        let env = Builder::temp().grant(Ucan::unrestricted()).build().await?;
        let repo = Repository::create(unique_location("home"))
            .perform(&storage)
            .await?;

        let signer = extract_signer(repo.credential());
        Ucan::delegate(
            repo.subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("data")),
        )
        .audience(env.authority.profile_did())
        .issuer(signer)
        .perform(&env)
        .await?;

        let data_cap = repo
            .subject()
            .attenuate(fx_storage::Storage)
            .attenuate(fx_storage::Store::new("data"));
        let authority = env.authority.build_authority(repo.did());
        let result = claim(&env, Issuer::new(&env, authority), &data_cap).await;
        assert!(
            result.is_ok(),
            "claim on delegated store 'data' should succeed: {:?}",
            result.err()
        );

        let secret_cap = repo
            .subject()
            .attenuate(fx_storage::Storage)
            .attenuate(fx_storage::Store::new("secret"));
        let authority = env.authority.build_authority(repo.did());
        let result = claim(&env, Issuer::new(&env, authority), &secret_cap).await;
        assert!(
            result.is_err(),
            "claim on non-delegated store 'secret' should be denied"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_validates_acquire_against_policy() -> anyhow::Result<()> {
        let storage = Storage::temp_storage();
        let env = Builder::temp().grant(Ucan::unrestricted()).build().await?;
        let repo = Repository::create(unique_location("home"))
            .perform(&storage)
            .await?;

        let signer = extract_signer(repo.credential());
        Ucan::delegate(
            repo.subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("data")),
        )
        .audience(env.authority.profile_did())
        .issuer(signer)
        .perform(&env)
        .await?;

        let result = Ucan::delegate(
            repo.subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("data")),
        )
        .audience(env.authority.operator_did())
        .acquire(&env)
        .await;
        assert!(
            result.is_ok(),
            "acquire for delegated store 'data' should succeed: {:?}",
            result.err()
        );

        let result = Ucan::delegate(
            repo.subject()
                .attenuate(fx_storage::Storage)
                .attenuate(fx_storage::Store::new("secret")),
        )
        .audience(env.authority.operator_did())
        .acquire(&env)
        .await;
        assert!(
            result.is_err(),
            "acquire for non-delegated store 'secret' should fail"
        );

        Ok(())
    }
}
