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

use dialog_capability::{Did, Provider, Subject};
use dialog_credentials::Ed25519Signer;
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::repository as repo_fx;
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

impl Repository {
    /// Load an existing named repository.
    ///
    /// Fails if no repository with this name exists.
    pub fn load(name: impl Into<String>) -> LoadRepository {
        LoadRepository { name: name.into() }
    }

    /// Open a named repository — loads existing or creates new.
    pub fn open(name: impl Into<String>) -> OpenRepository {
        OpenRepository { name: name.into() }
    }

    /// Create a named repository — fails if it already exists.
    pub fn create(name: impl Into<String>) -> CreateRepository {
        CreateRepository { name: name.into() }
    }
}

/// Command to load an existing named repository.
pub struct LoadRepository {
    name: String,
}

impl LoadRepository {
    /// Load an existing repository by name.
    ///
    /// Returns the repository with whatever credential was stored.
    /// Fails if no repository with this name exists.
    pub async fn perform<Env>(self, env: &Env) -> Result<Repository, RepositoryError>
    where
        Env: Provider<repo_fx::Load>,
    {
        let dummy = Subject::from(dialog_capability::did!(
            "key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        ));

        let credential = dummy
            .attenuate(repo_fx::Repository)
            .attenuate(repo_fx::Name::new(&self.name))
            .invoke(repo_fx::Load)
            .perform(env)
            .await
            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

        match credential {
            Some(credential) => Ok(credential.into()),
            None => Err(RepositoryError::NotFound(self.name)),
        }
    }
}

/// Command to open a named repository — loads existing or creates new.
pub struct OpenRepository {
    name: String,
}

impl OpenRepository {
    /// Load existing or create new repository.
    ///
    /// Returns `Repository<Credential>` — a signer if created or loaded as owner.
    pub async fn perform<Env>(self, env: &Env) -> Result<Repository, RepositoryError>
    where
        Env: Provider<repo_fx::Load> + Provider<repo_fx::Save>,
    {
        let dummy = Subject::from(dialog_capability::did!(
            "key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        ));

        let credential = dummy
            .clone()
            .attenuate(repo_fx::Repository)
            .attenuate(repo_fx::Name::new(&self.name))
            .invoke(repo_fx::Load)
            .perform(env)
            .await
            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

        match credential {
            Some(credential) => Ok(credential.into()),
            None => {
                let signer = Ed25519Signer::generate()
                    .await
                    .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

                dummy
                    .attenuate(repo_fx::Repository)
                    .attenuate(repo_fx::Name::new(&self.name))
                    .invoke(repo_fx::Save::new(signer.clone().into()))
                    .perform(env)
                    .await
                    .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

                Ok(Credential::Signer(SignerCredential(signer)).into())
            }
        }
    }
}

/// Command to create a new named repository.
pub struct CreateRepository {
    name: String,
}

impl CreateRepository {
    /// Create a new repository, generating a fresh keypair.
    ///
    /// Fails if a repository with this name already exists.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<Repository<SignerCredential>, RepositoryError>
    where
        Env: Provider<repo_fx::Load> + Provider<repo_fx::Save>,
    {
        let dummy = Subject::from(dialog_capability::did!(
            "key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        ));

        let existing = dummy
            .clone()
            .attenuate(repo_fx::Repository)
            .attenuate(repo_fx::Name::new(&self.name))
            .invoke(repo_fx::Load)
            .perform(env)
            .await
            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

        if existing.is_some() {
            return Err(RepositoryError::AlreadyExists(self.name));
        }

        let signer = Ed25519Signer::generate()
            .await
            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

        dummy
            .attenuate(repo_fx::Repository)
            .attenuate(repo_fx::Name::new(&self.name))
            .invoke(repo_fx::Save::new(signer.clone().into()))
            .perform(env)
            .await
            .map_err(|e| RepositoryError::StorageError(e.to_string()))?;

        Ok(SignerCredential(signer).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::{Artifact, Instruction};
    use crate::environment::{Builder, Ucan};
    use dialog_capability::ucan::{Issuer, claim};
    use dialog_effects::storage;
    use dialog_remote_s3::Address;
    use futures_util::stream;

    async fn test_signer() -> Ed25519Signer {
        Ed25519Signer::import(&[42; 32]).await.unwrap()
    }

    fn test_address() -> RemoteAddress {
        let s3_addr = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "bucket");
        RemoteAddress::S3(s3_addr)
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
        let env = Builder::temp().build().await?;

        let repo = Repository::open("home").perform(&env).await?;
        assert!(
            !repo.subject().to_string().is_empty(),
            "should produce a valid subject DID"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reopens_same_repository() -> anyhow::Result<()> {
        let env = Builder::temp().build().await?;

        let did1 = Repository::open("home").perform(&env).await?.subject();
        let did2 = Repository::open("home").perform(&env).await?.subject();

        assert_eq!(did1, did2, "reopening should return same subject DID");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_repositories_by_name() -> anyhow::Result<()> {
        let env = Builder::temp().build().await?;

        let repo1 = Repository::open("home").perform(&env).await?;
        let repo2 = Repository::open("work").perform(&env).await?;

        assert_ne!(
            repo1.subject(),
            repo2.subject(),
            "different names should produce different subjects"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_delegates_repo_to_profile_and_claims() -> anyhow::Result<()> {
        let env = Builder::temp().grant(Ucan::unrestricted()).build().await?;
        let repo = Repository::create("home").perform(&env).await?;

        Ucan::delegate(repo.subject())
            .issuer(repo.credential().clone())
            .audience(env.authority.profile_did())
            .perform(&env)
            .await?;

        let capability = repo
            .subject()
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("data"));

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
        let env = Builder::temp().grant(Ucan::unrestricted()).build().await?;
        let repo = Repository::create("home").perform(&env).await?;

        Ucan::delegate(
            repo.subject()
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new("data")),
        )
        .audience(env.authority.profile_did())
        .issuer(repo.credential().clone())
        .perform(&env)
        .await?;

        let data_cap = repo
            .subject()
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("data"));
        let authority = env.authority.build_authority(repo.did());
        let result = claim(&env, Issuer::new(&env, authority), &data_cap).await;
        assert!(
            result.is_ok(),
            "claim on delegated store 'data' should succeed: {:?}",
            result.err()
        );

        let secret_cap = repo
            .subject()
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new("secret"));
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
        let env = Builder::temp().grant(Ucan::unrestricted()).build().await?;
        let repo = Repository::create("home").perform(&env).await?;

        Ucan::delegate(
            repo.subject()
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new("data")),
        )
        .audience(env.authority.profile_did())
        .issuer(repo.credential().clone())
        .perform(&env)
        .await?;

        let result = Ucan::delegate(
            repo.subject()
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new("data")),
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
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new("secret")),
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
