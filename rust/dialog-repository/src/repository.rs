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
/// Command to create a new repository.
mod create;
/// Repository error types.
pub mod error;
/// Command to load an existing repository.
mod load;
/// Memory capability wrapper (`Subject -> Memory -> Space -> Cell`).
pub mod memory;
/// Node reference type for tree root hashes.
pub mod node_reference;
/// Occurence logical timestamp type.
pub mod occurence;
/// Command to open (load-or-create) a repository.
mod open;
/// Remote site / repository / branch cursor hierarchy.
pub mod remote;
/// Revision type and edition tracking.
pub mod revision;

use dialog_capability::{Capability, Did, Subject};
use dialog_credentials::credential::{Credential, SignerCredential};
use dialog_effects::space as space_fx;
use dialog_operator::profile::access::Access as ProfileAccess;
use dialog_varsig::Principal;

pub use branch::*;
pub use create::CreateRepository;
pub use error::*;
pub use load::LoadRepository;
pub use open::OpenRepository;
pub use remote::*;

/// A repository scoped to a specific subject.
///
/// The credential type parameter determines access level:
/// - `Repository<SignerCredential>` -- owns the keypair, can delegate
/// - `Repository<Credential>` -- either signer or verifier, determined at runtime
pub struct Repository<C: Principal = Credential> {
    credential: C,
}

impl<C: Principal> Repository<C> {
    fn new(credential: C) -> Self {
        Self { credential }
    }

    /// Get the credential.
    pub fn credential(&self) -> &C {
        &self.credential
    }

    /// The subject DID.
    pub fn did(&self) -> Did {
        self.credential.did()
    }

    /// The subject.
    pub fn subject(&self) -> Subject {
        self.did().into()
    }

    /// Get a branch reference for the given name.
    ///
    /// Call `.open()` or `.load()` on the returned reference.
    pub fn branch(&self, name: impl Into<branch::BranchName>) -> branch::BranchReference {
        use memory::MemoryExt;
        self.subject().branch(name)
    }

    /// Get a remote reference for the given name.
    ///
    /// Call `.create(address)` or `.load()` on the returned reference.
    pub fn remote(&self, name: impl Into<remote::RemoteName>) -> remote::RemoteReference {
        use memory::MemoryExt;
        self.subject().remote(name)
    }
}

impl<C: Principal> Principal for Repository<C> {
    fn did(&self) -> Did {
        self.credential.did()
    }
}

impl<C: Principal> From<&Repository<C>> for Capability<Subject> {
    fn from(r: &Repository<C>) -> Self {
        Subject::from(r.did()).into()
    }
}

impl Repository<SignerCredential> {
    /// Access handle for claiming and delegating capabilities.
    pub fn access(&self) -> ProfileAccess<'_> {
        ProfileAccess::new(&self.credential)
    }
}

impl Repository {
    /// Access handle for claiming and delegating capabilities.
    ///
    /// Returns `None` if the credential is verifier-only.
    pub fn try_access(&self) -> Option<ProfileAccess<'_>> {
        match &self.credential {
            Credential::Signer(s) => Some(ProfileAccess::new(s)),
            Credential::Verifier(_) => None,
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

impl TryFrom<Credential> for Repository<SignerCredential> {
    type Error = RepositoryError;

    fn try_from(credential: Credential) -> Result<Self, RepositoryError> {
        match credential {
            Credential::Signer(s) => Ok(Self::new(s)),
            Credential::Verifier(_) => Err(RepositoryError::StorageError(
                "repository credential is verifier-only".into(),
            )),
        }
    }
}

impl From<dialog_credentials::Ed25519Signer> for Repository<SignerCredential> {
    fn from(signer: dialog_credentials::Ed25519Signer) -> Self {
        SignerCredential::from(signer).into()
    }
}

use dialog_operator::profile::SpaceHandle;

/// Extension trait for opening repositories from a [`SpaceHandle`].
///
/// Enables `profile.repository("name").open().perform(&operator)`.
pub trait RepositoryExt {
    /// Open or create a repository, loading existing or creating new.
    fn open(self) -> OpenRepository;

    /// Load an existing repository, failing if not found.
    fn load(self) -> LoadRepository;

    /// Create a new repository, failing if one already exists.
    fn create(self) -> CreateRepository;
}

impl RepositoryExt for SpaceHandle {
    fn open(self) -> OpenRepository {
        OpenRepository(Subject::from(self.profile_did).attenuate(space_fx::Space::new(self.name)))
    }

    fn load(self) -> LoadRepository {
        LoadRepository(Subject::from(self.profile_did).attenuate(space_fx::Space::new(self.name)))
    }

    fn create(self) -> CreateRepository {
        CreateRepository(Subject::from(self.profile_did).attenuate(space_fx::Space::new(self.name)))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::{test_operator_with_profile, test_repo, unique_name};
    use crate::{Artifact, Instruction};
    use dialog_remote_s3::Address as S3Address;
    use futures_util::stream;

    fn test_site_address() -> SiteAddress {
        SiteAddress::S3(
            S3Address::builder("https://s3.us-east-1.amazonaws.com")
                .region("us-east-1")
                .bucket("bucket")
                .build()
                .unwrap(),
        )
    }

    #[dialog_common::test]
    async fn open_creates_repository() {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = profile
            .repository(unique_name("open"))
            .open()
            .perform(&operator)
            .await
            .unwrap();

        assert!(!repo.did().to_string().is_empty());
    }

    #[dialog_common::test]
    async fn create_then_load() {
        let (operator, profile) = test_operator_with_profile().await;
        let name = unique_name("create-load");

        let created = profile
            .repository(name.clone())
            .create()
            .perform(&operator)
            .await
            .unwrap();

        let loaded = profile
            .repository(name)
            .load()
            .perform(&operator)
            .await
            .unwrap();
        assert_eq!(created.did(), loaded.did());
    }

    #[dialog_common::test]
    async fn it_opens_branch_via_repository() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let _branch = repo.branch("main").open().perform(&operator).await?;
        let branch = repo.branch("main").load().perform(&operator).await?;
        assert_eq!(branch.name().as_str(), "main");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_via_repository() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let site = repo
            .remote("origin")
            .create(test_site_address())
            .perform(&operator)
            .await?;
        assert_eq!(site.name(), "origin");

        let loaded = repo.remote("origin").load().perform(&operator).await?;
        assert_eq!(loaded.name(), "origin");
        assert_eq!(loaded.address().site(), &test_site_address());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_opens_repository_by_name() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;

        let repo = profile
            .repository(unique_name("home"))
            .open()
            .perform(&operator)
            .await?;
        assert!(
            !repo.subject().to_string().is_empty(),
            "should produce a valid subject DID"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reopens_same_repository() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let name = unique_name("home");

        let did1 = profile
            .repository(name.clone())
            .open()
            .perform(&operator)
            .await?
            .subject();
        let did2 = profile
            .repository(name)
            .open()
            .perform(&operator)
            .await?
            .subject();

        assert_eq!(did1, did2, "reopening should return same subject DID");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_isolates_repositories_by_name() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;

        let repo1 = profile
            .repository(unique_name("home"))
            .open()
            .perform(&operator)
            .await?;
        let repo2 = profile
            .repository(unique_name("work"))
            .open()
            .perform(&operator)
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

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

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
            .claims()
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

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

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
            .claims()
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

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("main").open().perform(&operator).await?;

        let results: Vec<_> = branch
            .claims()
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

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

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
            .claims()
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
            .claims()
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

    mod delegation_tests {
        use super::*;
        use crate::helpers::{test_operator_with_profile, unique_name};
        use dialog_effects::memory as fx_memory;

        #[dialog_common::test]
        async fn it_delegates_repo_to_profile_and_claims() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = profile
                .repository(unique_name("home"))
                .create()
                .perform(&operator)
                .await?;

            // Repo delegates full ownership to the profile
            let chain = repo
                .access()
                .claim(&repo)
                .delegate(profile.did())
                .perform(&operator)
                .await?;
            profile.access().save(chain).perform(&operator).await?;

            // Profile should be able to claim access to any memory space
            let capability = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));

            let result = profile.access().claim(capability).perform(&operator).await;
            assert!(
                result.is_ok(),
                "should find delegation chain: {:?}",
                result.err()
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_enforces_scoped_delegation_policy() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = profile
                .repository(unique_name("home"))
                .create()
                .perform(&operator)
                .await?;

            // Repo delegates only memory/space("data") to the profile
            let scoped_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));
            let chain = repo
                .access()
                .claim(scoped_cap)
                .delegate(profile.did())
                .perform(&operator)
                .await?;
            profile.access().save(chain).perform(&operator).await?;

            // Claiming "data" space should succeed
            let data_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));
            let result = profile.access().claim(data_cap).perform(&operator).await;
            assert!(
                result.is_ok(),
                "claim on delegated space 'data' should succeed: {:?}",
                result.err()
            );

            // Claiming "secret" space should fail
            let secret_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("secret"));
            let result = profile.access().claim(secret_cap).perform(&operator).await;
            assert!(
                result.is_err(),
                "claim on non-delegated space 'secret' should be denied"
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_validates_delegation_against_policy() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = profile
                .repository(unique_name("home"))
                .create()
                .perform(&operator)
                .await?;

            // Repo delegates memory/space("data") to the profile
            let scoped_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));
            let chain = repo
                .access()
                .claim(scoped_cap)
                .delegate(profile.did())
                .perform(&operator)
                .await?;
            profile.access().save(chain).perform(&operator).await?;

            // Profile can re-delegate "data" space to operator
            let data_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("data"));
            let result = profile
                .access()
                .claim(data_cap)
                .delegate(operator.did())
                .perform(&operator)
                .await;
            assert!(
                result.is_ok(),
                "delegation for space 'data' should succeed: {:?}",
                result.err()
            );

            // Profile cannot delegate "secret" space (no chain)
            let secret_cap = repo
                .subject()
                .attenuate(fx_memory::Memory)
                .attenuate(fx_memory::Space::new("secret"));
            let result = profile
                .access()
                .claim(secret_cap)
                .delegate(operator.did())
                .perform(&operator)
                .await;
            assert!(
                result.is_err(),
                "delegation for non-delegated space 'secret' should fail"
            );

            Ok(())
        }
    }

    mod query_engine {
        use crate::helpers::{test_operator_with_profile, test_repo};
        use dialog_query::query::Output;
        use dialog_query::{Concept, Entity, Query, Term};

        mod employee {
            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Name(pub String);

            #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Role(pub String);
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Employee {
            this: Entity,
            name: employee::Name,
            role: employee::Role,
        }

        #[dialog_common::test]
        async fn it_queries_via_session() -> anyhow::Result<()> {
            let (operator, profile) = test_operator_with_profile().await;
            let repo = test_repo(&operator, &profile).await;
            let branch = repo.branch("main").open().perform(&operator).await?;

            branch
                .transaction()
                .assert(Employee {
                    this: Entity::new()?,
                    name: employee::Name("Alice".into()),
                    role: employee::Role("Engineer".into()),
                })
                .assert(Employee {
                    this: Entity::new()?,
                    name: employee::Name("Bob".into()),
                    role: employee::Role("Designer".into()),
                })
                .commit()
                .perform(&operator)
                .await?;

            let results: Vec<Employee> = branch
                .query()
                .select(Query::<Employee> {
                    this: Term::var("this"),
                    name: Term::var("name"),
                    role: Term::var("role"),
                })
                .perform(&operator)
                .try_vec()
                .await?;

            assert_eq!(results.len(), 2);
            Ok(())
        }
    }
}
