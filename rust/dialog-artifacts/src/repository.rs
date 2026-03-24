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
/// Remote site / repository / branch cursor hierarchy.
pub mod remote;
/// Revision type and edition tracking.
pub mod revision;

use dialog_capability::{Did, Subject};

use self::archive::Archive;
use self::memory::Memory;

pub use branch::*;
pub use error::*;
pub use node_reference::*;
pub use occurence::*;
pub use remote::*;
pub use revision::*;

/// A repository scoped to a specific subject and issuer.
///
/// Holds pre-attenuated memory, archive, and session capabilities so that
/// branches and remotes can further narrow without repeating attenuation.
///
/// ```text
/// let repo = Repository::new(subject);
/// let branch = repo.open_branch("main").perform(&env).await?;
/// repo.add_remote("origin", address).perform(&env).await?;
/// ```
pub struct Repository {
    subject: Did,
    memory: Memory,
    archive: Archive,
}

impl Repository {
    /// Create a repository for the given subject.
    ///
    /// The operator identity comes from the environment at operation time
    /// via `Provider<Identify>`.
    pub fn new(subject: Did) -> Self {
        let cap_subject = Subject::from(subject.clone());
        let memory = Memory::new(cap_subject.clone());
        let archive = Archive::new(cap_subject);
        Self {
            subject,
            memory,
            archive,
        }
    }

    /// The subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
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
        branch::Open::new(self.subject.clone(), self.memory.clone(), trace)
    }

    /// Load an existing branch (error if not found).
    pub fn load_branch(&self, name: impl Into<branch::BranchName>) -> branch::Load {
        let trace = self.memory.trace(name);
        branch::Load::new(self.subject.clone(), self.memory.clone(), trace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::{Artifact, Instruction};
    use dialog_capability::{Capability, Provider, credential};
    use dialog_effects::archive as archive_fx;
    use dialog_effects::memory as memory_fx;
    use dialog_remote_s3::Address;
    use dialog_storage::provider::Volatile;
    use futures_util::stream;

    fn test_subject() -> Did {
        "did:test:repository".parse().unwrap()
    }

    fn test_address() -> RemoteAddress {
        let s3_addr = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "bucket");
        RemoteAddress::S3(s3_addr)
    }

    struct TestEnv(Volatile);

    impl TestEnv {
        fn new() -> Self {
            Self(Volatile::new())
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<credential::Identify> for TestEnv {
        async fn execute(
            &self,
            _input: Capability<credential::Identify>,
        ) -> Result<credential::Identity, credential::CredentialError> {
            let did = test_subject();
            Ok(credential::Identity {
                profile: did.clone(),
                operator: did,
                account: None,
            })
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<archive_fx::Get> for TestEnv {
        async fn execute(
            &self,
            input: Capability<archive_fx::Get>,
        ) -> Result<Option<Vec<u8>>, archive_fx::ArchiveError> {
            <Volatile as Provider<archive_fx::Get>>::execute(&self.0, input).await
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<archive_fx::Put> for TestEnv {
        async fn execute(
            &self,
            input: Capability<archive_fx::Put>,
        ) -> Result<(), archive_fx::ArchiveError> {
            <Volatile as Provider<archive_fx::Put>>::execute(&self.0, input).await
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<memory_fx::Resolve> for TestEnv {
        async fn execute(
            &self,
            input: Capability<memory_fx::Resolve>,
        ) -> Result<Option<memory_fx::Publication>, memory_fx::MemoryError> {
            <Volatile as Provider<memory_fx::Resolve>>::execute(&self.0, input).await
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<memory_fx::Publish> for TestEnv {
        async fn execute(
            &self,
            input: Capability<memory_fx::Publish>,
        ) -> Result<Vec<u8>, memory_fx::MemoryError> {
            <Volatile as Provider<memory_fx::Publish>>::execute(&self.0, input).await
        }
    }

    #[dialog_common::test]
    async fn it_opens_branch_via_repository() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::new(test_subject());

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
        let env = Volatile::new();
        let repo = Repository::new(test_subject());

        let _branch = repo.open_branch("main").perform(&env).await?;

        let branch = repo.load_branch("main").perform(&env).await?;
        assert_eq!(branch.name().as_str(), "main");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_via_repository() -> anyhow::Result<()> {
        let env = TestEnv::new();
        let repo = Repository::new(test_subject());

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
        let env = Volatile::new();
        let repo = Repository::new(test_subject());

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
}
