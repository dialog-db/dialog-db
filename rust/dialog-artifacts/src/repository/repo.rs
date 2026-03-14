use dialog_capability::{Did, Subject};

use super::archive::Archive;
use super::branch::{BranchName, Load, Open};
use super::credentials::Credentials;
use super::memory::{Authorization, Memory};
use super::remote::SiteName;
use super::remote::site;
use crate::RemoteAddress;

/// A repository scoped to a specific subject and issuer.
///
/// Holds pre-attenuated memory, archive, and session capabilities so that
/// branches and remotes can further narrow without repeating attenuation.
///
/// ```text
/// let repo = Repository::new(issuer, subject);
/// let branch = repo.open_branch("main").perform(&env).await?;
/// repo.add_remote("origin", address).perform(&env).await?;
/// ```
pub struct Repository {
    session: Authorization,
    subject: Did,
    memory: Memory,
    archive: Archive,
}

impl Repository {
    /// Create a repository for the given issuer and subject.
    pub fn new(issuer: Credentials, subject: Did) -> Self {
        let cap_subject = Subject::from(subject.clone());
        let memory = Memory::new(cap_subject.clone());
        let session = memory.credentials(issuer);
        let archive = Archive::new(cap_subject);
        Self {
            session,
            subject,
            memory,
            archive,
        }
    }

    /// The issuer credentials.
    pub fn issuer(&self) -> &Credentials {
        self.session.issuer()
    }

    /// The issuer authorization (credentials + scoped credential space).
    pub fn authorization(&self) -> &Authorization {
        &self.session
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

    /// Open (load or create) a branch.
    pub fn open_branch(&self, name: impl Into<BranchName>) -> Open {
        let trace = self.memory.trace(name);
        Open::new(self.session.clone(), self.subject.clone(), self.memory.clone(), trace)
    }

    /// Load an existing branch (error if not found).
    pub fn load_branch(&self, name: impl Into<BranchName>) -> Load {
        let trace = self.memory.trace(name);
        Load::new(self.session.clone(), self.subject.clone(), self.memory.clone(), trace)
    }

    /// Add a new remote site to this repository.
    pub fn add_remote(&self, name: impl Into<SiteName>, address: RemoteAddress) -> site::Open {
        site::Open::new(name, address, self.memory.space("site"))
    }

    /// Load an existing remote site from this repository.
    pub fn load_remote(&self, name: impl Into<SiteName>) -> site::Load {
        site::Load::new(name, self.memory.space("site"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifacts::{Artifact, Instruction};
    use crate::repository::node_reference::NodeReference;
    use dialog_s3_credentials::Address as S3Address;
    use dialog_s3_credentials::s3::Credentials as S3Credentials;
    use dialog_storage::provider::Volatile;
    use futures_util::stream;

    fn test_subject() -> Did {
        "did:test:repository".parse().unwrap()
    }

    async fn test_issuer() -> Credentials {
        Credentials::from_passphrase("test").await.unwrap()
    }

    fn test_address() -> RemoteAddress {
        let s3_addr = S3Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "bucket");
        RemoteAddress::S3(S3Credentials::public(s3_addr).unwrap())
    }

    #[dialog_common::test]
    async fn it_opens_branch_via_repository() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::new(test_issuer().await, test_subject());

        let branch = repo.open_branch("main").perform(&env).await?;

        assert_eq!(branch.name().as_str(), "main");
        assert_eq!(branch.revision().tree(), &NodeReference::default());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_loads_branch_via_repository() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::new(test_issuer().await, test_subject());

        let _branch = repo.open_branch("main").perform(&env).await?;

        let branch = repo.load_branch("main").perform(&env).await?;
        assert_eq!(branch.name().as_str(), "main");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_commits_via_repository() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::new(test_issuer().await, test_subject());

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

        assert_ne!(branch.revision().tree(), &NodeReference::default());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_adds_and_loads_remote_via_repository() -> anyhow::Result<()> {
        let env = Volatile::new();
        let repo = Repository::new(test_issuer().await, test_subject());

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
