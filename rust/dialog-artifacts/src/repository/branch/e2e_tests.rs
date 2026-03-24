use dialog_capability::fork::{Fork, ForkInvocation};
use dialog_capability::{Capability, Did, Provider, Subject, authority, credential};
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_remote_s3::Address;
use dialog_remote_s3::S3;
use dialog_storage::provider::Volatile;

use crate::RemoteAddress;
use crate::repository::Repository;
use crate::repository::branch::state::UpstreamState;
use crate::repository::node_reference::NodeReference;
use crate::repository::remote::SiteName;

fn test_subject() -> Did {
    "did:test:e2e-subject".parse().unwrap()
}

fn test_address() -> Address {
    Address::new("http://localhost:9999", "us-east-1", "test-bucket")
}

/// In-memory remote that implements Fork<S3, Fx> authorization and invocation
/// backed by a Volatile store, suitable for e2e testing without HTTP.
struct InMemoryRemote {
    store: Volatile,
}

impl InMemoryRemote {
    fn new() -> Self {
        Self {
            store: Volatile::new(),
        }
    }
}

/// Composite test environment: local Volatile + InMemoryRemote.
struct TestEnv {
    local: Volatile,
    remote: InMemoryRemote,
}

impl TestEnv {
    fn new() -> Self {
        Self {
            local: Volatile::new(),
            remote: InMemoryRemote::new(),
        }
    }
}

// Fork<S3, Fx> handlers — execute against remote Volatile store.

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Fork<S3, archive_fx::Get>> for TestEnv {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, archive_fx::Get>,
    ) -> Result<Option<Vec<u8>>, archive_fx::ArchiveError> {
        <Volatile as Provider<archive_fx::Get>>::execute(
            &self.remote.store,
            invocation.authorization.capability,
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Fork<S3, archive_fx::Put>> for TestEnv {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, archive_fx::Put>,
    ) -> Result<(), archive_fx::ArchiveError> {
        <Volatile as Provider<archive_fx::Put>>::execute(
            &self.remote.store,
            invocation.authorization.capability,
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Fork<S3, memory_fx::Resolve>> for TestEnv {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, memory_fx::Resolve>,
    ) -> Result<Option<memory_fx::Publication>, memory_fx::MemoryError> {
        <Volatile as Provider<memory_fx::Resolve>>::execute(
            &self.remote.store,
            invocation.authorization.capability,
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Fork<S3, memory_fx::Publish>> for TestEnv {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, memory_fx::Publish>,
    ) -> Result<Vec<u8>, memory_fx::MemoryError> {
        <Volatile as Provider<memory_fx::Publish>>::execute(
            &self.remote.store,
            invocation.authorization.capability,
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Fork<S3, memory_fx::Retract>> for TestEnv {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, memory_fx::Retract>,
    ) -> Result<(), memory_fx::MemoryError> {
        <Volatile as Provider<memory_fx::Retract>>::execute(
            &self.remote.store,
            invocation.authorization.capability,
        )
        .await
    }
}

// Local effects — delegate to self.local.

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive_fx::Get> for TestEnv {
    async fn execute(
        &self,
        input: Capability<archive_fx::Get>,
    ) -> Result<Option<Vec<u8>>, archive_fx::ArchiveError> {
        <Volatile as Provider<archive_fx::Get>>::execute(&self.local, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<archive_fx::Put> for TestEnv {
    async fn execute(
        &self,
        input: Capability<archive_fx::Put>,
    ) -> Result<(), archive_fx::ArchiveError> {
        <Volatile as Provider<archive_fx::Put>>::execute(&self.local, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<memory_fx::Resolve> for TestEnv {
    async fn execute(
        &self,
        input: Capability<memory_fx::Resolve>,
    ) -> Result<Option<memory_fx::Publication>, memory_fx::MemoryError> {
        <Volatile as Provider<memory_fx::Resolve>>::execute(&self.local, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<memory_fx::Publish> for TestEnv {
    async fn execute(
        &self,
        input: Capability<memory_fx::Publish>,
    ) -> Result<Vec<u8>, memory_fx::MemoryError> {
        <Volatile as Provider<memory_fx::Publish>>::execute(&self.local, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<memory_fx::Retract> for TestEnv {
    async fn execute(
        &self,
        input: Capability<memory_fx::Retract>,
    ) -> Result<(), memory_fx::MemoryError> {
        <Volatile as Provider<memory_fx::Retract>>::execute(&self.local, input).await
    }
}

// Credential effects — simple stubs for testing.

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<authority::Identify> for TestEnv {
    async fn execute(
        &self,
        input: Capability<authority::Identify>,
    ) -> Result<authority::Authority, credential::CredentialError> {
        let did = test_subject();
        let subject_did = input.subject().clone();
        Ok(Subject::from(subject_did)
            .attenuate(authority::Profile::local(did.clone()))
            .attenuate(authority::Operator::new(did)))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<authority::Sign> for TestEnv {
    async fn execute(
        &self,
        _input: Capability<authority::Sign>,
    ) -> Result<Vec<u8>, credential::CredentialError> {
        Ok(vec![0u8; 64])
    }
}

async fn setup_repo_with_remote(env: &TestEnv) -> anyhow::Result<(Repository, super::Branch)> {
    let repo = Repository::new(test_subject());

    let site_address = RemoteAddress::S3(test_address());
    let _site = repo.add_remote("origin", site_address).perform(env).await?;

    let branch = repo.open_branch("main").perform(env).await?;

    branch
        .set_upstream(UpstreamState::Remote {
            name: SiteName::from("origin"),
            branch: "main".into(),
            subject: test_subject(),
            tree: NodeReference::default(),
        })
        .perform(env)
        .await?;

    Ok((repo, branch))
}

#[dialog_common::test]
async fn it_pushes_to_remote() -> anyhow::Result<()> {
    use crate::artifacts::{Artifact, Instruction};
    use futures_util::stream;

    let env = TestEnv::new();
    let (_repo, branch) = setup_repo_with_remote(&env).await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:123".parse()?,
        is: crate::Value::String("Alice".to_string()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&env)
        .await?;

    let result = branch.push().perform(&env).await?;
    assert!(result.is_some(), "Push should succeed");

    Ok(())
}

#[dialog_common::test]
async fn it_fetches_from_remote() -> anyhow::Result<()> {
    use crate::artifacts::{Artifact, Instruction};
    use futures_util::stream;

    let env = TestEnv::new();
    let (_repo, branch) = setup_repo_with_remote(&env).await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:456".parse()?,
        is: crate::Value::String("Bob".to_string()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&env)
        .await?;

    branch.push().perform(&env).await?;

    let fetched = branch.fetch().perform(&env).await?;
    assert!(fetched.is_some(), "Fetch should find remote state");

    Ok(())
}

#[dialog_common::test]
async fn it_pushes_and_pulls_roundtrip() -> anyhow::Result<()> {
    use crate::artifacts::{Artifact, Instruction};
    use futures_util::stream;

    let env = TestEnv::new();
    let (_repo, branch) = setup_repo_with_remote(&env).await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:789".parse()?,
        is: crate::Value::String("Charlie".to_string()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&env)
        .await?;

    let push_revision = branch.push().perform(&env).await?;
    assert!(push_revision.is_some(), "Push should succeed");

    let upstream = branch.upstream();
    assert!(upstream.is_some(), "Should have upstream after push");

    Ok(())
}
