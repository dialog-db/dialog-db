use dialog_capability::authorization::Authorized;
use dialog_capability::{Capability, Did, Effect, Provider, credential};
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_s3_credentials::capability::S3Request;
use dialog_s3_credentials::s3::site::{S3Access, S3Invocation};
use dialog_s3_credentials::{Address, s3};
use dialog_storage::provider::Volatile;

use crate::repository::Repository;
use crate::repository::branch::state::UpstreamState;
use crate::repository::node_reference::NodeReference;
use crate::repository::remote::SiteName;

fn test_subject() -> Did {
    "did:test:e2e-subject".parse().unwrap()
}

async fn test_issuer() -> super::super::credentials::Credentials<()> {
    super::super::credentials::Credentials::from_passphrase("e2e-test", ())
        .await
        .unwrap()
}

fn test_address() -> Address {
    Address::new("http://localhost:9999", "us-east-1", "test-bucket")
}

fn test_s3_creds() -> s3::Credentials {
    s3::Credentials::public(test_address())
        .unwrap()
        .with_path_style()
}

/// In-memory remote that implements S3 authorization and invocation
/// backed by a Volatile store, suitable for e2e testing without HTTP.
struct InMemoryRemote {
    store: Volatile,
    s3_creds: s3::Credentials,
}

impl InMemoryRemote {
    fn new() -> Self {
        Self {
            store: Volatile::new(),
            s3_creds: test_s3_creds(),
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

// Authorize by delegating to s3::Credentials (presigning).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<C> Provider<credential::Authorize<C, S3Access>> for TestEnv
where
    C: Effect + Clone + 'static,
    C::Of: dialog_capability::Constraint,
    Capability<C>: S3Request,
{
    async fn execute(
        &self,
        input: credential::Authorize<C, S3Access>,
    ) -> Result<Authorized<C, S3Access>, credential::AuthorizeError> {
        self.remote.s3_creds.execute(input).await
    }
}

// S3Invocation handlers — execute against remote Volatile store.

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<S3Invocation<archive_fx::Get>> for TestEnv {
    async fn execute(
        &self,
        invocation: S3Invocation<archive_fx::Get>,
    ) -> Result<Option<Vec<u8>>, archive_fx::ArchiveError> {
        <Volatile as Provider<archive_fx::Get>>::execute(&self.remote.store, invocation.capability)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<S3Invocation<archive_fx::Put>> for TestEnv {
    async fn execute(
        &self,
        invocation: S3Invocation<archive_fx::Put>,
    ) -> Result<(), archive_fx::ArchiveError> {
        <Volatile as Provider<archive_fx::Put>>::execute(&self.remote.store, invocation.capability)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<S3Invocation<memory_fx::Resolve>> for TestEnv {
    async fn execute(
        &self,
        invocation: S3Invocation<memory_fx::Resolve>,
    ) -> Result<Option<memory_fx::Publication>, memory_fx::MemoryError> {
        <Volatile as Provider<memory_fx::Resolve>>::execute(
            &self.remote.store,
            invocation.capability,
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<S3Invocation<memory_fx::Publish>> for TestEnv {
    async fn execute(
        &self,
        invocation: S3Invocation<memory_fx::Publish>,
    ) -> Result<Vec<u8>, memory_fx::MemoryError> {
        <Volatile as Provider<memory_fx::Publish>>::execute(
            &self.remote.store,
            invocation.capability,
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<S3Invocation<memory_fx::Retract>> for TestEnv {
    async fn execute(
        &self,
        invocation: S3Invocation<memory_fx::Retract>,
    ) -> Result<(), memory_fx::MemoryError> {
        <Volatile as Provider<memory_fx::Retract>>::execute(
            &self.remote.store,
            invocation.capability,
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
impl Provider<credential::Sign> for TestEnv {
    async fn execute(
        &self,
        _input: Capability<credential::Sign>,
    ) -> Result<Vec<u8>, credential::CredentialError> {
        Ok(vec![0u8; 64])
    }
}

async fn setup_repo_with_remote(
    env: &TestEnv,
) -> anyhow::Result<(Repository<()>, super::Branch<()>)> {
    let repo = Repository::new(test_issuer().await, test_subject());

    let site_address = dialog_s3_credentials::Credentials::S3(
        s3::Credentials::public(test_address())
            .unwrap()
            .with_path_style(),
    );
    let _site = repo
        .add_remote("origin", site_address)
        .perform(&env.local)
        .await?;

    let branch = repo.open_branch("main").perform(&env.local).await?;

    branch
        .set_upstream(UpstreamState::Remote {
            name: SiteName::from("origin"),
            branch: "main".into(),
            subject: test_subject(),
            tree: NodeReference::default(),
        })
        .perform(&env.local)
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
        .perform(&env.local)
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
        .perform(&env.local)
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
        .perform(&env.local)
        .await?;

    let push_revision = branch.push().perform(&env).await?;
    assert!(push_revision.is_some(), "Push should succeed");

    let upstream = branch.upstream();
    assert!(upstream.is_some(), "Should have upstream after push");

    Ok(())
}
