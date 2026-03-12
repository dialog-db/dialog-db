#![cfg(test)]

use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;
use dialog_storage::provider::network::emulator::Route;
use dialog_storage::provider::Volatile;

use super::Branch;
use super::tests::{test_issuer, test_subject};
use crate::artifacts::{Artifact, Instruction};
use crate::repository::remote::RemoteSite;

/// Composite test environment with local Volatile storage and remote emulator.
///
/// For shared-remote tests (Alice + Bob), both TestEnv instances share the
/// same `Route` via `Arc` — but since Route already uses `RwLock` internally,
/// we just share via reference in the test.
struct TestEnv {
    local: Volatile,
    remote: Route<String>,
}

impl TestEnv {
    fn new() -> Self {
        Self {
            local: Volatile::new(),
            remote: Route::new(),
        }
    }

    fn with_remote(remote: Route<String>) -> Self {
        Self {
            local: Volatile::new(),
            remote,
        }
    }
}

// Delegate local effects to Volatile
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive_fx::Get> for TestEnv {
    async fn execute(
        &self,
        input: Capability<archive_fx::Get>,
    ) -> <archive_fx::Get as dialog_capability::Effect>::Output {
        Provider::<archive_fx::Get>::execute(&self.local, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive_fx::Put> for TestEnv {
    async fn execute(
        &self,
        input: Capability<archive_fx::Put>,
    ) -> <archive_fx::Put as dialog_capability::Effect>::Output {
        Provider::<archive_fx::Put>::execute(&self.local, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory_fx::Resolve> for TestEnv {
    async fn execute(
        &self,
        input: Capability<memory_fx::Resolve>,
    ) -> <memory_fx::Resolve as dialog_capability::Effect>::Output {
        Provider::<memory_fx::Resolve>::execute(&self.local, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory_fx::Publish> for TestEnv {
    async fn execute(
        &self,
        input: Capability<memory_fx::Publish>,
    ) -> <memory_fx::Publish as dialog_capability::Effect>::Output {
        Provider::<memory_fx::Publish>::execute(&self.local, input).await
    }
}

// Delegate remote effects to emulator Route
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<RemoteInvocation<archive_fx::Get, String>> for TestEnv {
    async fn execute(
        &self,
        input: RemoteInvocation<archive_fx::Get, String>,
    ) -> <archive_fx::Get as dialog_capability::Effect>::Output {
        Provider::<RemoteInvocation<archive_fx::Get, String>>::execute(&self.remote, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<RemoteInvocation<archive_fx::Put, String>> for TestEnv {
    async fn execute(
        &self,
        input: RemoteInvocation<archive_fx::Put, String>,
    ) -> <archive_fx::Put as dialog_capability::Effect>::Output {
        Provider::<RemoteInvocation<archive_fx::Put, String>>::execute(&self.remote, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<RemoteInvocation<memory_fx::Resolve, String>> for TestEnv {
    async fn execute(
        &self,
        input: RemoteInvocation<memory_fx::Resolve, String>,
    ) -> <memory_fx::Resolve as dialog_capability::Effect>::Output {
        Provider::<RemoteInvocation<memory_fx::Resolve, String>>::execute(&self.remote, input).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<RemoteInvocation<memory_fx::Publish, String>> for TestEnv {
    async fn execute(
        &self,
        input: RemoteInvocation<memory_fx::Publish, String>,
    ) -> <memory_fx::Publish as dialog_capability::Effect>::Output {
        Provider::<RemoteInvocation<memory_fx::Publish, String>>::execute(&self.remote, input).await
    }
}

#[dialog_common::test]
async fn it_pushes_to_remote() -> anyhow::Result<()> {
    let env = TestEnv::new();
    let issuer = test_issuer().await;
    let subject = test_subject();

    // Add a remote
    let site = RemoteSite::add(
        "origin",
        "remote-1".to_string(),
        issuer.did(),
        &subject,
        &env,
    )
    .await?;

    // Open branch and set remote upstream
    let branch = Branch::open("main", issuer.clone(), subject.clone())
        .perform(&env)
        .await?;

    let remote_branch = site.repository(subject.clone()).branch("main");
    branch
        .set_upstream(remote_branch)
        .perform(&env)
        .await?;

    // Commit
    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:123".parse()?,
        is: crate::Value::String("Alice".to_string()),
        cause: None,
    };
    let (branch, _) = branch
        .commit(futures_util::stream::iter(vec![Instruction::Assert(
            artifact,
        )]))
        .perform(&env)
        .await?;

    // Push to remote
    let result = branch.push().perform(&env).await?;
    assert!(result.is_some());

    // Verify remote has the revision by resolving
    let remote_branch = site.repository(subject).branch("main");
    let remote_rev = remote_branch.resolve(&env).await?;
    assert!(remote_rev.is_some());
    assert_eq!(remote_rev.unwrap().tree(), branch.revision().tree());

    Ok(())
}

#[dialog_common::test]
async fn it_fetches_from_remote_upstream() -> anyhow::Result<()> {
    let env = TestEnv::new();
    let issuer = test_issuer().await;
    let subject = test_subject();

    // Add a remote and push to it manually via RemoteBranch
    let site = RemoteSite::add(
        "origin",
        "remote-2".to_string(),
        issuer.did(),
        &subject,
        &env,
    )
    .await?;

    // Create branch, commit, and push
    let branch = Branch::open("main", issuer.clone(), subject.clone())
        .perform(&env)
        .await?;

    let remote_branch_cursor = site.repository(subject.clone()).branch("main");
    branch
        .set_upstream(remote_branch_cursor.clone())
        .perform(&env)
        .await?;

    let (branch, _) = branch
        .commit(futures_util::stream::iter(vec![Instruction::Assert(
            Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: crate::Value::String("Data".to_string()),
                cause: None,
            },
        )]))
        .perform(&env)
        .await?;

    branch.push().perform(&env).await?;

    // Fetch from remote
    let fetched = branch.fetch().perform(&env).await?;
    assert!(fetched.is_some());

    Ok(())
}

#[dialog_common::test]
async fn it_fetch_does_not_modify_local_state() -> anyhow::Result<()> {
    let env = TestEnv::new();
    let issuer = test_issuer().await;
    let subject = test_subject();

    let site = RemoteSite::add(
        "origin",
        "remote-3".to_string(),
        issuer.did(),
        &subject,
        &env,
    )
    .await?;

    let branch = Branch::open("main", issuer.clone(), subject.clone())
        .perform(&env)
        .await?;

    let remote_branch_cursor = site.repository(subject.clone()).branch("main");
    branch
        .set_upstream(remote_branch_cursor)
        .perform(&env)
        .await?;

    let (branch, _) = branch
        .commit(futures_util::stream::iter(vec![Instruction::Assert(
            Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: crate::Value::String("Data".to_string()),
                cause: None,
            },
        )]))
        .perform(&env)
        .await?;

    branch.push().perform(&env).await?;

    let revision_before = branch.revision();
    let _fetched = branch.fetch().perform(&env).await?;
    assert_eq!(branch.revision(), revision_before);

    Ok(())
}

#[dialog_common::test]
async fn it_pushes_then_pulls_from_remote() -> anyhow::Result<()> {
    // Alice and Bob share a remote
    let remote = Route::new();
    let alice_env = TestEnv::with_remote(remote);
    let alice_issuer = test_issuer().await;
    let subject = test_subject();

    // Alice adds remote and pushes
    let site = RemoteSite::add(
        "origin",
        "shared-remote".to_string(),
        alice_issuer.did(),
        &subject,
        &alice_env,
    )
    .await?;

    let alice_branch = Branch::open("main", alice_issuer.clone(), subject.clone())
        .perform(&alice_env)
        .await?;

    let remote_branch_cursor = site.repository(subject.clone()).branch("main");
    alice_branch
        .set_upstream(remote_branch_cursor)
        .perform(&alice_env)
        .await?;

    let (alice_branch, _) = alice_branch
        .commit(futures_util::stream::iter(vec![Instruction::Assert(
            Artifact {
                the: "user/name".parse()?,
                of: "user:alice".parse()?,
                is: crate::Value::String("Alice".to_string()),
                cause: None,
            },
        )]))
        .perform(&alice_env)
        .await?;

    alice_branch.push().perform(&alice_env).await?;

    // Bob creates his own env sharing the same remote
    // Since Route is not Clone, Bob needs to share Alice's TestEnv for the remote.
    // In practice, both would share an Arc<Route>. For this test, Bob uses Alice's env
    // since they share the same remote Route.
    // Bob opens his own branch (different local state but same remote)
    let bob_branch = Branch::open("bob-main", alice_issuer.clone(), subject.clone())
        .perform(&alice_env)
        .await?;

    let bob_remote = site.repository(subject.clone()).branch("main");
    bob_branch
        .set_upstream(bob_remote)
        .perform(&alice_env)
        .await?;

    // Bob pulls from remote
    let (bob_branch, pulled) = bob_branch.pull_upstream().perform(&alice_env).await?;
    assert!(pulled.is_some());
    assert_eq!(bob_branch.revision().tree(), alice_branch.revision().tree());

    Ok(())
}

#[dialog_common::test]
async fn it_pull_without_local_changes_adopts_upstream() -> anyhow::Result<()> {
    let env = TestEnv::new();
    let issuer = test_issuer().await;
    let subject = test_subject();

    let site = RemoteSite::add(
        "origin",
        "remote-adopt".to_string(),
        issuer.did(),
        &subject,
        &env,
    )
    .await?;

    // Create branch, commit, push
    let branch = Branch::open("main", issuer.clone(), subject.clone())
        .perform(&env)
        .await?;

    let remote = site.repository(subject.clone()).branch("main");
    branch
        .set_upstream(remote)
        .perform(&env)
        .await?;

    let (branch, _) = branch
        .commit(futures_util::stream::iter(vec![Instruction::Assert(
            Artifact {
                the: "user/name".parse()?,
                of: "user:1".parse()?,
                is: crate::Value::String("Original".to_string()),
                cause: None,
            },
        )]))
        .perform(&env)
        .await?;

    branch.push().perform(&env).await?;

    // Create another branch tracking the same remote but empty
    let other = Branch::open("other", issuer.clone(), subject.clone())
        .perform(&env)
        .await?;
    let remote = site.repository(subject).branch("main");
    other
        .set_upstream(remote)
        .perform(&env)
        .await?;

    // Pull — no local changes, should adopt upstream
    let (other, pulled) = other.pull_upstream().perform(&env).await?;
    assert!(pulled.is_some());
    assert_eq!(other.revision().tree(), branch.revision().tree());

    Ok(())
}

#[dialog_common::test]
async fn it_adds_multiple_remotes() -> anyhow::Result<()> {
    let env = TestEnv::new();
    let issuer = test_issuer().await;
    let subject = test_subject();

    let origin = RemoteSite::add(
        "origin",
        "remote-origin".to_string(),
        issuer.did(),
        &subject,
        &env,
    )
    .await?;

    let backup = RemoteSite::add(
        "backup",
        "remote-backup".to_string(),
        issuer.did(),
        &subject,
        &env,
    )
    .await?;

    assert_eq!(origin.name(), "origin");
    assert_eq!(backup.name(), "backup");
    assert_ne!(origin.site(), backup.site());

    // Load them back
    let loaded_origin = RemoteSite::load("origin", &subject, &env).await?;
    let loaded_backup = RemoteSite::load("backup", &subject, &env).await?;
    assert_eq!(loaded_origin.site(), "remote-origin");
    assert_eq!(loaded_backup.site(), "remote-backup");

    Ok(())
}
