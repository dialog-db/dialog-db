#![cfg(test)]

use dialog_effects::environment::Environment;
use dialog_s3_credentials::Credentials;
use dialog_storage::provider::Volatile;
use dialog_storage::provider::network::emulator::Route;

use super::Branch;
use super::tests::{test_issuer, test_subject};
use crate::artifacts::{Artifact, Instruction};
use crate::repository::remote::RemoteSite;

type TestEnv = Environment<Volatile, Route<Credentials>>;

fn test_credentials(name: &str) -> Credentials {
    let address = dialog_s3_credentials::Address::new(
        "https://s3.us-east-1.amazonaws.com",
        "us-east-1",
        name,
    );
    Credentials::S3(dialog_s3_credentials::s3::Credentials::public(address).unwrap())
}

fn new_env() -> TestEnv {
    Environment::new(Volatile::new(), Route::new())
}

fn env_with_remote(remote: Route<Credentials>) -> TestEnv {
    Environment::new(Volatile::new(), remote)
}

#[dialog_common::test]
async fn it_pushes_to_remote() -> anyhow::Result<()> {
    let env = new_env();
    let issuer = test_issuer().await;
    let subject = test_subject();

    let site = RemoteSite::add(
        "origin",
        "remote-1".to_string(),
        issuer.did(),
        test_credentials("remote-1"),
        &subject,
        &env,
    )
    .await?;

    let branch = Branch::open("main", issuer.clone(), subject.clone())
        .perform(&env)
        .await?;

    let remote_branch = site.repository(subject.clone()).branch("main");
    branch
        .set_upstream(remote_branch)
        .perform(&env)
        .await?;

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

    let result = branch.push().perform(&env).await?;
    assert!(result.is_some());

    let remote_branch = site.repository(subject).branch("main");
    let remote_rev = remote_branch.resolve(&env).await?;
    assert!(remote_rev.is_some());
    assert_eq!(remote_rev.unwrap().tree(), branch.revision().tree());

    Ok(())
}

#[dialog_common::test]
async fn it_fetches_from_remote_upstream() -> anyhow::Result<()> {
    let env = new_env();
    let issuer = test_issuer().await;
    let subject = test_subject();

    let site = RemoteSite::add(
        "origin",
        "remote-2".to_string(),
        issuer.did(),
        test_credentials("remote-2"),
        &subject,
        &env,
    )
    .await?;

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

    let fetched = branch.fetch().perform(&env).await?;
    assert!(fetched.is_some());

    Ok(())
}

#[dialog_common::test]
async fn it_fetch_does_not_modify_local_state() -> anyhow::Result<()> {
    let env = new_env();
    let issuer = test_issuer().await;
    let subject = test_subject();

    let site = RemoteSite::add(
        "origin",
        "remote-3".to_string(),
        issuer.did(),
        test_credentials("remote-3"),
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
    let alice_env = env_with_remote(remote);
    let alice_issuer = test_issuer().await;
    let subject = test_subject();

    let site = RemoteSite::add(
        "origin",
        "shared-remote".to_string(),
        alice_issuer.did(),
        test_credentials("shared-remote"),
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

    // Bob opens his own branch sharing Alice's env (same remote)
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
    let env = new_env();
    let issuer = test_issuer().await;
    let subject = test_subject();

    let site = RemoteSite::add(
        "origin",
        "remote-adopt".to_string(),
        issuer.did(),
        test_credentials("remote-adopt"),
        &subject,
        &env,
    )
    .await?;

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

    let other = Branch::open("other", issuer.clone(), subject.clone())
        .perform(&env)
        .await?;
    let remote = site.repository(subject).branch("main");
    other
        .set_upstream(remote)
        .perform(&env)
        .await?;

    let (other, pulled) = other.pull_upstream().perform(&env).await?;
    assert!(pulled.is_some());
    assert_eq!(other.revision().tree(), branch.revision().tree());

    Ok(())
}

#[dialog_common::test]
async fn it_adds_multiple_remotes() -> anyhow::Result<()> {
    let env = new_env();
    let issuer = test_issuer().await;
    let subject = test_subject();

    let origin = RemoteSite::add(
        "origin",
        "remote-origin".to_string(),
        issuer.did(),
        test_credentials("remote-origin"),
        &subject,
        &env,
    )
    .await?;

    let backup = RemoteSite::add(
        "backup",
        "remote-backup".to_string(),
        issuer.did(),
        test_credentials("remote-backup"),
        &subject,
        &env,
    )
    .await?;

    assert_eq!(origin.name(), "origin");
    assert_eq!(backup.name(), "backup");
    assert_ne!(origin.site(), backup.site());

    let loaded_origin = RemoteSite::load("origin", &subject, &env).await?;
    let loaded_backup = RemoteSite::load("backup", &subject, &env).await?;
    assert_eq!(loaded_origin.site(), "remote-origin");
    assert_eq!(loaded_backup.site(), "remote-backup");

    Ok(())
}
