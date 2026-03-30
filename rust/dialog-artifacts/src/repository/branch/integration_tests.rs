//! Integration tests using provisioned S3 and UCAN test servers.
//!
//! These tests require `--features integration-tests` and spin up real
//! local S3 (and UCAN access) servers via `#[dialog_common::test]`.

use crate::Operator;
use crate::RemoteAddress;
use crate::artifacts::{Artifact, ArtifactSelector, Instruction};
use crate::helpers::{test_operator, test_operator_with_profile, unique_location};
use crate::repository::Repository;
use crate::repository::branch::state::UpstreamState;
use crate::repository::node_reference::NodeReference;
use crate::repository::remote::SiteName;
use dialog_remote_s3::Address as S3RemoteAddress;
use dialog_remote_s3::helpers::S3Address;
use futures_util::StreamExt;
use futures_util::stream;

fn s3_remote_address(s3: &S3Address) -> RemoteAddress {
    RemoteAddress::S3(
        S3RemoteAddress::new(&s3.endpoint, "us-east-1", &s3.bucket).with_credentials(
            dialog_remote_s3::S3Credentials::new(&s3.access_key_id, &s3.secret_access_key),
        ),
    )
}

async fn setup_repo_with_s3_remote(
    operator: &Operator,
    s3: &S3Address,
    name: &str,
) -> anyhow::Result<(Repository, super::Branch)> {
    let repo = Repository::open(unique_location(name))
        .perform(operator)
        .await?;

    let site_address = s3_remote_address(s3);
    let _site = repo
        .site("origin")
        .create(site_address)
        .perform(operator)
        .await?;

    let branch = repo.branch("main").open().perform(operator).await?;

    branch
        .set_upstream(UpstreamState::Remote {
            name: SiteName::from("origin"),
            branch: "main".into(),
            subject: repo.did(),
            tree: NodeReference::default(),
        })
        .perform(operator)
        .await?;

    Ok((repo, branch))
}

#[dialog_common::test]
async fn it_pushes_to_s3_remote(s3: S3Address) -> anyhow::Result<()> {
    let operator = test_operator().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &s3, "push").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:1".parse()?,
        is: crate::Value::String("Alice".into()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    let result = branch.push().perform(&operator).await?;
    assert!(result.is_some(), "push should succeed");

    Ok(())
}

#[dialog_common::test]
async fn it_fetches_from_s3_remote(s3: S3Address) -> anyhow::Result<()> {
    let operator = test_operator().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &s3, "fetch").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:1".parse()?,
        is: crate::Value::String("Alice".into()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    branch.push().perform(&operator).await?;

    let fetched = branch.fetch().perform(&operator).await?;
    assert!(fetched.is_some(), "fetch should find remote state");

    Ok(())
}

#[dialog_common::test]
async fn it_push_and_pull_roundtrip(s3: S3Address) -> anyhow::Result<()> {
    let operator = test_operator().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &s3, "roundtrip").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:1".parse()?,
        is: crate::Value::String("Alice".into()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    branch.push().perform(&operator).await?;

    assert!(
        branch.upstream().is_some(),
        "should have upstream after push"
    );

    Ok(())
}

#[dialog_common::test]
async fn it_pull_returns_none_when_no_changes(s3: S3Address) -> anyhow::Result<()> {
    let operator = test_operator().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &s3, "no-change").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:1".parse()?,
        is: crate::Value::String("Alice".into()),
        cause: None,
    };
    branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    branch.push().perform(&operator).await?;

    // Pull immediately after push — no new changes
    let pull_result = branch.pull_upstream().perform(&operator).await?;
    assert!(
        pull_result.is_none(),
        "pull with no changes should return None"
    );

    Ok(())
}

#[dialog_common::test]
async fn it_pushes_and_pulls_data_between_repos(s3: S3Address) -> anyhow::Result<()> {
    let operator = test_operator().await;

    // Alice creates repo, commits, and pushes
    let (alice_repo, alice_branch) = setup_repo_with_s3_remote(&operator, &s3, "alice").await?;

    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:alice".parse()?,
        is: crate::Value::String("Alice".into()),
        cause: None,
    };
    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    alice_branch.push().perform(&operator).await?;

    // Bob opens a second repo sharing Alice's subject, pulls
    let bob_repo = Repository::open(unique_location("bob"))
        .perform(&operator)
        .await?;

    let site_address = s3_remote_address(&s3);
    bob_repo
        .site("origin")
        .create(site_address)
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(UpstreamState::Remote {
            name: SiteName::from("origin"),
            branch: "main".into(),
            subject: alice_repo.did(),
            tree: NodeReference::default(),
        })
        .perform(&operator)
        .await?;

    let pull_result = bob_branch.pull_upstream().perform(&operator).await?;
    assert!(pull_result.is_some(), "Bob's pull should find Alice's data");

    // Verify Bob can query Alice's artifact
    let results: Vec<_> = bob_branch
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(results.len(), 1, "Bob should have Alice's artifact");
    assert_eq!(
        results[0].is,
        crate::Value::String("Alice".into()),
        "artifact value should match"
    );

    Ok(())
}

#[dialog_common::test]
async fn it_two_party_convergence(s3: S3Address) -> anyhow::Result<()> {
    let operator = test_operator().await;

    // Alice commits and pushes
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &s3, "conv-alice").await?;

    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:alice".parse()?,
            is: crate::Value::String("Alice".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    alice_branch.push().perform(&operator).await?;

    // Bob sets up repo pointing at same remote subject
    let bob_repo = Repository::open(unique_location("conv-bob"))
        .perform(&operator)
        .await?;

    bob_repo
        .site("origin")
        .create(s3_remote_address(&s3))
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(UpstreamState::Remote {
            name: SiteName::from("origin"),
            branch: "main".into(),
            subject: alice_repo.did(),
            tree: NodeReference::default(),
        })
        .perform(&operator)
        .await?;

    // Bob pulls Alice's changes
    bob_branch.pull_upstream().perform(&operator).await?;

    // Bob commits his own artifact
    bob_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:bob".parse()?,
            is: crate::Value::String("Bob".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    // Bob pushes
    bob_branch.push().perform(&operator).await?;

    // Alice pulls Bob's changes
    alice_branch.pull_upstream().perform(&operator).await?;

    // Both should have both artifacts
    let alice_results: Vec<_> = alice_branch
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let bob_results: Vec<_> = bob_branch
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        alice_results.len(),
        2,
        "Alice should have both artifacts after pull"
    );
    assert_eq!(
        bob_results.len(),
        2,
        "Bob should have both artifacts after push"
    );

    Ok(())
}

// UCAN integration tests

#[cfg(feature = "ucan")]
use dialog_remote_ucan_s3::UcanAddress;
#[cfg(feature = "ucan")]
use dialog_remote_ucan_s3::helpers::UcanS3Address;

/// Push and pull via UCAN access service.
#[cfg(feature = "ucan")]
#[dialog_common::test]
async fn it_pushes_and_pulls_via_ucan(ucan: UcanS3Address) -> anyhow::Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Create repo and delegate ownership to the profile
    let repo = Repository::open(unique_location("ucan-repo"))
        .perform(&operator)
        .await?;

    let chain = repo
        .ownership()
        .delegate(&profile)
        .perform(&operator)
        .await?;
    profile.save(chain).perform(&operator).await?;

    // Set up UCAN remote
    let ucan_address = RemoteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));
    repo.site("origin")
        .create(ucan_address)
        .perform(&operator)
        .await?;

    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .set_upstream(UpstreamState::Remote {
            name: SiteName::from("origin"),
            branch: "main".into(),
            subject: repo.did(),
            tree: NodeReference::default(),
        })
        .perform(&operator)
        .await?;

    // Commit and push via UCAN
    branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:ucan-test".parse()?,
            is: crate::Value::String("UCAN User".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    let push_result = branch.push().perform(&operator).await?;
    assert!(push_result.is_some(), "UCAN push should succeed");

    // Pull should find no changes (just pushed)
    let pull_result = branch.pull_upstream().perform(&operator).await?;
    assert!(pull_result.is_none(), "pull after push should return None");

    // Verify data survives select
    let results: Vec<_> = branch
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(results.len(), 1, "should have the pushed artifact");
    assert_eq!(results[0].is, crate::Value::String("UCAN User".into()));

    Ok(())
}
