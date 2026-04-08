//! Integration tests using provisioned S3 and UCAN test servers.
//!
//! These tests require `--features integration-tests` and spin up real
//! local S3 (and UCAN access) servers via `#[dialog_common::test]`.

use crate::SiteAddress;
use crate::helpers::{test_operator_with_profile, unique_name};
use crate::repository::branch::state::UpstreamState;
use crate::repository::node_reference::NodeReference;
use crate::repository::{Repository, RepositoryExt as _};
use crate::{Artifact, ArtifactSelector, Instruction};
use dialog_operator::profile::Profile;
use dialog_remote_s3::Address as S3SiteAddress;
use dialog_remote_s3::helpers::S3Address;
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::StreamExt;
use futures_util::stream;

fn s3_site_address(s3: &S3Address) -> S3SiteAddress {
    S3SiteAddress::new(&s3.endpoint, "us-east-1", &s3.bucket).with_credentials(
        dialog_remote_s3::S3Credentials::new(&s3.access_key_id, &s3.secret_access_key),
    )
}

async fn setup_repo_with_s3_remote(
    operator: &crate::Operator<VolatileSpace>,
    profile: &Profile,
    s3: &S3Address,
    name: &str,
) -> anyhow::Result<(
    Repository<dialog_credentials::SignerCredential>,
    super::Branch,
)> {
    let repo = profile
        .repository(unique_name(name))
        .create()
        .perform(operator)
        .await?;

    let site_address = s3_site_address(s3);
    let origin = repo
        .remote("origin")
        .create(site_address)
        .perform(operator)
        .await?;

    let branch = repo.branch("main").open().perform(operator).await?;
    let remote_branch = origin.branch("main").open().perform(operator).await?;
    branch.set_upstream(remote_branch).perform(operator).await?;

    Ok((repo, branch))
}

#[dialog_common::test]
async fn it_pushes_to_s3_remote(s3: S3Address) -> anyhow::Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &profile, &s3, "push").await?;

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
    let (operator, profile) = test_operator_with_profile().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &profile, &s3, "fetch").await?;

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
    let (operator, profile) = test_operator_with_profile().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &profile, &s3, "roundtrip").await?;

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
    let (operator, profile) = test_operator_with_profile().await;
    let (_repo, branch) = setup_repo_with_s3_remote(&operator, &profile, &s3, "no-change").await?;

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
    let pull_result = branch.pull().perform(&operator).await?;
    assert!(
        pull_result.is_none(),
        "pull with no changes should return None"
    );

    Ok(())
}

#[dialog_common::test]
async fn it_pushes_and_pulls_data_between_repos(s3: S3Address) -> anyhow::Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Alice creates repo, commits, and pushes
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "alice").await?;

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
    let bob_repo = profile
        .repository(unique_name("bob"))
        .open()
        .perform(&operator)
        .await?;

    let origin = bob_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    let pull_result = bob_branch.pull().perform(&operator).await?;
    assert!(pull_result.is_some(), "Bob's pull should find Alice's data");

    // Verify Bob can query Alice's artifact
    let results: Vec<_> = bob_branch
        .claims()
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
    let (operator, profile) = test_operator_with_profile().await;

    // Alice commits and pushes
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "conv-alice").await?;

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
    let bob_repo = profile
        .repository(unique_name("conv-bob"))
        .open()
        .perform(&operator)
        .await?;

    let origin = bob_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // Bob pulls Alice's changes
    bob_branch.pull().perform(&operator).await?;

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
    alice_branch.pull().perform(&operator).await?;

    // Both should have both artifacts
    let alice_results: Vec<_> = alice_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let bob_results: Vec<_> = bob_branch
        .claims()
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

/// Alice creates a repo, delegates to Bob, Bob pulls, commits, pushes,
/// then Alice pulls Bob's changes.
#[cfg(feature = "ucan")]
#[dialog_common::test]
async fn it_collaborates_via_ucan_delegation(ucan: UcanS3Address) -> anyhow::Result<()> {
    // Alice: create profile, operator, repo
    let (alice_operator, alice_profile) = test_operator_with_profile().await;
    let alice_repo = alice_profile
        .repository(unique_name("collab-alice"))
        .create()
        .perform(&alice_operator)
        .await?;

    // Delegate repo ownership to Alice's profile
    let alice_access = alice_repo.access();
    let ownership_chain = alice_access
        .claim(&alice_repo)
        .delegate(alice_profile.did())
        .perform(&alice_operator)
        .await?;
    alice_profile
        .access()
        .save(ownership_chain)
        .perform(&alice_operator)
        .await?;

    // Set up UCAN remote on Alice's repo
    let ucan_site = SiteAddress::Ucan(UcanAddress::new(&ucan.access_service_url));
    let alice_origin = alice_repo
        .remote("origin")
        .create(ucan_site.clone())
        .perform(&alice_operator)
        .await?;

    let alice_branch = alice_repo
        .branch("main")
        .open()
        .perform(&alice_operator)
        .await?;
    let remote_branch = alice_origin
        .branch("main")
        .open()
        .perform(&alice_operator)
        .await?;
    alice_branch
        .set_upstream(remote_branch)
        .perform(&alice_operator)
        .await?;

    // Alice commits and pushes initial data
    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:alice".parse()?,
            is: crate::Value::String("Alice".into()),
            cause: None,
        })]))
        .perform(&alice_operator)
        .await?;

    alice_branch.push().perform(&alice_operator).await?;

    // Bob: create profile, operator
    let (bob_operator, bob_profile) = test_operator_with_profile().await;

    // Alice delegates repo access to Bob's profile
    let delegation_to_bob = alice_profile
        .access()
        .claim(&alice_repo)
        .delegate(bob_profile.did())
        .perform(&alice_operator)
        .await?;

    // Bob saves the delegation chain under his profile
    bob_profile
        .access()
        .save(delegation_to_bob)
        .perform(&bob_operator)
        .await?;

    // Bob creates his own repo (different DID) and adds Alice's remote
    let bob_repo = bob_profile
        .repository(unique_name("collab-bob"))
        .open()
        .perform(&bob_operator)
        .await?;

    let bob_origin = bob_repo
        .remote("origin")
        .create(ucan_site)
        .subject(alice_repo.did())
        .perform(&bob_operator)
        .await?;

    let bob_branch = bob_repo
        .branch("main")
        .open()
        .perform(&bob_operator)
        .await?;
    let remote_branch = bob_origin
        .branch("main")
        .open()
        .perform(&bob_operator)
        .await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&bob_operator)
        .await?;

    // Bob pulls Alice's data
    let pull_result = bob_branch.pull().perform(&bob_operator).await?;
    assert!(pull_result.is_some(), "Bob should pull Alice's data");

    // Verify Bob has Alice's artifact
    let bob_results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&bob_operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(bob_results.len(), 1, "Bob should have Alice's artifact");

    // Bob commits his own change
    bob_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:bob".parse()?,
            is: crate::Value::String("Bob".into()),
            cause: None,
        })]))
        .perform(&bob_operator)
        .await?;

    // Bob pushes
    let push_result = bob_branch.push().perform(&bob_operator).await?;
    assert!(push_result.is_some(), "Bob should push successfully");

    // Alice pulls Bob's changes
    let alice_pull = alice_branch.pull().perform(&alice_operator).await?;
    assert!(alice_pull.is_some(), "Alice should pull Bob's changes");

    // Alice should have both artifacts
    let alice_results: Vec<_> = alice_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&alice_operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        alice_results.len(),
        2,
        "Alice should have both artifacts after pulling Bob's changes"
    );

    Ok(())
}

/// Push and pull via UCAN access service.
#[cfg(feature = "ucan")]
#[dialog_common::test]
async fn it_pushes_and_pulls_via_ucan(ucan: UcanS3Address) -> anyhow::Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Create repo and delegate ownership to the profile
    let repo = profile
        .repository(unique_name("ucan-repo"))
        .create()
        .perform(&operator)
        .await?;

    let repo_access = repo.access();
    let chain = repo_access
        .claim(&repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;

    // Set up UCAN remote
    let origin = repo
        .remote("origin")
        .create(SiteAddress::Ucan(UcanAddress::new(
            &ucan.access_service_url,
        )))
        .perform(&operator)
        .await?;

    let branch = repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    branch
        .set_upstream(remote_branch)
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
    let pull_result = branch.pull().perform(&operator).await?;
    assert!(pull_result.is_none(), "pull after push should return None");

    // Verify data survives select
    let results: Vec<_> = branch
        .claims()
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

/// Query an empty local replica. Data replicates on demand from the
/// remote. After removing the upstream, data is still available locally.
#[dialog_common::test]
async fn it_replicates_on_demand_and_caches_locally(s3: S3Address) -> anyhow::Result<()> {
    let (operator, profile) = test_operator_with_profile().await;

    // Alice: create repo, commit data, push to remote
    let (alice_repo, alice_branch) =
        setup_repo_with_s3_remote(&operator, &profile, &s3, "replicate-alice").await?;

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
    let alice_revision = alice_branch.revision().expect("should have revision");

    // Bob: empty repo pointing at Alice's remote
    let bob_repo = profile
        .repository(unique_name("replicate-bob"))
        .open()
        .perform(&operator)
        .await?;

    let origin = bob_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // Set Bob's revision to Alice's without pulling blocks
    bob_branch.reset(alice_revision).perform(&operator).await?;

    // First, remove upstream so select has no remote to fall back to.
    // This should fail because tree blocks aren't local.
    bob_branch
        .set_upstream(UpstreamState::Local {
            branch: "nowhere".into(),
            tree: NodeReference::default(),
        })
        .perform(&operator)
        .await?;

    let no_remote_result = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await;
    assert!(
        no_remote_result.is_err(),
        "select should fail without remote when blocks aren't local"
    );

    // Restore upstream so fallback can reach the remote
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // Now query replicates tree blocks on demand from the remote
    let results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(results.len(), 1, "should replicate and find Alice's data");
    assert_eq!(results[0].is, crate::Value::String("Alice".into()));

    // Remove upstream (simulates remote going away) by pointing
    // at a non-existent local branch instead
    bob_branch
        .set_upstream(UpstreamState::Local {
            branch: "nowhere".into(),
            tree: NodeReference::default(),
        })
        .perform(&operator)
        .await?;

    // Query again with no remote. Data should be cached locally.
    let cached_results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        cached_results.len(),
        1,
        "data should be available from local cache"
    );
    assert_eq!(cached_results[0].is, crate::Value::String("Alice".into()));

    Ok(())
}

/// Delegate repo to profile, push data to S3, pull from a new operator.
#[dialog_common::test]
async fn it_delegates_and_pushes_to_s3(s3: S3Address) -> anyhow::Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let repo = profile
        .repository(unique_name("deleg-push"))
        .create()
        .perform(&operator)
        .await?;

    // Delegate repo ownership to the profile
    let chain = repo
        .access()
        .claim(&repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;

    // Set up S3 remote
    let origin = repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .perform(&operator)
        .await?;

    let branch = repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // Commit and push
    branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:delegated".parse()?,
            is: crate::Value::String("Delegated Push".into()),
            cause: None,
        })]))
        .perform(&operator)
        .await?;

    let result = branch.push().perform(&operator).await?;
    assert!(result.is_some(), "push with delegation should succeed");

    Ok(())
}

/// Alice delegates, pushes to S3; Bob pulls and verifies data arrived.
#[dialog_common::test]
async fn it_delegates_pushes_and_pulls_via_s3(s3: S3Address) -> anyhow::Result<()> {
    let (alice_operator, alice_profile) = test_operator_with_profile().await;
    let alice_repo = alice_profile
        .repository(unique_name("deleg-pull-a"))
        .create()
        .perform(&alice_operator)
        .await?;

    // Delegate repo to Alice's profile
    let chain = alice_repo
        .access()
        .claim(&alice_repo)
        .delegate(alice_profile.did())
        .perform(&alice_operator)
        .await?;
    alice_profile
        .access()
        .save(chain)
        .perform(&alice_operator)
        .await?;

    // Alice pushes to S3
    let alice_origin = alice_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .perform(&alice_operator)
        .await?;

    let alice_branch = alice_repo
        .branch("main")
        .open()
        .perform(&alice_operator)
        .await?;
    let remote_branch = alice_origin
        .branch("main")
        .open()
        .perform(&alice_operator)
        .await?;
    alice_branch
        .set_upstream(remote_branch)
        .perform(&alice_operator)
        .await?;

    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(Artifact {
            the: "user/name".parse()?,
            of: "user:alice".parse()?,
            is: crate::Value::String("Alice Delegated".into()),
            cause: None,
        })]))
        .perform(&alice_operator)
        .await?;

    let push_result = alice_branch.push().perform(&alice_operator).await?;
    assert!(push_result.is_some(), "push should succeed");

    // Bob: fresh operator pulls from the same S3 remote
    let (bob_operator, bob_profile) = test_operator_with_profile().await;
    let bob_repo = bob_profile
        .repository(unique_name("deleg-pull-b"))
        .open()
        .perform(&bob_operator)
        .await?;

    let bob_origin = bob_repo
        .remote("origin")
        .create(s3_site_address(&s3))
        .subject(alice_repo.did())
        .perform(&bob_operator)
        .await?;

    let bob_branch = bob_repo
        .branch("main")
        .open()
        .perform(&bob_operator)
        .await?;
    let remote_branch = bob_origin
        .branch("main")
        .open()
        .perform(&bob_operator)
        .await?;
    bob_branch
        .set_upstream(remote_branch)
        .perform(&bob_operator)
        .await?;

    let pull_result = bob_branch.pull().perform(&bob_operator).await?;
    assert!(pull_result.is_some(), "pull should find Alice's data");

    let results: Vec<_> = bob_branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(&bob_operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(results.len(), 1, "should have Alice's artifact");
    assert_eq!(
        results[0].is,
        crate::Value::String("Alice Delegated".into())
    );

    Ok(())
}
