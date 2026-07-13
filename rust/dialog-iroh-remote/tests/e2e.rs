//! End-to-end Operator → Network → Iroh tests.
//!
//! These drive the real repository sync path with an iroh peer as the
//! remote: an [`Operator`] forks effects at an `IrohAddress`,
//! `IrohFork::authorize` builds the signed UCAN invocation from the
//! operator's stored delegations, and the serving peer verifies the chain
//! before performing each effect against its replica. They mirror
//! `dialog-repository`'s S3 collaboration tests, with a peer-to-peer QUIC
//! connection as the remote instead of an S3 bucket.
//!
//! Everything runs hermetically: nodes bind relay-less and connect over
//! direct localhost addresses.
//!
//! The provider path resolves the process-global client node, which can be
//! installed exactly once — so all scenarios share one test function.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
use dialog_iroh_remote::IrohNode;
use dialog_operator::helpers::{test_operator_with_profile, unique_name};
use dialog_repository::{RepositoryExt as _, SiteAddress};
use dialog_storage::provider::Volatile;
use futures_util::{StreamExt, stream};

#[tokio::test(flavor = "multi_thread")]
async fn it_pushes_and_pulls_repositories_via_iroh_remote() -> Result<()> {
    // The process-global client node used by the Iroh site providers,
    // hermetic (no relays / external lookup) for tests.
    let client = IrohNode::builder().direct_only().spawn().await?;
    dialog_iroh_remote::install(client)?;

    let (operator, profile) = test_operator_with_profile().await;

    // --- Alice: create a repository, serve its replica from a peer. ---
    let alice_repo = profile
        .repository(unique_name("iroh-alice"))
        .create()
        .perform(&operator)
        .await?;

    // Delegate repo ownership to the profile so prove can authorize forks.
    let chain = alice_repo
        .access()
        .claim(&alice_repo)
        .delegate(profile.did())
        .perform(&operator)
        .await?;
    profile.access().save(chain).perform(&operator).await?;

    // The serving peer replicates Alice's space in memory.
    let peer = IrohNode::builder()
        .direct_only()
        .host(alice_repo.did().clone(), Volatile::new())
        .spawn()
        .await?;

    let origin = alice_repo
        .remote("origin")
        .create(SiteAddress::Iroh(peer.address()))
        .perform(&operator)
        .await?;
    let alice_branch = alice_repo.branch("main").open().perform(&operator).await?;
    let remote_branch = origin.branch("main").open().perform(&operator).await?;
    alice_branch
        .set_upstream(remote_branch)
        .perform(&operator)
        .await?;

    // Commit and push through the iroh remote.
    let artifact = Artifact {
        the: "user/name".parse()?,
        of: "user:alice".parse()?,
        is: Value::String("Alice".into()),
        cause: None,
    };
    alice_branch
        .commit(stream::iter(vec![Instruction::Assert(artifact)]))
        .perform(&operator)
        .await?;

    let push = alice_branch.push().perform(&operator).await?;
    assert!(push.is_some(), "iroh push should succeed");

    // Pull right after push finds no new changes.
    let pull = alice_branch.pull().perform(&operator).await?;
    assert!(pull.is_none(), "pull after push should return None");

    // --- Bob: a second repo sharing Alice's subject pulls her data
    // through the same peer. ---
    let bob_repo = profile
        .repository(unique_name("iroh-bob"))
        .open()
        .perform(&operator)
        .await?;

    let bob_origin = bob_repo
        .remote("origin")
        .create(SiteAddress::Iroh(peer.address()))
        .subject(alice_repo.did())
        .perform(&operator)
        .await?;

    let bob_branch = bob_repo.branch("main").open().perform(&operator).await?;
    let bob_remote_branch = bob_origin.branch("main").open().perform(&operator).await?;
    bob_branch
        .set_upstream(bob_remote_branch)
        .perform(&operator)
        .await?;

    let pulled = bob_branch.pull().perform(&operator).await?;
    assert!(pulled.is_some(), "Bob's pull should find Alice's data");

    // Verify Bob can query Alice's artifact.
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
        Value::String("Alice".into()),
        "artifact value should match"
    );

    peer.shutdown().await;
    Ok(())
}
