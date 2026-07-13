//! Direct device-to-device sync: two live dialog instances, no server.
//!
//! Device A runs a repository over its own storage **and serves that same
//! storage** over iroh (`Storage` clones share state, so the host and the
//! local repository observe each other's writes). Because a remote
//! branch's upstream cell — `memory/branch/{name}/revision` at the
//! subject — is the very cell a local repository maintains as its branch
//! head, serving your own storage means peers pull your *live* head and
//! push straight into it (CAS-guarded), with no intermediate store and no
//! push-to-server step on your side:
//!
//! - **Pull**: device B resolves A's live branch head and reads the
//!   missing blocks out of A's archive, directly over QUIC.
//! - **Push**: device B uploads its novel blocks into A's archive and
//!   compare-and-swaps A's branch head. A sees the new revision as soon
//!   as it re-resolves the branch. A concurrent local commit on A makes
//!   the CAS fail with a version mismatch, exactly like a rejected
//!   non-fast-forward push — B pulls, merges, and retries.
//!
//! Authorization: B holds a delegation chain rooted in the repository
//! subject (repo → B's profile → B's operator), embedded in every
//! invocation and verified by A before any effect touches A's storage.
//!
//! Runs hermetically: relay-less nodes over direct localhost addresses.

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
use dialog_iroh_remote::IrohNode;
use dialog_operator::Operator;
use dialog_operator::helpers::{test_operator_with_profile, unique_name};
use dialog_repository::{Branch, RepositoryExt as _, SiteAddress};
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::{StreamExt, stream};

fn artifact(of: &str, name: &str) -> Result<Artifact> {
    Ok(Artifact {
        the: "user/name".parse()?,
        of: of.parse()?,
        is: Value::String(name.into()),
        cause: None,
    })
}

/// Collect the `user/name` values visible on a branch.
async fn names(branch: &Branch, env: &Operator<VolatileSpace>) -> Result<Vec<String>> {
    let artifacts: Vec<_> = branch
        .claims()
        .select(ArtifactSelector::new().the("user/name".parse()?))
        .perform(env)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    let mut names: Vec<String> = artifacts
        .into_iter()
        .filter_map(|artifact| match artifact.is {
            Value::String(name) => Some(name),
            _ => None,
        })
        .collect();
    names.sort();
    Ok(names)
}

#[tokio::test(flavor = "multi_thread")]
async fn it_pulls_and_pushes_directly_between_two_live_devices() -> Result<()> {
    // Outbound client node for fork invocations (both "devices" share one
    // process here, so they share the process-global client).
    let client = IrohNode::builder().direct_only().spawn().await?;
    dialog_iroh_remote::install(client)?;

    // --- Device A: repository over its own storage. ---
    let (operator_a, profile_a) = test_operator_with_profile().await;

    let repo_a = profile_a
        .repository(unique_name("device-a"))
        .create()
        .perform(&operator_a)
        .await?;

    // A serves the SAME storage its repository runs on (`Operator::storage`
    // hands back a shared handle): peers see A's live branch head, and
    // pushes land in A's own store.
    let node_a = IrohNode::builder()
        .direct_only()
        .host(repo_a.did().clone(), operator_a.storage())
        .spawn()
        .await?;

    // A joins its space's swarm and listens for head updates: the wake-up
    // signal that a peer moved a branch, so A reacts instead of polling.
    let swarm_a = node_a.join_swarm(&repo_a.did(), Vec::new()).await?;
    let mut updates_a = swarm_a.updates();

    // A commits locally. No push anywhere — A just has data.
    let branch_a = repo_a.branch("main").open().perform(&operator_a).await?;
    branch_a
        .commit(stream::iter(vec![Instruction::Assert(artifact(
            "user:alice",
            "Alice",
        )?)]))
        .perform(&operator_a)
        .await?;

    // --- Device B: its own profile, operator, and storage. ---
    let (operator_b, profile_b) = test_operator_with_profile().await;

    // A authorizes B's profile for the repository; B saves the chain so
    // its operator can prove invocations rooted in A's repo subject.
    let chain = repo_a
        .access()
        .claim(&repo_a)
        .delegate(profile_b.did())
        .perform(&operator_a)
        .await?;
    profile_b.access().save(chain).perform(&operator_b).await?;

    // B tracks A directly: remote "device-a" at A's endpoint, subject =
    // A's repository.
    let repo_b = profile_b
        .repository(unique_name("device-b"))
        .open()
        .perform(&operator_b)
        .await?;
    let origin_b = repo_b
        .remote("device-a")
        .create(SiteAddress::Iroh(node_a.address()))
        .subject(repo_a.did())
        .perform(&operator_b)
        .await?;
    let branch_b = repo_b.branch("main").open().perform(&operator_b).await?;
    let remote_branch_b = origin_b.branch("main").open().perform(&operator_b).await?;
    branch_b
        .set_upstream(remote_branch_b)
        .perform(&operator_b)
        .await?;

    // --- B pulls A's live head directly (A never pushed anything). ---
    let pulled = branch_b.pull().perform(&operator_b).await?;
    assert!(pulled.is_some(), "B should pull A's live branch head");
    assert_eq!(names(&branch_b, &operator_b).await?, vec!["Alice"]);

    // --- B commits and pushes straight into A's live store. ---
    branch_b
        .commit(stream::iter(vec![Instruction::Assert(artifact(
            "user:bob", "Bob",
        )?)]))
        .perform(&operator_b)
        .await?;
    let pushed = branch_b.push().perform(&operator_b).await?;
    assert!(
        pushed.is_some(),
        "B's push to the live device should succeed"
    );

    // --- A *reacts* to B's push: the head update wakes it up, and only
    // then does it re-resolve the branch. ---
    let update = tokio::time::timeout(std::time::Duration::from_secs(10), updates_a.recv())
        .await
        .expect("A should be woken up by B's push")?;
    assert_eq!(update.space, "branch/main");
    assert_eq!(update.cell, "revision");
    assert_eq!(update.origin, dialog_iroh_remote::HeadUpdateOrigin::Pushed);

    let branch_a = repo_a.branch("main").open().perform(&operator_a).await?;
    assert_eq!(
        names(&branch_a, &operator_a).await?,
        vec!["Alice", "Bob"],
        "A's live branch should now contain B's commit"
    );

    // --- Concurrent edits: both devices commit, B's push is rejected
    // (A's live head moved), B pulls to merge and retries. ---
    branch_a
        .commit(stream::iter(vec![Instruction::Assert(artifact(
            "user:carol",
            "Carol",
        )?)]))
        .perform(&operator_a)
        .await?;
    branch_b
        .commit(stream::iter(vec![Instruction::Assert(artifact(
            "user:dave",
            "Dave",
        )?)]))
        .perform(&operator_b)
        .await?;

    let conflicted = branch_b.push().perform(&operator_b).await;
    assert!(
        matches!(
            conflicted,
            Err(dialog_repository::PushError::NonFastForward { .. })
        ),
        "push against a moved live head should be non-fast-forward, got {conflicted:?}"
    );

    let merged = branch_b.pull().perform(&operator_b).await?;
    assert!(
        merged.is_some(),
        "pull should integrate A's concurrent commit"
    );
    let retried = branch_b.push().perform(&operator_b).await?;
    assert!(retried.is_some(), "push should succeed after merging");

    // The rejected push produced no update; the merged one wakes A again.
    let update = tokio::time::timeout(std::time::Duration::from_secs(10), updates_a.recv())
        .await
        .expect("A should be woken up by B's merged push")?;
    assert_eq!(update.origin, dialog_iroh_remote::HeadUpdateOrigin::Pushed);

    let branch_a = repo_a.branch("main").open().perform(&operator_a).await?;
    assert_eq!(
        names(&branch_a, &operator_a).await?,
        vec!["Alice", "Bob", "Carol", "Dave"],
        "both devices' concurrent commits should converge on A"
    );

    node_a.shutdown().await;
    Ok(())
}
