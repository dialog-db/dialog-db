//! Cross-peer collaboration over the swarm: three live devices editing
//! one shared branch, converging with no manual pulls.
//!
//! Device A hosts the repository over its own storage; devices B and C
//! track A as their remote and join the space's swarm. Every device runs
//! the full live-sync loop — local commits are announced
//! ([`IrohNode::announce_publishes`]) and a follower reacts to head
//! updates by pulling ([`SwarmHandle::follow`]) — so edits flow between
//! peers that never address each other directly:
//!
//! - A commits locally → the announce fans out → B *and* C converge.
//! - B commits and pushes to A → A's host announces the pushed head →
//!   C converges without ever contacting B.
//! - B and C commit concurrently and push — the loser's push is rejected
//!   by A's CAS (`NonFastForward`), it pulls to merge and retries — and
//!   all three replicas converge on the same fact set.
//!
//! A device that both edits and follows races itself by design: its
//! follower may move the local head between snapshot and publish, so a
//! local commit can fail with `VersionMismatch` the same way concurrent
//! handles do. The helpers here recover the way a real client would —
//! refresh + re-commit, pull + re-push — bounded, never silent.
//!
//! Runs hermetically: relay-less nodes over direct localhost addresses.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
use dialog_capability::Subject;
use dialog_iroh_remote::{HeadUpdateOrigin, IrohNode};
use dialog_network::Network;
use dialog_operator::Operator;
use dialog_operator::helpers::{test_operator_with_profile, unique_name};
use dialog_repository::{
    Branch, CommitError, PublishError, PushError, RepositoryExt as _, SiteAddress,
};
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

/// Commit one fact, recovering from races with the device's own follower:
/// a concurrent pull moves the local head, the commit's CAS fails with a
/// `VersionMismatch`, and refresh + re-commit reconciles.
async fn commit_name(
    branch: &Branch,
    env: &Operator<VolatileSpace>,
    of: &str,
    name: &str,
) -> Result<()> {
    for _ in 0..10 {
        let changes = stream::iter(vec![Instruction::Assert(artifact(of, name)?)]);
        match branch.commit(changes).perform(env).await {
            Ok(_) => return Ok(()),
            Err(CommitError::Publish(PublishError::VersionMismatch { .. })) => {
                branch.refresh(env).await?;
            }
            Err(other) => return Err(other.into()),
        }
    }
    anyhow::bail!("commit of {name:?} kept losing the race with concurrent writes")
}

/// Push, recovering from concurrent collaborators: a rejected
/// non-fast-forward push pulls to merge the remote head and retries.
async fn push_merging(branch: &Branch, env: &Operator<VolatileSpace>) -> Result<()> {
    for _ in 0..10 {
        match branch.push().perform(env).await {
            Ok(_) => return Ok(()),
            Err(PushError::NonFastForward { .. }) => {
                // The device's follower may be pulling the same head
                // concurrently; either pull integrating it is enough.
                let _ = branch.pull().perform(env).await;
            }
            Err(other) => return Err(other.into()),
        }
    }
    anyhow::bail!("push kept losing the race with concurrent collaborators")
}

/// Wait until the branch's replica converges on the expected fact set,
/// without any manual pull — refresh only re-reads the local head that
/// the device's follower maintains.
async fn converge(
    branch: &Branch,
    env: &Operator<VolatileSpace>,
    expected: &[&str],
    device: &str,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        branch.refresh(env).await?;
        let seen = names(branch, env).await?;
        if seen == expected {
            return Ok(());
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "{device} should converge on {expected:?} without a manual pull, has {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn it_collaborates_across_three_live_peers() -> Result<()> {
    // Outbound client node for fork invocations (all "devices" share one
    // process here, so they share the process-global client).
    let client = IrohNode::builder().direct_only().spawn().await?;
    dialog_iroh_remote::install(client.clone())?;

    // --- Device A: hosts the repository over its own live storage. ---
    let (operator_a, profile_a) = test_operator_with_profile().await;
    let repo_a = profile_a
        .repository(unique_name("device-a"))
        .create()
        .perform(&operator_a)
        .await?;
    let node_a = IrohNode::builder()
        .direct_only()
        .host(repo_a.did().clone(), operator_a.storage())
        .spawn()
        .await?;
    let swarm_a = node_a.join_swarm(&repo_a.did(), Vec::new()).await?;
    node_a.announce_publishes(operator_a.storage().publishes());
    let branch_a = repo_a.branch("main").open().perform(&operator_a).await?;

    // --- Devices B and C: distinct profiles, storage, swarm nodes. ---
    // Each tracks A as its remote, joins the swarm through its own node,
    // and runs a follower that pulls whenever the branch head moves.
    let mut devices = Vec::new();
    for label in ["device-b", "device-c"] {
        let (operator, profile) = test_operator_with_profile().await;

        // A authorizes the device's profile for the repository.
        let chain = repo_a
            .access()
            .claim(&repo_a)
            .delegate(profile.did())
            .perform(&operator_a)
            .await?;
        profile.access().save(chain).perform(&operator).await?;

        let repo = profile
            .repository(unique_name(label))
            .open()
            .perform(&operator)
            .await?;
        let origin = repo
            .remote("device-a")
            .create(SiteAddress::Iroh(node_a.address()))
            .subject(repo_a.did())
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let upstream = origin.branch("main").open().perform(&operator).await?;
        branch.set_upstream(upstream).perform(&operator).await?;

        let node = IrohNode::builder().direct_only().spawn().await?;
        let swarm = node
            .join_swarm(&repo_a.did(), vec![node_a.address()])
            .await?;

        // A background syncer: its own session key over the device's
        // storage, authorized through the repo → profile delegation.
        let follower_operator = Arc::new(
            profile
                .derive(label.as_bytes())
                .allow(Subject::any())
                .network(Network::default())
                .build(operator.storage())
                .await?,
        );
        let follower_branch = branch.clone();
        let follower = swarm.follow("branch/main", "revision", move |_| {
            let branch = follower_branch.clone();
            let operator = follower_operator.clone();
            async move {
                // Idempotent reaction: a pull that finds nothing is a no-op.
                let _ = branch.pull().perform(&*operator).await;
            }
        });

        devices.push((operator, branch, swarm, node, follower));
    }
    let (operator_b, branch_b, swarm_b, _node_b, follower_b) = {
        let d = devices.remove(0);
        (d.0, d.1, d.2, d.3, d.4)
    };
    let (operator_c, branch_c, swarm_c, _node_c, follower_c) = {
        let d = devices.remove(0);
        (d.0, d.1, d.2, d.3, d.4)
    };

    // Wait for the gossip mesh before relying on announce delivery.
    swarm_a.joined().await;
    swarm_b.joined().await;
    swarm_c.joined().await;
    let mut updates_c = swarm_c.updates();

    // --- A commits locally: the announce fans out, both peers converge
    // with no push from A and no manual pull anywhere. ---
    commit_name(&branch_a, &operator_a, "user:alice", "Alice").await?;
    converge(&branch_b, &operator_b, &["Alice"], "B").await?;
    converge(&branch_c, &operator_c, &["Alice"], "C").await?;

    // C learned of A's local commit via a swarm announce, not a push.
    let update = tokio::time::timeout(Duration::from_secs(10), updates_c.recv())
        .await
        .expect("C should have observed A's announce")?;
    assert_eq!(update.space, "branch/main");
    assert_eq!(update.origin, HeadUpdateOrigin::Announced);

    // --- B edits and pushes to A; C converges without ever contacting
    // B — the pushed head is announced by A's host into the swarm. ---
    commit_name(&branch_b, &operator_b, "user:bob", "Bob").await?;
    push_merging(&branch_b, &operator_b).await?;
    converge(&branch_a, &operator_a, &["Alice", "Bob"], "A").await?;
    converge(&branch_c, &operator_c, &["Alice", "Bob"], "C").await?;

    // --- Concurrent edits: B and C commit independently and push at the
    // same time. One push lands; the other is rejected by A's CAS, pulls
    // to merge, retries — and every replica converges on the union. ---
    commit_name(&branch_b, &operator_b, "user:carol", "Carol").await?;
    commit_name(&branch_c, &operator_c, "user:dave", "Dave").await?;
    let (pushed_b, pushed_c) = tokio::join!(
        push_merging(&branch_b, &operator_b),
        push_merging(&branch_c, &operator_c),
    );
    pushed_b?;
    pushed_c?;

    let everyone = ["Alice", "Bob", "Carol", "Dave"];
    converge(&branch_a, &operator_a, &everyone, "A").await?;
    converge(&branch_b, &operator_b, &everyone, "B").await?;
    converge(&branch_c, &operator_c, &everyone, "C").await?;

    follower_b.abort();
    follower_c.abort();
    node_a.shutdown().await;
    Ok(())
}
