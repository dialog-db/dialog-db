//! Edits relayed across a chain of *distinct* repositories.
//!
//! Unlike `collaboration.rs` — where every peer tracks the same hosted
//! repository — here each device owns a repository under its own subject
//! DID with its own branch name, and the devices are configured as
//! remotes *of one another* in a chain:
//!
//! ```text
//! A: repo did_A, branch "trunk"   (hosts did_A)
//! B: repo did_B, branch "draft"   (hosts did_B; remote → A, upstream "trunk")
//! C: repo did_C, branch "notes"   (remote → B, upstream "draft")
//! ```
//!
//! B is the only device that knows both ends. Downstream, B's follower
//! pulls A's announces into "draft", and B's own announce wakes C to pull
//! "draft" into "notes". Upstream, B relays with [`follow_publishes`]:
//! whenever its live draft head moves — a local commit, a pull from A,
//! or C pushing into B's host, all carried exactly once by B's storage
//! publish stream — B pushes the head on to A (the idle case is free: a
//! push with nothing novel is a no-op). So every edit flows both ways
//! through three different subjects and three branch names without any
//! end-to-end coordination.
//!
//! The authorization story is the point: C holds a delegation rooted in
//! *B's* repository only — nothing from A — and A never learns C exists.
//! C's edits reach A because B pushes its own replica under B's
//! delegation; A's edits reach C because B serves its replica under B's
//! authority. Capability mediation, not global ACLs.
//!
//! Runs hermetically: relay-less nodes over direct localhost addresses.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dialog_artifacts::{Artifact, ArtifactSelector, Instruction, Value};
use dialog_capability::Subject;
use dialog_iroh_remote::IrohNode;
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

/// Commit one fact, recovering from races with the device's own
/// followers: a concurrent pull moves the local head, the commit's CAS
/// fails with a `VersionMismatch`, and refresh + re-commit reconciles.
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
/// non-fast-forward push pulls to merge the upstream head and retries.
async fn push_merging(branch: &Branch, env: &Operator<VolatileSpace>) -> Result<()> {
    for _ in 0..10 {
        match branch.push().perform(env).await {
            Ok(_) => return Ok(()),
            Err(PushError::NonFastForward { .. }) => {
                let _ = branch.pull().perform(env).await;
            }
            Err(other) => return Err(other.into()),
        }
    }
    anyhow::bail!("push kept losing the race with concurrent collaborators")
}

/// Wait until the branch's replica converges on the expected fact set,
/// without any manual pull — refresh only re-reads the local head that
/// the device's followers maintain.
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
async fn it_relays_edits_across_distinct_repos_and_branches() -> Result<()> {
    // Outbound client node for fork invocations (all "devices" share one
    // process here, so they share the process-global client).
    let client = IrohNode::builder().direct_only().spawn().await?;
    dialog_iroh_remote::install(client)?;

    // --- Device A: repo did_A, branch "trunk", hosts its own storage. ---
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
    let branch_a = repo_a.branch("trunk").open().perform(&operator_a).await?;

    // --- Device B: repo did_B, branch "draft", remote → A. ---
    let (operator_b, profile_b) = test_operator_with_profile().await;

    // A authorizes B's profile for did_A; C never gets such a chain.
    let chain_ab = repo_a
        .access()
        .claim(&repo_a)
        .delegate(profile_b.did())
        .perform(&operator_a)
        .await?;
    profile_b
        .access()
        .save(chain_ab)
        .perform(&operator_b)
        .await?;

    let repo_b = profile_b
        .repository(unique_name("device-b"))
        .create()
        .perform(&operator_b)
        .await?;
    let origin_a = repo_b
        .remote("device-a")
        .create(SiteAddress::Iroh(node_a.address()))
        .subject(repo_a.did())
        .perform(&operator_b)
        .await?;
    let branch_b = repo_b.branch("draft").open().perform(&operator_b).await?;
    let upstream_a = origin_a.branch("trunk").open().perform(&operator_b).await?;
    branch_b
        .set_upstream(upstream_a)
        .perform(&operator_b)
        .await?;

    // B hosts did_B over its own live storage (so C can sync with it) and
    // participates in both swarms: A's (downstream) and its own (up- and
    // downstream around itself).
    let node_b = IrohNode::builder()
        .direct_only()
        .host(repo_b.did().clone(), operator_b.storage())
        .spawn()
        .await?;
    let swarm_ba = node_b
        .join_swarm(&repo_a.did(), vec![node_a.address()])
        .await?;
    let swarm_bb = node_b.join_swarm(&repo_b.did(), Vec::new()).await?;
    node_b.announce_publishes(operator_b.storage().publishes());

    // B's background syncer identity: a session key over B's storage,
    // carrying both the did_A chain (to pull/push A) and B's own
    // authority (to publish locally).
    let syncer_b = Arc::new(
        profile_b
            .derive(b"syncer")
            .allow(Subject::any())
            .network(Network::default())
            .build(operator_b.storage())
            .await?,
    );

    // Downstream follower: A announced "trunk" moved → pull it into
    // "draft".
    let puller_branch = branch_b.clone();
    let puller_operator = syncer_b.clone();
    let follower_b = swarm_ba.follow("branch/trunk", "revision", move |_| {
        let branch = puller_branch.clone();
        let operator = puller_operator.clone();
        async move {
            let _ = branch.pull().perform(&*operator).await;
        }
    });

    // Upstream relay: our own live "draft" head moved — a local commit,
    // a pull from A, or C pushing into our host — so forward it to A.
    // The swarm never echoes a device's own publishes back to it, so the
    // relay follows the storage publish stream instead, which carries
    // every movement of the live head exactly once. An idle tick is free
    // (a push with nothing novel is a no-op), and a lost race with a
    // concurrent A-side writer merges and retries.
    let relay_branch = branch_b.clone();
    let relay_operator = syncer_b.clone();
    let relay_b = dialog_iroh_remote::follow_publishes(
        operator_b.storage().publishes(),
        repo_b.did().clone(),
        "branch/draft",
        "revision",
        move |_| {
            let branch = relay_branch.clone();
            let operator = relay_operator.clone();
            async move {
                let _ = branch.refresh(&*operator).await;
                for _ in 0..5 {
                    match branch.push().perform(&*operator).await {
                        Err(PushError::NonFastForward { .. }) => {
                            let _ = branch.pull().perform(&*operator).await;
                        }
                        _ => break,
                    }
                }
            }
        },
    );

    // --- Device C: repo did_C, branch "notes", remote → B. ---
    let (operator_c, profile_c) = test_operator_with_profile().await;

    // B authorizes C for *B's* repository. C holds nothing from A.
    let chain_bc = repo_b
        .access()
        .claim(&repo_b)
        .delegate(profile_c.did())
        .perform(&operator_b)
        .await?;
    profile_c
        .access()
        .save(chain_bc)
        .perform(&operator_c)
        .await?;

    let repo_c = profile_c
        .repository(unique_name("device-c"))
        .open()
        .perform(&operator_c)
        .await?;
    let origin_b = repo_c
        .remote("device-b")
        .create(SiteAddress::Iroh(node_b.address()))
        .subject(repo_b.did())
        .perform(&operator_c)
        .await?;
    let branch_c = repo_c.branch("notes").open().perform(&operator_c).await?;
    let upstream_b = origin_b.branch("draft").open().perform(&operator_c).await?;
    branch_c
        .set_upstream(upstream_b)
        .perform(&operator_c)
        .await?;

    let node_c = IrohNode::builder().direct_only().spawn().await?;
    let swarm_cb = node_c
        .join_swarm(&repo_b.did(), vec![node_b.address()])
        .await?;

    let syncer_c = Arc::new(
        profile_c
            .derive(b"syncer")
            .allow(Subject::any())
            .network(Network::default())
            .build(operator_c.storage())
            .await?,
    );
    let follower_branch = branch_c.clone();
    let follower_operator = syncer_c.clone();
    let follower_c = swarm_cb.follow("branch/draft", "revision", move |_| {
        let branch = follower_branch.clone();
        let operator = follower_operator.clone();
        async move {
            let _ = branch.pull().perform(&*operator).await;
        }
    });

    // Wait for both gossip meshes before relying on announce delivery.
    swarm_a.joined().await;
    swarm_ba.joined().await;
    swarm_bb.joined().await;
    swarm_cb.joined().await;

    // --- A commits on "trunk": B pulls it into "draft", B's announce
    // wakes C to pull it into "notes". Two hops, no manual sync. ---
    commit_name(&branch_a, &operator_a, "user:alice", "Alice").await?;
    converge(&branch_b, &operator_b, &["Alice"], "B (draft)").await?;
    converge(&branch_c, &operator_c, &["Alice"], "C (notes)").await?;

    // --- C commits on "notes" and pushes to B's "draft"; B's relay
    // forwards it to A's "trunk". C's edit reaches a repository it holds
    // no delegation for, through B's mediation. ---
    commit_name(&branch_c, &operator_c, "user:carol", "Carol").await?;
    push_merging(&branch_c, &operator_c).await?;
    converge(&branch_a, &operator_a, &["Alice", "Carol"], "A (trunk)").await?;
    converge(&branch_b, &operator_b, &["Alice", "Carol"], "B (draft)").await?;

    // --- B commits on "draft": fully live in both directions at once —
    // the relay pushes it up to A while B's announce wakes C to pull it
    // down into "notes". No manual push or pull anywhere. ---
    commit_name(&branch_b, &operator_b, "user:bob", "Bob").await?;
    let everyone = ["Alice", "Bob", "Carol"];
    converge(&branch_a, &operator_a, &everyone, "A (trunk)").await?;
    converge(&branch_b, &operator_b, &everyone, "B (draft)").await?;
    converge(&branch_c, &operator_c, &everyone, "C (notes)").await?;

    follower_b.abort();
    relay_b.abort();
    follower_c.abort();
    node_a.shutdown().await;
    node_b.shutdown().await;
    Ok(())
}
