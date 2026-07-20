//! Read-amplification measurements for pull.
//!
//! Measures the cost of a pull in **block reads** (archive `Get`
//! executions, one digest per call) rather than wall time, across
//! history depths and the three sync shapes: a no-op tick, a
//! one-commit fast-forward, and a real merge. The causal-context
//! derivation (`context_of`, an O(ancestry) walk) is also measured on
//! its own, since it is the cost this design added to pull.
//!
//! Not part of the regular suite. Run explicitly, in release, with
//! output:
//!
//! ```text
//! cargo test -p dialog-repository --release --features integration-tests \
//!     read_amplification -- --ignored --nocapture
//! ```

use crate::RevisionExt as _;
use std::time::Instant;

use anyhow::Result;
use futures_util::stream;

use dialog_artifacts::history::{Context, context_of};
use dialog_artifacts::{Artifact, Instruction, Value};

use crate::RepositoryExt as _;
use crate::helpers::{Counting, test_operator_with_profile, unique_name};
use dialog_artifacts::tree::TreeStorageBridge;

fn assert_fact(entity: usize, value: &str) -> Instruction {
    Instruction::Assert(Artifact {
        the: "bench/field".parse().unwrap(),
        of: format!("user:{entity}").parse().unwrap(),
        is: Value::String(value.to_string()),
        cause: None,
    })
}

struct Sample {
    depth: usize,
    scenario: String,
    block_reads: u64,
    effects: u64,
    millis: u128,
}

impl Sample {
    fn row(&self) -> String {
        format!(
            "| {:>6} | {:<34} | {:>11} | {:>7} | {:>8} |",
            self.depth, self.scenario, self.block_reads, self.effects, self.millis
        )
    }
}

/// Which cascade path the two published watermarks route a tracked pull
/// to, mirroring the gate arithmetic in `Pull::prepare` (tree-vs-base
/// equality aside). Rows print this next to the scenario name so the
/// table is self-labeling — the harness once claimed a "graft" row that
/// the threshold actually routed through the screened path.
fn routed(ours: &Context, theirs: &Context) -> &'static str {
    if ours.includes(theirs) {
        "skip"
    } else if theirs.includes(ours) {
        "ff/adopt"
    } else if ours.divergence(theirs).min(theirs.divergence(ours)) > super::pull::SMALL_DIVERGENCE {
        "graft"
    } else if ours.divergence(theirs) <= theirs.divergence(ours) {
        "replay-ours"
    } else {
        "screen-theirs"
    }
}

/// The `routed` label for a pull of `upstream` into `branch`, from their
/// published heads; "legacy" when either head predates watermarks.
fn routed_label(branch: &crate::Branch, upstream: &crate::Branch) -> &'static str {
    match (
        branch.revision().and_then(|r| r.context),
        upstream.revision().and_then(|r| r.context),
    ) {
        (Some(ours), Some(theirs)) => routed(&ours, &theirs),
        _ => "legacy",
    }
}

/// One measured operation: reset the tally, run, record reads/effects
/// and wall time.
macro_rules! measured {
    ($samples:expr, $env:expr, $depth:expr, $name:expr, $op:expr) => {{
        let scenario = String::from($name);
        $env.reset();
        let started = Instant::now();
        let outcome = $op;
        $samples.push(Sample {
            depth: $depth,
            scenario,
            block_reads: $env.block_reads(),
            effects: $env.snapshot().values().sum(),
            millis: started.elapsed().as_millis(),
        });
        outcome
    }};
}

async fn measure_depth(depth: usize, samples: &mut Vec<Sample>) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let env = Counting::new(operator);
    let repo = profile
        .repository(unique_name("bench"))
        .open()
        .perform(&env)
        .await?;

    // `main` accumulates `depth` commits: the ancestry every later
    // pull's context derivation walks.
    let main = repo.branch("main").open().perform(&env).await?;
    for i in 0..depth {
        main.commit(stream::iter(vec![assert_fact(i, "seed")]))
            .perform(&env)
            .await?;
    }

    // `feature` tracks main. The initial pull is the "fresh replica
    // adopts a deep history" case.
    let feature = repo.branch("feature").open().perform(&env).await?;
    feature.set_upstream(&main).perform(&env).await?;
    measured!(
        samples,
        env,
        depth,
        "initial pull (adopt all)",
        feature.pull().perform(&env).await?
    );

    // The idle sync tick: upstream unchanged.
    measured!(
        samples,
        env,
        depth,
        "no-op tick",
        feature.pull().perform(&env).await?
    );

    // The common auto-sync case: upstream moved by one commit, the
    // receiver has no novelty.
    main.commit(stream::iter(vec![assert_fact(depth + 1, "ff")]))
        .perform(&env)
        .await?;
    measured!(
        samples,
        env,
        depth,
        "fast-forward (1 commit)",
        feature.pull().perform(&env).await?
    );

    // A real merge: both sides moved.
    main.commit(stream::iter(vec![assert_fact(depth + 2, "theirs")]))
        .perform(&env)
        .await?;
    feature
        .commit(stream::iter(vec![assert_fact(depth + 3, "ours")]))
        .perform(&env)
        .await?;
    measured!(
        samples,
        env,
        depth,
        "merge (both sides moved)",
        feature.pull().perform(&env).await?
    );

    // The context derivation alone, on the (now deep) feature head.
    let head = feature
        .revision()
        .expect("feature has a head after the merge");
    let history = feature.history(&env);
    measured!(
        samples,
        env,
        depth,
        "context_of alone",
        context_of(&head.version(), &history).await?
    );

    Ok(())
}

/// The cross-upstream (triangle) shape: adopt a bulky upstream by root,
/// then merge with a second upstream that has never seen that bulk. The
/// merge's cost should track the intersection of the two change sets,
/// not the adopted bulk — this is the scenario the graft merge exists
/// for, and the row that shows whether it is doing its job.
async fn measure_triangle(depth: usize, samples: &mut Vec<Sample>) -> Result<()> {
    let (operator, profile) = test_operator_with_profile().await;
    let env = Counting::new(operator);
    let repo = profile
        .repository(unique_name("bench"))
        .open()
        .perform(&env)
        .await?;

    // A shared seed both upstreams start from.
    let seed = repo.branch("seed").open().perform(&env).await?;
    seed.commit(stream::iter(vec![assert_fact(0, "seed")]))
        .perform(&env)
        .await?;

    // Bob diverges from the seed with a handful of commits.
    let bob = repo.branch("bob").open().perform(&env).await?;
    bob.set_upstream(&seed).perform(&env).await?;
    bob.pull().perform(&env).await?;
    for i in 0..5 {
        bob.commit(stream::iter(vec![assert_fact(depth + 10 + i, "bob")]))
            .perform(&env)
            .await?;
    }

    // Alice diverges from the seed with `depth` commits of bulk.
    let alice = repo.branch("alice").open().perform(&env).await?;
    alice.set_upstream(&seed).perform(&env).await?;
    alice.pull().perform(&env).await?;
    for i in 0..depth {
        alice
            .commit(stream::iter(vec![assert_fact(i + 1, "alice")]))
            .perform(&env)
            .await?;
    }

    // We adopt the seed, sync Bob while he is small (tracking him), then
    // adopt Alice's bulk, and finally pull Bob again after he moved: the
    // tracked cross-upstream merge, where our divergence is bulky and
    // his delta is tiny. The merge direction must follow the smaller
    // side, not the tracked-ness of the upstream.
    let us = repo.branch("us").open().perform(&env).await?;
    us.set_upstream(&seed).perform(&env).await?;
    us.pull().perform(&env).await?;
    us.pull().from(&bob).perform(&env).await?;
    for i in 0..3 {
        bob.commit(stream::iter(vec![assert_fact(depth + 20 + i, "bob late")]))
            .perform(&env)
            .await?;
    }
    measured!(
        samples,
        env,
        depth,
        format!("triangle: adopt alice [{}]", routed_label(&us, &alice)),
        us.pull().from(&alice).perform(&env).await?
    );
    // A 3-commit late delta sits under the graft threshold: this row
    // measures the screened direction.
    measured!(
        samples,
        env,
        depth,
        format!("triangle: bob small [{}]", routed_label(&us, &bob)),
        us.pull().from(&bob).perform(&env).await?
    );
    // A dozen more late commits push Bob's delta past the threshold:
    // this row measures the graft itself (partition, stitch, contested
    // integrate, coverage repair).
    for i in 0..12 {
        bob.commit(stream::iter(vec![assert_fact(depth + 30 + i, "bob later")]))
            .perform(&env)
            .await?;
    }
    us.commit(stream::iter(vec![assert_fact(depth + 50, "ours late")]))
        .perform(&env)
        .await?;
    measured!(
        samples,
        env,
        depth,
        format!("triangle: bob bulky [{}]", routed_label(&us, &bob)),
        us.pull().from(&bob).perform(&env).await?
    );

    Ok(())
}

/// Prints the actual shape of the harness tree at one depth: how many
/// entries and how many tree nodes it holds. Grounds the read counts:
/// a merge's reads should be compared against these totals.
async fn measure_shape(depth: usize) -> Result<()> {
    use crate::RepositoryArchiveExt as _;
    use dialog_search_tree::TreeDifference;

    let (operator, profile) = test_operator_with_profile().await;
    let env = Counting::new(operator);
    let repo = profile
        .repository(unique_name("bench"))
        .open()
        .perform(&env)
        .await?;
    let main = repo.branch("main").open().perform(&env).await?;
    for i in 0..depth {
        main.commit(stream::iter(vec![assert_fact(i, "seed")]))
            .perform(&env)
            .await?;
    }

    let root = main.revision().expect("committed").tree;
    let store = crate::NetworkedIndex::new(&env, main.archive().index(), None);
    let tree = crate::Index::from_hash(dialog_common::Blake3Hash::from(*root.hash()));
    let tree_store = dialog_search_tree::ContentAddressedStorage::new(TreeStorageBridge(store));

    let entries = {
        use futures_util::StreamExt as _;
        let stream = tree.stream(&tree_store);
        futures_util::pin_mut!(stream);
        let mut count = 0usize;
        while let Some(entry) = stream.next().await {
            entry?;
            count += 1;
        }
        count
    };

    let empty = crate::Index::from_hash(dialog_common::Blake3Hash::from(crate::EMPTY_TREE_HASH));
    let difference = TreeDifference::compute(&empty, &tree, &tree_store, &tree_store).await?;
    let nodes = {
        use futures_util::StreamExt as _;
        let stream = difference.novel_nodes();
        futures_util::pin_mut!(stream);
        let mut count = 0usize;
        while let Some(node) = stream.next().await {
            node?;
            count += 1;
        }
        count
    };

    println!("| shape at depth {depth}: {entries} entries in {nodes} tree nodes |");
    Ok(())
}

/// Prints a table of block reads, total effect dispatches, and wall
/// time per scenario and depth. `#[ignore]`d: a measurement, not an
/// assertion; see the module docs for the invocation.
#[dialog_common::test]
#[ignore]
async fn read_amplification_by_depth() -> Result<()> {
    let mut samples = Vec::new();
    for depth in [100, 1_000, 10_000] {
        measure_depth(depth, &mut samples).await?;
    }
    for depth in [1_000, 10_000] {
        measure_triangle(depth, &mut samples).await?;
    }
    measure_shape(10_000).await?;

    println!("| depth  | scenario                           | block reads | effects | wall ms  |");
    println!("|--------|------------------------------------|-------------|---------|----------|");
    for sample in &samples {
        println!("{}", sample.row());
    }
    Ok(())
}
