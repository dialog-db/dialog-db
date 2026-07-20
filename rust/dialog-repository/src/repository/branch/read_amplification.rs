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

use dialog_artifacts::history::context_of;
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
    scenario: &'static str,
    block_reads: u64,
    effects: u64,
    millis: u128,
}

impl Sample {
    fn row(&self) -> String {
        format!(
            "| {:>6} | {:<26} | {:>11} | {:>7} | {:>8} |",
            self.depth, self.scenario, self.block_reads, self.effects, self.millis
        )
    }
}

/// One measured operation: reset the tally, run, record reads/effects
/// and wall time.
macro_rules! measured {
    ($samples:expr, $env:expr, $depth:expr, $name:literal, $op:expr) => {{
        $env.reset();
        let started = Instant::now();
        let outcome = $op;
        $samples.push(Sample {
            depth: $depth,
            scenario: $name,
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
        "triangle: adopt alice",
        us.pull().from(&alice).perform(&env).await?
    );
    measured!(
        samples,
        env,
        depth,
        "triangle: tracked bob after",
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

/// Compares the canonical and buffered write paths on the same instruction
/// stream, at the same tree depth.
///
/// The question this answers: a commit must publish a canonical root, so the
/// buffered path has to canonicalize before the head is signed. Does buffering
/// still pay once that flush is charged to it?
///
/// Two regimes are measured, because they answer different questions:
///
/// - **per batch**: canonicalize after every batch, which is what a commit
///   publishing a root every time would do. Buffering can only lose here, and
///   the number says by how much.
/// - **per N batches**: canonicalize once per `N`, which is what a commit path
///   would look like if it could defer the flush (publishing a head only at a
///   sync/publish point). This is the regime the buffer is designed for.
async fn measure_write_paths(depth: usize, batches: usize) -> Result<()> {
    use crate::RepositoryArchiveExt as _;
    use dialog_artifacts::tree::ArtifactTreeExt as _;
    use dialog_artifacts::tree::write_instructions;
    use dialog_artifacts::{Instruction as I, apply_buffered};
    use dialog_effects::prelude::CatalogExt as _;
    use dialog_search_tree::Delta;

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
    let base = crate::Index::from_hash(dialog_common::Blake3Hash::from(*root.hash()));

    // One fact per batch: the interactive-commit shape, where the fixed
    // per-batch costs are least amortized and buffering has the most to prove.
    let batch = |i: usize| -> Vec<I> { vec![assert_fact(depth + i, "write-path")] };

    // Every batch's new nodes are imported into the archive, exactly as the
    // commit path does before it references the root in a revision. Without
    // this the next batch cannot read back what the last one wrote.
    macro_rules! persist {
        ($delta:expr) => {
            main.archive()
                .index()
                .import($delta.flush().map(|(_, buffer)| buffer))
                .perform(&env)
                .await?
        };
    }

    // Canonical: reshape per batch, exactly what the commit path does today.
    let mut store = crate::NetworkedIndex::new(&env, main.archive().index(), None);
    let mut canonical = base.clone();
    let started = Instant::now();
    for i in 0..batches {
        let mut delta = Delta::zero();
        canonical
            .apply_versioned(&mut store, &mut delta, None, stream::iter(batch(i)))
            .await?;
        persist!(delta);
    }
    let canonical_ms = started.elapsed().as_millis();

    // Buffered, flushed per batch: the same reshape work plus buffer overhead.
    let mut per_batch = base.clone();
    let started = Instant::now();
    for i in 0..batches {
        let mut delta = Delta::zero();
        apply_buffered(
            &mut per_batch,
            &mut store,
            &mut delta,
            None,
            stream::iter(batch(i)),
            true,
        )
        .await?;
        persist!(delta);
    }
    let per_batch_ms = started.elapsed().as_millis();

    // Buffered, flushed once at the end: the regime buffering is built for.
    let storage =
        dialog_search_tree::ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
    let mut deferred = dialog_search_tree::HitchhikerTree::open(&base);
    let started = Instant::now();
    for i in 0..batches {
        let (next, _) =
            write_instructions(deferred, &mut store, &storage, None, stream::iter(batch(i)))
                .await?;
        deferred = next;
    }
    let mut delta = Delta::zero();
    let _ = deferred.canonicalize(&storage, &mut delta).await?;
    let deferred_ms = started.elapsed().as_millis();
    persist!(delta);

    println!(
        "| write paths, depth {depth}, {batches} single-fact batches: \
         canonical {canonical_ms}ms | buffered-per-batch {per_batch_ms}ms | \
         buffered-deferred-flush {deferred_ms}ms |"
    );
    Ok(())
}

/// Prints a table of block reads, total effect dispatches, and wall
/// time per scenario and depth. `#[ignore]`d: a measurement, not an
/// assertion; see the module docs for the invocation.
#[dialog_common::test]
#[ignore]
async fn write_path_comparison() -> Result<()> {
    for depth in [1_000, 10_000] {
        measure_write_paths(depth, 100).await?;
    }
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

    println!("| depth  | scenario                   | block reads | effects | wall ms  |");
    println!("|--------|----------------------------|-------------|---------|----------|");
    for sample in &samples {
        println!("{}", sample.row());
    }
    Ok(())
}

/// The same measurements the pre-version-control baseline takes, so the two
/// can be compared directly: commit wall time at depth, merge reads, and the
/// zero-read paths.
#[dialog_common::test]
#[ignore]
async fn current_costs() -> Result<()> {
    for depth in [1_000usize, 10_000] {
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

        let started = Instant::now();
        for i in 0..100 {
            main.commit(stream::iter(vec![assert_fact(depth + i, "measure")]))
                .perform(&env)
                .await?;
        }
        let commit_ms = started.elapsed().as_millis();

        let feature = repo.branch("feature").open().perform(&env).await?;
        feature.set_upstream(&main).perform(&env).await?;
        feature.pull().perform(&env).await?;
        main.commit(stream::iter(vec![assert_fact(depth + 500, "theirs")]))
            .perform(&env)
            .await?;
        feature
            .commit(stream::iter(vec![assert_fact(depth + 501, "ours")]))
            .perform(&env)
            .await?;
        env.reset();
        let started = Instant::now();
        feature.pull().perform(&env).await?;
        let merge_ms = started.elapsed().as_millis();
        let merge_reads = env.block_reads();

        let ff = repo.branch("ff").open().perform(&env).await?;
        ff.set_upstream(&main).perform(&env).await?;
        ff.pull().perform(&env).await?;
        main.commit(stream::iter(vec![assert_fact(depth + 700, "ff")]))
            .perform(&env)
            .await?;
        env.reset();
        ff.pull().perform(&env).await?;
        let ff_reads = env.block_reads();
        env.reset();
        ff.pull().perform(&env).await?;
        let noop_reads = env.block_reads();

        println!(
            "| CURRENT depth {depth}: 100 commits {commit_ms}ms ({:.2}ms each) | \
             merge {merge_reads} reads / {merge_ms}ms | ff {ff_reads} reads | no-op {noop_reads} reads |",
            commit_ms as f64 / 100.0
        );
    }
    Ok(())
}
