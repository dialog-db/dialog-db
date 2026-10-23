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
//!
//! The causal context's own weight, by how many origins a head's
//! ancestry includes, is measured by `context_cost_by_origins` the same
//! way (`DIALOG_RA_MAX_ORIGINS` caps its sweep).

use std::env;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use futures_util::stream;

use dialog_artifacts::history::{Context, context_of};
use dialog_artifacts::{
    ATTRIBUTE_KEY_TAG, ArchiveDelta, Artifact, BLOB_KEY_TAG, COVERAGE_KEY_TAG, Datum,
    ENTITY_KEY_TAG, HISTORY_KEY_TAG, Instruction, Key, State, VALUE_KEY_TAG, Value,
};
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_search_tree::{FrameAttribution, LoadBlock, Manifest, PersistentNode};

use crate::RepositoryExt as _;
use crate::helpers::{Counting, prune_unreachable as prune};
use dialog_peer::helpers::{Meter, Metered, Tally, test_session_with_peer, unique_name};

/// How many setup commits a fixture runs between prunes of its store.
///
/// The volatile store keeps every block ever written, and a setup step
/// that commits rewrites the root (tens of kilobytes at depth), so a
/// fixture of thousands of steps would hold gigabytes of roots nothing
/// reaches: the harness's own garbage, not the product's memory. The
/// fixtures drop what their open branches no longer reach every this many
/// steps, so what the store holds at the end of setup is what a
/// persistent store would hold.
const PRUNE_EVERY: usize = 100;

/// The most resident memory this process ever held, in bytes, where the
/// platform tells (`VmHWM` on Linux); `None` elsewhere.
fn resident_peak() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        use std::fs::read_to_string;

        read_to_string("/proc/self/status")
            .ok()?
            .lines()
            .find(|line| line.starts_with("VmHWM:"))?
            .split_whitespace()
            .nth(1)?
            .parse::<usize>()
            .ok()
            .map(|kb| kb * 1024)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Prints the resident peak, when the platform tells it.
fn print_resident_peak() {
    if let Some(bytes) = resident_peak() {
        println!(
            "| resident peak | {:.1} MB |",
            bytes as f64 / (1024.0 * 1024.0)
        );
    }
}

fn assert_fact(entity: usize, value: &str) -> Instruction {
    Instruction::Assert(
        Artifact {
            the: "bench/field".parse().unwrap(),
            of: format!("user:{entity}").parse().unwrap(),
            is: Value::String(value.to_string()),
            cause: None,
        },
        dialog_artifacts::Pick::All,
    )
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
/// full contexts as the branch memo holds them (a head carries only its
/// active set); "legacy" when either is unknown.
async fn routed_label(branch: &crate::Branch, upstream: &crate::Branch) -> &'static str {
    let context = |branch: &crate::Branch| {
        let branch = branch.clone();
        async move {
            let head = branch.revision()?;
            branch.contexts().cached(&head.version()).await
        }
    };
    match (context(branch).await, context(upstream).await) {
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
    let (operator, profile) = test_session_with_peer().await;
    let env = Counting::new(operator);
    let repo = profile
        .space(unique_name("bench"))
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
        if i % PRUNE_EVERY == PRUNE_EVERY - 1 {
            prune(&env, &profile, &repo, &[&main]).await?;
        }
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
    let (operator, profile) = test_session_with_peer().await;
    let env = Counting::new(operator);
    let repo = profile
        .space(unique_name("bench"))
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
        if i % PRUNE_EVERY == PRUNE_EVERY - 1 {
            prune(&env, &profile, &repo, &[&seed, &bob, &alice]).await?;
        }
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
        format!(
            "triangle: adopt alice [{}]",
            routed_label(&us, &alice).await
        ),
        us.pull().from(&alice).perform(&env).await?
    );
    // A 3-commit late delta sits under the graft threshold: this row
    // measures the screened direction.
    measured!(
        samples,
        env,
        depth,
        format!("triangle: bob small [{}]", routed_label(&us, &bob).await),
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
        format!("triangle: bob bulky [{}]", routed_label(&us, &bob).await),
        us.pull().from(&bob).perform(&env).await?
    );

    Ok(())
}

/// Prints the actual shape of the harness tree at one depth: how many
/// entries and how many tree nodes it holds. Grounds the read counts:
/// a merge's reads should be compared against these totals.
async fn measure_shape(depth: usize) -> Result<()> {
    use dialog_search_tree::TreeDifference;

    let (operator, profile) = test_session_with_peer().await;
    let env = Counting::new(operator);
    let repo = profile
        .space(unique_name("bench"))
        .open()
        .perform(&env)
        .await?;
    let main = repo.branch("main").open().perform(&env).await?;
    for i in 0..depth {
        main.commit(stream::iter(vec![assert_fact(i, "seed")]))
            .perform(&env)
            .await?;
        if i % PRUNE_EVERY == PRUNE_EVERY - 1 {
            prune(&env, &profile, &repo, &[&main]).await?;
        }
    }

    let root = main.revision().expect("committed").tree;
    let store = crate::NetworkedIndex::new(&env, main.archive().index(), None);
    let tree = crate::Index::from_hash(dialog_common::Blake3Hash::from(*root.hash()));
    let tree_store = store;

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

    let empty = crate::Index::empty();
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
    use dialog_artifacts::tree::ArtifactTreeExt as _;
    use dialog_artifacts::tree::{Stamp, WriteScope, write_instructions};
    use dialog_artifacts::{Instruction as I, apply_buffered};

    let (operator, profile) = test_session_with_peer().await;
    let env = Counting::new(operator);
    let repo = profile
        .space(unique_name("bench"))
        .open()
        .perform(&env)
        .await?;
    let main = repo.branch("main").open().perform(&env).await?;
    for i in 0..depth {
        main.commit(stream::iter(vec![assert_fact(i, "seed")]))
            .perform(&env)
            .await?;
        if i % PRUNE_EVERY == PRUNE_EVERY - 1 {
            prune(&env, &profile, &repo, &[&main]).await?;
        }
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
                .import($delta.flush_blocks().chain($delta.flush_blobs()))
                .perform(&env)
                .await?
        };
    }

    // Canonical: reshape per batch, exactly what the commit path does today.
    let store = crate::NetworkedIndex::new(&env, main.archive().index(), None);
    let mut canonical = base.clone();
    let started = Instant::now();
    for i in 0..batches {
        let mut delta = ArchiveDelta::zero();
        canonical
            .apply_versioned(&store, &mut delta, None, stream::iter(batch(i)))
            .await?;
        persist!(delta);
    }
    let canonical_ms = started.elapsed().as_millis();

    // Buffered, flushed per batch: the same reshape work plus buffer overhead.
    let mut per_batch = base.clone();
    let started = Instant::now();
    for i in 0..batches {
        let mut delta = ArchiveDelta::zero();
        apply_buffered(
            &mut per_batch,
            &store,
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
    let mut delta = ArchiveDelta::zero();
    let staged = dialog_artifacts::DeltaOverlay::new(&delta, &store);
    let mut deferred = dialog_search_tree::HitchhikerTree::open(&base);
    let started = Instant::now();
    for i in 0..batches {
        let (next, _) = write_instructions(
            deferred,
            &staged,
            &mut delta,
            Stamp::Unversioned,
            // The bench tree carries the default manifest.
            &dialog_search_tree::Manifest::default(),
            stream::iter(batch(i)),
            WriteScope::Application,
        )
        .await?;
        deferred = next;
    }
    let _ = deferred.canonicalize(&staged, delta.blocks()).await?;
    let deferred_ms = started.elapsed().as_millis();
    persist!(delta);

    println!(
        "| write paths, depth {depth}, {batches} single-fact batches: \
         canonical {canonical_ms}ms | buffered-per-batch {per_batch_ms}ms | \
         buffered-deferred-flush {deferred_ms}ms |"
    );
    Ok(())
}

/// What a head whose ancestry includes `origins` one-commit origins
/// costs: the head on the wire and under the signature, a commit on top
/// of it, the memo entry it occupies, and the block reads of the two
/// frugal pulls (fast-forward and nothing-new) that every device runs.
///
/// The origins are minted the way a passing participant mints one: a
/// branch adopts the previous branch's head and commits once, so each
/// branch is one more `(branch, issuer)` origin and no merge revision
/// dilutes the count.
async fn measure_origins(origins: usize) -> Result<()> {
    use std::mem::size_of;

    use dialog_artifacts::history::{Origin, Watermark};

    let (operator, profile) = test_session_with_peer().await;
    let env = Counting::new(operator);
    let name = unique_name("bench");
    let repo = profile.space(name.clone()).open().perform(&env).await?;

    let mut previous: Option<crate::Branch> = None;
    for i in 0..origins {
        let writer = repo
            .branch(format!("writer-{i}"))
            .open()
            .perform(&env)
            .await?;
        if let Some(previous) = &previous {
            writer.pull().from(previous).perform(&env).await?;
        }
        writer
            .commit(stream::iter(vec![assert_fact(i, "origin")]))
            .perform(&env)
            .await?;
        // The writer just committed holds the only live head: the
        // previous writer's root is garbage once adopted.
        if i % PRUNE_EVERY == PRUNE_EVERY - 1 {
            prune(&env, &profile, &repo, &[&writer]).await?;
        }
        previous = Some(writer);
    }
    let last = previous.expect("at least one origin");

    // The branch under measurement starts from the last writer's head and
    // commits on top of it.
    let main = repo.branch("main").open().perform(&env).await?;
    main.pull().from(&last).perform(&env).await?;
    main.commit(stream::iter(vec![assert_fact(origins, "main")]))
        .perform(&env)
        .await?;

    const COMMITS: usize = 100;
    let started = Instant::now();
    for i in 0..COMMITS {
        main.commit(stream::iter(vec![assert_fact(origins + 1 + i, "measure")]))
            .perform(&env)
            .await?;
    }
    let commit_ms = started.elapsed().as_secs_f64() * 1_000.0 / COMMITS as f64;

    let head = main.revision().expect("main has a head");
    // What the head carries of its context inline: nothing, for a head
    // that keeps it in its tree's region.
    let inline = head.inline_context().map(Context::len).unwrap_or(0);
    let encoded = serde_ipld_dagcbor::to_vec(&head)?.len();
    let payload = head.payload().len();
    let memo = main
        .contexts()
        .cached(&head.version())
        .await
        .map(|context| context.len())
        .unwrap_or(0);
    // The size of one full context, a lower bound: the entries alone,
    // without the map's node overhead. The memo's entries for successive
    // heads share most of it.
    let memo_bytes = memo * (size_of::<Origin>() + size_of::<Watermark>());

    // The control: a history as deep, written by one origin, so the
    // difference between the two commit times is the context's alone.
    let control = repo.branch("control").open().perform(&env).await?;
    for i in 0..=origins {
        control
            .commit(stream::iter(vec![assert_fact(i, "control")]))
            .perform(&env)
            .await?;
    }
    let started = Instant::now();
    for i in 0..COMMITS {
        control
            .commit(stream::iter(vec![assert_fact(origins + 1 + i, "measure")]))
            .perform(&env)
            .await?;
    }
    let control_ms = started.elapsed().as_secs_f64() * 1_000.0 / COMMITS as f64;

    let feature = repo.branch("feature").open().perform(&env).await?;
    feature.set_upstream(&main).perform(&env).await?;
    feature.pull().perform(&env).await?;
    main.commit(stream::iter(vec![assert_fact(origins + COMMITS + 1, "ff")]))
        .perform(&env)
        .await?;

    // The two pulls are measured from a session of its own, which holds
    // only what storage holds: a device following the upstream, with
    // nothing remembered about either head.
    let device = profile
        .session(b"device")
        .space(profile.state())
        .allow(dialog_capability::Subject::any())
        .await?;
    let device = Counting::new(device);
    let local = profile.space(name).open().perform(&device).await?;
    let synced = local.branch("feature").open().perform(&device).await?;
    device.reset();
    synced.pull().perform(&device).await?;
    let ff_reads = device.block_reads();
    // Nothing new: the upstream moves, but only to a head we already
    // hold (it adopts our commit).
    synced
        .commit(stream::iter(vec![assert_fact(
            origins + COMMITS + 2,
            "noop",
        )]))
        .perform(&device)
        .await?;
    let upstream = local.branch("main").open().perform(&device).await?;
    upstream.pull().from(&synced).perform(&device).await?;
    device.reset();
    synced.pull().perform(&device).await?;
    let noop_reads = device.block_reads();

    println!(
        "| {origins:>7} | {inline:>6} | {encoded:>10} | {payload:>13} | {commit_ms:>9.2} | \
         {control_ms:>10.2} | {memo_bytes:>13} | {ff_reads:>8} | {noop_reads:>10} |"
    );
    Ok(())
}

/// Prints, per origin count, what the published causal context costs.
/// `#[ignore]`d: a measurement, not an assertion; see the module docs.
#[dialog_common::test]
#[ignore]
async fn context_cost_by_origins() -> Result<()> {
    let cap: usize = env::var("DIALOG_RA_MAX_ORIGINS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);
    println!(
        "| origins | inline | head bytes | payload bytes | commit ms | \
         control ms | context bytes | ff reads | noop reads |"
    );
    println!(
        "|---------|--------|------------|---------------|-----------|\
         ------------|---------------|----------|------------|"
    );
    for origins in [1, 10, 100, 1_000, 10_000]
        .into_iter()
        .filter(|n| *n <= cap)
    {
        measure_origins(origins).await?;
    }
    Ok(())
}

/// What one point query costs on a replica that adopted a head by root
/// and remembers nothing, the first query a device that faults blocks in
/// on demand runs: the root frame's bytes, and the blocks and bytes the
/// query reads in all. The head's history carries 100 facts and then
/// `origins` one-commit origins, each a writer adopting the previous one's
/// head.
async fn measure_first_query(origins: usize) -> Result<()> {
    use dialog_common::Blake3Hash;
    use dialog_query::attribute::Relation;
    use dialog_query::query::Output as _;
    use dialog_query::{AttributeQuery, Claim, Term, the};

    let (operator, profile) = test_session_with_peer().await;
    let name = unique_name("first-query");
    let repo = profile
        .space(name.clone())
        .open()
        .perform(&operator)
        .await?;
    let main = repo.branch("main").open().perform(&operator).await?;
    main.commit(stream::iter((0..100).map(|i| assert_fact(i, "seed"))))
        .perform(&operator)
        .await?;
    let mut previous = main.clone();
    for i in 0..origins {
        let writer = repo
            .branch(format!("writer-{i}"))
            .open()
            .perform(&operator)
            .await?;
        writer.pull().from(&previous).perform(&operator).await?;
        writer
            .commit(stream::iter(vec![assert_fact(100 + i, "origin")]))
            .perform(&operator)
            .await?;
        previous = writer;
    }
    main.pull().from(&previous).perform(&operator).await?;

    let reader = profile
        .session(b"reader")
        .space(profile.state())
        .allow(dialog_capability::Subject::any())
        .await?;
    let reader = Counting::new(reader);
    let local = profile.space(name).open().perform(&reader).await?;
    let upstream = local.branch("main").open().perform(&reader).await?;
    let adopter = local.branch("adopter").open().perform(&reader).await?;
    adopter.pull().from(&upstream).perform(&reader).await?;
    let head = adopter.revision().expect("the adopter took main's head");

    let cache = adopter.node_cache();
    let before = cache.bytes();
    reader.reset();
    let store = crate::NetworkedIndex::new(&reader, adopter.archive().index(), None);
    crate::Index::from_hash_with_cache(Blake3Hash::from(*head.tree.hash()), cache.clone())
        .manifest(&store)
        .await?;
    let root = cache.bytes() - before;

    let rows: Vec<Claim> = adopter
        .query()
        .select(AttributeQuery::from(
            Term::<Relation>::from(the!("bench/field"))
                .of(Term::<dialog_artifacts::Entity>::from(
                    "user:7".parse::<dialog_artifacts::Entity>()?,
                ))
                .is(Term::<String>::var("is")),
        ))
        .perform(&reader)
        .try_vec()
        .await?;
    assert_eq!(rows.len(), 1, "the point query finds its fact");
    let reads = reader.block_reads();
    let bytes = cache.bytes() - before;
    println!("| {origins:>7} | {root:>10} | {reads:>10} | {bytes:>10} |");
    Ok(())
}

/// Prints, per origin count, what a first point query costs on a replica
/// that adopted a head by root. `#[ignore]`d: a measurement.
#[dialog_common::test]
#[ignore]
async fn first_query_by_origins() -> Result<()> {
    let cap: usize = env::var("DIALOG_RA_MAX_ORIGINS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);
    println!("| origins | root bytes | block reads | bytes read |");
    println!("|---------|------------|-------------|------------|");
    for origins in [1, 1_000, 10_000].into_iter().filter(|n| *n <= cap) {
        measure_first_query(origins).await?;
    }
    Ok(())
}

/// One commit of one fact to `branch`.
async fn bump<Env: crate::ResolveEnv>(branch: &crate::Branch, at: usize, env: &Env) -> Result<()> {
    branch
        .commit(stream::iter(vec![assert_fact(at, "shape")]))
        .perform(env)
        .await?;
    Ok(())
}

/// The block reads of every pull shape, at `origins` one-commit origins
/// in the history, each pull run from a session of its own that
/// remembers nothing, as a device that faults blocks in on demand would.
async fn measure_pull_shapes(origins: usize) -> Result<Vec<(&'static str, u64)>> {
    let (operator, profile) = test_session_with_peer().await;
    let name = unique_name("shapes");
    let repo = profile
        .space(name.clone())
        .open()
        .perform(&operator)
        .await?;
    let mut previous: Option<crate::Branch> = None;
    for i in 0..origins {
        let writer = repo
            .branch(format!("writer-{i}"))
            .open()
            .perform(&operator)
            .await?;
        if let Some(previous) = &previous {
            writer.pull().from(previous).perform(&operator).await?;
        }
        bump(&writer, i, &operator).await?;
        previous = Some(writer);
    }
    let main = repo.branch("main").open().perform(&operator).await?;
    main.pull()
        .from(previous.as_ref().expect("at least one origin"))
        .perform(&operator)
        .await?;
    bump(&main, origins, &operator).await?;

    let mut at = origins + 1;
    let mut next = || {
        at += 1;
        at
    };
    let follower = |label: &'static str| {
        let repo = &repo;
        let main = &main;
        let operator = &operator;
        async move {
            let branch = repo.branch(label).open().perform(operator).await?;
            branch.set_upstream(main).perform(operator).await?;
            branch.pull().perform(operator).await?;
            anyhow::Ok(branch)
        }
    };
    let mut cold = 0u8;
    let mut measure = |label: &'static str| {
        cold += 1;
        let session = cold;
        let profile = &profile;
        let name = &name;
        async move {
            let device = profile
                .session([b'd', session])
                .space(profile.state())
                .allow(dialog_capability::Subject::any())
                .await?;
            let device = Counting::new(device);
            let local = profile.space(name.clone()).open().perform(&device).await?;
            let warmup = local
                .branch(format!("warmup-{session}"))
                .open()
                .perform(&device)
                .await?;
            let source = local
                .branch("warmup-source")
                .open()
                .perform(&device)
                .await?;
            warmup.pull().from(&source).perform(&device).await?;
            bump(&warmup, 0, &device).await?;
            let branch = local.branch(label).open().perform(&device).await?;
            device.reset();
            branch.pull().perform(&device).await?;
            anyhow::Ok(device.block_reads())
        }
    };
    // A small branch apart from main's history, which each measuring
    // session pulls first: what a session does once, on its first publish,
    // is then not counted against the shape it measures.
    let warmup_source = repo
        .branch("warmup-source")
        .open()
        .perform(&operator)
        .await?;
    bump(&warmup_source, next(), &operator).await?;
    let mut rows = Vec::new();

    follower("no-op").await?;
    rows.push(("no-op", measure("no-op").await?));

    let held = follower("nothing-new").await?;
    bump(&held, next(), &operator).await?;
    main.pull().from(&held).perform(&operator).await?;
    rows.push(("nothing-new", measure("nothing-new").await?));

    // Main takes its head back, so the followers hold a head of main's
    // own origin and main's next commits extend that chain.
    bump(&main, next(), &operator).await?;
    follower("ff-same-1").await?;
    follower("ff-same-k").await?;
    bump(&main, next(), &operator).await?;
    rows.push(("ff same origin, 1 ahead", measure("ff-same-1").await?));
    for _ in 0..9 {
        bump(&main, next(), &operator).await?;
    }
    rows.push(("ff same origin, 10 ahead", measure("ff-same-k").await?));

    follower("ff-other-1").await?;
    follower("ff-other-k").await?;
    let third = repo.branch("third").open().perform(&operator).await?;
    third.pull().from(&main).perform(&operator).await?;
    bump(&third, next(), &operator).await?;
    main.pull().from(&third).perform(&operator).await?;
    rows.push(("ff other origin, 1 ahead", measure("ff-other-1").await?));
    for _ in 0..9 {
        bump(&third, next(), &operator).await?;
    }
    main.pull().from(&third).perform(&operator).await?;
    rows.push(("ff other origin, 10 ahead", measure("ff-other-k").await?));

    let screened = follower("screened").await?;
    let replayed = follower("replay").await?;
    let grafted = follower("graft").await?;
    for _ in 0..4 {
        bump(&screened, next(), &operator).await?;
    }
    bump(&replayed, next(), &operator).await?;
    for _ in 0..(super::pull::SMALL_DIVERGENCE as usize + 2) {
        bump(&grafted, next(), &operator).await?;
    }
    bump(&main, next(), &operator).await?;
    rows.push(("screened merge", measure("screened").await?));
    for _ in 0..(super::pull::SMALL_DIVERGENCE as usize + 2) {
        bump(&main, next(), &operator).await?;
    }
    rows.push(("replay", measure("replay").await?));
    rows.push(("graft", measure("graft").await?));

    // Rival merges: two replicas pull each other at once, each minting a
    // merge, and the next pull settles on the greater.
    let a = follower("rival-a").await?;
    let b = follower("rival-b").await?;
    bump(&a, next(), &operator).await?;
    bump(&b, next(), &operator).await?;
    let into_a = a.pull().from(&b).prepare(&operator).await?;
    let into_b = b.pull().from(&a).prepare(&operator).await?;
    into_a.commit(&operator).await?;
    into_b.commit(&operator).await?;
    a.set_upstream(&b).perform(&operator).await?;
    rows.push(("rival settle", measure("rival-a").await?));
    Ok(rows)
}

/// Prints the block reads of every pull shape by origin count.
/// `#[ignore]`d: a measurement.
#[dialog_common::test]
#[ignore]
async fn pull_shapes_by_origins() -> Result<()> {
    let cap: usize = env::var("DIALOG_RA_MAX_ORIGINS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);
    for origins in [1, 10, 100, 1_000, 10_000]
        .into_iter()
        .filter(|n| *n <= cap)
    {
        for (shape, reads) in measure_pull_shapes(origins).await? {
            println!("| {origins:>7} | {shape:<26} | {reads:>6} |");
        }
    }
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
    // `DIALOG_RA_MAX_DEPTH` caps the sweep (e.g. `=1000`); reads per
    // scenario are depth-independent by design, so the smaller depths
    // measure the same table.
    let cap: usize = env::var("DIALOG_RA_MAX_DEPTH")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);
    let depths: Vec<usize> = [100, 1_000, 10_000]
        .into_iter()
        .filter(|depth| *depth <= cap)
        .collect();
    let mut samples = Vec::new();
    for depth in &depths {
        measure_depth(*depth, &mut samples).await?;
    }
    for depth in depths.iter().filter(|depth| **depth >= 1_000) {
        measure_triangle(*depth, &mut samples).await?;
    }
    measure_shape(*depths.last().expect("at least one depth")).await?;

    println!("| depth  | scenario                           | block reads | effects | wall ms  |");
    println!("|--------|------------------------------------|-------------|---------|----------|");
    for sample in &samples {
        println!("{}", sample.row());
    }
    print_resident_peak();
    Ok(())
}

/// The same measurements the pre-version-control baseline takes, so the two
/// can be compared directly: commit wall time at depth, merge reads, and the
/// zero-read paths.
#[dialog_common::test]
#[ignore]
async fn current_costs() -> Result<()> {
    for depth in [1_000usize, 10_000] {
        let (operator, profile) = test_session_with_peer().await;
        let env = Counting::new(operator);
        let repo = profile
            .space(unique_name("bench"))
            .open()
            .perform(&env)
            .await?;
        let main = repo.branch("main").open().perform(&env).await?;
        for i in 0..depth {
            main.commit(stream::iter(vec![assert_fact(i, "seed")]))
                .perform(&env)
                .await?;
            if i % PRUNE_EVERY == PRUNE_EVERY - 1 {
                prune(&env, &profile, &repo, &[&main]).await?;
            }
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
    print_resident_peak();
    Ok(())
}

/// The node type the artifact tree persists, read back for attribution.
type ArtifactNode = PersistentNode<Key, State<Datum>>;

/// Records the length and hash of every archive block the operator moves,
/// beneath every cache, so a block a commit wrote or a query read can be
/// fetched again by hash and attributed.
#[derive(Clone, Default)]
struct Recorder {
    tally: Tally,
    puts: Arc<Mutex<Vec<(usize, Blake3Hash)>>>,
    gets: Arc<Mutex<Vec<(usize, Blake3Hash)>>>,
}

impl Recorder {
    fn puts(&self) -> Vec<(usize, Blake3Hash)> {
        self.puts.lock().expect("the put log").clone()
    }

    fn gets(&self) -> Vec<(usize, Blake3Hash)> {
        self.gets.lock().expect("the get log").clone()
    }

    fn clear(&self) {
        self.puts.lock().expect("the put log").clear();
        self.gets.lock().expect("the get log").clear();
    }
}

impl Meter for Recorder {
    fn wrote(&self, block: &[u8]) {
        self.tally.wrote(block);
        self.puts
            .lock()
            .expect("the put log")
            .push((block.len(), Blake3Hash::hash(block)));
    }

    fn read(&self, block: &[u8]) {
        self.tally.read(block);
        self.gets
            .lock()
            .expect("the get log")
            .push((block.len(), Blake3Hash::hash(block)));
    }
}

/// The node stored under `hash`, read through `store`, with its bytes
/// attributed; `None` when the block is not a tree node.
async fn attribute_block<Env>(store: &Env, hash: &Blake3Hash) -> Result<Option<FrameAttribution>>
where
    Env: Provider<LoadBlock> + ConditionalSync,
{
    let Some(buffer) = LoadBlock::new(hash.clone()).perform(store).await? else {
        anyhow::bail!("block {hash} is not in the archive");
    };
    let Ok(node) = ArtifactNode::try_from(buffer) else {
        return Ok(None);
    };
    let manifest = node.manifest()?;
    Ok(Some(node.attribution(&manifest)?))
}

/// Prints one frame's attribution as a table: bytes per component beside
/// the weight the pacing metered for the same entries.
fn print_attribution(label: &str, frame: &FrameAttribution, manifest: &Manifest) {
    let kind = if frame.children > 0 {
        "index"
    } else {
        "segment"
    };
    println!();
    println!(
        "{label}: {kind}, {} bytes, {} children, {} entries",
        frame.total, frame.children, frame.entries
    );
    println!("| component | bytes | weight metered |");
    println!("|-----------|------:|---------------:|");
    println!("| prelude (layout, kind, manifest) | {} | |", frame.prelude);
    if frame.children > 0 {
        println!(
            "| link hashes ({} x 32) | {} | {} |",
            frame.children,
            frame.hashes,
            frame.children * (32 + manifest.link_overhead())
        );
        println!(
            "| separators (prefix, suffixes, ends) | {} | counted with links |",
            frame.separators
        );
        println!("| scales | {} | |", frame.scales);
    }
    println!(
        "| entry keys (columnar) | {} | {} (raw bytes) |",
        frame.keys, frame.raw_keys
    );
    println!(
        "| entry values (rkyv) | {} | {} (payload weight) |",
        frame.values, frame.value_weight
    );
    if frame.children > 0 {
        println!("| op polarity | {} | |", frame.polarity);
    }
    println!(
        "| per-entry overhead | | {} ({} x {}) |",
        frame.entries * manifest.entry_overhead(),
        frame.entries,
        manifest.entry_overhead()
    );
    println!(
        "| structure (rkyv headers, pointers, padding) | {} | |",
        frame.structure
    );
    println!(
        "| entries in all | {} | {} |",
        frame.entry_bytes(),
        frame.weight
    );
    println!("| total | {} | |", frame.total);
    if frame.entries > 0 {
        println!(
            "per entry: {:.1} bytes serialized against {:.1} weight ({:.2} bytes per weight unit)",
            frame.entry_bytes() as f64 / frame.entries as f64,
            frame.weight as f64 / frame.entries as f64,
            frame.entry_bytes() as f64 / frame.weight as f64
        );
        println!();
        println!(
            "| key layout (region) | entries | raw key bytes | value bytes (rkyv) | value weight |"
        );
        println!(
            "|---------------------|--------:|--------------:|-------------------:|-------------:|"
        );
        for (layout, share) in &frame.layouts {
            let region = match *layout {
                ENTITY_KEY_TAG => "entity index",
                ATTRIBUTE_KEY_TAG => "attribute index",
                VALUE_KEY_TAG => "value index",
                HISTORY_KEY_TAG => "history",
                COVERAGE_KEY_TAG => "coverage",
                BLOB_KEY_TAG => "blob index",
                _ => "other",
            };
            println!(
                "| {layout} ({region}) | {} | {} | {} | {} |",
                share.entries, share.raw_keys, share.values, share.value_weight
            );
        }
    }
}

/// The root frame of a replica's first query, attributed: the history
/// carries 100 facts and then `origins` one-commit origins, each a writer
/// adopting the previous one's head; a fresh session adopts the head by
/// root and runs one point query. Prints the attribution of every block
/// the query read, the root first.
async fn attribute_first_query(origins: usize) -> Result<()> {
    use dialog_query::attribute::Relation;
    use dialog_query::query::Output as _;
    use dialog_query::{AttributeQuery, Claim, Term, the};

    let (operator, profile) = test_session_with_peer().await;
    let name = unique_name("first-query");
    let repo = profile
        .space(name.clone())
        .open()
        .perform(&operator)
        .await?;
    let main = repo.branch("main").open().perform(&operator).await?;
    main.commit(stream::iter((0..100).map(|i| assert_fact(i, "seed"))))
        .perform(&operator)
        .await?;
    let mut previous = main.clone();
    for i in 0..origins {
        let writer = repo
            .branch(format!("writer-{i}"))
            .open()
            .perform(&operator)
            .await?;
        writer.pull().from(&previous).perform(&operator).await?;
        writer
            .commit(stream::iter(vec![assert_fact(100 + i, "origin")]))
            .perform(&operator)
            .await?;
        previous = writer;
    }
    main.pull().from(&previous).perform(&operator).await?;

    let reader = profile
        .session(b"reader")
        .space(profile.state())
        .allow(dialog_capability::Subject::any())
        .await?;
    let recorder = Recorder::default();
    let reader = Metered::new(reader, recorder.clone());
    let local = profile.space(name).open().perform(&reader).await?;
    let upstream = local.branch("main").open().perform(&reader).await?;
    let adopter = local.branch("adopter").open().perform(&reader).await?;
    adopter.pull().from(&upstream).perform(&reader).await?;
    let head = adopter.revision().expect("the adopter took main's head");

    recorder.clear();
    let rows: Vec<Claim> = adopter
        .query()
        .select(AttributeQuery::from(
            Term::<Relation>::from(the!("bench/field"))
                .of(Term::<dialog_artifacts::Entity>::from(
                    "user:7".parse::<dialog_artifacts::Entity>()?,
                ))
                .is(Term::<String>::var("is")),
        ))
        .perform(&reader)
        .try_vec()
        .await?;
    assert_eq!(rows.len(), 1, "the point query finds its fact");
    let reads = recorder.gets();
    println!(
        "first query at {origins} origins: {} blocks, {} bytes read",
        reads.len(),
        reads.iter().map(|(len, _)| len).sum::<usize>()
    );
    assert_eq!(
        reads.first().map(|(_, hash)| hash),
        Some(&Blake3Hash::from(*head.tree.hash())),
        "the first block the query reads is the root"
    );

    let store = crate::NetworkedIndex::new(&reader, adopter.archive().index(), None);
    for (at, (len, hash)) in reads.iter().enumerate() {
        match attribute_block(&store, hash).await? {
            Some(frame) => {
                let node = LoadBlock::new(hash.clone())
                    .perform(&store)
                    .await?
                    .expect("just read");
                let manifest = ArtifactNode::try_from(node)?.manifest()?;
                let label = if at == 0 {
                    format!("root ({len} bytes)")
                } else {
                    format!("block {at} read ({len} bytes)")
                };
                print_attribution(&label, &frame, &manifest);
            }
            None => println!("block {at} read: {len} bytes, not a tree node"),
        }
    }
    Ok(())
}

/// Attributes the root a replica's first query reads, at
/// `DIALOG_RA_ORIGINS` origins (1 000 by default). `#[ignore]`d: a
/// measurement.
#[dialog_common::test]
#[ignore]
async fn first_query_root_attribution() -> Result<()> {
    let origins: usize = env::var("DIALOG_RA_ORIGINS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000);
    attribute_first_query(origins).await
}

/// The crowd shape: `origins` one-commit origins, each a writer adopting
/// the previous one's head, then a branch that adopts the last head and
/// makes 100 one-fact commits. Prints, per commit, the root's size, its
/// buffered ops and their weight, and the blocks the commit wrote; then
/// attributes the root at the start, the largest root, and the largest
/// block any commit wrote.
async fn attribute_crowd(origins: usize) -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let recorder = Recorder::default();
    let env = Metered::new(operator, recorder.clone());
    let name = unique_name("crowd");
    let repo = profile.space(name.clone()).open().perform(&env).await?;

    let mut previous: Option<crate::Branch> = None;
    for i in 0..origins {
        let writer = repo
            .branch(format!("writer-{i}"))
            .open()
            .perform(&env)
            .await?;
        if let Some(previous) = &previous {
            writer.pull().from(previous).perform(&env).await?;
        }
        writer
            .commit(stream::iter(vec![assert_fact(i, "origin")]))
            .perform(&env)
            .await?;
        previous = Some(writer);
    }
    let last = previous.expect("at least one origin");

    let main = repo.branch("main").open().perform(&env).await?;
    main.pull().from(&last).perform(&env).await?;
    main.commit(stream::iter(vec![assert_fact(origins, "main")]))
        .perform(&env)
        .await?;

    let store = crate::NetworkedIndex::new(&env, main.archive().index(), None);
    let root_of =
        |branch: &crate::Branch| Blake3Hash::from(*branch.revision().expect("a head").tree.hash());
    let manifest = {
        let hash = root_of(&main);
        let node = LoadBlock::new(hash)
            .perform(&store)
            .await?
            .expect("the root is stored");
        ArtifactNode::try_from(node)?.manifest()?
    };

    let start = attribute_block(&store, &root_of(&main))
        .await?
        .expect("the root is a tree node");
    let mut largest_root = (0usize, start.clone());
    let mut largest_block: Option<(usize, usize, Blake3Hash)> = None;

    const COMMITS: usize = 100;
    println!("crowd at {origins} origins: {COMMITS} one-fact commits atop the last writer's head");
    println!(
        "| commit | root bytes | root ops | root op weight | blocks written | bytes written | largest block |"
    );
    println!(
        "|-------:|-----------:|---------:|---------------:|---------------:|--------------:|--------------:|"
    );
    for i in 0..COMMITS {
        recorder.clear();
        main.commit(stream::iter(vec![assert_fact(origins + 1 + i, "measure")]))
            .perform(&env)
            .await?;
        let puts = recorder.puts();
        let root = attribute_block(&store, &root_of(&main))
            .await?
            .expect("the root is a tree node");
        let biggest = puts.iter().max_by_key(|(len, _)| *len);
        println!(
            "| {:>6} | {:>10} | {:>8} | {:>14} | {:>14} | {:>13} | {:>13} |",
            i + 1,
            root.total,
            root.entries,
            root.weight,
            puts.len(),
            puts.iter().map(|(len, _)| len).sum::<usize>(),
            biggest.map(|(len, _)| *len).unwrap_or(0)
        );
        if root.total > largest_root.1.total {
            largest_root = (i + 1, root);
        }
        if let Some((len, hash)) = biggest
            && largest_block
                .as_ref()
                .is_none_or(|(_, largest, _)| len > largest)
        {
            largest_block = Some((i + 1, *len, hash.clone()));
        }
    }

    print_attribution(
        "root at the start (after the first commit atop the crowd)",
        &start,
        &manifest,
    );
    print_attribution(
        &format!("largest root (after commit {})", largest_root.0),
        &largest_root.1,
        &manifest,
    );
    if let Some((commit, len, hash)) = largest_block {
        match attribute_block(&store, &hash).await? {
            Some(frame) => print_attribution(
                &format!("largest block written (commit {commit}, {len} bytes)"),
                &frame,
                &manifest,
            ),
            None => {
                println!("largest block written (commit {commit}): {len} bytes, not a tree node")
            }
        }
    }
    Ok(())
}

/// Attributes the crowd shape's roots and flush blocks, at
/// `DIALOG_RA_ORIGINS` origins (1 000 by default). `#[ignore]`d: a
/// measurement.
#[dialog_common::test]
#[ignore]
async fn crowd_root_attribution() -> Result<()> {
    let origins: usize = env::var("DIALOG_RA_ORIGINS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000);
    attribute_crowd(origins).await
}
