//! Node-size sweep: is the ~50 KB node target actually optimal?
//!
//! The design goal behind `max_segment` was nodes sized for network reads
//! (partial replication fetches blocks on demand: cost = round trips x
//! latency + bytes / bandwidth), but the target was never measured. This
//! target builds the same SE dataset under one `DIALOG_TREE_MAX_SEGMENT`
//! setting per process (the manifest reads it once), and reports every
//! side of the trade:
//!
//! - local write cost (buffered SE replay, us/txn) and canonicalize cost
//! - the block-size distribution the setting actually produces
//! - cold-read fetch profiles: block fetches + bytes for a point read
//!   (EAV), an entity load (`of` scan), and a value-indexed lookup (VAE),
//!   each against a freshly loaded branch with empty caches — the
//!   partial-replication shape, with the branch's own load cost reported
//!   separately
//!
//! Drive it across settings with a shell loop, e.g.:
//!
//! ```sh
//! for seg in 8192 16384 32768 49152 65536 131072 262144; do
//!   DIALOG_TREE_MAX_SEGMENT=$seg cargo run --release -p dialog-baseline \
//!     --example node_size_sweep -- 2000
//! done
//! ```

use std::collections::HashMap;
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, Entity, Relation, Value};
use dialog_baseline::metered::{Meter, Tally};
use dialog_baseline::repo::{DialogRepo, MeteredRepo};
use dialog_baseline::se::SeLog;
use dialog_common::{Blake3Hash, Buffer};

/// Counts the branch's archive traffic and keeps the size of every
/// distinct block it ever wrote.
#[derive(Clone, Default)]
struct Sweep {
    tally: Tally,
    written: Arc<Mutex<HashMap<Blake3Hash, usize>>>,
}

impl Meter for Sweep {
    fn wrote(&self, block: &[u8]) {
        self.tally.wrote(block);
        let hash = Buffer::from(block.to_vec()).blake3_hash().clone();
        self.written
            .lock()
            .expect("written lock")
            .insert(hash, block.len());
    }

    fn read(&self, block: &[u8]) {
        self.tally.read(block);
    }
}

/// Runs each selector against a freshly loaded branch (empty caches) and
/// reports the average block fetches and bytes per query — the
/// partial-replication cold-read shape.
async fn profile(
    repo: &MeteredRepo<Sweep>,
    tally: &Tally,
    label: &str,
    selectors: Vec<ArtifactSelector<Constrained>>,
) -> anyhow::Result<()> {
    let mut fetches = 0usize;
    let mut bytes = 0usize;
    let mut rows = 0usize;
    let queries = selectors.len();
    for selector in selectors {
        let cold = repo.reopen().await?;
        let before = (tally.reads(), tally.read_bytes());
        rows += repo.collect_from(&cold, selector).await?.len();
        fetches += tally.reads() - before.0;
        bytes += tally.read_bytes() - before.1;
    }
    println!(
        "  {label}: {:.1} fetches, {:.0} bytes per query ({} queries, {} rows)",
        fetches as f64 / queries as f64,
        bytes as f64 / queries as f64,
        queries,
        rows,
    );
    Ok(())
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[rank]
}

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(2000);
    let max_segment =
        std::env::var("DIALOG_TREE_MAX_SEGMENT").unwrap_or_else(|_| "65536 (default)".into());
    let log = SeLog::load(count)?;

    // Sample entities for the cold-read profiles: distinct titled posts
    // (their titles are edited, so the point read crosses supersession).
    let mut titled: Vec<String> = Vec::new();
    for fact in log.transactions.iter().flatten() {
        if fact.the == "se.post/title" && !titled.contains(&fact.of) {
            titled.push(fact.of.clone());
            if titled.len() >= 24 {
                break;
            }
        }
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let sweep = Sweep::default();
        let tally = sweep.tally.clone();
        let repo = DialogRepo::metered(sweep.clone()).await?;

        // Build: buffered replay, then canonicalize (the bulk-import shape;
        // a cold replica's dataset is dominated by canonical nodes).
        let replay_started = Instant::now();
        repo.replay_se(&log).await?;
        let replay = replay_started.elapsed();
        let canonicalize_started = Instant::now();
        repo.canonicalize().await?;
        let canonicalize = canonicalize_started.elapsed();

        // Block census over every block the branch wrote.
        let mut sizes: Vec<usize> = sweep
            .written
            .lock()
            .expect("written lock")
            .values()
            .copied()
            .collect();
        sizes.sort_unstable();
        let total: usize = sizes.iter().sum();

        println!(
            "max_segment={max_segment} txns={} facts={}",
            log.transactions.len(),
            log.fact_count()
        );
        println!(
            "  write: {:.0} us/txn buffered, canonicalize {:?}",
            replay.as_micros() as f64 / log.transactions.len() as f64,
            canonicalize
        );
        println!(
            "  blocks: {} totaling {:.1} MiB, p50 {} p90 {} p99 {} max {} bytes",
            sizes.len(),
            total as f64 / (1024.0 * 1024.0),
            percentile(&sizes, 0.50),
            percentile(&sizes, 0.90),
            percentile(&sizes, 0.99),
            sizes.last().copied().unwrap_or(0),
        );

        // Cold-read profiles: each query runs on a freshly loaded branch
        // (empty caches), fetch counts and bytes read from the metered
        // archive traffic. The load itself (head + root resolution) is
        // reported once, separately.
        let (open_fetches, open_bytes) = {
            let before = (tally.reads(), tally.read_bytes());
            let cold = repo.reopen().await?;
            // Force the root fetch that a first query would pay.
            let selector = ArtifactSelector::new()
                .the(Relation::from_str("se.post/kind")?)
                .of(Entity::from_str(&titled[0])?);
            repo.collect_from(&cold, selector).await?;
            (tally.reads() - before.0, tally.read_bytes() - before.1)
        };
        println!("  cold load + first point query: {open_fetches} fetches, {open_bytes} bytes");

        let title_gets = titled
            .iter()
            .map(|post| {
                Ok(ArtifactSelector::new()
                    .the(Relation::from_str("se.post/title")?)
                    .of(Entity::from_str(post)?))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        profile(&repo, &tally, "point get (title)", title_gets).await?;

        let entity_loads = titled
            .iter()
            .map(|post| Ok(ArtifactSelector::new().of(Entity::from_str(post)?)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        profile(&repo, &tally, "entity load (of scan)", entity_loads).await?;

        profile(
            &repo,
            &tally,
            "kind lookup (VAE)",
            vec![
                ArtifactSelector::new()
                    .the(Relation::from_str("se.post/kind")?)
                    .is(Value::String("question".into())),
            ],
        )
        .await?;

        Ok(())
    })
}
