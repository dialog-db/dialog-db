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
//!   each against a freshly opened store with an empty node cache — the
//!   partial-replication shape, with the store's own open cost reported
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

use std::str::FromStr as _;
use std::time::Instant;

use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStoreMut as _, ArtifactViewStream as _, Artifacts,
    Attribute, Entity, Value,
};
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_storage::{Blake3Hash, MeasuredStorage, MemoryStorageBackend, StorageSource as _};
use futures_util::{StreamExt as _, TryStreamExt as _, stream};

const IDENTIFIER: &str = "node-size-sweep";

/// Runs each selector against a freshly opened store (empty node cache)
/// and reports the average block fetches and bytes per query — the
/// partial-replication cold-read shape.
async fn profile(
    backend: &MeasuredStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    label: &str,
    selectors: Vec<ArtifactSelector<dialog_artifacts::selector::Constrained>>,
) -> anyhow::Result<()> {
    let mut fetches = 0usize;
    let mut bytes = 0usize;
    let mut rows = 0usize;
    let queries = selectors.len();
    for selector in selectors {
        let cold: Artifacts<_> = Artifacts::open(IDENTIFIER.into(), backend.clone()).await?;
        let before = (backend.reads(), backend.read_bytes());
        let found: Vec<Artifact> = cold.select(selector).owned().try_collect().await?;
        rows += found.len();
        fetches += backend.reads() - before.0;
        bytes += backend.read_bytes() - before.1;
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
        let inner = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
        let backend = MeasuredStorage::new(inner.clone());
        let mut store = Artifacts::open(IDENTIFIER.into(), backend.clone()).await?;

        // Build: buffered replay, then canonicalize (the bulk-import shape;
        // a cold replica's dataset is dominated by canonical nodes).
        let replay_started = Instant::now();
        for commit in &log.transactions {
            store.commit(stream::iter(se_instructions(commit)?)).await?;
        }
        let replay = replay_started.elapsed();
        let canonicalize_started = Instant::now();
        store.canonicalize().await?;
        let canonicalize = canonicalize_started.elapsed();

        // Block census over the raw backend.
        let mut sizes: Vec<usize> = inner
            .read()
            .map(|entry| entry.map(|(_, bytes)| bytes.len()))
            .try_collect()
            .await?;
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

        // Cold-read profiles: each query runs on a freshly opened store
        // (empty node cache), fetch counts and bytes read from the
        // measured backend. The open itself (head + root resolution) is
        // reported once, separately.
        let (open_fetches, open_bytes) = {
            let before = (backend.reads(), backend.read_bytes());
            let cold: Artifacts<_> = Artifacts::open(IDENTIFIER.into(), backend.clone()).await?;
            // Force the root fetch that a first query would pay.
            let selector = ArtifactSelector::new()
                .the(Attribute::from_str("se.post/kind")?)
                .of(Entity::from_str(&titled[0])?);
            let _: Vec<Artifact> = cold.select(selector).owned().try_collect().await?;
            (backend.reads() - before.0, backend.read_bytes() - before.1)
        };
        println!("  cold open + first point query: {open_fetches} fetches, {open_bytes} bytes");

        let title_gets = titled
            .iter()
            .map(|post| {
                Ok(ArtifactSelector::new()
                    .the(Attribute::from_str("se.post/title")?)
                    .of(Entity::from_str(post)?))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        profile(&backend, "point get (title)", title_gets).await?;

        let entity_loads = titled
            .iter()
            .map(|post| Ok(ArtifactSelector::new().of(Entity::from_str(post)?)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        profile(&backend, "entity load (of scan)", entity_loads).await?;

        profile(
            &backend,
            "kind lookup (VAE)",
            vec![
                ArtifactSelector::new()
                    .the(Attribute::from_str("se.post/kind")?)
                    .is(Value::String("question".into())),
            ],
        )
        .await?;

        Ok(())
    })
}
