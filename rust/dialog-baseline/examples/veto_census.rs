//! Veto census: does the seam veto cause issues on the real tree?
//!
//! The veto (boundary-policy step 1) clamps any seam whose shortest
//! separator exceeds `max_separator` to rank 0 — no coin may cut there —
//! which is what creates uncuttable stretches, the weight bank, forced
//! anchors with over-long separators, and the widening machinery on the
//! edit path (where the stale-path reshape bug lived). This walks the
//! CANONICAL tree in key order, evaluates the actual `vetoes` rule on
//! every adjacent-key seam, and reports:
//!
//! - how much of the key sequence is vetoed, and in which key regions
//!   (by leading tag byte)
//! - the maximal vetoed stretch distribution (entries and proxy weight),
//!   and how many stretches exceed the pacing target / the ceiling
//! - forced boundaries actually stored (index links whose separator
//!   exceeds `max_separator` — the self-identifying mark)
//! - whether the oversized leaves are the vetoed ones (per-leaf veto
//!   share vs leaf size)
//!
//! ```sh
//! DIALOG_TREE_MAX_SEGMENT=65536 cargo run --release -p dialog-baseline \
//!   --example veto_census -- 10000
//! ```

use dialog_artifacts::Key;
use dialog_baseline::nodes::walk;
use dialog_baseline::repo::DialogRepo;
use dialog_baseline::se::SeLog;
use dialog_search_tree::{Distribution as _, Geometric, Manifest, NodeBody};
use std::collections::BTreeMap;

const ENTRY_OVERHEAD: usize = 32;

#[derive(Default)]
struct Census {
    seams: usize,
    vetoed: usize,
    vetoed_by_tag: BTreeMap<u8, usize>,
    seams_by_tag: BTreeMap<u8, usize>,
    /// (entries, proxy weight) per maximal vetoed stretch.
    stretches: Vec<(usize, usize)>,
    open_stretch: Option<(usize, usize)>,
    previous_key: Option<Vec<u8>>,
    /// (leaf bytes, leaf seams, leaf vetoed seams).
    leaves: Vec<(usize, usize, usize)>,
    forced_links: usize,
    forced_separator_bytes: usize,
}

impl Census {
    fn seam(&mut self, left: &[u8], right: &[u8], manifest: &Manifest) -> bool {
        self.seams += 1;
        let tag = right.first().copied().unwrap_or(0);
        *self.seams_by_tag.entry(tag).or_default() += 1;
        let vetoed = Geometric::vetoes(left, right, manifest);
        if vetoed {
            self.vetoed += 1;
            *self.vetoed_by_tag.entry(tag).or_default() += 1;
            let weight = left.len() + ENTRY_OVERHEAD;
            match &mut self.open_stretch {
                Some((entries, total)) => {
                    *entries += 1;
                    *total += weight;
                }
                None => {
                    self.open_stretch = Some((2, left.len() + right.len() + 2 * ENTRY_OVERHEAD))
                }
            }
        } else if let Some(stretch) = self.open_stretch.take() {
            self.stretches.push(stretch);
        }
        vetoed
    }
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    if sorted.is_empty() {
        0
    } else {
        sorted[((sorted.len() - 1) as f64 * p).round() as usize]
    }
}

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(10000);
    let log = SeLog::load(count)?;
    let manifest = Manifest::default();
    let target = manifest.max_segment as usize;
    let ceiling = manifest.frame_ceiling();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let repo = DialogRepo::volatile().await?;
        repo.replay_se(&log).await?;
        repo.canonicalize().await?;
        let root = repo
            .root()
            .ok_or_else(|| anyhow::anyhow!("the branch has commits, so it has a tree"))?;

        // In-order walk, so the global key sequence stays contiguous for
        // stretch accounting.
        let mut census = Census::default();
        walk(&repo.index(), root, |visit| {
            match visit.node.body() {
                NodeBody::Index(index) => {
                    for at in 1..index.len() {
                        let separator = index.separator(at)?;
                        if separator.len() as u32 > manifest.max_separator {
                            census.forced_links += 1;
                            census.forced_separator_bytes += separator.len();
                        }
                    }
                }
                NodeBody::Segment(segment) => {
                    let mut keys = segment.keys::<Key>()?;
                    let mut leaf_seams = 0usize;
                    let mut leaf_vetoed = 0usize;
                    while let Some((_, key)) = keys.next_key()? {
                        if let Some(previous) = census.previous_key.take() {
                            leaf_seams += 1;
                            if census.seam(&previous, key, &manifest) {
                                leaf_vetoed += 1;
                            }
                        }
                        census.previous_key = Some(key.to_vec());
                    }
                    census.leaves.push((visit.size, leaf_seams, leaf_vetoed));
                }
            }
            Ok(())
        })
        .await?;
        if let Some(stretch) = census.open_stretch.take() {
            census.stretches.push(stretch);
        }

        println!(
            "max_segment={target} ceiling={ceiling} max_separator={} txns={} facts={}",
            manifest.max_separator,
            log.transactions.len(),
            log.fact_count()
        );
        println!(
            "seams: {} total, {} vetoed ({:.1}%)",
            census.seams,
            census.vetoed,
            100.0 * census.vetoed as f64 / census.seams.max(1) as f64
        );
        for (tag, seams) in &census.seams_by_tag {
            let vetoed = census.vetoed_by_tag.get(tag).copied().unwrap_or(0);
            println!(
                "  tag 0x{tag:02x}: {seams} seams, {vetoed} vetoed ({:.1}%)",
                100.0 * vetoed as f64 / (*seams).max(1) as f64
            );
        }

        let mut weights: Vec<usize> = census.stretches.iter().map(|(_, w)| *w).collect();
        weights.sort_unstable();
        let over_target = weights.iter().filter(|&&w| w > target).count();
        let over_ceiling = weights.iter().filter(|&&w| w > ceiling).count();
        let mut entries: Vec<usize> = census.stretches.iter().map(|(n, _)| *n).collect();
        entries.sort_unstable();
        println!(
            "vetoed stretches: {} (entries p50 {} p99 {} max {}; proxy weight p50 {} p99 {} max {}; {} over the target, {} over the ceiling)",
            weights.len(),
            percentile(&entries, 0.50),
            percentile(&entries, 0.99),
            entries.last().copied().unwrap_or(0),
            percentile(&weights, 0.50),
            percentile(&weights, 0.99),
            weights.last().copied().unwrap_or(0),
            over_target,
            over_ceiling,
        );
        println!(
            "forced links stored: {} carrying {} separator bytes",
            census.forced_links, census.forced_separator_bytes
        );

        // Are the oversized leaves the vetoed ones?
        let mut small = (0usize, 0usize, 0usize);
        let mut big = (0usize, 0usize, 0usize);
        for (size, seams, vetoed) in &census.leaves {
            let bucket = if *size > ceiling { &mut big } else { &mut small };
            bucket.0 += 1;
            bucket.1 += seams;
            bucket.2 += vetoed;
        }
        println!(
            "leaves at or under the byte ceiling: {} (veto share {:.1}%); over it: {} (veto share {:.1}%)",
            small.0,
            100.0 * small.2 as f64 / small.1.max(1) as f64,
            big.0,
            100.0 * big.2 as f64 / big.1.max(1) as f64,
        );
        Ok(())
    })
}
