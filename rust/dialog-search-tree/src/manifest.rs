//! The format manifest carried by every tree node.
//!
//! The tree's format constants — the branching parameter, the separator-length
//! bound, and the value inline-vs-spill threshold — determine node bytes, and
//! node bytes are the content address, so those constants are secretly part of
//! the format. Keeping them only in code means two peers on different builds
//! silently produce non-convergent trees for the same data. The manifest makes
//! them **data**: it is inlined into every node so any node hash stays a
//! complete, self-describing tree root (the differ, structural sharing, and
//! `from_hash` all rely on a bare node hash being a usable root).
//!
//! The manifest is a handful of bytes, identical across every node in a tree,
//! so front coding and structural sharing store it once in practice.
//!
//! The `version` pins interpretation: a peer reading a node with a version it
//! knows uses the exact matching constants. Changing a constant means bumping
//! the version, which changes every node hash — a visible, intentional fork
//! rather than a silent one.
//!
//! Every edit runs under the manifest of the tree it edits: an edit over a
//! stored root adopts the header that root carries (see
//! `TransientTree::load`), and a new tree states its manifest when it is
//! created. [`Manifest::default`] is only the format a new tree starts with;
//! code working with an existing tree reads that tree's manifest rather than
//! the default. Pure reads do not check the header — the node encoding is
//! self-delimiting and version 1 is the only shipped format.

use rkyv::{Archive, Deserialize, Serialize};

/// The current format version. Bump when any format constant's meaning or a
/// node encoding changes AFTER data in the prior format has shipped; format
/// evolution before the first ship stays at version 1, since there is no
/// stored data anywhere for a bump to protect.
///
/// Version 1 includes: per-child-link novelty grouping with each link's
/// buffer encoded via the segment codec (schema-split columns, per-buffer
/// dictionaries, front-coded arenas, op polarity as a column).
pub const FORMAT_VERSION: u8 = 1;

/// The self-describing format constants of a tree, inlined into every node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedManifest)]
pub struct Manifest {
    /// Format version; pins how the rest of the node is interpreted.
    pub version: u8,
    /// Branching parameter `n`; expected fanout is `2^n`.
    pub fanout_n: u8,
    /// Keys longer than this never become boundaries (separator bound).
    pub max_separator: u32,
    /// Values longer than this spill to the block store, leaving a key-prefix
    /// plus whole-value hash in the key.
    pub inline_n: u32,
    /// How many leading raw value bytes a spilled value's key carries as its
    /// order-preserving prefix.
    pub spill_prefix: u16,
    /// Leaf-run weight cap; 0 disables it. A run between accepted seams whose
    /// summed entry weight exceeds this is force-split at deterministic,
    /// leaf-level-only positions.
    pub max_segment: u32,
    /// Hard ceiling on a frame's weight, as a multiple of `max_segment`; 0
    /// disables it. A frame — the entries between coin-decided cuts — over
    /// `frame_ceiling_factor * max_segment` is force-split at deterministic,
    /// leaf-level-only accepted seams.
    pub frame_ceiling_factor: u32,
    /// Which candidate seam a forced cut anchors at: 0 = rendezvous
    /// (hash-minimal), 1 = hybrid (shortest-separator class, then
    /// hash-minimal within it).
    pub anchor_selector: u32,
}

/// The format a new tree is created under.
///
/// Only for choosing the format of a tree that does not exist yet. An
/// existing tree's format is the manifest its nodes carry (read with
/// [`PersistentTree::manifest`](crate::PersistentTree::manifest)), which may
/// differ from this in any field, so code working with a tree reads that and
/// never these values.
impl Default for Manifest {
    fn default() -> Self {
        // Experiment plumbing for the boundary-policy arms (see
        // notes/boundary-policy-experiment.md): the manifest a fresh tree is
        // created under can be overridden through the environment, so the
        // whole artifact stack runs a capture under an arm's format without
        // threading configuration through every layer. Unset variables leave
        // the shipped values untouched; existing trees always keep the
        // manifest their root node carries.
        //
        // Read once per process: tree creation can sit on a per-commit path,
        // and the environment scan showed up as ~4% of a profiled commit
        // before this memo. The environment of a running process does not
        // change underneath it.
        //
        // `DIALOG_TREE_FANOUT_N` overrides the branching parameter `n` for
        // fresh trees (clamped to the representable 0..=63; see
        // `branch_factor`), so the sync soak harness can sweep expected
        // fanout (e.g. 5 = 32, 8 = 256) across processes without a code
        // change.
        static DEFAULT: std::sync::OnceLock<Manifest> = std::sync::OnceLock::new();
        *DEFAULT.get_or_init(|| Self {
            version: FORMAT_VERSION,
            // Expected fanout 2^8 = 256.
            fanout_n: env_override("DIALOG_TREE_FANOUT_N", 8).min(63) as u8,
            // The length-guarded coin (plan 5.7a): longer keys never become
            // boundaries, bounding every separator by construction.
            max_separator: 512,
            // Sized for a networked store with large nodes, not a 4 KiB disk
            // page (plan 3.1/4).
            inline_n: env_override("DIALOG_TREE_INLINE_N", 4096),
            // Spilled values sort INTO their type band next to inline values,
            // and prefix/range predicates decide from the key whenever the
            // answer lies within this many bytes.
            spill_prefix: 64,
            // ~64 KiB per node between coin-decided cuts; 0 would recover the
            // old per-key geometric coin byte-for-byte.
            max_segment: env_override("DIALOG_TREE_MAX_SEGMENT", 65536),
            // Caps the largest node near three times the target for a modest
            // commit-CPU cost (the boundary-policy experiment measured 2 and
            // 3; 2 is available where tighter variance outweighs write CPU).
            frame_ceiling_factor: env_override("DIALOG_TREE_CEILING_FACTOR", 3),
            // Hybrid: the experiment showed it anchors forced cuts at the most
            // stable semantic breaks (inserts never move them) for no
            // measurable cost over pure rendezvous (0).
            anchor_selector: env_override("DIALOG_TREE_ANCHOR_SELECTOR", 1),
        })
    }
}

/// Reads a `u32` manifest override from the environment, falling back to the
/// built-in value when the variable is unset or unparsable. On targets
/// without an environment (wasm) the fallback always wins.
fn env_override(name: &str, fallback: u32) -> u32 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(fallback)
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = name;
        fallback
    }
}

impl Manifest {
    /// The geometric split factor `m = 2^n` that the boundary coin uses. This
    /// is the effective average branching factor of the tree.
    ///
    /// Clamped so `n` in `1..=63` maps to a real `u64` factor; `n = 0` would
    /// mean fanout 1 (no branching) and is disallowed, and `n >= 64` would
    /// overflow, so both saturate to the representable extremes.
    pub fn branch_factor(&self) -> u64 {
        match self.fanout_n {
            0 => 2,
            n if n >= 64 => u64::MAX,
            n => 1u64 << n,
        }
    }

    /// The effective frame ceiling in weighted bytes:
    /// `frame_ceiling_factor * max_segment`. Zero — disabled — when either
    /// knob is zero, so the ceiling can never outlive the coin it bounds.
    pub fn frame_ceiling(&self) -> usize {
        self.frame_ceiling_factor as usize * self.max_segment as usize
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]
    // The dialog_common::test macro requires async test fns; these pure tests
    // await nothing.
    #![allow(clippy::unused_async)]

    use super::Manifest;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// `n` maps to `2^n`, and the default gives the intended fanout.
    #[dialog_common::test]
    async fn it_maps_fanout_n_to_two_to_the_n() -> anyhow::Result<()> {
        let manifest = Manifest {
            fanout_n: 8,
            ..Manifest::default()
        };
        assert_eq!(manifest.branch_factor(), 256);

        assert_eq!(
            Manifest {
                fanout_n: 1,
                ..Manifest::default()
            }
            .branch_factor(),
            2
        );
        assert_eq!(
            Manifest {
                fanout_n: 10,
                ..Manifest::default()
            }
            .branch_factor(),
            1024
        );
        // Degenerate n saturate rather than overflow or divide by one.
        assert_eq!(
            Manifest {
                fanout_n: 0,
                ..Manifest::default()
            }
            .branch_factor(),
            2
        );
        assert_eq!(
            Manifest {
                fanout_n: 200,
                ..Manifest::default()
            }
            .branch_factor(),
            u64::MAX
        );
        Ok(())
    }

    /// The manifest round-trips through rkyv unchanged.
    #[dialog_common::test]
    async fn it_round_trips_through_rkyv() -> anyhow::Result<()> {
        let manifest = Manifest {
            version: 1,
            fanout_n: 8,
            max_separator: 512,
            inline_n: 4096,
            spill_prefix: 64,
            max_segment: 131072,
            frame_ceiling_factor: 2,
            anchor_selector: 1,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&manifest)?;
        let decoded: Manifest = rkyv::from_bytes::<Manifest, rkyv::rancor::Error>(&bytes)?;
        assert_eq!(decoded, manifest);
        Ok(())
    }
}
