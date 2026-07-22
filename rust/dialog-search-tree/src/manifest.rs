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
//! Enforcement today is at the EDIT boundary: loading a root whose header
//! differs from the edit's manifest (including an unknown version) fails
//! loudly (see `TransientTree::load`), because an edit under the wrong
//! parameters would re-coin the touched spine and silently break shape
//! convergence. Pure reads do not check the header — the node encoding is
//! self-delimiting and version 1 is the only shipped format. Adopting the
//! loaded root's manifest for edits (instead of rejecting) is the tracked
//! follow-up on `TransientTree::manifest`.

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

/// The branching parameter as `n`, where the geometric split factor (expected
/// fanout) is `2^n`. One byte spans the whole practical range; `n = 8` gives a
/// fanout of 256.
pub const DEFAULT_FANOUT_N: u8 = 8;

/// Default separator-length bound (the length-guarded coin, plan 5.7a): keys
/// longer than this are ranked 0 so they never become boundaries, bounding
/// every separator by construction.
pub const DEFAULT_MAX_SEPARATOR: u32 = 512;

/// Default value inline-vs-spill threshold (plan 3.1/4): values whose encoded
/// form exceeds this go to the block store, addressed by the whole-value
/// hash appended to the key; smaller values inline in order-preserving form.
/// Sized for a networked store with large nodes, not a 4 KiB disk page.
pub const DEFAULT_INLINE_N: u32 = 4096;

/// Default spilled-value key-prefix length: a spilled value's key carries the
/// order-preserving encoding of this many leading raw value bytes, so spilled
/// values sort INTO their type band next to inline values and prefix/range
/// predicates decide from the key whenever the answer lies within this many
/// bytes (beyond it, the scan loads the block and post-filters).
pub const DEFAULT_SPILL_PREFIX: u16 = 64;

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
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: FORMAT_VERSION,
            fanout_n: DEFAULT_FANOUT_N,
            max_separator: DEFAULT_MAX_SEPARATOR,
            inline_n: DEFAULT_INLINE_N,
            spill_prefix: DEFAULT_SPILL_PREFIX,
        }
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
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]
    // The dialog_common::test macro requires async test fns; these pure tests
    // await nothing.
    #![allow(clippy::unused_async)]

    use super::{DEFAULT_FANOUT_N, Manifest};

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
        assert_eq!(DEFAULT_FANOUT_N, 8);
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
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&manifest)?;
        let decoded: Manifest = rkyv::from_bytes::<Manifest, rkyv::rancor::Error>(&bytes)?;
        assert_eq!(decoded, manifest);
        Ok(())
    }
}
