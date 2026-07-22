use crate::{BOTTOM_RANK, Manifest};

/// Measurement-only hash accounting (uncommitted experiment plumbing): every
/// blake3 invocation on the shaping paths bumps a counter, split by purpose,
/// so a replay can attribute hash cost. Snapshot and reset from the harness.
#[allow(missing_docs, clippy::missing_docs_in_private_items)]
pub mod audit {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static KEY_HASHES: AtomicU64 = AtomicU64::new(0);
    pub static KEY_HASH_BYTES: AtomicU64 = AtomicU64::new(0);
    pub static SEAM_HASHES: AtomicU64 = AtomicU64::new(0);
    pub static SEAM_HASH_BYTES: AtomicU64 = AtomicU64::new(0);
    pub static ELECTION_HASHES: AtomicU64 = AtomicU64::new(0);
    pub static ELECTION_HASH_BYTES: AtomicU64 = AtomicU64::new(0);
    pub static NODE_HASHES: AtomicU64 = AtomicU64::new(0);
    pub static NODE_HASH_BYTES: AtomicU64 = AtomicU64::new(0);

    pub fn key(bytes: usize) {
        KEY_HASHES.fetch_add(1, Ordering::Relaxed);
        KEY_HASH_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn seam(bytes: usize) {
        SEAM_HASHES.fetch_add(1, Ordering::Relaxed);
        SEAM_HASH_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn election(bytes: usize) {
        ELECTION_HASHES.fetch_add(1, Ordering::Relaxed);
        ELECTION_HASH_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn node(bytes: usize) {
        NODE_HASHES.fetch_add(1, Ordering::Relaxed);
        NODE_HASH_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub static MEMO_HITS: AtomicU64 = AtomicU64::new(0);
    pub static MEMO_HIT_BYTES: AtomicU64 = AtomicU64::new(0);
    pub fn memo_hit(bytes: usize) {
        MEMO_HITS.fetch_add(1, Ordering::Relaxed);
        MEMO_HIT_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub fn report() -> String {
        format!(
            "key_hashes={} key_bytes={} seam_hashes={} seam_bytes={} election_hashes={} election_bytes={} node_hashes={} node_bytes={}",
            KEY_HASHES.swap(0, Ordering::Relaxed),
            KEY_HASH_BYTES.swap(0, Ordering::Relaxed),
            SEAM_HASHES.swap(0, Ordering::Relaxed),
            SEAM_HASH_BYTES.swap(0, Ordering::Relaxed),
            ELECTION_HASHES.swap(0, Ordering::Relaxed),
            ELECTION_HASH_BYTES.swap(0, Ordering::Relaxed),
            NODE_HASHES.swap(0, Ordering::Relaxed),
            NODE_HASH_BYTES.swap(0, Ordering::Relaxed),
        ) + &format!(
            " memo_hits={} memo_hit_bytes={}",
            MEMO_HITS.swap(0, Ordering::Relaxed),
            MEMO_HIT_BYTES.swap(0, Ordering::Relaxed),
        )
    }
}

/// A bounded, thread-local memo of `blake3(bytes)` for the shaping paths:
/// the coins, the ladders, and the anchor elections all hash the same key
/// and separator strings over and over — every regroup of a widened run
/// rehashes its whole window, and the audit measured those recomputations
/// at ~95% of the pacing machinery's added hash bytes. A memo of a pure
/// function changes no decision; it only remembers. Keys are the exact
/// input bytes (full compare on lookup, so collisions are impossible), the
/// map is cleared wholesale when it reaches capacity (regularly-reused
/// strings immediately repopulate), and thread-locality keeps it lock-free
/// on native and trivially correct on wasm.
mod hash_memo {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use dialog_common::Blake3Hash;

    /// Entries retained before the memo resets. At ~150 bytes per typical
    /// key this bounds the memo near 20 MB per thread, far under the node
    /// cache's own footprint.
    const CAPACITY: usize = 1 << 17;

    thread_local! {
        static MEMO: RefCell<HashMap<Vec<u8>, Blake3Hash>> =
            RefCell::new(HashMap::with_capacity(1024));
    }

    /// The blake3 hash of `bytes`, memoized.
    pub fn hash(bytes: &[u8]) -> Blake3Hash {
        MEMO.with(|memo| {
            let mut memo = memo.borrow_mut();
            if let Some(hash) = memo.get(bytes) {
                super::audit::memo_hit(bytes.len());
                return hash.clone();
            }
            if memo.len() >= CAPACITY {
                memo.clear();
            }
            let hash = Blake3Hash::hash(bytes);
            memo.insert(bytes.to_vec(), hash.clone());
            hash
        })
    }
}

/// The rank of a node in the prolly tree.
pub type Rank = u64;

/// Strategy for assigning ranks to keys and separators, and for deriving the
/// separators themselves.
///
/// The distribution decides which entries end leaf segments (the leaf coin,
/// [`rank`](Self::rank)), which seams punch boundaries through index levels
/// (the seam coin, [`seam_rank`](Self::seam_rank)), and what byte string an
/// index link stores to route across a seam ([`separator`](Self::separator)).
/// Together these determine the shape of the tree as a pure function of its
/// key set, which is what keeps it history-independent.
///
/// A separator follows the lower-bound convention: the separator carried by a
/// link is the shortest byte string that sorts strictly above everything in
/// the left-adjacent subtree and at or below everything in the link's own
/// subtree. The global leftmost link at every level carries the empty
/// separator (negative infinity). Because a separator is always a prefix of
/// its own subtree's minimum leaf key, it can be maintained from the edited
/// side of a seam alone (see [`reseparate`](Self::reseparate)).
///
/// The default, [`Geometric`], hashes keys and separators with blake3 and
/// stores shortest-distinguishing separators, producing the canonical
/// production shape. Tests may inject an alternative distribution to force
/// exact tree shapes.
pub trait Distribution {
    /// The leaf coin: computes the rank of an entry key from its bytes and the
    /// tree's [`Manifest`]. An entry whose rank exceeds
    /// [`BOTTOM_RANK`](crate::BOTTOM_RANK) proposes to end its leaf segment;
    /// the proposal stands unless the seam to the entry's successor is
    /// vetoed ([`vetoes`](Self::vetoes)).
    ///
    /// The coin sees only the key bytes: every key is ranked, whatever its
    /// length. The separator bound (`manifest.max_separator`) is enforced per
    /// seam by the veto, which rejects exactly the seams whose shortest
    /// separator would exceed it, instead of demoting every long key to rank
    /// 0 (the retired length guard, which glued all long keys into one
    /// unbounded run even where they diverged early). The branching parameter
    /// (`manifest.branch_factor`) sets the split probability, i.e. the
    /// expected fanout.
    fn rank(key: &[u8], manifest: &Manifest) -> Rank;

    /// The seam coin: computes the rank of a seam from its separator bytes.
    /// A child whose separator rank exceeds the level threshold starts a new
    /// index node at that level. The same separator string serves every level
    /// along a vertical boundary, so a high rank punches through several
    /// levels at once, exactly like the key-rank recursion it replaces.
    ///
    /// The default applies the key coin to the separator bytes, which is the
    /// right choice for any hash-based distribution (the two coins stay
    /// independent because their inputs never collide: a separator sorts
    /// strictly between two keys), behind a length guard that ranks any
    /// separator longer than `max_separator` at 0.
    ///
    /// The guard is the stored-form face of the veto
    /// ([`vetoes`](Self::vetoes)): an accepted seam's separator is within the
    /// bound by construction (a vetoed seam never forms), so for natural
    /// seams the guard is inert and the coin decides. A separator over the
    /// bound therefore marks a seam that was never accepted by the veto —
    /// today a forced seam from the segment cap ([`cap`]), whose separator
    /// exceeds `max_separator` by construction precisely so this guard keeps
    /// it quiet: forced seams are leaf-level only and must never punch an
    /// index cut. Guard and veto MUST agree (both compare the same separator
    /// against the same bound), or a cut accepted at the leaf could go
    /// missing at index levels.
    fn seam_rank(separator: &[u8], manifest: &Manifest) -> Rank {
        if separator.len() as u32 > manifest.max_separator {
            return 0;
        }
        Self::rank(separator, manifest)
    }

    /// The veto: whether the seam between `left` and `right` — adjacent keys
    /// in the FULL key order, `left` the immediate predecessor — is rejected
    /// as a boundary. One decision per seam: the seam between two keys is
    /// the same seam at every level, so a veto suppresses the leaf cut and
    /// every index cut above it at once, and the two keys always share a
    /// segment.
    ///
    /// The default rejects a seam exactly when its shortest distinguishing
    /// separator ([`separator`](Self::separator)) would exceed
    /// `manifest.max_separator`: accepting it would store an over-long
    /// separator. Since vetoed seams never exist in stored form, every
    /// stored natural separator stays within the bound by construction —
    /// the property the retired length-guard demotion bought by ranking
    /// every over-long KEY at 0, which glued long keys into one unbounded
    /// run even where they diverged early. The veto rejects only the seams
    /// that are genuinely indistinguishable within the bound (near-duplicate
    /// neighbors), so a run of long keys with early divergences still splits
    /// naturally.
    ///
    /// Edit stability, the property incremental maintenance leans on: a
    /// seam's separator — and with it this decision — is a function of its
    /// two adjacent keys alone, and under the lower-bound convention it is
    /// invariant under every edit that keeps both partner keys (keys routed
    /// left of a separator share exactly its divergence with the right key;
    /// see `raise_to_floor` for the right-hand side). Only an edit that
    /// removes a partner key can change a stored seam's status, and those
    /// edits (boundary deletes, orphan appends, min-moves) widen their
    /// re-shape window across the seam before regrouping.
    ///
    /// An override must stay consistent with [`separator`](Self::separator)
    /// and [`seam_rank`](Self::seam_rank): `vetoes(left, right)` must hold
    /// exactly when `separator(left, right)` is longer than `max_separator`,
    /// or the cut decision and the stored form disagree and canonical shape
    /// breaks. It must also be downward closed between neighbors: for
    /// `p < q < s`, `vetoes(p, s)` implies `vetoes(p, q)` and
    /// `vetoes(q, s)`. The default has this by construction (a key between
    /// two keys shares at least their common prefix, and cannot be that
    /// prefix itself, so it cannot diverge from either any earlier); the
    /// edit fast path leans on it to skip seam re-checks on insert.
    fn vetoes(left: &[u8], right: &[u8], manifest: &Manifest) -> bool {
        seam_vetoed(left, right, manifest)
    }

    /// The authoritative leaf-level cut decision for the ACCEPTED seam after
    /// `key`, given `weight`: the total charge this seam meters — the
    /// entry's own weight ([`Entry::weight`](crate::Entry::weight), key
    /// bytes plus the value's payload weight) plus the bank accumulated
    /// since the previous accepted seam.
    ///
    /// The bank is the rule's accumulator: walking seams left to right,
    /// every vetoed seam adds its left entry's weight to the bank (no cut
    /// is possible there), and every accepted seam spends the bank into
    /// this decision and resets it — RESET AT EVERY ACCEPTED SEAM, cut or
    /// no cut. That makes the bank "weight since the last accepted seam", a
    /// structural property of the key sequence, never "weight since the
    /// last cut": no coin anywhere reads a cut outcome, which is what keeps
    /// the decision a pure function of the key set (a cut-outcome bank
    /// would cascade every downstream decision off one flip and break
    /// convergence). An uncuttable vetoed stretch therefore funds the first
    /// accepted seam at its end with its whole weight, so byte pacing holds
    /// across near-duplicate clusters instead of flipping one key-sized
    /// coin per cluster.
    ///
    /// The draw still reads only the KEY's hash; the weight — including the
    /// value's payload — sets the threshold. A value change therefore moves
    /// this decision, and the edit path treats payload-weight changes as
    /// shape-relevant wherever pacing is armed. With `max_segment == 0` the
    /// weight is ignored entirely and the decision is the entry-counted
    /// geometric coin (`rank`), byte for byte the shipped baseline.
    fn leaf_cut(key: &[u8], weight: usize, manifest: &Manifest) -> bool {
        if manifest.max_segment == 0 {
            Self::rank(key, manifest) > BOTTOM_RANK
        } else {
            weight_paced_cut(key, weight, manifest)
        }
    }

    /// Derives the separator for a fresh seam from the two keys adjacent to
    /// it: `left` is the last leaf key before the seam and `right` the first
    /// leaf key after it, with `left < right`. Defaults to the canonical
    /// shortest-distinguishing prefix of `right`.
    fn separator(left: &[u8], right: &[u8]) -> Vec<u8> {
        shortest_separator(left, right)
    }

    /// Re-derives a child's separator after an edit may have changed the
    /// child's minimum leaf key, without access to the left neighbor.
    ///
    /// `min` is the child's (possibly new) minimum leaf key and `floor` its
    /// previous separator. The previous separator encodes everything needed
    /// about the unloaded left neighbor: it sorts strictly above the
    /// neighbor's maximum, and routing guarantees `min >= floor` (every key
    /// an edit delivers to the child was routed by `key >= separator`). The
    /// canonical result is the shortest prefix of `min` that is `>= floor`,
    /// the default.
    ///
    /// An override must stay consistent with [`separator`](Self::separator):
    /// for any valid seam, `reseparate(min, separator(left, min))` must
    /// reproduce `separator(left, min)`, or canonical form breaks.
    fn reseparate(min: &[u8], floor: &[u8]) -> Vec<u8> {
        raise_to_floor(min, floor)
    }
}

/// The default [`Distribution`]: geometric coins over blake3 hashes with
/// shortest-distinguishing separators (see [`geometric`]).
#[derive(Clone, Debug, Default)]
pub struct Geometric;

impl Distribution for Geometric {
    fn rank(key: &[u8], manifest: &Manifest) -> Rank {
        // Every key is ranked by a coin over its own bytes alone. The
        // separator bound is enforced per seam by the veto
        // (`Distribution::vetoes`) and the seam coin's length guard, not by
        // demoting long keys: a demoted key could never end a segment even
        // where it diverged from its neighbor in the first byte, which is
        // what let near-duplicate-free runs of long keys grow into unbounded
        // leaves.
        //
        // Which coin depends on the manifest: `max_segment == 0` keeps the
        // entry-counted geometric coin (the shipped baseline, byte for
        // byte); a non-zero `max_segment` switches to the byte-pacing
        // weight coin, whose cut probability is the entry's weight share of
        // the target so segments average `max_segment` weighted bytes
        // whatever the key-size mix (see [`weight_paced_rank`]).
        if manifest.max_segment == 0 {
            audit::key(key.len());
            geometric::compute_geometric_rank(&hash_memo::hash(key), manifest.branch_factor())
        } else {
            weight_paced_rank(key, manifest)
        }
    }

    /// The seam coin follows the leaf coin's mode. With `max_segment == 0`
    /// it is the entry-counted geometric ladder over the separator's hash,
    /// byte for byte the shipped baseline. Under byte pacing it becomes the
    /// weight-paced ladder ([`weight_paced_seam_rank`]): one independent
    /// coin per index level over the separator's hash, each firing with the
    /// probability that is the LINK's weight share of `max_segment` — so
    /// index nodes average `max_segment` bytes of links at every level,
    /// exactly as leaves average `max_segment` bytes of entries, and no
    /// level escapes pacing. Nesting is unchanged in shape: the rank is the
    /// count of consecutive levels the ladder fires, and `regroup_children`
    /// keeps its monotone thresholds, so a level-n cut is a level-(n-1) cut
    /// by construction.
    ///
    /// The length guard is the stored-form face of the veto, exactly as in
    /// the trait default: accepted seams are within the bound by
    /// construction, so it fires only for forced leaf separators (the cap
    /// backstop), keeping them leaf-level only.
    fn seam_rank(separator: &[u8], manifest: &Manifest) -> Rank {
        if separator.len() as u32 > manifest.max_separator {
            return 0;
        }
        audit::seam(separator.len());
        if manifest.max_segment == 0 {
            geometric::compute_geometric_rank(&hash_memo::hash(separator), manifest.branch_factor())
        } else {
            weight_paced_seam_rank(separator, manifest)
        }
    }
}

/// The byte-pacing seam ladder: the paced analog of the geometric seam
/// rank. Level `k`'s coin fires with probability
/// `link_weight(separator) / max_segment`, drawn from lane `k` of the
/// separator's blake3 hash (eight bytes at offset `8 * ((k - 1) % 4)`), and
/// the rank is `BOTTOM_RANK + 1` plus the number of consecutive levels that
/// fire — the same monotone scale `regroup_children` thresholds against, so
/// a seam that cuts level `n` cuts every level below it and nesting holds
/// by construction.
///
/// Per-level lanes keep the levels independent: the SAME separator string
/// serves a node's left seam at every level of its spine, so a single draw
/// would promote all-or-nothing. Lane 1 reuses the leaf coin's byte range,
/// which is safe because the leaf coin hashes KEYS and this ladder hashes
/// separators — under the cut-after convention a separator is never the
/// coin key of its own seam. Levels beyond four wrap lanes; a paced tree
/// reaches level five only past roughly `(max_segment / 100)^4` leaves,
/// where the wrapped correlation (all-or-nothing promotion between level
/// `k` and `k + 4`) is structurally harmless because nesting is enforced by
/// the recursion, not by the draws.
///
/// Expected fanout per level is `max_segment / mean link weight` (several
/// hundred at the default target), so index nodes are byte-bounded at the
/// same scale as leaves — which is what keeps the per-commit root rewrite
/// flat as the tree grows, instead of one flat root accumulating every leaf
/// link.
pub fn weight_paced_seam_rank(separator: &[u8], manifest: &Manifest) -> Rank {
    let hash = hash_memo::hash(separator);
    let bytes = *hash.as_bytes();
    let weight = cap::link_weight(separator) as u128;
    let target = manifest.max_segment as u128;
    let mut rank = BOTTOM_RANK + 1;
    for level in 0..8usize {
        let lane = (level % 4) * 8;
        let draw = u64::from_le_bytes(
            bytes[lane..lane + 8]
                .try_into()
                .expect("lane slice is eight bytes"),
        );
        if (draw as u128) * target < (weight << 64) {
            rank += 1;
        } else {
            break;
        }
    }
    rank
}

/// The byte-pacing leaf coin in its bank-zero form: cut after `key` with
/// probability `entry_weight(key) / max_segment`, decided from
/// `blake3(key)` alone. This is what [`Geometric::rank`] answers under a
/// non-zero `max_segment` — the conservative per-key floor the edit path's
/// structural checks read (a bank can only raise the cut probability, so
/// `rank > BOTTOM_RANK` here implies the authoritative decision cuts too).
/// The grouped decision, which adds the bank an uncuttable vetoed stretch
/// accumulated, is [`Distribution::leaf_cut`].
///
/// Per renewal reasoning, independent per-key cuts with probability
/// proportional to weight make the expected weighted run length between
/// cuts `max_segment`, with an exponential tail (`P(run > W) ≈ e^(-W /
/// max_segment)`) — a soft cap: segments AVERAGE the target instead of
/// never exceeding it, which is what removes the whole-run re-shape
/// machinery a hard cap needed. An entry at or above the target weight
/// always cuts (probability clamps to one), subject to the veto like any
/// other proposed boundary.
///
/// The coin reads the SEAM'S LEFT key (the entry that closes its segment,
/// the existing cut-after convention): a separator is a prefix of the seam's
/// RIGHT key and sorts strictly above the left key, so the leaf coin and the
/// seam coin can never read the same bytes even when a separator equals a
/// whole key — the two stay independent by construction. Returns
/// `BOTTOM_RANK + 1` (cut) or `BOTTOM_RANK` (no cut); index promotion is the
/// seam coin's job alone.
pub fn weight_paced_rank(key: &[u8], manifest: &Manifest) -> Rank {
    if weight_paced_cut(key, cap::entry_weight(key), manifest) {
        BOTTOM_RANK + 1
    } else {
        BOTTOM_RANK
    }
}

/// The weight coin's core draw: cut at the accepted seam after `key` with
/// probability `weight / max_segment`, decided from `blake3(key)` alone.
/// `weight` is the total funding the seam charges — the key's own entry
/// weight plus whatever bank an uncuttable vetoed stretch behind it
/// accumulated ([`Distribution::leaf_cut`]).
///
/// The comparison runs in 128-bit arithmetic so neither product can
/// overflow or lose precision, and the probability saturates cleanly: any
/// `weight >= max_segment` makes the inequality hold for every draw (a
/// stretch several times the target cuts with certainty, which is exactly
/// the pacing intent).
pub fn weight_paced_cut(key: &[u8], weight: usize, manifest: &Manifest) -> bool {
    audit::key(key.len());
    let [b0, b1, b2, b3, b4, b5, b6, b7, ..] = *hash_memo::hash(key).as_bytes();
    let draw = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7]);
    let weight = weight as u128;
    let target = manifest.max_segment as u128;
    (draw as u128) * target < (weight << 64)
}

/// The shortest prefix of `right` that sorts strictly above `left`, given
/// `left < right`: the canonical shortest-distinguishing separator of a seam
/// (RocksDB's `FindShortestSeparator`, taken as a prefix of the right-hand
/// key so the lower-bound convention holds).
///
/// `left < right` guarantees `right` is not a prefix of `left`, so the byte
/// at the divergence point always exists in `right`.
pub fn shortest_separator(left: &[u8], right: &[u8]) -> Vec<u8> {
    debug_assert!(
        left < right,
        "separator requires ordered seam keys: {left:02x?} < {right:02x?}"
    );
    let lcp = left
        .iter()
        .zip(right.iter())
        .take_while(|(a, b)| a == b)
        .count();
    right[..=lcp.min(right.len() - 1)].to_vec()
}

/// Whether the shortest distinguishing separator of the seam between
/// adjacent keys `left < right` exceeds `manifest.max_separator`: the
/// default veto rule (see [`Distribution::vetoes`]).
///
/// The shortest separator is `min(lcp + 1, len(right))` bytes long
/// ([`shortest_separator`]), so it exceeds the bound exactly when `right`
/// outgrows the bound and the two keys agree on its first `max_separator`
/// bytes; checked here without allocating the separator. (`lcp >= bound`
/// with `len(right) == bound` cannot occur: `right` would be a prefix of
/// `left` and sort at or below it.)
pub fn seam_vetoed(left: &[u8], right: &[u8], manifest: &Manifest) -> bool {
    let bound = manifest.max_separator as usize;
    right.len() > bound && left.len() >= bound && left[..bound] == right[..bound]
}

/// The shortest prefix of `min` that sorts at or above `floor`: the canonical
/// separator of a seam whose right-hand minimum is `min`, re-derived from the
/// seam's previous separator `floor` instead of the (unavailable) left key.
///
/// Correctness relies on two invariants the tree maintains: `floor` sorts
/// strictly above the left neighbor's maximum and diverges from it exactly at
/// its own last byte, and `min >= floor` (routing sends a key into the seam's
/// right side only when the key is at or above the stored separator). Under
/// those, the result equals `shortest_separator(left_max, min)` byte for
/// byte, so incremental maintenance and a fresh build converge.
pub fn raise_to_floor(min: &[u8], floor: &[u8]) -> Vec<u8> {
    let lcp = min
        .iter()
        .zip(floor.iter())
        .take_while(|(a, b)| a == b)
        .count();
    if lcp == floor.len() {
        // The floor is a prefix of (or equal to) min: it remains the shortest
        // prefix of min at or above itself. In particular an empty floor (the
        // global leftmost seam) stays empty.
        floor.to_vec()
    } else if lcp < min.len() && min[lcp] > floor[lcp] {
        min[..=lcp].to_vec()
    } else {
        // min < floor: outside the maintained invariant. The full minimum is
        // always a correct (if untruncated) separator, so degrade gracefully
        // rather than misroute.
        debug_assert!(false, "reseparate invariant violated: min < floor");
        min.to_vec()
    }
}

/// Pure helpers for weighing entries and force-splitting over-target
/// vetoed stretches: the last-resort backstop behind the weight coin.
///
/// The veto ([`Distribution::vetoes`](crate::Distribution::vetoes)) rejects
/// every seam whose shortest separator exceeds `max_separator`, so
/// near-duplicate neighbors — keys agreeing beyond the bound, e.g. copies
/// of one long value under VAE order — can never split among themselves.
/// Where such keys cluster, the stretch between accepted seams grows
/// without bound and every edit rewrites the whole block, and no coin can
/// help: every proposal inside the stretch is vetoed. The backstop steps in
/// exactly there, and ONLY there: when a fully vetoed stretch's summed
/// [`entry_weight`] exceeds `max_segment`, it is split at the deterministic
/// anchors [`forced_cut_positions`] chooses. (The arm-1 hard cap invoked
/// the same splitting for EVERY over-target run, which dragged whole-run
/// merge machinery onto the common edit path — a 7x regression; the weight
/// coin paces everything the veto allows, so the backstop stays off the
/// common path by construction.)
///
/// [`entry_weight`] also feeds the weight-paced leaf coin
/// ([`weight_paced_rank`](super::weight_paced_rank)), so the backstop and
/// the coin meter the same byte scale.
///
/// Every function here is a pure function of the run's key list (never of
/// edit history or remembered state), which is what lets differently ordered
/// edits converge on the same forced cuts. Forced seams are leaf-level only:
/// their separators exceed `max_separator` by construction (see
/// [`forced_separator`]), so the seam coin's length guard ranks them 0 and
/// they can never punch an index-level cut.
pub mod cap {
    use dialog_common::Blake3Hash;

    use crate::{Key, Manifest};

    /// Weight charged per entry beyond its key bytes: a stand-in for the
    /// value slot and per-entry encoding overhead. The cap needs a
    /// deterministic per-entry weight at cut time, when the encoded node
    /// size (front coding, per-node dictionaries) is not yet known; for
    /// artifact trees the key carries the value, so key length dominates
    /// real cost and front coding brings real blocks in under the proxy.
    pub const ENTRY_WEIGHT_OVERHEAD: usize = 32;

    /// The weight an entry contributes toward `Manifest::max_segment`.
    pub fn entry_weight(key: &[u8]) -> usize {
        key.len() + ENTRY_WEIGHT_OVERHEAD
    }

    /// Weight charged per link beyond its separator bytes at index levels:
    /// the 32-byte child hash plus per-link encoding overhead (offsets,
    /// front-coding bookkeeping). The index analog of
    /// [`ENTRY_WEIGHT_OVERHEAD`].
    pub const LINK_WEIGHT_OVERHEAD: usize = 16;

    /// The weight a link contributes toward an index node's `max_segment`
    /// budget: its separator bytes plus the child hash plus fixed overhead.
    pub fn link_weight(separator: &[u8]) -> usize {
        separator.len() + 32 + LINK_WEIGHT_OVERHEAD
    }

    /// Deterministic forced cut positions bounding an INDEX-level frame —
    /// the run of children between one level's coin cuts — at `ceiling`
    /// weighted bytes of links. Returns positions `p` (a cut falls BEFORE
    /// child `p` of the frame), sorted ascending; empty when the frame
    /// fits.
    ///
    /// The index analog of [`frame_cut_positions`], with two
    /// simplifications the derived-quietness property allows: every
    /// interior seam is a candidate (no stored mark is needed, so no
    /// candidacy test), and the anchor identity is the seam's separator
    /// (the seam's one canonical name at index levels). Elections order by
    /// the separator hash's tail ([`anchor_order`]) under the same selector
    /// knob; the hybrid's semantic metric is the separator's own length
    /// (index separators are already shortest-form).
    ///
    /// Placement is EDIT-STABLE greedy, not recursive bisection: the frame is
    /// swept left to right accumulating link weight, and whenever adding the
    /// next child would carry the open window past the ceiling, a cut is
    /// placed at the window's best anchor (by [`AnchorSelector`]) and the
    /// window restarts from there. Because each window is decided only by the
    /// keys inside it, an edit re-anchors only the window it lands in and the
    /// windows after it — every window strictly before the edit is decided by
    /// unchanged keys and keeps its byte-identical boundaries. That locality
    /// is what lets an untouched index piece survive an edit elsewhere in the
    /// frame and be passed through as its stored link (see `IndexPieceOrigin`).
    /// Recursive bisection lacked it: an added byte could move the frame's
    /// top-level split and cascade fresh boundaries through both halves.
    pub fn index_frame_cut_positions(
        separators: &[&[u8]],
        weights: &[usize],
        ceiling: usize,
        manifest: &Manifest,
    ) -> Vec<usize> {
        if separators.len() < 2 || weights.iter().sum::<usize>() <= ceiling {
            return Vec::new();
        }
        // Candidacy per seam: a long separator marks a forced LEAF seam, and
        // cutting an index frame there would scatter that leaf run's pieces
        // across two parents and break the leaf widening's contiguity
        // contract, so such seams are never index anchors. `candidate[at]`
        // holds `(separator length, separator hash)` for the seam before
        // child `at` when it qualifies — the two keys the selector orders by.
        let mut candidate: Vec<Option<(usize, Blake3Hash)>> = Vec::with_capacity(separators.len());
        candidate.push(None); // seam 0 is the frame's left edge, never a cut.
        for separator in &separators[1..] {
            if separator.len() as u32 > manifest.max_separator {
                candidate.push(None);
            } else {
                super::audit::election(separator.len());
                candidate.push(Some((separator.len(), super::hash_memo::hash(separator))));
            }
        }

        let selector = AnchorSelector::from_manifest(manifest);
        // The best (winning) anchor among candidates in `(lo, hi]`: the one
        // the selector ranks smallest — shortest separator then hash tail for
        // the hybrid, hash tail alone for pure rendezvous. `None` when the
        // span holds no qualifying seam.
        let best_in = |lo: usize, hi: usize| -> Option<usize> {
            (lo + 1..=hi)
                .filter_map(|at| candidate[at].as_ref().map(|entry| (at, entry)))
                .min_by(|a, b| match selector {
                    AnchorSelector::Rendezvous => anchor_order(&a.1.1).cmp(anchor_order(&b.1.1)),
                    AnchorSelector::Hybrid => {
                        a.1.0
                            .cmp(&b.1.0)
                            .then_with(|| anchor_order(&a.1.1).cmp(anchor_order(&b.1.1)))
                    }
                })
                .map(|(at, _)| at)
        };

        let mut cuts = Vec::new();
        let mut window_start = 0usize;
        let mut window_weight = weights[0];
        for at in 1..separators.len() {
            if window_weight + weights[at] > ceiling {
                // Adding this child would carry the open window past the
                // ceiling: close the window at its best anchor and restart it
                // there. A window with no qualifying seam (a long-separator
                // run whose seams are all forced leaf seams) cannot be cut, so
                // it keeps growing until one appears — the same tolerance the
                // recursive placement had for a piece with no candidate.
                if let Some(cut) = best_in(window_start, at) {
                    cuts.push(cut);
                    window_start = cut;
                    window_weight = weights[cut..at].iter().sum();
                }
            }
            window_weight += weights[at];
        }
        cuts
    }

    /// Longest common prefix length of two byte strings.
    fn lcp(a: &[u8], b: &[u8]) -> usize {
        a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
    }

    /// Whether the seam between adjacent keys `left < right` may carry a
    /// forced cut: true exactly when [`forced_separator`] would exceed
    /// `max_separator`, keeping the seam quiet at index levels through the
    /// existing length guard and self-identifying in stored form (a
    /// separator longer than `max_separator` joins a segment to its left
    /// sibling's run when an edit widens its window).
    pub fn is_forced_candidate(left: &[u8], right: &[u8], manifest: &Manifest) -> bool {
        let bound = manifest.max_separator as usize;
        right.len() > bound || lcp(left, right) + 1 > bound
    }

    /// The separator of a forced seam between adjacent keys `left < right`:
    /// the shortest prefix of `right` that sorts strictly above `left` AND
    /// exceeds `max_separator`, so the seam coin's length guard keeps the
    /// seam out of every index level. Only defined for seams where
    /// [`is_forced_candidate`] holds.
    ///
    /// `left < right` guarantees `right` is not a prefix of `left`, so the
    /// divergence byte exists in `right` and the cut never exceeds
    /// `right.len()`.
    pub fn forced_separator(left: &[u8], right: &[u8], manifest: &Manifest) -> Vec<u8> {
        debug_assert!(
            is_forced_candidate(left, right, manifest),
            "forced separator requested for a seam that is not a candidate"
        );
        let bound = manifest.max_separator as usize;
        let cut = (lcp(left, right) + 1).max((bound + 1).min(right.len()));
        right[..cut].to_vec()
    }

    /// How a forced cut chooses its anchor among a piece's candidate seams.
    /// Read from `Manifest::anchor_selector`; both are pure functions of the
    /// key set, so either converges — they trade stability characters.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum AnchorSelector {
        /// Pure rendezvous: the candidate whose right key has the minimal
        /// hash tail ([`anchor_order`] — bytes disjoint from the coin's
        /// draw). Uniformly sticky: any churn relocates an anchor only by
        /// beating or removing the local minimum.
        Rendezvous,
        /// Semantic first: the candidate whose shortest distinguishing
        /// separator is shortest (the most semantically different adjacent
        /// pair), hash-minimal within that class. By the sandwich property
        /// (a key between two keys shares their common prefix) INSERTS can
        /// never mint a strictly shorter class, so the class floor only
        /// moves on deletes that merge seams; within large tie classes
        /// (natural runs, where most seams diverge equally early) this
        /// degrades toward pure rendezvous.
        Hybrid,
    }

    impl AnchorSelector {
        /// Decodes `Manifest::anchor_selector`; unknown values fall back to
        /// rendezvous rather than failing, keeping old nodes readable.
        pub fn from_manifest(manifest: &Manifest) -> Self {
            match manifest.anchor_selector {
                1 => AnchorSelector::Hybrid,
                _ => AnchorSelector::Rendezvous,
            }
        }
    }

    /// A candidate anchor: the seam before `keys[at]`, its shortest
    /// distinguishing separator length (the semantic-distance metric the
    /// hybrid selector orders by first), and its rendezvous hash.
    struct Anchor {
        at: usize,
        separator_len: usize,
        hash: Blake3Hash,
    }

    /// The bytes an anchor election orders by: the TAIL of the key's blake3
    /// hash, bytes 8..32 — disjoint from the leading 8 bytes every coin
    /// draw reads ([`weight_paced_cut`](super::weight_paced_cut) and the
    /// geometric rank both consume `hash[0..8]`).
    ///
    /// The disjointness is load-bearing: inside an over-target frame every
    /// key's draw came up tails, so ordering elections by the full hash
    /// (whose leading bytes ARE the draw) would systematically anchor at
    /// the key that came closest to cutting, entangling coin outcomes with
    /// anchor placement. Reading only the tail keeps the two decisions
    /// independent while staying a pure function of the key. One rule for
    /// both backstops: the vetoed-stretch anchors and the frame-ceiling
    /// anchors order by this same slice.
    pub fn anchor_order(hash: &Blake3Hash) -> &[u8] {
        &hash.as_bytes()[8..]
    }

    /// Recursively splits `keys` at chosen anchors until every piece's
    /// weight is at or under `threshold`: while a piece exceeds it, cut at
    /// the selector's best candidate inside the piece. Returns cut
    /// positions sorted ascending. Shared by the vetoed-stretch backstop
    /// ([`forced_cut_positions`]) and the frame ceiling
    /// ([`frame_cut_positions`]); a pure function of the candidate list and
    /// weights, so the same inputs always split the same way, whatever
    /// edits produced them. The threshold is symmetric — the same value
    /// creates and dissolves cuts, no hysteresis — because any
    /// trajectory-dependent rule would break the byte-identity independent
    /// imports and canonicalize promise.
    fn choose_cuts(
        weights: &[usize],
        candidates: &[Anchor],
        threshold: usize,
        selector: AnchorSelector,
    ) -> Vec<usize> {
        if candidates.is_empty() {
            return Vec::new();
        }
        // Prefix weights: weight of items[lo..hi] is prefix[hi] - prefix[lo].
        let mut prefix = Vec::with_capacity(weights.len() + 1);
        prefix.push(0usize);
        for weight in weights {
            let last = *prefix.last().expect("prefix starts non-empty");
            prefix.push(last + weight);
        }

        let mut cuts = Vec::new();
        let mut pieces = vec![(0usize, weights.len())];
        while let Some((lo, hi)) = pieces.pop() {
            if prefix[hi] - prefix[lo] <= threshold {
                continue;
            }
            let best = candidates
                .iter()
                .filter(|anchor| anchor.at > lo && anchor.at < hi)
                .min_by(|a, b| match selector {
                    AnchorSelector::Rendezvous => anchor_order(&a.hash).cmp(anchor_order(&b.hash)),
                    AnchorSelector::Hybrid => a
                        .separator_len
                        .cmp(&b.separator_len)
                        .then_with(|| anchor_order(&a.hash).cmp(anchor_order(&b.hash))),
                });
            let Some(anchor) = best else {
                continue;
            };
            cuts.push(anchor.at);
            pieces.push((lo, anchor.at));
            pieces.push((anchor.at, hi));
        }
        cuts.sort_unstable();
        cuts
    }

    /// The shortest distinguishing separator length of the seam between
    /// adjacent keys `left < right`: `min(lcp + 1, len(right))`, the
    /// semantic-distance metric of the hybrid selector.
    fn shortest_separator_len(left: &[u8], right: &[u8]) -> usize {
        (lcp(left, right) + 1).min(right.len())
    }

    /// Deterministic forced cut positions for a fully vetoed leaf stretch,
    /// as indices into `keys` (a cut at `i` starts a new segment at
    /// `keys[i]`), sorted ascending. Empty when the stretch's weight is
    /// within `max_segment`, the target is unset, or no seam qualifies.
    ///
    /// Anchors are chosen by the manifest's [`AnchorSelector`] over the
    /// candidate seams, recursively until every piece fits ([`choose_cuts`]):
    /// boundaries are stable under churn by PLACEMENT, not memory. Under
    /// rendezvous an insertion relocates an anchor only by beating the
    /// piece's hash-tail minimum ([`anchor_order`], coin-disjoint bits) and
    /// a deletion only by removing the anchor key; under the hybrid the
    /// same holds within the shortest-separator class, and the class floor
    /// itself can only drop on deletes (sandwich property). In a vetoed
    /// stretch the hybrid also shrinks the STORED forced separators: they
    /// carry `lcp + 1` bytes, and it anchors where that is smallest.
    ///
    /// A piece with no candidate seam stays whole even over the target (a
    /// run of short keys offers no separator the quietness rule accepts);
    /// in the fully vetoed stretches the backstop is scoped to, every seam
    /// qualifies, so this is a formality there.
    pub fn forced_cut_positions<K>(
        keys: &[&K],
        weights: &[usize],
        manifest: &Manifest,
    ) -> Vec<usize>
    where
        K: Key,
    {
        let cap = manifest.max_segment as usize;
        if cap == 0 || keys.len() < 2 {
            return Vec::new();
        }
        if weights.iter().sum::<usize>() <= cap {
            return Vec::new();
        }

        let mut candidates: Vec<Anchor> = Vec::new();
        for at in 1..keys.len() {
            let left = keys[at - 1].as_ref();
            let right = keys[at].as_ref();
            if is_forced_candidate(left, right, manifest) {
                super::audit::election(right.len());
                candidates.push(Anchor {
                    at,
                    separator_len: shortest_separator_len(left, right),
                    hash: super::hash_memo::hash(right),
                });
            }
        }
        choose_cuts(
            weights,
            &candidates,
            cap,
            AnchorSelector::from_manifest(manifest),
        )
    }

    /// Whether the accepted seam between adjacent keys `left < right` can
    /// carry a frame anchor: true when a valid lower-bound separator longer
    /// than `max_separator` exists for it (see [`frame_separator`]), so the
    /// forced seam stays self-identifying in stored form and rank 0 at
    /// index levels, exactly like a stretch anchor.
    pub fn is_frame_candidate(left: &[u8], right: &[u8], manifest: &Manifest) -> bool {
        frame_separator(left, right, manifest).is_some()
    }

    /// The stored separator for a frame anchor at the accepted seam between
    /// `left < right`: a valid lower-bound string (`> left`, `<= right`)
    /// longer than `max_separator`, so the seam coin's length guard keeps
    /// the seam leaf-level and the widening join predicate recognizes the
    /// piece. When `right` outgrows the bound this is the same
    /// right-prefix form the stretch anchors use ([`forced_separator`]);
    /// otherwise `left` is padded with `0x00` just past the bound — still
    /// strictly above `left` (a proper extension) and at or below `right`
    /// whenever `right` does not sit inside the padding gap, the rare case
    /// where no over-bound separator exists at all and the seam is not a
    /// candidate. The padded form is not a prefix of the right key, which
    /// the min-move floor rule normally relies on; forced seams never meet
    /// that rule (the widening dissolves them before any regroup or
    /// reseparate, exactly as for stretch anchors), which is why this is
    /// safe.
    pub fn frame_separator(left: &[u8], right: &[u8], manifest: &Manifest) -> Option<Vec<u8>> {
        let bound = manifest.max_separator as usize;
        if right.len() > bound {
            return Some(forced_separator(left, right, manifest));
        }
        let mut separator = left.to_vec();
        separator.resize(bound.max(left.len()) + 1, 0);
        (separator.as_slice() <= right).then_some(separator)
    }

    /// The stored separator for ANY forced seam — stretch or frame anchor —
    /// between adjacent keys `left < right`. Total over both anchor kinds:
    /// a stretch anchor's right key is over the bound by the veto's own
    /// condition, so it always takes the right-prefix form, and a frame
    /// anchor was admitted by [`is_frame_candidate`].
    pub fn forced_seam_separator(left: &[u8], right: &[u8], manifest: &Manifest) -> Vec<u8> {
        frame_separator(left, right, manifest)
            .expect("forced seam chosen at a seam with no over-bound separator")
    }

    /// Deterministic forced cut positions bounding a FRAME — the entries
    /// between two coin-decided cuts — at the hard ceiling
    /// (`Manifest::frame_ceiling`), as indices into `keys` sorted
    /// ascending. `vetoed[i]` describes the seam between `keys[i]` and
    /// `keys[i + 1]`: anchors land only on ACCEPTED seams (a vetoed seam
    /// may never cut, whatever the weight), and only where a
    /// self-identifying separator exists ([`is_frame_candidate`]).
    ///
    /// This bounds the weight coin's natural exponential tail: the coin
    /// leaves a frame over the ceiling with probability `e^(-ceiling /
    /// max_segment)`, and the ceiling converts that tail into forced
    /// splits. Frames are delimited by coin cuts ONLY — forced cuts (this
    /// function's own output, or the stretch backstop's) never feed back
    /// into frame definition, so there is no cascade: the frame partition
    /// is a pure function of the key set, and so are the anchors.
    pub fn frame_cut_positions<K>(
        keys: &[&K],
        weights: &[usize],
        vetoed: &[bool],
        manifest: &Manifest,
    ) -> Vec<usize>
    where
        K: Key,
    {
        let ceiling = manifest.frame_ceiling();
        if ceiling == 0 || keys.len() < 2 {
            return Vec::new();
        }
        if weights.iter().sum::<usize>() <= ceiling {
            return Vec::new();
        }

        let mut candidates: Vec<Anchor> = Vec::new();
        for at in 1..keys.len() {
            if vetoed[at - 1] {
                continue;
            }
            let left = keys[at - 1].as_ref();
            let right = keys[at].as_ref();
            if is_frame_candidate(left, right, manifest) {
                super::audit::election(right.len());
                candidates.push(Anchor {
                    at,
                    separator_len: shortest_separator_len(left, right),
                    hash: super::hash_memo::hash(right),
                });
            }
        }
        choose_cuts(
            weights,
            &candidates,
            ceiling,
            AnchorSelector::from_manifest(manifest),
        )
    }
}

/// Geometric distribution for computing node ranks.
pub mod geometric {
    use dialog_common::Blake3Hash;

    use super::Rank;

    /// Computes the rank of a node from its hash using a geometric
    /// distribution with the default [`Manifest`](crate::Manifest)'s branch
    /// factor — the same modulus [`Geometric`](super::Geometric) uses for a
    /// default-manifest tree, so callers (test oracles, diagnostics)
    /// classify boundaries exactly as the tree does.
    pub fn rank(hash: &Blake3Hash) -> Rank {
        compute_geometric_rank(hash, crate::Manifest::default().branch_factor())
    }

    /// Compute the rank of a hash using a threshold-based geometric
    /// distribution.
    ///
    /// The first 8 bytes of the hash are interpreted as a little-endian `u64`
    /// prefix, uniformly distributed in `[0, u64::MAX]`. The rank is
    /// determined by how many geometrically decreasing thresholds
    /// (`u64::MAX / m`, `u64::MAX / m²`, ...) the prefix falls below:
    ///
    /// ```text
    ///   rank = 1  if  prefix >= threshold_1                (probability: 1 - 1/m)
    ///   rank = 2  if  threshold_2 <= prefix < threshold_1  (probability: 1/m - 1/m²)
    ///   rank = 3  if  threshold_3 <= prefix < threshold_2  (probability: 1/m² - 1/m³)
    ///   ...
    /// ```
    ///
    /// This gives an exact `1/m` split probability at each level, so the
    /// effective branch factor matches the declared one.
    ///
    /// The loop terminates on its own: integer division drives the threshold
    /// to zero after `floor(log_m(2^64))` steps, and no prefix is below zero,
    /// so ranks naturally top out at `floor(log_m(2^64)) + 1` (9 for m=256,
    /// enough for trees with ~10^19 entries).
    pub(crate) fn compute_geometric_rank(hash: &Blake3Hash, m: u64) -> Rank {
        debug_assert!(m >= 2, "branch factor must be at least 2, got {m}");

        // Destructuring the first 8 bytes of the (fixed-size) hash makes the
        // prefix extraction infallible: there is no slice conversion to fail.
        // Little-endian is an arbitrary but deterministic choice; uniformity
        // is unaffected, but the same bytes must always produce the same rank
        // for the tree structure to be consistent.
        let [b0, b1, b2, b3, b4, b5, b6, b7, ..] = *hash.as_bytes();
        let prefix = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7]);

        let mut rank: Rank = 1;
        let mut threshold = u64::MAX / m;

        while prefix < threshold {
            rank += 1;
            threshold /= m;
        }

        rank
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use super::geometric::compute_geometric_rank;
    use super::{cap, weight_paced_rank};
    use crate::{BOTTOM_RANK, Manifest};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Fixed seed so the statistical tests are deterministic; the assertions
    /// then verify exact, reproducible outcomes rather than racing sigma
    /// tolerances against an unseeded RNG.
    fn test_rng() -> StdRng {
        StdRng::seed_from_u64(0x_D1A1_06DB)
    }

    fn hash_for(prefix: u64) -> Blake3Hash {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&prefix.to_le_bytes());
        // Bytes 8..32 must not affect the result; make them non-zero to
        // prove it.
        bytes[8..].fill(0xFF);
        Blake3Hash::from(bytes)
    }

    /// The threshold comparisons are exact, deterministic golden values: a
    /// prefix at a threshold stays at the lower rank (the comparison is
    /// strict), a prefix one below it is promoted, a prefix of zero falls
    /// through every nonzero threshold and a prefix of `u64::MAX` is never
    /// promoted.
    #[dialog_common::test]
    async fn it_has_correct_rank_boundaries() -> Result<()> {
        let factor = 254u64;

        // For m=254 the thresholds are u64::MAX / 254^k for k = 1..=8
        // (254^8 < 2^64), so the maximum rank is 9.
        assert_eq!(compute_geometric_rank(&hash_for(0), factor), 9);
        assert_eq!(compute_geometric_rank(&hash_for(u64::MAX), factor), 1);

        let threshold_1 = u64::MAX / factor;
        let threshold_2 = threshold_1 / factor;

        assert_eq!(compute_geometric_rank(&hash_for(threshold_1), factor), 1);
        assert_eq!(
            compute_geometric_rank(&hash_for(threshold_1 - 1), factor),
            2
        );
        assert_eq!(compute_geometric_rank(&hash_for(threshold_2), factor), 2);
        assert_eq!(
            compute_geometric_rank(&hash_for(threshold_2 - 1), factor),
            3
        );

        Ok(())
    }

    /// `P(rank >= 2)` must be approximately `1/m` so that segments average
    /// `m` entries.
    #[dialog_common::test]
    async fn it_splits_with_branch_factor_probability() -> Result<()> {
        let factor = 254u64;
        let rounds = 1_000_000u32;
        let mut rng = test_rng();

        let mut promoted = 0u32;
        for _ in 0..rounds {
            let mut bytes = [0u8; 32];
            rng.fill(&mut bytes);
            if compute_geometric_rank(&Blake3Hash::from(bytes), factor) >= 2 {
                promoted += 1;
            }
        }

        let p_promoted = f64::from(promoted) / f64::from(rounds);
        let expected_p = 1.0 / factor as f64;

        assert!(
            (p_promoted - expected_p).abs() / expected_p < 0.2,
            "P(rank >= 2) = {p_promoted:.6} should be close to 1/{factor} = {expected_p:.6}"
        );

        Ok(())
    }

    /// The promotion probability must also be `1/m` at every level above the
    /// first, i.e. `P(rank >= k+1 | rank >= k) ≈ 1/m`.
    ///
    /// This is the regression test for the bit-batch implementation this
    /// module replaced: there, batches straddling byte boundaries were
    /// zero-filled, which inflated the conditional promotion probabilities to
    /// 1/2, 1/4, 1/8 instead of 1/m, producing much taller trees whose upper
    /// levels averaged only 2-4 children.
    #[dialog_common::test]
    async fn it_has_geometric_promotion_at_every_level() -> Result<()> {
        let factor = 16u64;
        let rounds = 2_000_000u32;
        let mut rng = test_rng();

        let mut at_least = [0u32; 4];
        for _ in 0..rounds {
            let mut bytes = [0u8; 32];
            rng.fill(&mut bytes);
            let rank = compute_geometric_rank(&Blake3Hash::from(bytes), factor);
            for (level, count) in at_least.iter_mut().enumerate() {
                if rank >= (level + 1) as u64 {
                    *count += 1;
                }
            }
        }

        for level in 1..at_least.len() - 1 {
            let conditional = f64::from(at_least[level + 1]) / f64::from(at_least[level]);
            let expected = 1.0 / factor as f64;
            assert!(
                (conditional - expected).abs() / expected < 0.15,
                "promotion from rank {} to {} should happen with probability ~1/{factor}, got {conditional:.6}",
                level + 1,
                level + 2,
            );
        }

        Ok(())
    }

    /// The weight-paced coin's runs are byte-denominated exponential: with
    /// cut probability `entry_weight / max_segment` per key, run weights
    /// between cuts average `max_segment` and the tail decays as
    /// `e^(-W / max_segment)`, whatever the key-length mix. The geometric
    /// coin cannot do this: it paces by entry count, so runs of long keys
    /// average `m` times the mean entry size however large that is.
    #[dialog_common::test]
    async fn it_paces_runs_at_the_weight_target() -> Result<()> {
        let target = 8192u32;
        let manifest = Manifest {
            max_segment: target,
            ..Manifest::default()
        };
        let mut rng = test_rng();

        let mut runs: Vec<usize> = Vec::new();
        let mut run = 0usize;
        for _ in 0..60_000u32 {
            let len = rng.gen_range(20..=600usize);
            let mut key = vec![0u8; len];
            rng.fill(&mut key[..]);
            run += cap::entry_weight(&key);
            if weight_paced_rank(&key, &manifest) > BOTTOM_RANK {
                runs.push(run);
                run = 0;
            }
        }
        assert!(runs.len() > 1_000, "enough runs to be statistical");

        let mean = runs.iter().sum::<usize>() as f64 / runs.len() as f64;
        let expected = target as f64;
        assert!(
            (mean - expected).abs() / expected < 0.1,
            "mean run weight {mean:.0} should be within 10% of the {target} target"
        );

        // Exponential tail: P(run > 2 * target) ≈ e^-2 ≈ 0.135.
        let beyond = runs
            .iter()
            .filter(|weight| **weight > 2 * target as usize)
            .count() as f64
            / runs.len() as f64;
        let e2 = (-2.0f64).exp();
        assert!(
            (beyond - e2).abs() / e2 < 0.35,
            "P(run > 2 * target) = {beyond:.4} should be near e^-2 = {e2:.4}"
        );

        Ok(())
    }

    /// With `max_segment == 0` the leaf cut is the entry-counted geometric
    /// coin and the bank is ignored entirely — whatever weight a vetoed
    /// stretch would have accumulated, the decision is byte-identical to
    /// the shipped baseline. This is the identity half of the bank rule:
    /// the banked coin exists only under a non-zero target.
    #[dialog_common::test]
    async fn it_ignores_the_bank_when_the_target_is_unset() -> Result<()> {
        let manifest = Manifest {
            max_segment: 0,
            ..Manifest::default()
        };
        let mut rng = test_rng();
        for _ in 0..500 {
            let len = rng.gen_range(4..=700usize);
            let mut key = vec![0u8; len];
            rng.fill(&mut key[..]);
            let baseline =
                <super::Geometric as super::Distribution>::rank(&key, &manifest) > BOTTOM_RANK;
            for bank in [0usize, 1, 512, 65_536, 10 << 20] {
                assert_eq!(
                    <super::Geometric as super::Distribution>::leaf_cut(&key, bank, &manifest),
                    baseline,
                    "a zero target must ignore the bank"
                );
            }
        }
        Ok(())
    }

    /// Anchor elections read hash bits DISJOINT from the coin's draw: the
    /// ordering ([`cap::anchor_order`]) is unchanged when only the leading
    /// 8 bytes — the bytes every coin draw consumes — differ, and it does
    /// respond to any tail byte. Without this, in an over-target frame
    /// (where every draw came up tails) a full-hash-minimal election would
    /// systematically anchor at the key that came closest to cutting,
    /// entangling coin outcomes with anchor placement.
    #[dialog_common::test]
    async fn it_elects_anchors_on_coin_disjoint_bits() -> Result<()> {
        let mut rng = test_rng();
        for _ in 0..200 {
            let mut bytes = [0u8; 32];
            rng.fill(&mut bytes);
            let hash = Blake3Hash::from(bytes);

            // Flipping every coin bit leaves the election order untouched.
            let mut lead_flipped = bytes;
            for byte in lead_flipped.iter_mut().take(8) {
                *byte = !*byte;
            }
            assert_eq!(
                cap::anchor_order(&hash),
                cap::anchor_order(&Blake3Hash::from(lead_flipped)),
                "anchor order must ignore the coin's hash bytes"
            );

            // Flipping any tail byte changes the ordering key.
            let mut tail_flipped = bytes;
            tail_flipped[8 + (bytes[0] as usize % 24)] ^= 0x01;
            assert_ne!(
                cap::anchor_order(&hash),
                cap::anchor_order(&Blake3Hash::from(tail_flipped)),
                "anchor order must read the hash tail"
            );
        }
        Ok(())
    }

    /// The greedy [`cap::index_frame_cut_positions`] is EDIT-STABLE from the
    /// left: a window is decided entirely by the children inside it, so an
    /// edit that changes only the tail of a frame leaves every window that
    /// closed before it byte-identical. Growing the frame (appending a child)
    /// or editing a child past the last cut therefore never moves an existing
    /// cut — it only re-decides the open tail. This is the locality that lets
    /// an untouched index piece survive an edit elsewhere in the frame and be
    /// reused as its stored link; recursive bisection lacked it, because the
    /// frame's top split moved when its total weight changed.
    #[dialog_common::test]
    async fn it_places_index_cuts_locally_under_edits() -> Result<()> {
        let manifest = Manifest {
            max_segment: 512,
            frame_ceiling_factor: 3,
            anchor_selector: 1,
            ..Manifest::default()
        };
        let ceiling = manifest.frame_ceiling();
        let mut rng = test_rng();

        for _ in 0..300 {
            // A frame of distinct short separators (all under max_separator, so
            // every interior seam is a candidate), each with its own link
            // weight.
            let count = rng.gen_range(20..60usize);
            let separators: Vec<Vec<u8>> = (0..count)
                .map(|i| format!("s{i:05}").into_bytes())
                .collect();
            let weights: Vec<usize> = (0..count).map(|_| rng.gen_range(20..600usize)).collect();

            let refs: Vec<&[u8]> = separators.iter().map(Vec::as_slice).collect();
            let cuts = cap::index_frame_cut_positions(&refs, &weights, ceiling, &manifest);
            if cuts.is_empty() {
                continue;
            }

            // Edit strictly past the last cut: append one child at the tail.
            // Every window closed before the last cut is untouched, so all
            // existing cuts must reappear unchanged (the tail may gain a new
            // cut, never lose an old one).
            let mut sep2 = separators.clone();
            let mut w2 = weights.clone();
            sep2.push(format!("s{count:05}").into_bytes());
            w2.push(rng.gen_range(20..600usize));
            let refs2: Vec<&[u8]> = sep2.iter().map(Vec::as_slice).collect();
            let cuts2 = cap::index_frame_cut_positions(&refs2, &w2, ceiling, &manifest);
            for &cut in &cuts {
                assert!(
                    cuts2.contains(&cut),
                    "appending a child moved an existing cut {cut}: \
                     before {cuts:?} after {cuts2:?}"
                );
            }
        }
        Ok(())
    }

    /// An INTERIOR edit near the end of the frame leaves every window that
    /// closed before it untouched: inserting a child at the last position
    /// re-decides only the final window, so every original cut but the last
    /// reappears unchanged. (The last cut's window can extend past it into the
    /// tail, so only it may move — the same "the piece the edit lands in is
    /// dirty" rule the leaf reuse follows.)
    #[dialog_common::test]
    async fn it_keeps_earlier_index_cuts_stable_under_a_tail_insert() -> Result<()> {
        let manifest = Manifest {
            max_segment: 512,
            frame_ceiling_factor: 3,
            anchor_selector: 1,
            ..Manifest::default()
        };
        let ceiling = manifest.frame_ceiling();
        let mut rng = test_rng();
        let mut exercised = 0usize;

        for _ in 0..400 {
            let count = rng.gen_range(30..70usize);
            let separators: Vec<Vec<u8>> = (0..count)
                .map(|i| format!("s{i:05}").into_bytes())
                .collect();
            let weights: Vec<usize> = (0..count).map(|_| rng.gen_range(20..600usize)).collect();
            let refs: Vec<&[u8]> = separators.iter().map(Vec::as_slice).collect();
            let cuts = cap::index_frame_cut_positions(&refs, &weights, ceiling, &manifest);
            if cuts.len() < 2 {
                continue;
            }
            exercised += 1;

            // Insert a child at the very last slot (before the final child).
            // Every window except the frame's tail closed strictly earlier, so
            // all cuts but the last must reappear unchanged.
            let mut sep2 = separators.clone();
            let mut w2 = weights.clone();
            sep2.insert(count - 1, b"s99998tail".to_vec());
            w2.insert(count - 1, rng.gen_range(20..600usize));
            let refs2: Vec<&[u8]> = sep2.iter().map(Vec::as_slice).collect();
            let cuts2 = cap::index_frame_cut_positions(&refs2, &w2, ceiling, &manifest);

            for &cut in cuts.iter().rev().skip(1) {
                assert!(
                    cuts2.contains(&cut),
                    "a tail insert moved the earlier cut {cut}: \
                     before {cuts:?} after {cuts2:?}"
                );
            }
        }
        assert!(exercised > 0, "the fixture must exercise multi-cut frames");
        Ok(())
    }

    /// [`cap::index_frame_cut_positions`] is a pure function of its inputs and
    /// keeps every piece within the ceiling: the same frame always cuts the
    /// same way, and no piece between consecutive cuts (nor the head or tail)
    /// outweighs the ceiling once a qualifying seam exists.
    #[dialog_common::test]
    async fn it_bounds_index_pieces_within_the_ceiling() -> Result<()> {
        let manifest = Manifest {
            max_segment: 512,
            frame_ceiling_factor: 3,
            anchor_selector: 1,
            ..Manifest::default()
        };
        let ceiling = manifest.frame_ceiling();
        let mut rng = test_rng();

        for _ in 0..200 {
            let count = rng.gen_range(20..80usize);
            let separators: Vec<Vec<u8>> = (0..count)
                .map(|i| format!("s{i:05}").into_bytes())
                .collect();
            let weights: Vec<usize> = (0..count).map(|_| rng.gen_range(20..600usize)).collect();
            let refs: Vec<&[u8]> = separators.iter().map(Vec::as_slice).collect();

            let cuts = cap::index_frame_cut_positions(&refs, &weights, ceiling, &manifest);
            assert_eq!(
                cuts,
                cap::index_frame_cut_positions(&refs, &weights, ceiling, &manifest),
                "the placement must be a pure function of its inputs"
            );

            // Each piece between consecutive cuts (with a candidate seam
            // available at its right edge) fits the ceiling plus one link of
            // slack — the greedy closes a window before it overflows.
            let bounds: Vec<usize> = std::iter::once(0)
                .chain(cuts.iter().copied())
                .chain(std::iter::once(count))
                .collect();
            let slack = weights.iter().copied().max().unwrap_or(0);
            for pair in bounds.windows(2) {
                let [lo, hi] = pair else { continue };
                let weight: usize = weights[*lo..*hi].iter().sum();
                assert!(
                    weight <= ceiling + slack,
                    "index piece [{lo},{hi}) weighs {weight}, over ceiling {ceiling} + {slack}"
                );
            }
        }
        Ok(())
    }

    /// The same hash must always produce the same rank, and only the first
    /// 8 bytes of the hash participate.
    #[dialog_common::test]
    async fn it_is_deterministic_and_uses_only_the_prefix() -> Result<()> {
        let mut rng = test_rng();
        for _ in 0..1000 {
            let mut bytes = [0u8; 32];
            rng.fill(&mut bytes);
            let hash = Blake3Hash::from(bytes);

            let rank = compute_geometric_rank(&hash, 254);
            assert_eq!(rank, compute_geometric_rank(&hash, 254));

            let mut tail_mutated = bytes;
            tail_mutated[8..].fill(0xAB);
            assert_eq!(
                rank,
                compute_geometric_rank(&Blake3Hash::from(tail_mutated), 254)
            );
        }

        Ok(())
    }
}
