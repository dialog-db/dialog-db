//! Logarithmic subtree size estimates.
//!
//! A [`Scale`] is a one-byte, base-`sqrt(2)` logarithm of an entry count. It
//! answers "roughly how big is this subtree" from an index node alone, without
//! descending into it, which is what a cost-based planner needs to order joins
//! and to choose between scanning a range and probing it.
//!
//! # Why logarithmic
//!
//! An exact count would be a poor fit here for two reasons beyond its size.
//!
//! It would churn the tree. An exact count changes on *every* insert, so every
//! ancestor link changes, so every ancestor node re-hashes. Node hashes are the
//! unit of both structural sharing and sync, so an exact count would dirty the
//! full root path on every commit even when the tree's shape was untouched. A
//! [`Scale`] only changes when a subtree's size crosses a `sqrt(2)` boundary,
//! so the overwhelming majority of edits leave it, and therefore every ancestor
//! hash, alone.
//!
//! It would also be more precision than any consumer wants. Planning decisions
//! turn on ratios and comparisons: which of two ranges is smaller, whether one
//! side of a join is orders of magnitude smaller than the other. The AGM bound
//! is itself stated over `log2` of relation sizes, so a logarithm is the
//! quantity the formulation consumes rather than a lossy stand-in for it.
//!
//! # Precision and error
//!
//! Steps are half-exponents (`sqrt(2)` apart, not `2`), so encoding a known
//! count is accurate to a factor of `sqrt(2)`, never below it.
//!
//! Error does, however, **compound with height**, and that is inherent rather
//! than a deficiency of this encoding. A parent has only its children's
//! *stored* scales to work with, each already rounded, so summing them and
//! re-encoding rounds a rounded value. The worst case is a factor of
//! `sqrt(2)^height`: about 2x at height two, 8x at height six.
//!
//! Rounding direction cannot fix this, only choose its sign. Rounding up
//! drifts high (~3.4x by height six on a fanout-100 tree); rounding to nearest
//! or half-to-even drifts *low* (~0.55x) as the same rounding is applied to
//! every identical sibling and never cancels. This implementation rounds up,
//! so the estimate is a consistent **upper bound**: it can overstate a
//! subtree, never understate it. Upper bounds are the right direction for AGM,
//! which is itself an upper-bound argument, and they fail safe for planning,
//! since overestimating a range means declining to treat it as cheap.
//!
//! Order-of-magnitude comparison, which is all any consumer needs, survives
//! this comfortably: both sides of a comparison are biased the same way, and
//! sibling subtrees at equal height carry equal bias.
//!
//! # Advisory only
//!
//! A [`Scale`] is a hint for planning, never a fact to act on. It is an upper
//! bound, it excludes ops still pending in a node's novelty buffer (which are
//! not yet in the subtree they are destined for), and a tree written by an
//! older or buggy implementation may carry stale values. Never use it to skip
//! work: not to decide a range is empty, not to terminate a scan early, and not
//! to size an allocation that must fit. Wrong scales must only ever cost a
//! worse query plan, never a wrong answer.

use rkyv::{Archive, Deserialize, Serialize};

/// A logarithmic estimate of the number of entries in a subtree.
///
/// Encoded as `ceil(log2(n) * 2) + 1`, with zero reserved for an empty
/// subtree, so one byte covers every count a tree could hold. See the
/// [module documentation](self) for the precision guarantees and for the
/// rules on combining scales.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Archive, Serialize, Deserialize,
)]
#[rkyv(archived = ArchivedScale)]
pub struct Scale(u8);

impl Scale {
    /// The scale of an empty subtree.
    pub const EMPTY: Self = Self(0);

    /// The largest representable scale, standing for any count at or above
    /// `2^127`. A tree can never hold this many entries; the saturation exists
    /// so encoding is total rather than fallible.
    pub const MAX: Self = Self(u8::MAX);

    /// Encodes an exact count as a scale, rounding up.
    ///
    /// This is the only rounding point in the whole scheme. Build a parent's
    /// scale with [`total`](Self::total), which sums exactly and encodes once,
    /// rather than by combining children's scales pairwise.
    pub fn of(count: u64) -> Self {
        if count == 0 {
            return Self::EMPTY;
        }

        // `doubled` is the smallest d with 2^(d/2) >= count, i.e.
        // ceil(log2(count) * 2). Squaring both sides keeps this in integers:
        // 2^(d/2) >= count  <=>  2^d >= count^2. Squaring a u64 needs u128,
        // and `next_power_of_two` on a u128 above 2^127 would overflow, so
        // rounding up to the next power of two is done via bit length instead.
        let squared = (count as u128) * (count as u128);
        let doubled = if squared.is_power_of_two() {
            squared.trailing_zeros()
        } else {
            u128::BITS - squared.leading_zeros()
        };

        // Codes are the doubled log plus one, with zero reserved for empty, so
        // saturate at MAX rather than wrapping a huge subtree into a small one.
        // `doubled` can reach u32::MAX's neighbourhood only via the u128 bit
        // length above, so widen before adding.
        Self(u8::try_from(doubled as u64 + 1).unwrap_or(u8::MAX))
    }

    /// Decodes this scale back to an estimated count.
    ///
    /// An upper bound on the count this scale was encoded from, within a
    /// factor of `sqrt(2)`. Note that a scale built by [`total`](Self::total)
    /// has already accumulated its children's rounding, so its distance from
    /// the true subtree size grows with height; see the [module
    /// documentation](self). Returns zero only for [`EMPTY`](Self::EMPTY).
    pub fn estimate(&self) -> u64 {
        if self.0 == 0 {
            return 0;
        }

        let doubled = (self.0 - 1) as u32;
        let whole = doubled / 2;

        // Saturate rather than wrap: codes above 63 name counts no u64 can
        // hold, and a wrapped estimate would read as a tiny subtree.
        if whole >= 63 {
            return u64::MAX;
        }
        let base = 1u64 << whole;

        if doubled.is_multiple_of(2) {
            base
        } else {
            // base * sqrt(2), rounded up, in integers. sqrt(2) as a 32-bit
            // fixed-point fraction; the product needs u128 headroom.
            let scaled = (base as u128 * 6_074_001_000u128).div_ceil(4_294_967_296u128);
            u64::try_from(scaled).unwrap_or(u64::MAX)
        }
    }

    /// Builds a parent's scale from its children's.
    ///
    /// Sums the children's decoded estimates in `u64` and encodes the total
    /// once. Because each child's estimate is itself already rounded up, the
    /// result inherits their error and adds its own: see the [module
    /// documentation](self) for the `sqrt(2)^height` bound and why no rounding
    /// rule avoids it.
    pub fn total<I>(children: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        let sum = children
            .into_iter()
            .fold(0u64, |acc, scale| acc.saturating_add(scale.estimate()));
        Self::of(sum)
    }

    /// Whether this scale describes an empty subtree.
    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// The raw encoded byte. For serialization and tests; prefer
    /// [`estimate`](Self::estimate) when reasoning about size.
    pub fn as_u8(&self) -> u8 {
        self.0
    }

    /// Rebuilds a scale from its raw encoded byte.
    pub fn from_u8(encoded: u8) -> Self {
        Self(encoded)
    }
}

impl ArchivedScale {
    /// Reads this archived scale back into an owned [`Scale`].
    pub fn get(&self) -> Scale {
        Scale(self.0)
    }
}

impl From<&ArchivedScale> for Scale {
    fn from(archived: &ArchivedScale) -> Self {
        archived.get()
    }
}

#[cfg(test)]
mod tests {
    use super::Scale;

    #[dialog_common::test]
    fn it_reserves_zero_for_empty() {
        assert_eq!(Scale::of(0), Scale::EMPTY);
        assert!(Scale::of(0).is_empty());
        assert_eq!(Scale::of(0).estimate(), 0);

        // One must be distinguishable from empty, or "is this subtree empty"
        // and "does it hold a single entry" become the same question.
        assert_ne!(Scale::of(1), Scale::EMPTY);
        assert!(!Scale::of(1).is_empty());
        assert_eq!(Scale::of(1).estimate(), 1);
    }

    #[dialog_common::test]
    fn it_never_underestimates() {
        // Includes counts past 2^32, where a naive shift-based decode wraps
        // and reports a huge subtree as a tiny one.
        for count in (1u64..4096).chain([
            1 << 20,
            1 << 30,
            (1u64 << 40) + 7,
            1 << 50,
            (1u64 << 62) + 1,
        ]) {
            let estimate = Scale::of(count).estimate();
            assert!(
                estimate >= count,
                "scale underestimated {count}: got {estimate}"
            );
        }
    }

    #[dialog_common::test]
    fn it_stays_within_sqrt_two() {
        // sqrt(2) as a rational, to avoid floating point in the assertion:
        // estimate / count <= 1.4143 for every count.
        for count in (1u64..4096).chain([12_345, 1 << 20, 1 << 30, (1u64 << 40) + 7]) {
            let estimate = Scale::of(count).estimate();
            assert!(
                (estimate as u128) * 10_000 <= (count as u128) * 14_143,
                "scale overestimated {count} by more than sqrt(2): got {estimate}"
            );
        }
    }

    #[dialog_common::test]
    fn it_keeps_exact_powers_of_two_exact() {
        for exponent in 0..40u32 {
            let count = 1u64 << exponent;
            assert_eq!(
                Scale::of(count).estimate(),
                count,
                "power of two {count} should encode exactly"
            );
        }
    }

    #[dialog_common::test]
    fn it_is_monotonic() {
        let mut previous = Scale::of(1);
        for count in 2u64..8192 {
            let current = Scale::of(count);
            assert!(
                current >= previous,
                "scale went backwards at {count}: {current:?} < {previous:?}"
            );
            previous = current;
        }
    }

    #[dialog_common::test]
    fn it_stays_an_upper_bound_as_error_compounds_with_height() {
        // A parent can only read its children's already-rounded scales, so
        // error compounds by up to sqrt(2) per level. What must NOT break is
        // the direction: the estimate stays an upper bound at every height, so
        // a planner can never be told a subtree is smaller than it is.
        const FANOUT: usize = 100;
        const LEAF: u64 = 64;

        let mut scale = Scale::of(LEAF);
        let mut truth = LEAF;

        for height in 1..6u32 {
            scale = Scale::total(std::iter::repeat_n(scale, FANOUT));
            truth *= FANOUT as u64;

            let estimate = scale.estimate();
            assert!(
                estimate >= truth,
                "height {height}: underestimated, {estimate} < {truth}"
            );

            // Bound the compounding at sqrt(2)^height, the theoretical worst
            // case, so a regression that makes it drift faster is caught.
            let bound = 2f64.powf(0.5 * height as f64) * 1.001;
            assert!(
                estimate as f64 <= truth as f64 * bound,
                "height {height}: drifted past sqrt(2)^{height} = {bound:.3}x, \
                 {estimate} vs {truth}"
            );
        }
    }

    #[dialog_common::test]
    fn it_totals_skewed_children_without_losing_the_small_ones() {
        // One large child among many tiny ones is where log-domain combining
        // fails worst: the small siblings vanish entirely. Summing exactly
        // keeps them.
        let big = Scale::of(1_000_000);
        let small = Scale::of(1);

        let total = Scale::total(std::iter::once(big).chain(std::iter::repeat_n(small, 99)));

        assert!(
            total.estimate() >= 1_000_099,
            "small siblings were lost: {} < 1000099",
            total.estimate()
        );
    }

    #[dialog_common::test]
    fn it_round_trips_through_its_encoded_byte() {
        for count in [0u64, 1, 2, 3, 100, 4096, 1 << 30] {
            let scale = Scale::of(count);
            assert_eq!(Scale::from_u8(scale.as_u8()), scale);
        }
    }

    #[dialog_common::test]
    fn it_saturates_rather_than_overflowing() {
        // The largest count a u64 can express sits near code 129 (2*64+1), far
        // below the u8 ceiling: `Scale::MAX` names 2^127, which no count of
        // entries could ever reach. What matters is that the extremes neither
        // panic nor wrap a huge subtree into a small-looking one.
        let biggest = Scale::of(u64::MAX);
        assert!(biggest.estimate() >= u64::MAX / 2, "wrapped: {biggest:?}");
        assert!(
            biggest < Scale::MAX,
            "u64::MAX should not reach the ceiling"
        );

        // Totalling at the ceiling must saturate, not wrap to a small scale.
        let total = Scale::total([Scale::MAX, Scale::MAX]);
        assert!(
            total >= biggest,
            "totalling maxima must not wrap: {total:?} < {biggest:?}"
        );

        // And the ceiling must survive a decode/encode round trip.
        assert!(Scale::MAX.estimate() == u64::MAX);
    }
}
