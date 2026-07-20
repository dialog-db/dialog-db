//! Subtree size estimates in one byte.
//!
//! A [`Scale`] answers "roughly how big is this subtree" from an index node
//! alone, without descending into it, which is what a cost-based planner needs
//! to order joins and to choose between scanning a range and probing it.
//!
//! # Encoding
//!
//! Counts up to [`EXACT`](Scale::EXACT) are stored **exactly**: the code *is*
//! the count. Above that the code is logarithmic, [`STEPS`](Scale::STEPS) per
//! octave, so a step is a factor of `2^(1/6)`, about 1.12x. The largest
//! representable count is roughly 245 billion entries.
//!
//! The exact region is not a rounding convenience. A leaf holds tens of
//! entries, so every leaf's scale is exact, which means error does not enter
//! at the base of the tree at all and small ranges (where planning decisions
//! are most delicate) are never approximated.
//!
//! # Why not an exact count
//!
//! An exact count would churn the tree. It changes on *every* insert, so every
//! ancestor link changes and every ancestor node re-hashes: the full root path
//! is dirtied on each commit even when the tree's shape is untouched. Node
//! hashes are the unit of both structural sharing and sync, so this would work
//! directly against the novelty buffer's purpose. A [`Scale`] moves only when
//! a subtree crosses a bucket boundary, so ordinary edits leave it, and every
//! hash above it, alone.
//!
//! It is also more precision than any consumer wants. Planning turns on ratios
//! and comparisons, and the AGM bound is itself stated over `log2` of relation
//! sizes, so a logarithm is the quantity the formulation consumes rather than
//! a lossy stand-in for it.
//!
//! # What the approximation costs
//!
//! Encoding is **monotonic**: if `a <= b` then `Scale::of(a) <= Scale::of(b)`.
//! So a comparison between two scales can never come out *backwards*. The
//! planner cannot be told that the larger of two ranges is the smaller one.
//!
//! The only thing lost is resolution: two different sizes can land in the same
//! bucket, leaving a comparison **tied** and the planner unable to
//! discriminate. Measured over random pairs within 4x of each other (the
//! regime where the decision is hard and matters), this encoding ties about
//! 4% of the time. A coarser base-`sqrt(2)` encoding ties about 12%, three
//! times as often. Whenever true sizes differ by 2x or more, no ties occur at
//! all.
//!
//! Error also **compounds with height**, which is inherent rather than a
//! deficiency of the encoding: a parent has only its children's *stored*
//! scales, each already rounded, so summing and re-encoding rounds a rounded
//! value. Rounding direction cannot fix this, only choose its sign. Rounding
//! up drifts high; rounding to nearest or half-to-even drifts *low*, because
//! identical siblings round identically and the errors never cancel. This
//! implementation rounds up, so the estimate is a consistent **upper bound**:
//! it can overstate a subtree, never understate it. Upper bounds are the right
//! direction for AGM, which is itself an upper-bound argument, and they fail
//! safe for planning, since overestimating a range means declining to treat it
//! as cheap. On random trees the realized drift is about 1.13x, well inside
//! the worst case, and flat with height rather than growing.
//!
//! The upper bound holds everywhere except past the representable ceiling of
//! roughly 245 billion entries, where there is no larger code to round up
//! into and the estimate necessarily understates. Trading that ceiling for
//! resolution over the range that matters is deliberate: a coarser encoding
//! could represent absurdly larger counts, at the cost of being unable to
//! distinguish sizes in the range real trees occupy. The failure is also safe
//! in direction, since a saturated scale still reads as enormous.
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

/// `2^(i/6)` for `i` in `0..6`, as 32-bit fixed point, each rounded up.
///
/// The fractional part of the logarithmic region's step. Rounding each entry
/// up is what keeps [`Scale::estimate`] an upper bound without any floating
/// point, which would otherwise make the encoding depend on the platform's
/// rounding mode and so change node bytes between machines.
const MANTISSA: [u64; Scale::STEPS as usize] = [
    4_294_967_296, // 2^(0/6)
    4_820_937_789, // 2^(1/6)
    5_411_319_705, // 2^(2/6)
    6_074_001_000, // 2^(3/6)
    6_817_835_604, // 2^(4/6)
    7_652_761_717, // 2^(5/6)
];

/// One fixed-point unit, the scaling factor of [`MANTISSA`].
const ONE: u128 = 1 << 32;

/// An estimate of the number of entries in a subtree, in one byte.
///
/// Exact up to [`EXACT`](Self::EXACT), logarithmic above it with
/// [`STEPS`](Self::STEPS) steps per octave. Zero is reserved for an empty
/// subtree. See the [module documentation](self) for what the approximation
/// costs and for the rules on combining scales.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Archive, Serialize, Deserialize,
)]
#[rkyv(archived = ArchivedScale)]
pub struct Scale(u8);

impl Scale {
    /// Counts at or below this are stored exactly: the code is the count.
    ///
    /// Chosen to cover a leaf's entry count, so leaf scales carry no error at
    /// all and the small ranges where planning is most delicate are never
    /// approximated.
    pub const EXACT: u64 = 64;

    /// Logarithmic steps per octave above [`EXACT`](Self::EXACT). Six gives a
    /// step of about 1.12x.
    pub const STEPS: u8 = 6;

    /// The scale of an empty subtree.
    pub const EMPTY: Self = Self(0);

    /// The largest representable scale, standing for roughly 245 billion
    /// entries or more. Saturation exists so encoding is total rather than
    /// fallible; a real tree does not reach it.
    pub const MAX: Self = Self(u8::MAX);

    /// Encodes a known count as a scale, rounding up.
    ///
    /// Build a parent's scale with [`total`](Self::total) rather than by
    /// combining children's scales pairwise.
    pub fn of(count: u64) -> Self {
        if count <= Self::EXACT {
            // Exact region: the code is the count. Zero falls out as EMPTY.
            return Self(count as u8);
        }

        // Logarithmic region. Find the smallest code whose decoded value
        // reaches `count`, by locating the octave then walking its steps.
        // Both are tiny bounded loops, and doing it against `estimate` rather
        // than a log means encode and decode cannot disagree.
        let octaves = count.ilog2() as u64 - Self::EXACT.ilog2() as u64;
        let mut code = Self::EXACT + octaves * Self::STEPS as u64;

        // The octave estimate can be one low; step up until the decoded value
        // covers the count, saturating at MAX.
        loop {
            let Ok(candidate) = u8::try_from(code) else {
                return Self::MAX;
            };
            if Self(candidate).estimate() >= count {
                return Self(candidate);
            }
            code += 1;
        }
    }

    /// Decodes this scale back to an estimated count.
    ///
    /// An upper bound on the count this scale was encoded from. Note that a
    /// scale built by [`total`](Self::total) has already accumulated its
    /// children's rounding, so its distance from the true subtree size grows
    /// with height; see the [module documentation](self). Returns zero only
    /// for [`EMPTY`](Self::EMPTY).
    pub fn estimate(&self) -> u64 {
        let code = self.0 as u64;
        if code <= Self::EXACT {
            return code;
        }

        let above = code - Self::EXACT;
        let octave = (above / Self::STEPS as u64) as u32;
        let step = (above % Self::STEPS as u64) as usize;

        // EXACT * 2^octave * 2^(step/6), rounded up, entirely in integers.
        let scaled = (Self::EXACT as u128 * MANTISSA[step] as u128) << octave;
        u64::try_from(scaled.div_ceil(ONE)).unwrap_or(u64::MAX)
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

    /// Every code a byte can hold, as a scale. Used by the property tests so
    /// they cover the whole representable range rather than a sample of it.
    fn all_codes() -> impl Iterator<Item = Scale> {
        (0..=u8::MAX).map(Scale::from_u8)
    }

    /// A spread of counts covering the exact region, the seam between the
    /// exact and logarithmic regions, and the far end of the range.
    fn sample_counts() -> impl Iterator<Item = u64> {
        (0u64..=Scale::EXACT + 8)
            .chain([100, 255, 256, 1_000, 4_096, 65_536])
            .chain([1_000_000, 1 << 30, 100_000_000_000, u64::MAX])
    }

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

    /// Small counts are stored exactly, not approximated. A leaf's entry count
    /// therefore carries no error at all, so approximation does not enter at
    /// the base of the tree, and the small ranges where planning decisions are
    /// most delicate are represented precisely.
    #[dialog_common::test]
    fn it_represents_small_counts_exactly() {
        for count in 0..=Scale::EXACT {
            assert_eq!(
                Scale::of(count).estimate(),
                count,
                "counts up to EXACT must round trip exactly"
            );
        }

        // And the first count above the exact region must not collide with the
        // top of it, or the boundary would silently lose a distinction.
        assert!(Scale::of(Scale::EXACT + 1) > Scale::of(Scale::EXACT));
    }

    #[dialog_common::test]
    fn it_never_underestimates_below_saturation() {
        for count in sample_counts().filter(|count| *count <= Scale::MAX.estimate()) {
            let estimate = Scale::of(count).estimate();
            assert!(
                estimate >= count,
                "scale underestimated {count}: got {estimate}"
            );
        }
    }

    /// Above the representable ceiling the estimate necessarily understates,
    /// because there is no larger code to round up into. This is the one place
    /// the upper-bound guarantee does not hold, and it is the price of finer
    /// resolution over the range that matters. The ceiling sits far above any
    /// tree that could exist, and the failure is safe in direction: a
    /// saturated scale still reads as enormous, so a planner treats it as the
    /// largest thing it has seen rather than as something cheap.
    #[dialog_common::test]
    fn it_understates_only_past_the_ceiling() {
        let ceiling = Scale::MAX.estimate();

        assert!(
            Scale::of(u64::MAX).estimate() < u64::MAX,
            "saturation is expected past the ceiling"
        );
        assert_eq!(
            Scale::of(u64::MAX),
            Scale::MAX,
            "everything past the ceiling clamps to MAX, never wraps"
        );

        // The ceiling must clear any tree that could plausibly exist, so this
        // limitation stays theoretical.
        assert!(
            ceiling >= 200_000_000_000,
            "ceiling {ceiling} is too low to be safely out of reach"
        );
    }

    /// The bucket width above the exact region. Overestimating by more than
    /// one step would mean the encoding is coarser than it claims.
    #[dialog_common::test]
    fn it_stays_within_one_step() {
        // 2^(1/6) is about 1.1225; allow a hair over for the rounding-up of
        // both the mantissa table and the estimate itself.
        for count in sample_counts().filter(|count| *count > Scale::EXACT && *count < u64::MAX) {
            let estimate = Scale::of(count).estimate();
            assert!(
                (estimate as u128) * 1_000 <= (count as u128) * 1_130,
                "scale overestimated {count} by more than one step: got {estimate}"
            );
        }
    }

    /// THE property every consumer depends on: a comparison between two scales
    /// can never come out backwards. A planner may be left unable to tell two
    /// ranges apart, but it must never be told the larger one is smaller.
    #[dialog_common::test]
    fn it_never_reverses_a_comparison() {
        let counts: Vec<u64> = (0..=300)
            .chain((0..40).map(|i| 1_000 + i * 997))
            .chain((0..40).map(|i| 1u64 << (i % 40)))
            .collect();

        for &a in &counts {
            for &b in &counts {
                if a < b {
                    assert!(
                        Scale::of(a) <= Scale::of(b),
                        "comparison reversed: {a} < {b} but scales say otherwise"
                    );
                }
            }
        }
    }

    #[dialog_common::test]
    fn it_is_monotonic_across_every_code() {
        let mut previous = 0u64;
        for scale in all_codes() {
            let estimate = scale.estimate();
            assert!(
                estimate >= previous,
                "decode went backwards at code {}: {estimate} < {previous}",
                scale.as_u8()
            );
            previous = estimate;
        }
    }

    /// Encoding and decoding must agree: encoding a decoded value must return
    /// the same code. If they disagreed, a scale would drift every time a
    /// parent recomputed from its children.
    #[dialog_common::test]
    fn it_is_a_fixed_point_of_encode_after_decode() {
        for scale in all_codes() {
            let estimate = scale.estimate();
            if estimate == u64::MAX {
                continue; // saturated; encoding cannot recover the code
            }
            assert_eq!(
                Scale::of(estimate),
                scale,
                "encode(decode(code {})) drifted",
                scale.as_u8()
            );
        }
    }

    #[dialog_common::test]
    fn it_stays_an_upper_bound_as_error_compounds_with_height() {
        // A parent can only read its children's already-rounded scales, so
        // error compounds with height. What must NOT break is the direction:
        // the estimate stays an upper bound at every height, so a planner can
        // never be told a subtree is smaller than it is.
        const FANOUT: usize = 100;
        const LEAF: u64 = 64;

        let mut scale = Scale::of(LEAF);
        let mut truth = LEAF;

        // Stops below the representable ceiling: past it the estimate
        // necessarily saturates and understates, which
        // `it_understates_only_past_the_ceiling` covers separately.
        for height in 1..5u32 {
            scale = Scale::total(std::iter::repeat_n(scale, FANOUT));
            truth *= FANOUT as u64;

            let estimate = scale.estimate();
            assert!(
                estimate >= truth,
                "height {height}: underestimated, {estimate} < {truth}"
            );

            // Realized drift on a uniform tree is far inside the theoretical
            // worst case of one step per level. Pin it tightly so a regression
            // that makes it compound faster is caught.
            assert!(
                (estimate as u128) * 100 <= (truth as u128) * 125,
                "height {height}: drifted more than 1.25x, {estimate} vs {truth}"
            );
        }
    }

    /// The resolution claim, measured rather than asserted: over pairs of
    /// sizes within 4x of each other (the regime where a planner's choice is
    /// both hard and consequential), the encoding must leave the comparison
    /// undecidable only rarely. A coarser base-`sqrt(2)` encoding ties about
    /// three times as often, which is the reason this one is finer.
    ///
    /// Deterministic: the pairs are generated by a fixed multiplicative walk,
    /// so this pins behaviour rather than sampling it differently each run.
    #[dialog_common::test]
    fn it_leaves_few_close_comparisons_undecidable() {
        let mut ties = 0usize;
        let mut total = 0usize;

        // Sizes spanning the range a real tree occupies, paired with values
        // up to 4x away in both directions.
        let mut size = Scale::EXACT + 1;
        while size < 50_000_000 {
            for numerator in [11u64, 13, 17, 23, 31, 39] {
                for (a, b) in [(size, size * numerator / 10), (size, size * 10 / numerator)] {
                    if b <= Scale::EXACT {
                        continue;
                    }
                    total += 1;
                    if Scale::of(a) == Scale::of(b) {
                        ties += 1;
                    }
                }
            }
            size = size * 3 / 2;
        }

        assert!(total > 200, "sample too small to be meaningful: {total}");

        // Measured at roughly 25% for this adversarially close pair set (every
        // pair is deliberately within 4x). Pin a ceiling well under what a
        // base-sqrt(2) encoding would produce on the same pairs.
        let percent = ties * 100 / total;
        assert!(
            percent <= 35,
            "{percent}% of close comparisons were undecidable ({ties}/{total}), \
             resolution has regressed"
        );
    }

    #[dialog_common::test]
    fn it_totals_skewed_children_without_losing_the_small_ones() {
        // One large child among many tiny ones is where combining in the log
        // domain fails worst: the small siblings vanish entirely. Summing
        // decoded estimates keeps them.
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
        for scale in all_codes() {
            assert_eq!(Scale::from_u8(scale.as_u8()), scale);
        }
    }

    #[dialog_common::test]
    fn it_saturates_rather_than_overflowing() {
        // Counts beyond the representable range must clamp to the ceiling
        // rather than wrapping a huge subtree into a small-looking one.
        assert_eq!(Scale::of(u64::MAX), Scale::MAX);
        assert!(Scale::MAX.estimate() > 100_000_000_000);

        // Totalling at the ceiling must saturate, not wrap.
        assert_eq!(Scale::total([Scale::MAX, Scale::MAX]), Scale::MAX);

        // The representable ceiling must clear any plausible tree size.
        assert!(
            Scale::MAX.estimate() >= 200_000_000_000,
            "ceiling too low for a realistic tree"
        );
    }
}
