//! The key ranges a scoped difference is confined to, sorted and disjoint.

use std::borrow::Cow;
use std::ops::RangeInclusive;

use crate::Key;

/// The ranges a scoped difference keeps, sorted by start and merged where
/// they overlap, so a key or a node's span is tested against the one or two
/// ranges it could meet, found by search, rather than against every range of
/// the scope.
///
/// A subscription's cover holds a range per entity it read, so its scope can
/// hold tens of thousands of ranges while the difference it gates is one
/// changed fact. Every frontier node of every pass, every buffered op and
/// every changed entry is tested against the scope, so a test that walks the
/// ranges costs the number of ranges times all of those: the work of a
/// difference grew with the square of the subscription's answer.
///
/// A scope is either a list of ranges or a union of [`KeyRanges`]. A range
/// whose start sorts after its end holds no key and is left out. A list
/// already sorted and disjoint is borrowed, not copied. A union of sets is
/// borrowed as it is: building it touches none of their ranges, so a scope
/// over a cover of any size costs the same to build and to drop. A key's
/// [`Ord`] agrees with the order of its bytes, so the ranges sorted by key
/// are sorted by bytes, which is what they are tested against.
#[derive(Clone, Debug)]
pub(crate) enum Scope<'s, K: Key> {
    /// Ranges given as a list, sorted and merged.
    Ranges(Cow<'s, [RangeInclusive<K>]>),
    /// The union of sets that are each sorted and disjoint already.
    Sets(&'s [&'s KeyRanges<K>]),
}

impl<'s, K: Key> Scope<'s, K> {
    /// The scope of `ranges`, in any order, overlapping or not.
    pub(crate) fn new(ranges: &'s [RangeInclusive<K>]) -> Self {
        built(ranges.len());
        let ordered = ranges.iter().all(|range| range.start() <= range.end())
            && ranges
                .windows(2)
                .all(|pair| pair[0].end() < pair[1].start());
        if ordered {
            return Scope::Ranges(Cow::Borrowed(ranges));
        }
        let mut sorted: Vec<RangeInclusive<K>> = ranges
            .iter()
            .filter(|range| range.start() <= range.end())
            .cloned()
            .collect();
        sorted.sort_by(|one, other| one.start().cmp(other.start()));
        let mut merged: Vec<RangeInclusive<K>> = Vec::with_capacity(sorted.len());
        for range in sorted {
            match merged.last_mut() {
                Some(last) if range.start() <= last.end() => {
                    if range.end() > last.end() {
                        *last = last.start().clone()..=range.end().clone();
                    }
                }
                _ => merged.push(range),
            }
        }
        Scope::Ranges(Cow::Owned(merged))
    }

    /// The scope of the union of `sets`, borrowed: nothing is copied or
    /// checked, since each set keeps itself sorted and disjoint.
    pub(crate) fn sets(sets: &'s [&'s KeyRanges<K>]) -> Self {
        Scope::Sets(sets)
    }

    /// Whether `key` falls inside a range: inside the last range that starts
    /// at or before it, the only one that can hold it.
    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        match self {
            Scope::Ranges(ranges) => {
                let at = ranges.partition_point(|range| {
                    probed();
                    range.start().as_ref() <= key
                });
                at > 0 && {
                    probed();
                    key <= ranges[at - 1].end().as_ref()
                }
            }
            Scope::Sets(sets) => sets.iter().any(|set| set.contains(key)),
        }
    }

    /// Whether the span from `lower` (inclusive) to `upper` (exclusive, or
    /// open above when `None`) meets a range. The first range that ends at or
    /// after `lower` is the only one to check: every range before it ends
    /// below the span, and every one after it starts after it does.
    pub(crate) fn intersects(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        match self {
            Scope::Ranges(ranges) => {
                let at = ranges.partition_point(|range| {
                    probed();
                    range.end().as_ref() < lower
                });
                ranges.get(at).is_some_and(|range| {
                    probed();
                    upper.is_none_or(|upper| upper > range.start().as_ref())
                })
            }
            Scope::Sets(sets) => sets.iter().any(|set| set.intersects(lower, upper)),
        }
    }
}

/// A set of inclusive key ranges kept sorted and disjoint as ranges are
/// added: a range that overlaps others is merged with them, and one that a
/// range already holds changes nothing. Testing a key or a span is a binary
/// search, as it is over a sorted slice, and a scoped difference over
/// sets ([`differentiate_covered`](crate::PersistentTree::differentiate_covered))
/// borrows them without copying a range.
///
/// The ranges are one sorted list, so a search costs what it does over a
/// slice: a cover is searched once per node, op and entry a difference
/// walks, far more often than it grows. Adding a range that changes the
/// set is a search and a shift of the ranges after it; one the set holds
/// already is a search alone.
///
/// Ranges are compared by the bytes of their keys, which agree with the
/// keys' [`Ord`].
#[derive(Clone, Debug)]
pub struct KeyRanges<K> {
    ranges: Vec<RangeInclusive<K>>,
}

impl<K> Default for KeyRanges<K> {
    fn default() -> Self {
        KeyRanges { ranges: Vec::new() }
    }
}

impl<K: Key> KeyRanges<K> {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `range`, merged with every range it overlaps. Returns whether the
    /// set changed: `false` when a range held already contains it, or when it
    /// holds no key (its start sorts after its end).
    pub fn insert(&mut self, range: RangeInclusive<K>) -> bool {
        if self.contains_range(&range) {
            return false;
        }
        let (start, end) = range.into_inner();
        // The ranges it overlaps: from the first that ends at or after its
        // start to the last that starts at or before its end.
        let from = self
            .ranges
            .partition_point(|held| held.end().as_ref() < start.as_ref());
        let to = self
            .ranges
            .partition_point(|held| held.start().as_ref() <= end.as_ref());
        if from == to {
            self.ranges.insert(from, start..=end);
            return true;
        }
        let start = if self.ranges[from].start().as_ref() < start.as_ref() {
            self.ranges[from].start().clone()
        } else {
            start
        };
        let end = if self.ranges[to - 1].end().as_ref() > end.as_ref() {
            self.ranges[to - 1].end().clone()
        } else {
            end
        };
        self.ranges.splice(from..to, [start..=end]);
        true
    }

    /// Whether a range held contains every key of `range`: whether
    /// [`insert`](Self::insert)ing it would change nothing. A range that
    /// holds no key is contained.
    pub fn contains_range(&self, range: &RangeInclusive<K>) -> bool {
        range.start().as_ref() > range.end().as_ref()
            || self
                .before(range.start().as_ref())
                .is_some_and(|held| held.end().as_ref() >= range.end().as_ref())
    }

    /// Remove every range that lies wholly inside `within`, leaving any that
    /// reaches past it. Returns how many were removed.
    pub fn remove_within(&mut self, within: &RangeInclusive<K>) -> usize {
        let (lower, upper) = (within.start().as_ref(), within.end().as_ref());
        let from = self
            .ranges
            .partition_point(|held| held.start().as_ref() < lower);
        // Disjoint and sorted, so the ranges inside are the run from `from`
        // up to the first that ends past `within`.
        let to = from
            + self.ranges[from..]
                .iter()
                .take_while(|held| held.end().as_ref() <= upper)
                .count();
        self.ranges.drain(from..to);
        to - from
    }

    /// The range that starts at or before `key`: the only one that can hold
    /// it.
    fn before(&self, key: &[u8]) -> Option<&RangeInclusive<K>> {
        let at = self.ranges.partition_point(|held| {
            probed();
            held.start().as_ref() <= key
        });
        at.checked_sub(1).map(|at| &self.ranges[at])
    }

    /// Whether `key` falls inside a range.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.before(key).is_some_and(|held| {
            probed();
            key <= held.end().as_ref()
        })
    }

    /// Whether the span from `lower` (inclusive) to `upper` (exclusive, or
    /// open above when `None`) meets a range: the first range that ends at or
    /// after `lower`, when it starts below `upper`.
    pub fn intersects(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        let at = self.ranges.partition_point(|held| {
            probed();
            held.end().as_ref() < lower
        });
        self.ranges.get(at).is_some_and(|held| {
            probed();
            upper.is_none_or(|upper| upper > held.start().as_ref())
        })
    }

    /// The ranges, in order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &K)> {
        self.ranges.iter().map(|range| (range.start(), range.end()))
    }

    /// How many ranges the set holds.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Whether the set holds no range.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

/// Count one comparison of a key or a span against a scope range, for the
/// tests that pin how the work of a scoped difference grows with its scope.
#[inline(always)]
pub(crate) fn probed() {
    #[cfg(any(test, feature = "helpers"))]
    probes::COUNT.with(|count| count.set(count.get() + 1));
}

/// Count `ranges` ranges a scope was built from: each is checked for order,
/// and copied when it is not, so building a scope from a list costs its
/// length however few of its ranges the difference then meets.
#[inline(always)]
fn built(ranges: usize) {
    #[cfg(any(test, feature = "helpers"))]
    probes::BUILT.with(|count| count.set(count.get() + ranges));
    #[cfg(not(any(test, feature = "helpers")))]
    let _ = ranges;
}

/// How many scope ranges this thread has compared a key or a span against,
/// and how many it has built scopes from. Per thread, so tests running side
/// by side do not see each other's work.
#[cfg(any(test, feature = "helpers"))]
pub(crate) mod probes {
    use std::cell::Cell;

    thread_local! {
        pub(crate) static COUNT: Cell<usize> = const { Cell::new(0) };
        pub(crate) static BUILT: Cell<usize> = const { Cell::new(0) };
    }

    /// The comparisons so far on this thread.
    pub(crate) fn count() -> usize {
        COUNT.with(Cell::get)
    }

    /// The ranges scopes were built from so far on this thread.
    pub(crate) fn built() -> usize {
        BUILT.with(Cell::get)
    }
}

#[cfg(test)]
mod tests {
    use super::{KeyRanges, Scope};

    fn key(n: u32) -> [u8; 4] {
        n.to_be_bytes()
    }

    /// A scope answers as the plain list of its ranges does, whatever order
    /// and overlap the ranges come in, including ranges that hold nothing.
    #[dialog_common::test]
    fn it_answers_as_its_ranges_do() {
        let raw = vec![
            key(40)..=key(50),
            key(10)..=key(20),
            key(15)..=key(30),
            key(60)..=key(60),
            key(90)..=key(80),
            key(45)..=key(55),
        ];
        let scope = Scope::new(&raw);
        let plain_contains = |at: u32| {
            raw.iter()
                .any(|range| range.start() <= range.end() && range.contains(&key(at)))
        };
        for at in 0..100u32 {
            assert_eq!(scope.contains(&key(at)), plain_contains(at), "key {at}");
        }
        for lower in 0..100u32 {
            let plain = |upper: Option<u32>| {
                raw.iter().any(|range| {
                    range.start() <= range.end()
                        && key(lower) <= *range.end()
                        && upper.is_none_or(|upper| key(upper) > *range.start())
                })
            };
            assert_eq!(
                scope.intersects(&key(lower), None),
                plain(None),
                "span {lower}.."
            );
            for upper in lower + 1..=100u32 {
                assert_eq!(
                    scope.intersects(&key(lower), Some(&key(upper))),
                    plain(Some(upper)),
                    "span {lower}..{upper}"
                );
            }
        }
    }

    /// A set answers as a scope over the same ranges does, whatever order
    /// and overlap they are added in; a range it holds already changes
    /// nothing, and removing the ranges inside a span leaves the ones that
    /// reach past it.
    #[dialog_common::test]
    fn it_keeps_a_set_sorted_and_disjoint_as_ranges_are_added() {
        let raw = vec![
            key(40)..=key(50),
            key(10)..=key(20),
            key(15)..=key(30),
            key(60)..=key(60),
            key(90)..=key(80),
            key(45)..=key(55),
            key(70)..=key(72),
        ];
        let mut set = KeyRanges::new();
        for range in &raw {
            set.insert(range.clone());
        }
        assert!(
            !set.insert(key(41)..=key(54)),
            "a held range changes nothing"
        );
        assert!(set.contains_range(&(key(11)..=key(29))));
        assert!(!set.contains_range(&(key(29)..=key(41))));
        let scope = Scope::new(&raw);
        let held: Vec<_> = set.iter().map(|(start, end)| *start..=*end).collect();
        assert_eq!(
            held,
            vec![
                key(10)..=key(30),
                key(40)..=key(55),
                key(60)..=key(60),
                key(70)..=key(72)
            ]
        );
        let sets = [&set];
        let union = Scope::sets(&sets);
        for at in 0..100u32 {
            assert_eq!(
                union.contains(&key(at)),
                scope.contains(&key(at)),
                "key {at}"
            );
        }
        for lower in 0..100u32 {
            assert_eq!(
                union.intersects(&key(lower), None),
                scope.intersects(&key(lower), None),
                "span {lower}.."
            );
            for upper in lower + 1..=100u32 {
                assert_eq!(
                    union.intersects(&key(lower), Some(&key(upper))),
                    scope.intersects(&key(lower), Some(&key(upper))),
                    "span {lower}..{upper}"
                );
            }
        }
        assert_eq!(set.remove_within(&(key(35)..=key(65))), 2);
        let held: Vec<_> = set.iter().map(|(start, end)| *start..=*end).collect();
        assert_eq!(held, vec![key(10)..=key(30), key(70)..=key(72)]);
        assert_eq!(set.remove_within(&(key(20)..=key(80))), 1);
        assert_eq!(set.len(), 1, "a range reaching past the span stays");
    }
}
