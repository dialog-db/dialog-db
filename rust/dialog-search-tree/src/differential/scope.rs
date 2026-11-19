//! The key ranges a scoped difference is confined to, sorted and disjoint.

use std::borrow::Cow;
use std::ops::RangeInclusive;

use crate::Key;

/// The ranges a scoped difference keeps, sorted by start and merged where
/// they overlap, so a key or a node's span is tested against the one or two
/// ranges it could meet, found by binary search, rather than against every
/// range of the scope.
///
/// A subscription's cover holds a range per entity it read, so its scope can
/// hold tens of thousands of ranges while the difference it gates is one
/// changed fact. Every frontier node of every pass, every buffered op and
/// every changed entry is tested against the scope, so a test that walks the
/// ranges costs the number of ranges times all of those: the work of a
/// difference grew with the square of the subscription's answer.
///
/// A range whose start sorts after its end holds no key and is left out. A
/// scope already sorted and disjoint (as a cover is) is borrowed, not
/// copied. A key's [`Ord`] agrees with the order of its bytes, so the
/// ranges sorted by key are sorted by bytes, which is what they are tested
/// against.
#[derive(Clone, Debug)]
pub(crate) struct Scope<'s, K: Key> {
    ranges: Cow<'s, [RangeInclusive<K>]>,
}

impl<'s, K: Key> Scope<'s, K> {
    /// The scope of `ranges`, in any order, overlapping or not.
    pub(crate) fn new(ranges: &'s [RangeInclusive<K>]) -> Self {
        let ordered = ranges.iter().all(|range| range.start() <= range.end())
            && ranges
                .windows(2)
                .all(|pair| pair[0].end() < pair[1].start());
        if ordered {
            return Scope {
                ranges: Cow::Borrowed(ranges),
            };
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
        Scope {
            ranges: Cow::Owned(merged),
        }
    }

    /// Whether `key` falls inside a range: inside the last range that starts
    /// at or before it, the only one that can hold it.
    pub(crate) fn contains(&self, key: &[u8]) -> bool {
        let at = self.ranges.partition_point(|range| {
            probed();
            range.start().as_ref() <= key
        });
        at > 0 && {
            probed();
            key <= self.ranges[at - 1].end().as_ref()
        }
    }

    /// Whether the span from `lower` (inclusive) to `upper` (exclusive, or
    /// open above when `None`) meets a range. The first range that ends at or
    /// after `lower` is the only one to check: every range before it ends
    /// below the span, and every one after it starts after it does.
    pub(crate) fn intersects(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        let at = self.ranges.partition_point(|range| {
            probed();
            range.end().as_ref() < lower
        });
        self.ranges.get(at).is_some_and(|range| {
            probed();
            upper.is_none_or(|upper| upper > range.start().as_ref())
        })
    }
}

/// Count one comparison of a key or a span against a scope range, for the
/// tests that pin how the work of a scoped difference grows with its scope.
#[inline(always)]
pub(crate) fn probed() {
    #[cfg(any(test, feature = "helpers"))]
    probes::COUNT.with(|count| count.set(count.get() + 1));
}

/// How many scope ranges this thread has compared a key or a span against.
/// Per thread, so tests running side by side do not see each other's work.
#[cfg(any(test, feature = "helpers"))]
pub(crate) mod probes {
    use std::cell::Cell;

    thread_local! {
        pub(crate) static COUNT: Cell<usize> = const { Cell::new(0) };
    }

    /// The comparisons so far on this thread.
    pub(crate) fn count() -> usize {
        COUNT.with(Cell::get)
    }
}

#[cfg(test)]
mod tests {
    use super::Scope;

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
}
