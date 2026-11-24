//! An ordered map held in bounded chunks, each shared by `Arc`.
//!
//! A clone shares every chunk, so it costs one reference count
//! whatever the map holds. A write to a map no clone shares changes it
//! in place, as a plain [`BTreeMap`] would. A write to a shared map
//! copies the list of chunks (one pointer each) and the one chunk it
//! changes (at most [`MAX`] entries), and leaves every other chunk
//! shared, so a clone taken before the write still reads as it did.

use std::collections::{BTreeMap, btree_map};
use std::mem;
use std::ops::Bound;
use std::slice;
use std::sync::Arc;

/// The most entries a chunk holds; one more splits it in two.
const MAX: usize = 128;

/// A chunk left with fewer entries than this by a removal is merged
/// into a neighbour, when the two fit in one.
const MIN: usize = 32;

/// See the [module docs](self).
#[derive(Clone, Debug)]
pub(crate) struct Chunks<K, V> {
    spine: Spine<K, V>,
    len: usize,
}

/// The chunks in key order. Most maps fit one chunk, which is held
/// without a list, so a small map costs one allocation more than a
/// plain one, and an empty map none.
#[derive(Clone, Debug)]
enum Spine<K, V> {
    Empty,
    One(Arc<Chunk<K, V>>),
    Many(Arc<Vec<Arc<Chunk<K, V>>>>),
}

/// One run of the map's entries. Chunk `i` holds every key from its
/// `lower` bound up to the next chunk's; the first chunk's bound is
/// ignored, so it also holds every key beneath the second's.
#[derive(Clone, Debug)]
struct Chunk<K, V> {
    lower: Option<K>,
    entries: BTreeMap<K, V>,
}

impl<K, V> Default for Chunks<K, V> {
    fn default() -> Self {
        Self {
            spine: Spine::Empty,
            len: 0,
        }
    }
}

/// The chunk that holds `key`, or would hold it.
fn locate<K: Ord, V>(chunks: &[Arc<Chunk<K, V>>], key: &K) -> usize {
    if chunks.len() < 2 {
        return 0;
    }
    chunks
        .partition_point(|chunk| chunk.lower.as_ref().is_none_or(|lower| lower <= key))
        .saturating_sub(1)
}

impl<K: Ord + Clone, V: Clone> Chunks<K, V> {
    fn slice(&self) -> &[Arc<Chunk<K, V>>] {
        match &self.spine {
            Spine::Empty => &[],
            Spine::One(chunk) => slice::from_ref(chunk),
            Spine::Many(chunks) => chunks,
        }
    }

    /// Chunk `at`, copied first if a clone shares it.
    fn chunk_mut(&mut self, at: usize) -> &mut Chunk<K, V> {
        match &mut self.spine {
            Spine::Empty => unreachable!("an empty map has no chunk"),
            Spine::One(chunk) => Arc::make_mut(chunk),
            Spine::Many(chunks) => Arc::make_mut(&mut Arc::make_mut(chunks)[at]),
        }
    }

    /// Whether no clone shares chunk `at`, so a write to it changes it
    /// in place. A clone of this map is made only through it, so while
    /// it is borrowed mutably no other can be.
    fn owns(&self, at: usize) -> bool {
        match &self.spine {
            Spine::Empty => true,
            Spine::One(chunk) => Arc::strong_count(chunk) == 1,
            Spine::Many(chunks) => {
                Arc::strong_count(chunks) == 1 && Arc::strong_count(&chunks[at]) == 1
            }
        }
    }

    /// The list of chunks, copied first if a clone shares it: what a
    /// split or a merge changes.
    fn spine_mut(&mut self) -> &mut Vec<Arc<Chunk<K, V>>> {
        if !matches!(self.spine, Spine::Many(_)) {
            let chunks = match mem::replace(&mut self.spine, Spine::Empty) {
                Spine::One(chunk) => vec![chunk],
                _ => Vec::new(),
            };
            self.spine = Spine::Many(Arc::new(chunks));
        }
        let Spine::Many(chunks) = &mut self.spine else {
            unreachable!("the spine is a list");
        };
        Arc::make_mut(chunks)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool {
        let chunks = self.slice();
        !chunks.is_empty() && chunks[locate(chunks, key)].entries.contains_key(key)
    }

    /// Hold `value` under `key`, returning the value it replaced.
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        if matches!(self.spine, Spine::Empty) {
            self.spine = Spine::One(Arc::new(Chunk {
                lower: None,
                entries: BTreeMap::new(),
            }));
        }
        let at = locate(self.slice(), &key);
        let chunk = self.chunk_mut(at);
        let prior = chunk.entries.insert(key, value);
        let full = chunk.entries.len() > MAX;
        if prior.is_none() {
            self.len += 1;
        }
        if full {
            self.split(at);
        }
        prior
    }

    /// Hold each of `entries`, none of whose keys the map holds: one
    /// fact's three index entries. A map of one chunk takes them with
    /// one check for sharing, a map of more one at a time.
    pub(crate) fn insert_new<const N: usize>(&mut self, entries: [(K, V); N]) {
        if matches!(self.spine, Spine::Many(_)) {
            for (key, value) in entries {
                self.insert(key, value);
            }
            return;
        }
        if matches!(self.spine, Spine::Empty) {
            self.spine = Spine::One(Arc::new(Chunk {
                lower: None,
                entries: BTreeMap::new(),
            }));
        }
        let Spine::One(chunk) = &mut self.spine else {
            unreachable!("a map of one chunk");
        };
        let chunk = Arc::make_mut(chunk);
        for (key, value) in entries {
            if chunk.entries.insert(key, value).is_none() {
                self.len += 1;
            }
        }
        if chunk.entries.len() > MAX {
            self.split(0);
        }
    }

    /// Split chunk `at`, which holds more than [`MAX`] entries, in two.
    fn split(&mut self, at: usize) {
        let chunk = self.chunk_mut(at);
        let middle = chunk
            .entries
            .keys()
            .nth(chunk.entries.len() / 2)
            .cloned()
            .expect("an overfull chunk has a middle");
        let upper = chunk.entries.split_off(&middle);
        self.spine_mut().insert(
            at + 1,
            Arc::new(Chunk {
                lower: Some(middle),
                entries: upper,
            }),
        );
    }

    /// Drop the entry under `key`, returning its value. A key the map
    /// does not hold copies nothing, shared or not.
    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let chunks = self.slice();
        if chunks.is_empty() {
            return None;
        }
        let at = locate(chunks, key);
        // A chunk only this map holds is changed in place, so the key is
        // looked up once; a shared one is checked first, so a key the
        // map does not hold copies nothing.
        if !self.owns(at) && !chunks[at].entries.contains_key(key) {
            return None;
        }
        let count = chunks.len();
        let chunk = self.chunk_mut(at);
        let removed = chunk.entries.remove(key)?;
        let left = chunk.entries.len();
        self.len -= 1;
        if count == 1 {
            // The last chunk stays, emptied or not, as a plain map's
            // root does.
            return Some(removed);
        }
        if left == 0 {
            self.spine_mut().remove(at);
        } else if left < MIN {
            let chunks = self.spine_mut();
            let other = if at + 1 < chunks.len() {
                at + 1
            } else {
                at - 1
            };
            if left + chunks[other].entries.len() <= MAX {
                let (into, from) = (at.min(other), at.max(other));
                let from = chunks.remove(from);
                let mut from = Arc::try_unwrap(from).unwrap_or_else(|shared| (*shared).clone());
                Arc::make_mut(&mut chunks[into])
                    .entries
                    .append(&mut from.entries);
            }
        }
        Some(removed)
    }

    /// The entries from `start` to `end`, both included, in key order.
    pub(crate) fn range<'a>(&'a self, start: &'a K, end: &'a K) -> Range<'a, K, V> {
        let chunks = self.slice();
        let (current, rest) = match chunks.split_at_checked(locate(chunks, start)) {
            Some((_, [first, rest @ ..])) => (
                Some(
                    first
                        .entries
                        .range::<K, _>((Bound::Included(start), Bound::Included(end))),
                ),
                rest,
            ),
            _ => (None, &[][..]),
        };
        Range {
            current,
            rest,
            start,
            end,
        }
    }

    /// How many of this map's chunks `other` shares, and how many it
    /// has: what a test of the sharing counts.
    #[cfg(test)]
    pub(crate) fn shared_with(&self, other: &Self) -> (usize, usize) {
        let theirs = other.slice();
        let shared = self
            .slice()
            .iter()
            .filter(|chunk| theirs.iter().any(|their| Arc::ptr_eq(chunk, their)))
            .count();
        (shared, self.slice().len())
    }
}

/// The entries of a [`Chunks`] map within a range, chunk by chunk.
pub(crate) struct Range<'a, K, V> {
    current: Option<btree_map::Range<'a, K, V>>,
    rest: &'a [Arc<Chunk<K, V>>],
    start: &'a K,
    end: &'a K,
}

impl<'a, K: Ord, V> Iterator for Range<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.current.as_mut()?.next() {
                return Some(entry);
            }
            match self.rest.split_first() {
                Some((chunk, rest))
                    if chunk.lower.as_ref().is_some_and(|lower| lower <= self.end) =>
                {
                    self.rest = rest;
                    self.current =
                        Some(chunk.entries.range::<K, _>((
                            Bound::Included(self.start),
                            Bound::Included(self.end),
                        )));
                }
                _ => {
                    self.current = None;
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    /// A seeded xorshift64* generator: the same seed yields the same
    /// steps on every platform, with no dependency.
    pub(in crate::repository::ephemeral) struct Xorshift(u64);

    impl Xorshift {
        pub(in crate::repository::ephemeral) fn new(seed: u64) -> Self {
            let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            Self((z ^ (z >> 31)).max(1))
        }

        /// A number in `0..n`.
        pub(in crate::repository::ephemeral) fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
        }
    }

    /// The map answers as a plain ordered map through inserts and
    /// removals that split, empty and merge its chunks, and a clone
    /// taken along the way keeps answering as the map did then.
    #[dialog_common::test]
    fn it_answers_as_a_plain_map_and_clones_stay_put() {
        let mut rng = Xorshift::new(7);
        let mut map: Chunks<u32, u32> = Chunks::default();
        let mut model: BTreeMap<u32, u32> = BTreeMap::new();
        let mut held: Vec<(Chunks<u32, u32>, BTreeMap<u32, u32>)> = Vec::new();
        for step in 0..20_000u32 {
            let key = rng.below(2_000) as u32;
            if rng.below(100) < 55 {
                assert_eq!(map.insert(key, step), model.insert(key, step));
            } else {
                assert_eq!(map.remove(&key), model.remove(&key));
            }
            if step % 1_000 == 0 {
                held.push((map.clone(), model.clone()));
            }
        }
        held.push((map, model));
        for (map, model) in &held {
            assert_eq!(map.len(), model.len());
            let all: Vec<_> = map.range(&0, &u32::MAX).collect();
            assert_eq!(all, model.iter().collect::<Vec<_>>());
            for (start, end) in [(0, 0), (10, 900), (1_999, 2_000), (500, 501)] {
                assert_eq!(
                    map.range(&start, &end).collect::<Vec<_>>(),
                    model.range(start..=end).collect::<Vec<_>>()
                );
            }
            for key in 0..2_000 {
                assert_eq!(map.contains_key(&key), model.contains_key(&key));
            }
        }
    }

    /// A write to a shared map copies the one chunk it changes and
    /// leaves the clone's every chunk where it was.
    #[dialog_common::test]
    fn it_copies_only_the_chunk_a_shared_write_changes() {
        let mut map: Chunks<u32, u32> = Chunks::default();
        for key in 0..10_000 {
            map.insert(key, key);
        }
        let held = map.clone();
        let (shared, total) = held.shared_with(&map);
        assert_eq!(shared, total, "a clone shares every chunk");
        assert!(total > 50, "ten thousand entries span many chunks");

        map.insert(5_000, 0);
        map.remove(&7);
        let (shared, total) = held.shared_with(&map);
        assert_eq!(shared, total - 2, "two writes copied two chunks");
        assert_eq!(held.range(&5_000, &5_000).next(), Some((&5_000, &5_000)));
        assert!(held.contains_key(&7));
    }
}
