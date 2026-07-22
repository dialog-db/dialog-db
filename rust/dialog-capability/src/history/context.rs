use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{Edition, Origin, Version};

/// The causal context of a revision: a per-origin watermark summarizing
/// everything in the revision's ancestry.
///
/// Because an origin is a *sequential actor* (the origin invariant) and
/// editions are Lamport timestamps, a head's ancestry restricted to one
/// origin is a prefix — so "every revision this head causally includes"
/// compresses losslessly to `origin → max edition`. This makes the
/// observed-remove question exact and O(1):
///
/// ```text
/// observed(v) ⇔ v.edition ≤ context[v.origin]
/// ```
///
/// A claim that is *observed* but not live in the active index was
/// covered by some record in the log (a retraction or a superseding
/// replace) — which is what lets the merge reject a stale peer's copy
/// of a deleted fact without any tombstone in the active index. See
/// `notes/version-control.md`.
///
/// In CRDT terms this is the causal context of an optimized OR-set,
/// with `(Origin, Edition)` versions as its dots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Context(BTreeMap<Origin, Watermark>);

/// One origin's entry in a [`Context`]: the highest edition seen from it,
/// and how many of its revisions the ancestry actually contains.
///
/// The count exists because editions are Lamport depths, not write
/// counts: an origin's first commit atop a deep adopted history mints an
/// edition near the whole depth, so edition arithmetic alone wildly
/// overestimates how much that origin has written. The count is exact
/// and comparable across replicas — an origin's revisions form a chain,
/// any watermark cuts a *prefix* of it, and prefixes are nested, so the
/// count of the union of two prefixes is the larger count, and the
/// difference of two counts is exactly the number of revisions one side
/// has that the other lacks. That is what
/// [`divergence`](Context::divergence) sums.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    /// The highest edition seen from the origin.
    pub edition: Edition,
    /// How many of the origin's revisions the ancestry contains: the
    /// length of the observed prefix of its chain.
    pub count: u64,
}

/// Serde encodes a [`Context`] as an ordered array of
/// `(origin, edition, count)` triples, NOT as the derive's map: heads
/// travel as dag-cbor, whose spec allows only string map keys, while a
/// derive-encoded `BTreeMap` produces byte-string keys that today's
/// lenient encoder accepts and a spec-enforcing one (or any other IPLD
/// tooling) rejects. The array iterates the map in ascending origin
/// order, so the encoding stays deterministic — which the head signature
/// relies on.
impl Serialize for Context {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for (origin, watermark) in &self.0 {
            seq.serialize_element(&(origin, watermark.edition, watermark.count))?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Context {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<(Origin, Edition, u64)>::deserialize(deserializer)?;
        Ok(Self(
            entries
                .into_iter()
                .map(|(origin, edition, count)| (origin, Watermark { edition, count }))
                .collect(),
        ))
    }
}

impl Context {
    /// An empty context: observes nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `version` is within this context — i.e. whether the
    /// revision it names is an ancestor of (or equal to) the head this
    /// context summarizes.
    pub fn observes(&self, version: &Version) -> bool {
        self.0
            .get(&version.origin)
            .map(|watermark| version.edition <= watermark.edition)
            .unwrap_or(false)
    }

    /// Extend the context by one NEWLY MINTED revision: its origin's
    /// watermark rises to its edition and the origin's revision count
    /// grows by one. The version must sit above the origin's current
    /// watermark (a fresh commit or merge always does — its edition is
    /// `max(parents) + 1`); a version at or below it is already part of
    /// the summarized ancestry and folds as a no-op. For revisions
    /// discovered in arbitrary order (an ancestry walk, a delta's
    /// records), use [`tally`](Context::tally) /
    /// [`absorb`](Context::absorb) instead — this method would skip
    /// counting a revision that arrives below an already-raised
    /// watermark.
    pub fn record(&mut self, version: Version) {
        match self.0.get_mut(&version.origin) {
            None => {
                self.0.insert(
                    version.origin,
                    Watermark {
                        edition: version.edition,
                        count: 1,
                    },
                );
            }
            Some(watermark) if version.edition > watermark.edition => {
                watermark.edition = version.edition;
                watermark.count += 1;
            }
            Some(_) => {}
        }
    }

    /// Fold one KNOWN-DISTINCT revision into the context, in any order:
    /// the watermark rises to at least its edition and the count grows
    /// by one unconditionally. The caller guarantees each revision is
    /// folded exactly once (an ancestry walk's visited set, a delta's
    /// per-version-unique record keys); double-folding overcounts.
    pub fn tally(&mut self, version: Version) {
        let entry = self.0.entry(version.origin).or_insert(Watermark {
            edition: version.edition,
            count: 0,
        });
        entry.edition = entry.edition.max(version.edition);
        entry.count += 1;
    }

    /// Union with another context: per origin, the entry with the higher
    /// edition stands (its count counts the longer prefix; prefixes of
    /// one origin's chain are nested, so the higher watermark's count is
    /// the union's count). This is the context of a merge — the merged
    /// head's ancestry is the union of its parents' ancestries.
    pub fn merge(&mut self, other: &Context) {
        for (origin, watermark) in &other.0 {
            let entry = self.0.entry(*origin).or_insert(*watermark);
            if watermark.edition > entry.edition {
                *entry = *watermark;
            } else if watermark.edition == entry.edition {
                // Same prefix; counts should agree, tolerate drift by max.
                entry.count = entry.count.max(watermark.count);
            }
        }
    }

    /// Fold a set of DISTINCT revision versions — the revision records
    /// riding a pull's delta — into the context. Per origin, only
    /// versions ABOVE the current watermark are news (the rest are
    /// already inside the summarized prefix): the watermark rises to
    /// their maximum and the count grows by how many there are. The
    /// caller guarantees the versions are distinct (delta record keys
    /// are per-version unique).
    pub fn absorb(&mut self, versions: impl IntoIterator<Item = Version>) {
        // Screen against the ORIGINAL watermarks throughout: mutating as
        // versions stream would make a later, lower-edition version of the
        // same origin look already-summarized when it is in fact news
        // (folding 8 then 7 above a watermark of 5 must count both).
        let mut news: BTreeMap<Origin, Watermark> = BTreeMap::new();
        for version in versions {
            if self
                .0
                .get(&version.origin)
                .is_some_and(|watermark| version.edition <= watermark.edition)
            {
                continue;
            }
            let entry = news.entry(version.origin).or_insert(Watermark {
                edition: version.edition,
                count: 0,
            });
            entry.edition = entry.edition.max(version.edition);
            entry.count += 1;
        }
        for (origin, addition) in news {
            match self.0.get_mut(&origin) {
                Some(watermark) => {
                    watermark.edition = watermark.edition.max(addition.edition);
                    watermark.count += addition.count;
                }
                None => {
                    self.0.insert(origin, addition);
                }
            }
        }
    }

    /// Number of origins this context has observed.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the context observes nothing.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether this context has observed everything `other` has: for
    /// every origin in `other`, this context's watermark is at least as
    /// high. This is the knowledge order between replicas, and it is
    /// what gates the frugal pull paths: an upstream whose context is
    /// included in ours has nothing new for us (skip the pull), and an
    /// upstream whose context includes ours can have its subtrees
    /// adopted wholesale where we have no local novelty (nothing we
    /// know could contradict what survived its screen).
    pub fn includes(&self, other: &Context) -> bool {
        other.0.iter().all(|(origin, theirs)| {
            self.0
                .get(origin)
                .is_some_and(|ours| ours.edition >= theirs.edition)
        })
    }

    /// The per-origin watermarks, sorted by origin. The iteration order
    /// is deterministic, which is what lets a signing payload commit to
    /// the context byte-for-byte.
    pub fn iter(&self) -> impl Iterator<Item = (&Origin, &Watermark)> {
        self.0.iter()
    }

    /// How many revisions this context has seen that `other` has not:
    /// the summed per-origin revision-count excess. This is the exact
    /// size (in revisions) of the delta a replica holding this context
    /// would have to send a replica holding `other` — which is what lets
    /// a pull pick the cheaper merge direction from the two published
    /// watermarks alone, at zero reads.
    ///
    /// Exactness rests on the counts, not the editions: an origin's
    /// revisions form a chain, any watermark cuts a prefix of it, and
    /// prefixes are nested, so `my count − their count` is precisely the
    /// number of that origin's revisions in my ancestry and not theirs.
    /// Editions (Lamport depths) would overweigh origins writing atop
    /// deep adopted history — one fresh-session commit atop a
    /// ten-thousand-deep history is ONE revision to send, not ten
    /// thousand.
    pub fn divergence(&self, other: &Context) -> u64 {
        self.0
            .iter()
            .map(|(origin, watermark)| match other.0.get(origin) {
                Some(seen) => watermark.count.saturating_sub(seen.count),
                None => watermark.count,
            })
            .fold(0u64, u64::saturating_add)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(origin_seed: u8, edition: u64) -> Version {
        Version::new(Origin::from([origin_seed; 32]), Edition::new(edition))
    }

    #[test]
    fn it_observes_up_to_the_watermark_per_origin() {
        let mut context = Context::new();
        context.record(version(1, 3));
        context.record(version(2, 1));

        assert!(context.observes(&version(1, 3)));
        assert!(context.observes(&version(1, 2)), "prefix is observed");
        assert!(!context.observes(&version(1, 4)), "the future is not");
        assert!(context.observes(&version(2, 1)));
        assert!(!context.observes(&version(3, 0)), "unknown origin is not");
    }

    /// `includes` is presence-based at the equality boundary: an equal
    /// watermark includes, one edition short does not, and an origin
    /// absent from `self` fails inclusion even at edition 0 (a genesis
    /// revision is a real revision the other side has not seen).
    #[test]
    fn it_gates_inclusion_on_presence_and_the_equality_boundary() {
        let mut ours = Context::new();
        ours.record(version(1, 3));
        let mut theirs = Context::new();
        theirs.record(version(1, 3));

        assert!(ours.includes(&theirs), "equal watermarks include");
        assert!(ours.includes(&Context::new()), "everything includes empty");
        assert!(!Context::new().includes(&ours), "empty includes only empty");

        theirs.record(version(1, 4));
        assert!(!ours.includes(&theirs), "one edition short fails");

        let mut genesis_only = Context::new();
        genesis_only.record(version(2, 0));
        assert!(
            !ours.includes(&genesis_only),
            "an unseen origin fails inclusion even at edition 0"
        );
    }

    /// Divergence sums per-origin revision-COUNT excesses, so an origin
    /// absent from the other side contributes exactly its revision count
    /// (a genesis-only origin diverges by one), and a partially seen
    /// origin contributes only the revisions beyond the shared prefix.
    #[test]
    fn it_counts_revisions_not_editions_in_the_divergence() {
        let mut ours = Context::new();
        ours.record(version(1, 0));
        assert_eq!(
            ours.divergence(&Context::new()),
            1,
            "an unseen genesis revision diverges by one"
        );

        let mut theirs = Context::new();
        theirs.record(version(2, 0));
        assert_eq!(ours.divergence(&theirs), 1);
        assert_eq!(theirs.divergence(&ours), 1);
        assert!(
            !ours.includes(&theirs) && !theirs.includes(&ours),
            "disjoint contexts diverge both ways and include neither way"
        );

        // Partially seen origins count only the excess revisions.
        let mut ahead = Context::new();
        let mut behind = Context::new();
        for edition in 0..=5 {
            ahead.record(version(1, edition));
        }
        for edition in 0..=2 {
            behind.record(version(1, edition));
        }
        assert_eq!(ahead.divergence(&behind), 3);
        assert_eq!(behind.divergence(&ahead), 0);
    }

    /// One fresh-session commit atop a deep adopted history is ONE
    /// revision to send, not the whole depth: divergence follows counts,
    /// not Lamport editions, so the cascade routes the small delta the
    /// cheap way.
    #[test]
    fn it_weighs_a_fresh_commit_atop_deep_history_as_one_revision() {
        // The upstream's deep history: one origin, ten thousand commits.
        let mut upstream = Context::new();
        for edition in 0..10_000 {
            upstream.record(version(1, edition));
        }

        // We adopt it wholesale, then commit ONCE from a fresh session:
        // the new origin's edition is the whole depth, its count is one.
        let mut ours = upstream.clone();
        ours.record(version(2, 10_000));

        // The upstream meanwhile made fifty genuine commits.
        let mut theirs = upstream;
        for edition in 10_000..10_050 {
            theirs.record(version(1, edition));
        }

        assert_eq!(ours.divergence(&theirs), 1, "our delta is one revision");
        assert_eq!(theirs.divergence(&ours), 50, "theirs is fifty");
        assert!(
            ours.divergence(&theirs) < theirs.divergence(&ours),
            "the cascade replays our side, never the upstream's churn"
        );
    }

    /// `absorb` folds delta revisions in any order, screening against the
    /// original watermark: versions above it all count, versions at or
    /// below it are already summarized.
    #[test]
    fn it_absorbs_out_of_order_delta_revisions_exactly() {
        let mut context = Context::new();
        for edition in 0..=5 {
            context.record(version(1, edition));
        }

        // The delta carries editions 8, 7, 6 (out of order) and a stale 4.
        context.absorb([version(1, 8), version(1, 7), version(1, 6), version(1, 4)]);

        assert!(context.observes(&version(1, 8)));
        assert_eq!(
            context.divergence(&Context::new()),
            9,
            "six original + three new revisions, the stale one uncounted"
        );
    }

    #[test]
    fn it_merges_as_per_origin_maximum() {
        let mut a = Context::new();
        a.record(version(1, 5));
        a.record(version(2, 1));
        let mut b = Context::new();
        b.record(version(1, 2));
        b.record(version(3, 7));

        a.merge(&b);
        assert!(a.observes(&version(1, 5)));
        assert!(a.observes(&version(2, 1)));
        assert!(a.observes(&version(3, 7)));
        assert!(!a.observes(&version(3, 8)));
    }
}
