use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::btree_map;
use std::fmt::{self, Debug};
use std::iter::{Peekable, empty};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{EDITION_CEILING, Edition, HistoryError, Origin, Version};

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
///
/// A context holds one watermark per origin its ancestry ever included,
/// and every commit derives its successor from its parent's by raising
/// one of them. Copying the map for that would make every commit cost the
/// number of origins, so the map is kept in two parts: settled entries,
/// shared behind a reference count by every context derived from them,
/// and the few entries written since, which a copy duplicates. The recent
/// part settles into a fresh map once it outgrows the square root of the
/// settled one, so a copy and an amortized settle both stay well below
/// the size of the map. Two contexts holding the same watermarks are
/// equal however their entries are split.
#[derive(Clone, Default)]
pub struct Context(Watermarks);

/// The storage behind a [`Context`]: settled entries shared between the
/// contexts derived from them, and the entries written since, which take
/// precedence over a settled entry for the same origin.
#[derive(Clone, Default)]
struct Watermarks {
    settled: Arc<BTreeMap<Origin, Watermark>>,
    recent: BTreeMap<Origin, Watermark>,
    /// How many origins the two parts hold between them.
    len: usize,
}

/// The fewest recent entries a context keeps before it settles them.
const SETTLE_FLOOR: usize = 64;

impl Watermarks {
    fn get(&self, origin: &Origin) -> Option<&Watermark> {
        self.recent.get(origin).or_else(|| self.settled.get(origin))
    }

    fn contains(&self, origin: &Origin) -> bool {
        self.recent.contains_key(origin) || self.settled.contains_key(origin)
    }

    fn insert(&mut self, origin: Origin, watermark: Watermark) {
        if self.recent.insert(origin, watermark).is_none() && !self.settled.contains_key(&origin) {
            self.len += 1;
        }
        if self.recent.len() > SETTLE_FLOOR.max(self.settled.len().isqrt()) {
            // Copies the settled map only when another context shares it.
            Arc::make_mut(&mut self.settled).append(&mut self.recent);
        }
    }

    fn iter(&self) -> Entries<'_> {
        Entries {
            settled: self.settled.iter().peekable(),
            recent: self.recent.iter().peekable(),
        }
    }
}

impl From<BTreeMap<Origin, Watermark>> for Watermarks {
    fn from(settled: BTreeMap<Origin, Watermark>) -> Self {
        Self {
            len: settled.len(),
            settled: Arc::new(settled),
            recent: BTreeMap::new(),
        }
    }
}

/// A context's watermarks in ascending origin order: the settled and
/// recent parts merged, a recent entry standing in for a settled one.
struct Entries<'a> {
    settled: Peekable<btree_map::Iter<'a, Origin, Watermark>>,
    recent: Peekable<btree_map::Iter<'a, Origin, Watermark>>,
}

impl<'a> Iterator for Entries<'a> {
    type Item = (&'a Origin, &'a Watermark);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.settled.peek(), self.recent.peek()) {
            (Some((settled, _)), Some((recent, _))) => match settled.cmp(recent) {
                Ordering::Less => self.settled.next(),
                Ordering::Greater => self.recent.next(),
                Ordering::Equal => {
                    self.settled.next();
                    self.recent.next()
                }
            },
            (Some(_), None) => self.settled.next(),
            (None, _) => self.recent.next(),
        }
    }
}

impl PartialEq for Context {
    fn eq(&self, other: &Self) -> bool {
        self.0.len == other.0.len && self.iter().eq(other.iter())
    }
}

impl Eq for Context {}

impl Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Context").field(&DebugEntries(self)).finish()
    }
}

/// A context's watermarks rendered as one map, however they are split.
struct DebugEntries<'a>(&'a Context);

impl Debug for DebugEntries<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.0.iter()).finish()
    }
}

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

impl Watermark {
    /// Refuse a watermark no legitimate chain can produce: an edition at or
    /// above [`EDITION_CEILING`], or a count above `edition + 1`.
    ///
    /// The ceiling keeps a validly signed hostile entry near `u64::MAX`
    /// from pinning the origin as fully seen on every replica that merges
    /// it. The count bound holds because an origin's chain has strictly
    /// increasing editions, so at most `edition + 1` of its revisions fit
    /// at or below the watermark; an inflated count would misroute every
    /// peer's merge direction.
    ///
    /// This is the check every watermark crosses at the trust boundary,
    /// wherever it was read from: a head's published context
    /// ([`Revision::verify`](crate::identity::Revision::verify)) or a
    /// tree's context region.
    pub fn check(&self) -> Result<(), HistoryError> {
        if self.edition.value() >= EDITION_CEILING {
            return Err(HistoryError::InvalidReference(format!(
                "watermark edition {} exceeds the protocol ceiling; \
                 no legitimate chain reaches it",
                self.edition
            )));
        }
        if self.count > self.edition.value().saturating_add(1) {
            return Err(HistoryError::InvalidReference(format!(
                "watermark count {} exceeds its edition {} — more revisions \
                 than the chain can hold",
                self.count, self.edition
            )));
        }
        Ok(())
    }
}

/// How many watermarks a head carries inline: its **active set**.
///
/// A head publishes the `ACTIVE_CONTEXT` watermarks with the highest
/// editions (ties broken by origin bytes) and a [`Spill`] summarizing the
/// rest; the full map lives in its tree's context region. Ranking by
/// edition is what lets a passing participant fall off on its own: an
/// origin stays inline until this many other origins have written after
/// it, and then it costs its region entry and nothing per head.
///
/// A protocol constant: two replicas must project the same map to the
/// same bytes, and a head carrying more inline entries is refused. It
/// bounds the inline part of a head at 48 bytes times this many.
pub const ACTIVE_CONTEXT: usize = 64;

/// The domain tag opening every spill digest's input, so the digest can
/// never collide with a hash of any other dialog structure.
const SPILL_DOMAIN: &[u8] = b"dialog/context-spill@1\n";

/// What a head says about the watermarks it does not carry inline: the
/// part of its context outside the active set.
///
/// Two heads whose spills are equal have identical spilled watermarks
/// (the digest is a hash of every one of them), so comparing two such
/// heads' contexts needs only their active sets: the spilled parts are
/// the same entries on both sides and contribute nothing to inclusion or
/// divergence. That is the steady state of a device pulling its own
/// upstream, and it is what keeps that pull at zero reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spill {
    /// Blake3 of every spilled watermark, in origin order, as
    /// `origin (32) ++ edition (8, big-endian) ++ count (8, big-endian)`
    /// after a domain tag.
    #[serde(with = "serde_bytes")]
    pub digest: [u8; 32],
    /// How many watermarks are spilled.
    pub count: u64,
    /// The highest edition among the spilled watermarks, genesis when none
    /// are. Every active watermark sits at or above it: that is what makes
    /// the active set the top of the map, checkable from the head alone.
    pub highest: Edition,
}

impl Spill {
    /// The spill of a context that fits its active set: nothing spilled.
    pub fn empty() -> Self {
        Self::of(empty())
    }

    /// The spill summarizing `entries`, which must arrive in origin order.
    fn of<'a>(entries: impl Iterator<Item = (&'a Origin, &'a Watermark)>) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(SPILL_DOMAIN);
        let mut count = 0u64;
        let mut highest = Edition::GENESIS;
        for (origin, watermark) in entries {
            hasher.update(origin.key_bytes());
            hasher.update(&watermark.edition.key_bytes());
            hasher.update(&watermark.count.to_be_bytes());
            count += 1;
            highest = highest.max(watermark.edition);
        }
        Self {
            digest: *hasher.finalize().as_bytes(),
            count,
            highest,
        }
    }

    /// Whether nothing is spilled.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
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
        let mut seq = serializer.serialize_seq(Some(self.0.len))?;
        for (origin, watermark) in self.iter() {
            seq.serialize_element(&(origin, watermark.edition, watermark.count))?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Context {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<(Origin, Edition, u64)>::deserialize(deserializer)?;
        Ok(entries
            .into_iter()
            .map(|(origin, edition, count)| (origin, Watermark { edition, count }))
            .collect())
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

    /// The watermark this context holds for `origin`, if it has observed
    /// any of its revisions.
    pub fn get(&self, origin: &Origin) -> Option<&Watermark> {
        self.0.get(origin)
    }

    /// Whether this context has observed ANY revision minted by
    /// `origin`.
    ///
    /// This is the one question a screen skip asks of the other side:
    /// a replica none of whose origins the other has observed has
    /// never had a claim seen there, so nothing of its can have been
    /// covered or superseded there. It is deliberately a per-origin
    /// point query rather than an enumeration, so that a compacted
    /// context encoding (deltas with backlinks, say) only has to keep
    /// this lookup cheap, never a full origin listing.
    pub fn observes_origin(&self, origin: &Origin) -> bool {
        self.0.contains(origin)
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
        match self.0.get(&version.origin).copied() {
            None => self.0.insert(
                version.origin,
                Watermark {
                    edition: version.edition,
                    count: 1,
                },
            ),
            Some(watermark) if version.edition > watermark.edition => self.0.insert(
                version.origin,
                Watermark {
                    edition: version.edition,
                    count: watermark.count + 1,
                },
            ),
            Some(_) => {}
        }
    }

    /// Fold one KNOWN-DISTINCT revision into the context, in any order:
    /// the watermark rises to at least its edition and the count grows
    /// by one unconditionally. The caller guarantees each revision is
    /// folded exactly once (an ancestry walk's visited set, a delta's
    /// per-version-unique record keys); double-folding overcounts.
    pub fn tally(&mut self, version: Version) {
        let watermark = match self.0.get(&version.origin) {
            Some(watermark) => Watermark {
                edition: watermark.edition.max(version.edition),
                count: watermark.count + 1,
            },
            None => Watermark {
                edition: version.edition,
                count: 1,
            },
        };
        self.0.insert(version.origin, watermark);
    }

    /// Union with another context: per origin, the entry with the higher
    /// edition stands (its count counts the longer prefix; prefixes of
    /// one origin's chain are nested, so the higher watermark's count is
    /// the union's count). This is the context of a merge — the merged
    /// head's ancestry is the union of its parents' ancestries.
    pub fn merge(&mut self, other: &Context) {
        for (origin, watermark) in other.iter() {
            match self.0.get(origin).copied() {
                None => self.0.insert(*origin, *watermark),
                Some(ours) if watermark.edition > ours.edition => {
                    self.0.insert(*origin, *watermark);
                }
                // Same prefix; counts should agree, tolerate drift by max.
                Some(ours) if watermark.edition == ours.edition && watermark.count > ours.count => {
                    self.0.insert(*origin, *watermark);
                }
                Some(_) => {}
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
            let watermark = match self.0.get(&origin) {
                Some(watermark) => Watermark {
                    edition: watermark.edition.max(addition.edition),
                    count: watermark.count + addition.count,
                },
                None => addition,
            };
            self.0.insert(origin, watermark);
        }
    }

    /// Project the context onto what a head carries: the active set of at
    /// most `limit` watermarks with the highest editions (ties broken by
    /// ascending origin bytes), and the [`Spill`] summarizing the rest.
    ///
    /// A pure function of the map, so every replica that holds the same
    /// context, however it was assembled (commit by commit, by an ancestry
    /// walk, by a merge, or read back from a tree's context region), lands
    /// on identical bytes. Linear in the size of the map: the active set is
    /// selected, not sorted, and the spilled watermarks are hashed once.
    pub fn project(&self, limit: usize) -> (Context, Spill) {
        if self.0.len <= limit {
            return (self.clone(), Spill::empty());
        }
        let mut ranked: Vec<(&Origin, &Watermark)> = self.iter().collect();
        let rank = |a: &(&Origin, &Watermark), b: &(&Origin, &Watermark)| {
            b.1.edition.cmp(&a.1.edition).then_with(|| a.0.cmp(b.0))
        };
        if limit > 0 {
            ranked.select_nth_unstable_by(limit - 1, rank);
        }
        let active: Context = ranked[..limit]
            .iter()
            .map(|(origin, watermark)| (**origin, **watermark))
            .collect();
        let spill = Spill::of(
            self.iter()
                .filter(|(origin, _)| !active.observes_origin(origin)),
        );
        (active, spill)
    }

    /// The projection of this context, given the projection of the
    /// context it was derived from by raising `origin`'s watermark alone.
    ///
    /// A raised watermark only climbs the ranking, so when `origin` was
    /// already active the active set keeps its members and the spill does
    /// not change: the projection is the previous active set with
    /// `origin`'s entry replaced, at the cost of the active set alone.
    /// That is the commit of a writer that is still active. Otherwise the
    /// membership changes, and the projection is recomputed in full.
    pub fn project_from(
        &self,
        limit: usize,
        previous: (&Context, &Spill),
        origin: &Origin,
    ) -> (Context, Spill) {
        let (active, spill) = previous;
        match (active.observes_origin(origin), self.0.get(origin)) {
            (true, Some(watermark)) if active.len() <= limit => {
                let mut active = active.clone();
                active.0.insert(*origin, *watermark);
                (active, *spill)
            }
            _ => self.project(limit),
        }
    }

    /// The full context of a head whose spill equals the spill this
    /// context projects to beside `ours`, its active set, when that head's
    /// active set is `theirs`.
    ///
    /// Equal spills are equal spilled watermarks, so the other head's full
    /// context is this one with the active parts exchanged: what we spill
    /// is what it spills, and its active set is the rest. That is how a
    /// pull learns an upstream's whole context from its head alone, with
    /// no read, whenever the two sides spill the same origins.
    pub fn exchange_active(&self, ours: &Context, theirs: &Context) -> Context {
        self.iter()
            .filter(|(origin, _)| !ours.observes_origin(origin))
            .chain(theirs.iter())
            .map(|(origin, watermark)| (*origin, *watermark))
            .collect()
    }

    /// Number of origins this context has observed.
    pub fn len(&self) -> usize {
        self.0.len
    }

    /// Whether the context observes nothing.
    pub fn is_empty(&self) -> bool {
        self.0.len == 0
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
        other.iter().all(|(origin, theirs)| {
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
        self.iter()
            .map(|(origin, watermark)| match other.0.get(origin) {
                Some(seen) => watermark.count.saturating_sub(seen.count),
                None => watermark.count,
            })
            .fold(0u64, u64::saturating_add)
    }
}

/// A context assembled from `(origin, watermark)` entries, as a tree's
/// context region lists them. A repeated origin keeps its last entry.
impl FromIterator<(Origin, Watermark)> for Context {
    fn from_iter<I: IntoIterator<Item = (Origin, Watermark)>>(entries: I) -> Self {
        Self(Watermarks::from(
            entries.into_iter().collect::<BTreeMap<_, _>>(),
        ))
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

    /// A context holds the same watermarks however its storage is split:
    /// one built commit by commit past many settles, one collected at once,
    /// and copies of either compare equal and iterate in origin order, and
    /// a copy raised afterwards leaves the context it was copied from as it
    /// was.
    #[test]
    fn it_keeps_its_watermarks_across_settles_and_copies() {
        let mut expected = BTreeMap::new();
        let mut built = Context::new();
        let mut copies = Vec::new();
        for i in 0u32..5_000 {
            let mut seed = [0u8; 32];
            seed[..4].copy_from_slice(&i.wrapping_mul(2_654_435_761).to_be_bytes());
            let origin = Origin::from(seed);
            let version = Version::new(origin, Edition::new(u64::from(i)));
            built.record(version);
            expected.insert(
                origin,
                Watermark {
                    edition: version.edition,
                    count: 1,
                },
            );
            if i % 997 == 0 {
                copies.push((built.clone(), expected.clone()));
            }
        }
        let collected: Context = expected.iter().map(|(o, w)| (*o, *w)).collect();
        assert_eq!(built, collected);
        assert_eq!(built.len(), expected.len());
        assert!(built.iter().map(|(o, w)| (*o, *w)).eq(expected.clone()));

        for (copy, at) in copies {
            assert_eq!(copy.len(), at.len(), "a copy keeps what it was copied with");
            assert!(copy.iter().map(|(o, w)| (*o, *w)).eq(at));
        }

        let mut raised = built.clone();
        let (origin, watermark) = expected
            .iter()
            .next()
            .map(|(o, w)| (*o, *w))
            .expect("entries");
        raised.record(Version::new(origin, watermark.edition.successor()));
        assert_ne!(raised, built);
        assert_eq!(
            built.get(&origin),
            Some(&watermark),
            "the original is untouched"
        );
        assert_eq!(raised.get(&origin).map(|w| w.count), Some(2));
    }

    /// The watermark bounds refuse exactly what no chain can produce: an
    /// edition at the ceiling, and a count above `edition + 1`; a genesis
    /// watermark (edition 0, one revision) and a dense chain pass.
    #[test]
    fn it_refuses_watermarks_no_chain_can_produce() {
        let sound = [
            Watermark {
                edition: Edition::new(0),
                count: 1,
            },
            Watermark {
                edition: Edition::new(9),
                count: 10,
            },
            Watermark {
                edition: Edition::new(EDITION_CEILING - 1),
                count: 1,
            },
        ];
        for watermark in sound {
            watermark.check().expect("a producible watermark passes");
        }

        let hostile = [
            Watermark {
                edition: Edition::new(EDITION_CEILING),
                count: 1,
            },
            Watermark {
                edition: Edition::new(u64::MAX),
                count: 1,
            },
            Watermark {
                edition: Edition::new(9),
                count: 11,
            },
        ];
        for watermark in hostile {
            assert!(
                matches!(watermark.check(), Err(HistoryError::InvalidReference(_))),
                "{watermark:?} must be refused"
            );
        }
    }

    /// A context of `origins` origins, origin `i` (seeded `i`) at
    /// edition `i` with one revision.
    fn spread(origins: u8) -> Vec<Version> {
        (0..origins).map(|i| version(i, u64::from(i))).collect()
    }

    /// The projection is a function of the map alone: built commit by
    /// commit, absorbed out of order, or merged from two halves, the same
    /// context projects to the same active set and the same spill.
    #[test]
    fn it_projects_identically_however_the_context_was_assembled() {
        let versions = spread(100);

        let mut recorded = Context::new();
        for version in &versions {
            recorded.record(*version);
        }

        let mut absorbed = Context::new();
        // A fixed stride visits every index once, out of order.
        absorbed.absorb((0..versions.len()).map(|i| versions[(i * 37) % versions.len()]));

        let mut low = Context::new();
        let mut high = Context::new();
        for version in &versions {
            if version.edition.value() % 2 == 0 {
                low.record(*version);
            } else {
                high.record(*version);
            }
        }
        high.merge(&low);

        assert_eq!(recorded, absorbed);
        assert_eq!(recorded, high);
        let projection = recorded.project(ACTIVE_CONTEXT);
        assert_eq!(absorbed.project(ACTIVE_CONTEXT), projection);
        assert_eq!(high.project(ACTIVE_CONTEXT), projection);

        let (active, spill) = projection;
        assert_eq!(active.len(), ACTIVE_CONTEXT);
        assert_eq!(spill.count, (100 - ACTIVE_CONTEXT) as u64);
        assert!(
            active
                .iter()
                .all(|(_, watermark)| watermark.edition > spill.highest),
            "the active set is the top of the map"
        );
    }

    /// Ties at one edition are broken by origin bytes, so two origins that
    /// wrote at the same depth land in the active set deterministically.
    #[test]
    fn it_breaks_edition_ties_by_origin() {
        let mut context = Context::new();
        for seed in 0..3 {
            context.record(version(seed, 5));
        }
        let (active, spill) = context.project(2);
        assert!(active.observes_origin(&Origin::from([0u8; 32])));
        assert!(active.observes_origin(&Origin::from([1u8; 32])));
        assert_eq!(spill.count, 1);
        assert_eq!(spill.highest, Edition::new(5));
    }

    /// An origin stays active until `ACTIVE_CONTEXT` other origins have
    /// written after it, and leaves the active set on exactly that write.
    #[test]
    fn it_spills_an_origin_once_enough_others_write_after_it() {
        let passing = Origin::from([255u8; 32]);
        let mut context = Context::new();
        context.record(Version::new(passing, Edition::new(0)));

        for later in 1..=ACTIVE_CONTEXT {
            let seed = u8::try_from(later).expect("the active set fits a byte");
            context.record(version(seed, later as u64));
            let (active, spill) = context.project(ACTIVE_CONTEXT);
            if later < ACTIVE_CONTEXT {
                assert!(
                    active.observes_origin(&passing),
                    "after {later} later writers the passing origin is still active"
                );
                assert!(spill.is_empty());
            } else {
                assert!(
                    !active.observes_origin(&passing),
                    "the {later}th later writer pushes the passing origin out"
                );
                assert_eq!(spill.count, 1);
            }
        }
    }

    /// The digest covers every spilled watermark: two contexts that differ
    /// only in one spilled entry, by edition or by count, digest apart.
    #[test]
    fn it_digests_every_spilled_watermark() {
        let base: Context = spread(100).into_iter().fold(Context::new(), |mut c, v| {
            c.record(v);
            c
        });
        let (_, spill) = base.project(ACTIVE_CONTEXT);

        let mut raised = base.clone();
        // Origin 1 sits deep in the spill; raising it a little keeps it there.
        raised.record(version(1, 2));
        let (_, raised_spill) = raised.project(ACTIVE_CONTEXT);
        assert_ne!(raised_spill.digest, spill.digest, "an edition change shows");

        let mut recounted = base.clone();
        recounted.tally(version(1, 1));
        let (_, recounted_spill) = recounted.project(ACTIVE_CONTEXT);
        assert_ne!(recounted_spill.digest, spill.digest, "a count change shows");
    }

    /// Two contexts whose spills are equal compare exactly through their
    /// active sets: inclusion and divergence agree with the full maps, and
    /// either side's full map is the other's with the active parts
    /// exchanged.
    #[test]
    fn it_compares_equal_spills_through_the_active_sets_alone() {
        let shared: Context = spread(100).into_iter().fold(Context::new(), |mut c, v| {
            c.record(v);
            c
        });

        // Both sides move on from the shared history with writers of their
        // own, high enough to stay active, so neither spill changes.
        let mut ours = shared.clone();
        ours.record(version(200, 500));
        ours.record(version(201, 501));
        let mut theirs = shared.clone();
        theirs.record(version(200, 500));
        theirs.record(version(202, 502));

        let (our_active, our_spill) = ours.project(ACTIVE_CONTEXT);
        let (their_active, their_spill) = theirs.project(ACTIVE_CONTEXT);
        assert_eq!(
            our_spill, their_spill,
            "the same origins spill on both sides"
        );
        assert_ne!(our_active, their_active);

        assert_eq!(our_active.includes(&their_active), ours.includes(&theirs));
        assert_eq!(their_active.includes(&our_active), theirs.includes(&ours));
        assert_eq!(
            our_active.divergence(&their_active),
            ours.divergence(&theirs)
        );
        assert_eq!(
            their_active.divergence(&our_active),
            theirs.divergence(&ours)
        );
        assert_eq!(ours.exchange_active(&our_active, &their_active), theirs);

        assert_eq!(theirs.exchange_active(&their_active, &our_active), ours);

        // An active writer moving on keeps the spill, and inclusion still
        // reads off the active sets.
        let mut ahead = theirs.clone();
        ahead.record(version(202, 600));
        let (ahead_active, ahead_spill) = ahead.project(ACTIVE_CONTEXT);
        assert_eq!(ahead_spill, their_spill);
        assert!(ahead_active.includes(&their_active) && ahead.includes(&theirs));
        assert!(!their_active.includes(&ahead_active) && !theirs.includes(&ahead));
    }

    /// The successor projection, taken from the parent's, equals the one
    /// computed from scratch, whether the committing origin was active or
    /// not.
    #[test]
    fn it_projects_a_successor_from_its_parents_projection() {
        let parent: Context = spread(100).into_iter().fold(Context::new(), |mut c, v| {
            c.record(v);
            c
        });
        let (active, spill) = parent.project(ACTIVE_CONTEXT);
        for seed in [99u8, 50, 3, 250] {
            let mut child = parent.clone();
            child.record(version(seed, 1_000));
            assert_eq!(
                child.project_from(ACTIVE_CONTEXT, (&active, &spill), &Origin::from([seed; 32])),
                child.project(ACTIVE_CONTEXT),
                "a commit by origin {seed}"
            );
        }
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
