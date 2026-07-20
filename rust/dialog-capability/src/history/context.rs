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
pub struct Context(BTreeMap<Origin, Edition>);

/// Serde encodes a [`Context`] as an ordered array of `(origin, edition)`
/// pairs, NOT as the derive's map: heads travel as dag-cbor, whose spec
/// allows only string map keys, while a derive-encoded `BTreeMap<Origin,
/// Edition>` produces byte-string keys that today's lenient encoder
/// accepts and a spec-enforcing one (or any other IPLD tooling) rejects.
/// The array iterates the map in ascending origin order, so the encoding
/// stays deterministic — which the head signature relies on.
impl Serialize for Context {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for entry in &self.0 {
            seq.serialize_element(&entry)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Context {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<(Origin, Edition)>::deserialize(deserializer)?;
        Ok(Self(entries.into_iter().collect()))
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
            .map(|edition| version.edition <= *edition)
            .unwrap_or(false)
    }

    /// Fold one version into the context (its origin's watermark rises
    /// to at least its edition).
    pub fn record(&mut self, version: Version) {
        let entry = self.0.entry(version.origin).or_insert(version.edition);
        if version.edition > *entry {
            *entry = version.edition;
        }
    }

    /// Union with another context: per-origin maximum. This is the
    /// context of a merge — the merged head's ancestry is the union of
    /// its parents' ancestries.
    pub fn merge(&mut self, other: &Context) {
        for (origin, edition) in &other.0 {
            let entry = self.0.entry(*origin).or_insert(*edition);
            if *edition > *entry {
                *entry = *edition;
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
        other.0.iter().all(|(origin, edition)| {
            self.0
                .get(origin)
                .is_some_and(|watermark| watermark >= edition)
        })
    }

    /// The per-origin watermarks, sorted by origin. The iteration order
    /// is deterministic, which is what lets a signing payload commit to
    /// the context byte-for-byte.
    pub fn iter(&self) -> impl Iterator<Item = (&Origin, &Edition)> {
        self.0.iter()
    }

    /// How far this context reaches beyond `other`: the summed per-origin
    /// edition excess. This is a zero-read proxy for the size of the
    /// delta a replica holding this context would have to send a replica
    /// holding `other` — which is what lets a first-contact pull pick the
    /// cheaper merge direction from the two published watermarks alone.
    ///
    /// An origin absent from `other` contributes its full revision count,
    /// `edition + 1`: edition 0 is a real revision, and counting it 0
    /// would make `divergence == 0` coexist with `includes == false`
    /// (two disjoint genesis-only contexts diverge by nothing while
    /// neither includes the other).
    ///
    /// A known bias, documented rather than solved: editions are Lamport
    /// depths, not per-origin write counts, so for an origin `other` has
    /// never seen the excess measures how deep in history that origin
    /// writes, not how much it wrote. One fresh-session commit atop a
    /// deep adopted history weighs as the whole depth. The proxy stays a
    /// routing heuristic — soundness never depends on it — but the
    /// misrouting costs reads; a per-origin size hint on published heads
    /// would remove the bias.
    pub fn divergence(&self, other: &Context) -> u64 {
        self.0
            .iter()
            .map(|(origin, edition)| match other.0.get(origin) {
                Some(seen) => edition.value().saturating_sub(seen.value()),
                None => edition.value().saturating_add(1),
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

    /// Divergence counts an origin absent from the other side at its full
    /// revision count (`edition + 1`): edition 0 is a real revision, and
    /// counting it as 0 made `divergence == 0` coexist with
    /// `includes == false`.
    #[test]
    fn it_counts_unseen_origins_in_full_in_the_divergence() {
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

        // Partially seen origins count only the excess.
        let mut ahead = Context::new();
        ahead.record(version(1, 5));
        let mut behind = Context::new();
        behind.record(version(1, 2));
        assert_eq!(ahead.divergence(&behind), 3);
        assert_eq!(behind.divergence(&ahead), 0);
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
