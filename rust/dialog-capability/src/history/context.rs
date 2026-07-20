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
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Context(BTreeMap<Origin, Edition>);

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
    /// edition excess. Editions count writes, so this is a zero-read
    /// proxy for the size of the delta a replica holding this context
    /// would have to send a replica holding `other` — which is what lets
    /// a first-contact pull pick the cheaper merge direction from the
    /// two published watermarks alone.
    pub fn divergence(&self, other: &Context) -> u64 {
        self.0
            .iter()
            .map(|(origin, edition)| {
                let seen = other
                    .0
                    .get(origin)
                    .map(|edition| edition.value())
                    .unwrap_or(0);
                edition.value().saturating_sub(seen)
            })
            .sum()
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
