use std::collections::HashSet;

use dialog_search_tree::Cache;

use crate::DialogArtifactsError;

use super::Context;
use super::{History, Version};

/// Derive the [`Context`] of `head` by walking its ancestry through the
/// history index's revision records.
///
/// Every revision in the ancestry is folded in; a missing record
/// surfaces as [`IncompleteHistory`](DialogArtifactsError::IncompleteHistory)
/// — a replica derives the context of its *own* head, whose ancestry
/// records it holds by construction (they arrived with the merges that
/// produced the head).
///
/// The walk is O(ancestry). Callers on a hot path should memoize per
/// head (the context of a fixed head never changes) or maintain the
/// vector incrementally: `context(commit) = context(parent) + own
/// version`, `context(merge) = union(parents' contexts) + own version`.
pub async fn context_of<H: History>(
    head: &Version,
    history: &H,
) -> Result<Context, DialogArtifactsError> {
    let mut context = Context::new();
    let mut visited: HashSet<Version> = HashSet::new();
    let mut frontier: Vec<Version> = vec![*head];
    visited.insert(*head);

    while let Some(version) = frontier.pop() {
        // `tally`, not `record`: the walk visits revisions in DAG order,
        // not edition order, and every visited revision is distinct (the
        // visited set guarantees it) — an advance-only fold would skip
        // counting a revision reached below an already-raised watermark.
        context.tally(version);
        let Some(record) = history.revision_record(&version).await? else {
            return Err(DialogArtifactsError::IncompleteHistory(format!(
                "{version}"
            )));
        };
        for parent in record.parents {
            if visited.insert(parent) {
                frontier.push(parent);
            }
        }
    }

    Ok(context)
}

/// Memoized causal contexts, keyed by head version.
///
/// The context of a *fixed* head never changes: history is append-only,
/// so later revisions extend the DAG above a head, never beneath it. An
/// entry therefore never needs invalidation, which is what makes sharing
/// one cache across every pull on a branch handle sound. Storage is
/// bounded ([`Cache`] evicts with SIEVE); an evicted context simply
/// re-derives by the ancestry walk.
///
/// The walk is the expensive path — O(ancestry) record reads, each with
/// an issuer-signature verification — so callers that know how a head
/// was *constructed* should derive its context incrementally and
/// [`insert`](Self::insert) it instead: `context(commit) =
/// context(parent) + own version`, and a pull can fold the versions of
/// the revision records riding its delta into the local context (see
/// `merge::observe_revisions`), paying zero extra reads.
#[derive(Clone, Debug, Default)]
pub struct ContextCache {
    contexts: Cache<Version, Context>,
}

impl ContextCache {
    /// A fresh, empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// The context of `head`: answered from memory when possible,
    /// otherwise derived by the O(ancestry) [`context_of`] walk and
    /// remembered.
    pub async fn context_of<H: History>(
        &self,
        head: &Version,
        history: &H,
    ) -> Result<Context, DialogArtifactsError> {
        Ok(self
            .contexts
            .get_or_fetch::<_, DialogArtifactsError>(head, async |head| {
                context_of(head, history).await.map(Some)
            })
            .await?
            .expect("the context fetcher always yields a context"))
    }

    /// The memoized context of `head`, if present. Never derives.
    pub async fn cached(&self, head: &Version) -> Option<Context> {
        self.contexts
            .get_or_fetch::<_, DialogArtifactsError>(head, async |_| Ok(None))
            .await
            .expect("a fetcher that returns Ok cannot fail")
    }

    /// Remember `context` as the context of `head` — for callers that
    /// derived it incrementally from the head's construction.
    pub fn insert(&self, head: Version, context: Context) {
        self.contexts.insert(head, context);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{Edition, Origin};

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
