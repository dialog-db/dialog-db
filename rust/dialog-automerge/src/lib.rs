#![warn(missing_docs)]

//! Automerge CRDT documents as Dialog record values.
//!
//! This crate bridges [automerge](https://automerge.org) documents into
//! Dialog's [`RecordFormat`] so that a collaborative document can live as a
//! single atomic value in one `{the, of, is}` claim. It is an *edge* crate:
//! only applications that declare automerge-typed attributes link it — the
//! Dialog core (storage, sync, wasm blob) takes no automerge dependency.
//!
//! See [`notes/automerge-integration-spec.md`](https://github.com/dialog-db/dialog-db/blob/main/notes/automerge-integration-spec.md)
//! for the full design.
//!
//! ## Canonical bytes are identity
//!
//! A record value is stored, keyed, and compared by its bytes, so every
//! encode of the same document state must produce the same bytes — two
//! replicas that independently merge the same set of edits must mint the
//! identical value, and therefore the identical tree key.
//!
//! Automerge's own `save()` does not quite provide this: it encodes changes
//! in the order they entered the local change graph, and for *concurrent*
//! changes that order depends on the order the document merged them — the
//! same change-set can save to different bytes on different replicas.
//! [`canonical_bytes`] closes the gap by encoding changes in a deterministic
//! topological order (lexicographic change-hash tiebreak), making the bytes
//! a pure function of the change-set. Every [`RecordFormat`] in this crate
//! encodes through it, with [`canonical_options`] (no DEFLATE, no orphans) so
//! identity never depends on a compression library either.
//!
//! Canonical output is stable per automerge version, not across majors: every
//! participant that writes record bytes must pin the workspace's automerge
//! version, which this crate re-exports as [`automerge`].
//!
//! ## Documents must share ancestry
//!
//! Merge unifies documents that descend from a common ancestor. Two documents
//! created independently (two calls to [`TextDocument::new`]) do not share a
//! root object: merging them keeps both histories but only one side's content
//! is visible at the text root. Create a document once, then replicate it by
//! storing and realizing the record (or by [`TextDocument::fork`]).
//!
//! ## Editing sessions
//!
//! An open editor is a window during which concurrent writers keep
//! committing. [`DocumentSession`] — the doc-handle — holds a live in-memory
//! document through that window: it folds every stored sibling at open,
//! absorbs siblings that sync lands mid-session (pending local edits
//! survive), and commits through the typed layer so the write supersedes
//! every sibling it folded. See the [`session`](DocumentSession) docs for
//! the discipline and its rationale.
//!
//! [`RecordFormat`]: dialog_artifacts::RecordFormat

use std::collections::{BTreeMap, HashMap};

pub use automerge;
use automerge::{Automerge, Change, SaveOptions};
use dialog_artifacts::RecordError;

mod session;
pub use session::{DocumentSession, SessionError};

mod text;
pub use text::TextDocument;

/// The save configuration every [`RecordFormat`] in this crate encodes with.
///
/// - `deflate: false` — DEFLATE output is not specified across compressor
///   versions, so compressed bytes cannot serve as identity. Size is
///   reclaimed below the identity layer (block/wire compression), the same
///   arrangement git uses: hash uncompressed, compress at rest.
/// - `retain_orphans: false` — changes whose dependencies are missing are
///   not part of the document state and must not perturb its identity.
///
/// The struct is constructed field-by-field on purpose: if automerge grows a
/// new save option, this stops compiling and the canonical form must be
/// re-decided rather than silently drift.
///
/// [`RecordFormat`]: dialog_artifacts::RecordFormat
pub fn canonical_options() -> SaveOptions {
    SaveOptions {
        deflate: false,
        retain_orphans: false,
    }
}

/// Encode `document` such that the bytes are a pure function of its
/// change-set: any two documents holding the same changes — whatever order
/// they merged them in — encode byte-identically.
///
/// Automerge's saved form lists changes in change-graph insertion order,
/// which is deterministic for a linear history but merge-order-dependent once
/// concurrent changes exist (its own canonicality tests only cover
/// single-lineage histories). This function therefore dispatches on a
/// property of the change-set itself, so that both the path taken and its
/// output are the same on every replica:
///
/// - A *linear* history — single head, no change with more than one
///   dependency — has exactly one topological order, which insertion order
///   must equal; the document's own save is already canonical. This is the
///   never-diverged common case and costs nothing extra.
/// - Otherwise the document is rebuilt by applying its changes in canonical
///   order — a topological sort of the change DAG, smallest change hash
///   first among the concurrently-ready — at a cost proportional to the full
///   history, paid only by documents whose history has ever diverged.
///
/// No cheaper check on the diverged path is possible from outside: the order
/// [`Automerge::get_changes`] reports is sorted by `(actor, seq)`, not the
/// order `save` encodes, so a document's save order cannot be observed —
/// only reconstructed.
pub fn canonical_bytes(document: &Automerge) -> Result<Vec<u8>, RecordError> {
    let changes = document.get_changes(&[]);

    let linear =
        document.get_heads().len() == 1 && changes.iter().all(|change| change.deps().len() <= 1);
    if linear {
        return Ok(document.save_with_options(canonical_options()));
    }

    let order = canonical_order(&changes)?;
    let mut rebuilt = Automerge::new();
    rebuilt
        .apply_changes(order.into_iter().map(|index| changes[index].clone()))
        .map_err(|error| RecordError::Encode(error.to_string()))?;
    Ok(rebuilt.save_with_options(canonical_options()))
}

/// The canonical order of `changes`, as indices into the slice: a topological
/// sort of the dependency DAG that breaks ties between concurrently-ready
/// changes by their (content-addressed) hash.
fn canonical_order(changes: &[Change]) -> Result<Vec<usize>, RecordError> {
    let index_by_hash: HashMap<_, _> = changes
        .iter()
        .enumerate()
        .map(|(index, change)| (change.hash(), index))
        .collect();

    let mut blocking_dependencies = vec![0usize; changes.len()];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); changes.len()];
    for (index, change) in changes.iter().enumerate() {
        for dependency in change.deps() {
            let dependency = *index_by_hash.get(dependency).ok_or_else(|| {
                RecordError::Encode("change depends on a change outside the document".to_string())
            })?;
            blocking_dependencies[index] += 1;
            dependents[dependency].push(index);
        }
    }

    let mut ready: BTreeMap<_, _> = changes
        .iter()
        .enumerate()
        .filter(|(index, _)| blocking_dependencies[*index] == 0)
        .map(|(index, change)| (change.hash(), index))
        .collect();

    let mut order = Vec::with_capacity(changes.len());
    while let Some((_, index)) = ready.pop_first() {
        order.push(index);
        for &dependent in &dependents[index] {
            blocking_dependencies[dependent] -= 1;
            if blocking_dependencies[dependent] == 0 {
                ready.insert(changes[dependent].hash(), dependent);
            }
        }
    }

    if order.len() != changes.len() {
        return Err(RecordError::Encode(
            "dependency cycle among the document's changes".to_string(),
        ));
    }
    Ok(order)
}
#[cfg(test)]
mod tests {
    use dialog_artifacts::RecordFormat;

    use super::*;
    use crate::TextDocument;

    /// Rebuild `document` from scratch in canonical order, bypassing the
    /// linear fast path.
    fn rebuilt_bytes(document: &Automerge) -> Vec<u8> {
        let changes = document.get_changes(&[]);
        let order = canonical_order(&changes).unwrap();
        let mut rebuilt = Automerge::new();
        rebuilt
            .apply_changes(order.into_iter().map(|index| changes[index].clone()))
            .unwrap();
        rebuilt.save_with_options(canonical_options())
    }

    /// The fast path's soundness condition: for a linear history, the
    /// document's direct save equals a from-scratch rebuild.
    #[test]
    fn linear_fast_path_agrees_with_rebuild() {
        let mut document = TextDocument::new();
        document.splice(0, 0, "a strictly linear history").unwrap();
        document.splice(0, 0, "with ").unwrap();

        let fast = canonical_bytes(document.as_automerge()).unwrap();
        assert_eq!(fast, rebuilt_bytes(document.as_automerge()));
    }

    /// Documents with no shared ancestry at all (concurrent root changes)
    /// still encode identically whichever way they were merged. The
    /// repetition covers random actor-id draws, which permute the change
    /// hashes that canonical ordering tie-breaks on: a single round only
    /// exercises one permutation.
    #[test]
    fn dual_init_merges_encode_identically() {
        for _ in 0..64 {
            let mut one = TextDocument::new();
            let mut other = TextDocument::new();
            one.splice(0, 0, "one").unwrap();
            other.splice(0, 0, "other").unwrap();

            let ab = TextDocument::merge(&one, &other).encode().unwrap();
            let ba = TextDocument::merge(&other, &one).encode().unwrap();
            assert_eq!(ab, ba);
        }
    }

    /// A merged (non-linear) history takes the rebuild path and yields the
    /// rebuild's bytes exactly.
    #[test]
    fn diverged_history_encodes_as_the_rebuild() {
        let mut base = TextDocument::new();
        base.splice(0, 0, "base").unwrap();
        let mut left = base.fork();
        let mut right = base.fork();
        left.splice(0, 0, "L").unwrap();
        right.splice(4, 0, "R").unwrap();

        let merged = TextDocument::merge(&left, &right);
        assert_eq!(
            canonical_bytes(merged.as_automerge()).unwrap(),
            rebuilt_bytes(merged.as_automerge())
        );
    }

    /// The empty document — no changes, no heads — encodes deterministically
    /// and round-trips.
    #[test]
    fn empty_document_encodes_and_round_trips() {
        let document = Automerge::new();
        let bytes = canonical_bytes(&document).unwrap();
        let reloaded = Automerge::load(&bytes).unwrap();
        assert_eq!(canonical_bytes(&reloaded).unwrap(), bytes);
    }
}
