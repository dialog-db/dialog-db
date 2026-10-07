use std::collections::{BinaryHeap, HashSet};

use crate::DialogArtifactsError;

use super::{History, RevisionRecord, Version};

/// The revisions reachable from `head`, newest first.
///
/// Walks the revision DAG through each record's parents, yielding at
/// most `limit` `(version, record)` pairs in reverse topological order:
/// every revision appears before any of its ancestors. The order is
/// total and deterministic — versions sort by causal depth (edition,
/// ties broken by origin), and a parent's edition is always strictly
/// below its child's, so a max-heap on the frontier suffices, with no
/// bookkeeping beyond the visited set.
///
/// Replication holes truncate rather than fail: a parent whose record
/// has not been replicated is skipped, along with everything reachable
/// only through it — the log lists what this replica can vouch for.
/// And "vouch" is literal: [`History`] implementations over
/// peer-supplied storage verify each record's signature and slot
/// binding on read (see [`TreeHistory`](super::TreeHistory)), so a
/// forged record errors rather than lies.
pub async fn log<H: History>(
    head: &Version,
    history: &H,
    limit: usize,
) -> Result<Vec<(Version, RevisionRecord)>, DialogArtifactsError> {
    let mut frontier = BinaryHeap::new();
    let mut seen = HashSet::new();
    let mut entries = Vec::new();

    seen.insert(*head);
    frontier.push(*head);

    while let Some(version) = frontier.pop() {
        if entries.len() >= limit {
            break;
        }
        let Some(record) = history.revision_record(&version).await? else {
            continue;
        };
        for parent in &record.parents {
            if seen.insert(*parent) {
                frontier.push(*parent);
            }
        }
        entries.push((version, record));
    }

    Ok(entries)
}

/// The revisions reachable from `head` that `known` does not hold, ancestors
/// first: what adopting `head` would bring into a replica whose history is
/// `known`.
///
/// Walks the revision DAG of `upstream` from `head` through each record's
/// parents, and stops at each revision that `known` records — the replica
/// holds it, and everything it leads to, already. The result is in causal
/// order: every revision after each of its ancestors that it lists.
///
/// Unlike [`log`], a hole is an error. [`log`] lists what a replica can
/// vouch for, and skips a revision whose record it lacks; a puller deciding
/// whether to adopt `head` must see every revision that it would bring in,
/// so a revision that neither `upstream` nor `known` records — a head whose
/// history its peer does not serve — fails the walk with
/// [`DialogArtifactsError::IncompleteHistory`], and nothing of `head` can be
/// vouched for.
pub async fn novelty<U: History, K: History>(
    head: &Version,
    upstream: &U,
    known: &K,
) -> Result<Vec<(Version, RevisionRecord)>, DialogArtifactsError> {
    let mut frontier = BinaryHeap::new();
    let mut seen = HashSet::new();
    let mut entries = Vec::new();

    seen.insert(*head);
    frontier.push(*head);

    while let Some(version) = frontier.pop() {
        if known.revision_record(&version).await?.is_some() {
            continue;
        }
        let Some(record) = upstream.revision_record(&version).await? else {
            return Err(DialogArtifactsError::IncompleteHistory(format!(
                "the history of {head:?} holds the revision {version:?}, \
                 whose record is not present"
            )));
        };
        for parent in &record.parents {
            if seen.insert(*parent) {
                frontier.push(*parent);
            }
        }
        entries.push((version, record));
    }

    // The heap yields every revision before its ancestors.
    entries.reverse();
    Ok(entries)
}
