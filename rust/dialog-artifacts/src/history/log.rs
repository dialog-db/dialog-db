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
