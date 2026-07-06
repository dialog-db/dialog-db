use std::collections::{HashMap, HashSet};

use dialog_common::ConditionalSync;

use crate::{Attribute, DialogArtifactsError, Entity};

use super::{Claim, RevisionRecord, Version};

/// The causal relationship between two claims on the same
/// `(entity, attribute)`
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Causality {
    /// Both claims were produced by the same revision; neither supersedes the
    /// other
    Equal,
    /// The first claim has seen and superseded the second
    Supersedes,
    /// The first claim has been seen and superseded by the second
    Superseded,
    /// Neither claim has seen the other
    Concurrent,
}

impl Causality {
    /// This relationship as seen from the other claim's perspective
    pub fn inverse(&self) -> Self {
        match self {
            Causality::Supersedes => Causality::Superseded,
            Causality::Superseded => Causality::Supersedes,
            other => *other,
        }
    }
}

/// Read access to the history index, sufficient to traverse claim lineages
/// and the revision DAG.
///
/// Uses native `async fn` (rather than `async_trait`'s boxed futures) so that
/// implementations over [`dialog_search_tree`]'s borrowing streams do not
/// have to promise `Send` futures; executors that require `Send` can demand
/// it at the call site.
#[allow(async_fn_in_trait)]
pub trait History: ConditionalSync {
    /// The claims written to `(of, the)` by the revision identified by
    /// `version`. Multiple claims are possible for cardinality-many
    /// attributes; an empty result means the claims have not been replicated.
    async fn claims_at(
        &self,
        version: &Version,
        of: &Entity,
        the: &Attribute,
    ) -> Result<Vec<Claim>, DialogArtifactsError>;

    /// The [`RevisionRecord`] minted by the revision identified by
    /// `version` — its parents, skip links, and attribution, atomically.
    /// `None` means the revision has not been replicated.
    async fn revision_record(
        &self,
        version: &Version,
    ) -> Result<Option<RevisionRecord>, DialogArtifactsError>;
}

/// Determine the causal relationship between two claims on the same
/// `(entity, attribute)`, each paired with the [`Version`] of the revision
/// that produced it.
///
/// Detection proceeds in tiers, per `notes/version-control.md`:
///
/// - **Tier 0** (O(1), no reads): same version means causally equal; same
///   edition with different origins means concurrent (neither can have seen
///   the other, since seeing it would have forced a higher edition); same
///   origin means causally ordered by edition (an origin is a single
///   sequential actor).
/// - **Tier 1** (O(1)): if either claim's version appears directly in the
///   other's cause, the latter supersedes it.
/// - **Tier 2** (O(k)): traverse the higher-edition claim's causal history
///   backward through the history index looking for the lower-edition
///   claim's version. Because a cause may contain multiple entries, the
///   history is a DAG rather than a chain: traversal maintains a frontier of
///   unvisited versions. Editions strictly decrease along every causal path,
///   so a branch is pruned as soon as its edition is at or below the target
///   edition without matching the target version.
///
/// If the traversal encounters a version whose claims have not been
/// replicated, causal ordering cannot be determined locally and an
/// [`DialogArtifactsError::IncompleteHistory`] error is returned: a partial
/// replica does not have enough information to resolve conflicts it has not
/// fully received yet.
pub async fn causality<H: History>(
    (a, a_version): (&Claim, &Version),
    (b, b_version): (&Claim, &Version),
    history: &H,
) -> Result<Causality, DialogArtifactsError> {
    debug_assert_eq!(
        (&a.of, &a.the),
        (&b.of, &b.the),
        "causality is only defined between claims on the same (entity, attribute)"
    );

    // Tier 0: version comparison, no reads
    if a_version == b_version {
        return Ok(Causality::Equal);
    }
    if a_version.origin == b_version.origin {
        return Ok(if a_version.edition > b_version.edition {
            Causality::Supersedes
        } else {
            Causality::Superseded
        });
    }
    if a_version.edition == b_version.edition {
        return Ok(Causality::Concurrent);
    }

    // Only the higher edition can have seen the lower one: the lower edition
    // cannot have seen something with a higher edition.
    let (higher, target, relationship) = if a_version.edition > b_version.edition {
        ((a, a_version), b_version, Causality::Supersedes)
    } else {
        ((b, b_version), a_version, Causality::Superseded)
    };

    // Tier 1: direct cause check
    if higher.0.cause.contains(target) {
        return Ok(relationship);
    }

    // Tier 2: cause traversal through the history index
    let mut visited = HashSet::new();
    let mut frontier: Vec<Version> = Vec::new();
    for version in higher.0.cause.versions() {
        if visited.insert(*version) {
            frontier.push(*version);
        }
    }

    while let Some(version) = frontier.pop() {
        if version == *target {
            return Ok(relationship);
        }
        if version.edition <= target.edition {
            // Editions strictly decrease along every causal path: nothing
            // reachable from here can match the target.
            continue;
        }

        let claims = history
            .claims_at(&version, &higher.0.of, &higher.0.the)
            .await?;
        if claims.is_empty() {
            return Err(DialogArtifactsError::IncompleteHistory(format!(
                "{version}"
            )));
        }
        for claim in claims {
            for cause in claim.cause.versions() {
                if visited.insert(*cause) {
                    frontier.push(*cause);
                }
            }
        }
    }

    Ok(Causality::Concurrent)
}

/// Find a common ancestor of two revisions by traversing the revision DAG
/// backward via `cause` pointers, expanding the frontier in descending
/// edition order. Returns the first version reachable from both heads (the
/// one with the greatest edition), or `None` when the lineages share no
/// history.
///
/// Where the traversed revisions recorded skip links (see
/// [`extend_skips`](super::extend_skips)), long linear runs are leapt over
/// instead of walked edge by edge, so a head far ahead of the other
/// descends to the other's causal depth in O(log gap) reads. The result is
/// unchanged: a leap never crosses a merge (merges record no skip table),
/// and no leap descends past the horizon — the lower head's edition —
/// below which every remaining ancestor of the lower head lives. The
/// region a leap jumps over is therefore strictly linear and strictly
/// above anything the other side can reach, so nothing the stepwise walk
/// would have found is missed.
pub async fn common_ancestor<H: History>(
    a: &Version,
    b: &Version,
    history: &H,
) -> Result<Option<Version>, DialogArtifactsError> {
    use std::collections::BinaryHeap;

    if a == b {
        return Ok(Some(*a));
    }

    // No common ancestor can sit above the lower head: editions strictly
    // decrease along causal paths. Leaps must not descend past this line.
    let horizon = a.edition.min(b.edition);

    // Which heads (bit 0b01 = a, bit 0b10 = b) each version is reachable from
    let mut reached: HashMap<Version, u8> = HashMap::new();
    let mut frontier = BinaryHeap::new();

    reached.insert(*a, 0b01);
    reached.insert(*b, 0b10);
    frontier.push((*a, 0b01u8));
    frontier.push((*b, 0b10u8));

    while let Some((version, side)) = frontier.pop() {
        let reachable = reached.get(&version).copied().unwrap_or(side);
        if reachable == 0b11 {
            return Ok(Some(version));
        }

        let Some(record) = history.revision_record(&version).await? else {
            return Err(DialogArtifactsError::IncompleteHistory(format!(
                "{version}"
            )));
        };

        // The farthest recorded leap that stays at or above the horizon.
        // Deepest target = longest leap.
        let leap = record
            .skips
            .iter()
            .filter(|target| target.edition >= horizon)
            .min_by_key(|target| target.edition)
            .copied();

        let mut push = |version: Version| {
            let entry = reached.entry(version).or_insert(0);
            if *entry & side != side {
                *entry |= side;
                frontier.push((version, side));
            }
        };

        match leap {
            // A single-parent revision with a safe leap: the leapt region
            // is linear (skip chains never cross merges), so the parent
            // edge is subsumed by the leap.
            Some(target) if record.parents.len() == 1 => push(target),
            // A merge, or a leap that would cross the horizon: walk the
            // parent edges. (A merge records no skips, but if one ever
            // carried both, following both stays exact — just redundant.)
            Some(target) => {
                record.parents.into_iter().for_each(&mut push);
                push(target);
            }
            None => record.parents.into_iter().for_each(&mut push),
        }
    }

    Ok(None)
}
