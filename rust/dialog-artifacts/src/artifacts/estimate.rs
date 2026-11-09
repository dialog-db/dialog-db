use dialog_capability::Command;

use crate::selector::Constrained;
use crate::{ArtifactSelector, DialogArtifactsError};

/// Command for estimating how many artifacts a selector's key range spans,
/// without scanning them.
///
/// Answered from the range's two edge paths: children the range covers
/// whole contribute their [`Scale`](dialog_search_tree::Scale) unread, the
/// edge children are descended (at most two blocks per level, the blocks a
/// scan reads first and last anyway), and a leaf is counted exactly. The
/// result is an advisory upper bound on the entry count, suitable for a
/// planner choosing between join strategies, not an exact count.
///
/// A planner uses it to compare the range sizes of independent scans: a merge
/// that reads every range in full only wins over a nested loop when the ranges
/// are comparably broad, which this lets the evaluator check with real
/// tree-derived numbers instead of the cost model's fixed constants.
pub struct Estimate;

/// What an [`Estimate`] is asked: a selector's range, and optionally the
/// count past which its exact size does not matter.
#[derive(Clone)]
pub struct EstimateRequest {
    /// The range to estimate.
    pub selector: ArtifactSelector<Constrained>,
    /// When set, the estimate may stop once its count passes `limit` and
    /// report the count so far: some number above `limit`, at most what
    /// the whole estimate would be. A range whose estimate is at most
    /// `limit` counts exactly what it counts without one. A planner asking
    /// only whether a range is wider than some bound passes the bound, and
    /// a broad range is then answered from its top levels.
    pub limit: Option<u64>,
}

impl From<ArtifactSelector<Constrained>> for EstimateRequest {
    fn from(selector: ArtifactSelector<Constrained>) -> Self {
        Self {
            selector,
            limit: None,
        }
    }
}

impl Command for Estimate {
    type Input = EstimateRequest;
    /// An advisory upper-bound entry count for the selector's range. `None`
    /// when the tree is empty or the estimate is unavailable, in which case a
    /// caller should treat the range as unknown (maximally broad).
    type Output = Result<Option<u64>, DialogArtifactsError>;
}
