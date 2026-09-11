use dialog_capability::Command;

use crate::selector::Constrained;
use crate::{ArtifactSelector, DialogArtifactsError};

/// Command for estimating how many artifacts a selector's key range spans,
/// without scanning them.
///
/// Answered by reading a single shallow index node (the tree root) and summing
/// the [`Scale`](dialog_search_tree::Scale)s of the children the selector's
/// range touches, so the cost is one block read rather than a full scan. The
/// result is an advisory upper bound on the entry count, suitable for a
/// planner choosing between join strategies, not an exact count.
///
/// A planner uses it to compare the range sizes of independent scans: a merge
/// that reads every range in full only wins over a nested loop when the ranges
/// are comparably broad, which this lets the evaluator check with real
/// tree-derived numbers instead of the cost model's fixed constants.
pub struct Estimate;

impl Command for Estimate {
    type Input = ArtifactSelector<Constrained>;
    /// An advisory upper-bound entry count for the selector's range. `None`
    /// when the tree is empty or the estimate is unavailable, in which case a
    /// caller should treat the range as unknown (maximally broad).
    type Output = Result<Option<u64>, DialogArtifactsError>;
}
