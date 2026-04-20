use dialog_artifacts::ArtifactSelector;
use dialog_artifacts::selector::Constrained;

use super::Branch;
use super::select::Select;

/// The branch's artifact index with remote fallback.
///
/// Created by [`Branch::index`]. Queries will replicate blocks
/// from the remote on demand if the branch has a remote upstream.
pub struct BranchClaims<'a> {
    branch: &'a Branch,
}

impl<'a> BranchClaims<'a> {
    /// Select artifacts matching the selector.
    pub fn select(self, selector: ArtifactSelector<Constrained>) -> Select<'a> {
        Select::new(self.branch, selector)
    }
}

impl Branch {
    /// The branch's artifact index.
    ///
    /// Use `.select(selector).perform(&env)` to query artifacts.
    /// If the branch has a remote upstream, missing blocks are
    /// replicated on demand.
    pub fn claims(&self) -> BranchClaims<'_> {
        BranchClaims { branch: self }
    }
}
