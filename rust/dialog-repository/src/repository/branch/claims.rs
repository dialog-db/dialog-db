use crate::{Branch, Select};
use dialog_corpus::{Selector, selector::Constrained};

/// The branch's fact index with remote fallback.
///
/// Created by [`Branch::index`]. Queries will replicate blocks
/// from the remote on demand if the branch has a remote upstream.
pub struct BranchClaims<'a> {
    branch: &'a Branch,
}

impl<'a> BranchClaims<'a> {
    /// Select facts matching the selector.
    pub fn select(self, selector: Selector<Constrained>) -> Select<'a> {
        Select::new(self.branch, selector)
    }
}

impl Branch {
    /// The branch's fact index.
    ///
    /// Use `.select(selector).perform(&env)` to query facts.
    /// If the branch has a remote upstream, missing blocks are
    /// replicated on demand.
    pub fn claims(&self) -> BranchClaims<'_> {
        BranchClaims { branch: self }
    }
}
