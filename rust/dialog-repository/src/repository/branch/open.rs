use std::sync::{Arc, Mutex};

use crate::rules::RuleCache;
use crate::sealing::TreeSpace;
use crate::{Branch, BranchReference, Ephemeral, ResolveError};
use dialog_artifacts::history::{CausalityCache, ContextCache};
use dialog_artifacts::tree::spill_cache;
use dialog_capability::Provider;
use dialog_effects::branch::invalid_name;
use dialog_effects::memory::Resolve;
use dialog_query::concept::query::PlanCache;

/// Command to open a branch. Resolves the branch's revision and upstream
/// cells without ever erroring on a missing revision — a freshly-opened
/// branch that has never been committed to simply has `None` revision.
pub struct OpenBranch {
    branch: BranchReference,
    sealing: Option<TreeSpace>,
}

impl From<BranchReference> for OpenBranch {
    fn from(branch: BranchReference) -> Self {
        Self {
            branch,
            sealing: None,
        }
    }
}

impl OpenBranch {
    /// Open the branch as a sealed line under `space`: its commits persist
    /// layered envelopes instead of nodes, and its reads open them. See
    /// [`crate::sealing`].
    ///
    /// Sealing is a property of how this handle reads and writes, not of
    /// the branch's cells: a handle opened without the space reads a
    /// sealed line's heads but none of its tree.
    #[must_use]
    pub fn sealed(mut self, space: TreeSpace) -> Self {
        self.sealing = Some(space);
        self
    }

    /// Execute the open operation.
    ///
    /// A name that is not a branch name (see
    /// [`invalid_name`](dialog_effects::branch::invalid_name)) is refused
    /// before any cell is touched: a store lays a branch's cells out under
    /// its name, and a name such as `meta?x` or `x/../meta` would open
    /// another branch's cells, however it got here.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, ResolveError>
    where
        Env: Provider<Resolve>,
    {
        if let Some(reason) = invalid_name(self.branch.name()) {
            return Err(ResolveError::Storage(format!(
                "{:?} is not a branch name: {reason}",
                self.branch.name()
            )));
        }

        let revision = self.branch.revision();
        revision.resolve().perform(env).await?;

        let tracking = self.branch.tracking();
        tracking.resolve().perform(env).await?;

        let induction = self.branch.induction();
        induction.resolve().perform(env).await?;

        Ok(Branch {
            writer: Branch::writer_of(&self.branch),
            reference: self.branch,
            revision,
            tracking,
            induction,
            node_cache: dialog_search_tree::Cache::new(),
            spill_cache: spill_cache(),
            rule_cache: Arc::new(RuleCache::new()),
            plan_cache: PlanCache::default(),
            causality_cache: CausalityCache::new(),
            context_cache: ContextCache::new(),
            record_cache: dialog_search_tree::Cache::new(),
            spine: dialog_artifacts::SpineSlot::new(),
            identity_cache: Arc::new(Mutex::new(None)),
            metadata_cache: Arc::new(Mutex::new(None)),
            layer_metadata_cache: Arc::new(Mutex::new(None)),
            overlay: Ephemeral::default(),
            answers: Arc::default(),
            sealing: self.sealing,
        })
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    use crate::RepositoryMemoryExt;

    #[dialog_common::test]
    async fn it_opens_branch_with_no_revision() -> Result<()> {
        let provider = Volatile::new();
        let branch = Subject::from(did!("key:zBranchOpenTest"))
            .branch("main")
            .open()
            .perform(&provider)
            .await?;

        assert_eq!(branch.name(), "main");
        assert!(branch.revision().is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_reopens_same_branch() -> Result<()> {
        let provider = Volatile::new();
        let subject = Subject::from(did!("key:zBranchReopenTest"));

        subject.branch("main").open().perform(&provider).await?;
        let branch = subject.branch("main").open().perform(&provider).await?;

        assert_eq!(branch.name(), "main");
        Ok(())
    }

    /// A name that is not a branch name is refused on open and on
    /// load, not only on create and delete: a store lays a branch's
    /// cells out under its name, so `meta?x` or `x/../meta` would open
    /// the registry's cells, and a name that was stored somewhere is no
    /// more a name for having been stored.
    #[dialog_common::test]
    async fn it_refuses_to_open_a_name_that_is_not_a_branch_name() -> Result<()> {
        use crate::{LoadBranchError, ResolveError};

        let provider = Volatile::new();
        let subject = Subject::from(did!("key:zBranchOpenBadName"));

        for name in ["meta?x", "meta#x", "x/../meta", "mét@", ".meta", ""] {
            let opened = subject.branch(name).open().perform(&provider).await;
            assert!(
                matches!(&opened, Err(ResolveError::Storage(reason)) if reason.contains("not a branch name")),
                "opening {name:?}: {:?}",
                opened.map(|branch| branch.name().to_string())
            );

            let loaded = subject.branch(name).load().perform(&provider).await;
            assert!(
                matches!(
                    &loaded,
                    Err(LoadBranchError::Resolve(ResolveError::Storage(reason)))
                        if reason.contains("not a branch name")
                ),
                "loading {name:?}: {:?}",
                loaded.map(|branch| branch.name().to_string())
            );
        }
        Ok(())
    }
}
