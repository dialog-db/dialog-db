use std::sync::{Arc, Mutex};

use crate::{Branch, BranchReference, Ephemeral, HeldCaches, ResolveError};
use dialog_capability::Provider;
use dialog_common::Holds;
use dialog_effects::branch::invalid_name;
use dialog_effects::memory::Resolve;

/// Command to open a branch. Resolves the branch's revision and upstream
/// cells without ever erroring on a missing revision — a freshly-opened
/// branch that has never been committed to simply has `None` revision.
pub struct OpenBranch {
    branch: BranchReference,
}

impl From<BranchReference> for OpenBranch {
    fn from(branch: BranchReference) -> Self {
        Self { branch }
    }
}

impl OpenBranch {
    /// Execute the open operation.
    ///
    /// A name that is not a branch name (see
    /// [`invalid_name`](dialog_effects::branch::invalid_name)) is refused
    /// before any cell is touched: a store lays a branch's cells out under
    /// its name, and a name such as `meta?x` or `x/../meta` would open
    /// another branch's cells, however it got here.
    ///
    /// The branch reads and commits through the caches `env` holds for it
    /// (see [`HeldCaches`](crate::HeldCaches)), so opening it again through
    /// the same environment finds them warm.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, ResolveError>
    where
        Env: Provider<Resolve> + Holds,
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

        let caches = HeldCaches::of(env).branch(&self.branch);

        Ok(Branch {
            writer: Branch::writer_of(env, &self.branch),
            reference: self.branch,
            revision,
            tracking,
            induction,
            node_cache: caches.nodes,
            spill_cache: caches.spills,
            rule_cache: caches.rules,
            plan_cache: caches.plans,
            causality_cache: caches.causality,
            context_cache: caches.contexts,
            record_cache: caches.records,
            spine: caches.spine,
            identity_cache: Arc::new(Mutex::new(None)),
            metadata_cache: Arc::new(Mutex::new(None)),
            layer_metadata_cache: Arc::new(Mutex::new(None)),
            overlay: Ephemeral::default(),
            answers: Arc::default(),
        })
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use std::sync::Arc;

    use anyhow::Result;
    use dialog_capability::{Command, Provider, Subject};
    use dialog_common::{Held, Holdings, Holds};
    use dialog_effects::memory::Resolve;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    use crate::RepositoryMemoryExt;

    /// A volatile store as an environment that holds what its branches
    /// leave with it, the way a peer does. `Volatile` alone holds nothing.
    #[derive(Default)]
    struct Holding {
        storage: Volatile,
        holdings: Holdings,
    }

    impl Holds for Holding {
        fn held(&self, key: &str) -> Option<Held> {
            self.holdings.held(key)
        }

        fn hold(&self, key: String, handle: Held) {
            self.holdings.hold(key, handle)
        }

        fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
            self.holdings.held_or(key, make)
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Provider<Resolve> for Holding {
        async fn execute(
            &self,
            input: <Resolve as Command>::Input,
        ) -> <Resolve as Command>::Output {
            Provider::<Resolve>::execute(&self.storage, input).await
        }
    }

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

    /// The handles of a branch opened through one environment share its
    /// writer, so their commits and pulls take turns. Another branch has
    /// its own, and so does the same branch opened through another
    /// environment.
    #[dialog_common::test]
    async fn it_shares_a_writer_between_handles_of_one_environment() -> Result<()> {
        let provider = Holding::default();
        let elsewhere = Holding::default();
        let subject = Subject::from(did!("key:zBranchWriterTest"));

        let first = subject.branch("main").open().perform(&provider).await?;
        let second = subject.branch("main").open().perform(&provider).await?;
        let other = subject.branch("feature").open().perform(&provider).await?;
        let apart = subject.branch("main").open().perform(&elsewhere).await?;

        assert!(Arc::ptr_eq(&first.writer(), &second.writer()));
        assert!(!Arc::ptr_eq(&first.writer(), &other.writer()));
        assert!(!Arc::ptr_eq(&first.writer(), &apart.writer()));
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
