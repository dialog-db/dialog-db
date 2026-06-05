//! Querying a transaction's uncommitted writes.
//!
//! [`Transaction::query`](crate::repository::branch::Transaction::query)
//! returns a [`TransactionQuery`] that surfaces the transaction's
//! pending writes against the underlying branch — "as-if committed"
//! semantics so a caller can run normal queries mid-transaction.
//!
//! # Pending asserts and retracts
//!
//! The transaction's pending [`Changes`] flow directly into the query
//! engine through `Provider<Select> for Changes` — no in-memory tree
//! materialization. Asserts/Replaces surface as positive facts unioned
//! with the branch's stream; Retracts lift into tombstones (via
//! [`tombstones_from`]) that filter matching branch facts via
//! [`filter_tombstones`] before the merge, so a `tx.retract(x)` shadows
//! `x` in the underlying branch view without modifying the branch's
//! persistent tree.
//!
//! # Tombstone scope: source-only
//!
//! Tombstones suppress facts only in the branch source, not the
//! pending Changes overlay. So `tx.retract(X).assert(X)` correctly
//! shows `X`: the pending Changes surface `X` via Provider<Select>;
//! the branch's `X`, if any, is tombstoned but the overlay's `X`
//! passes through unmodified.
//!
//! # Auto-injected schema metadata
//!
//! `Transaction::query` resolves the operator's identity at
//! `.perform(env)` time and folds in the same schema-metadata overlay
//! that [`Branch::query`](crate::Branch::query) does (see
//! [`QueryLayer::overlay`](crate::QueryLayer::overlay)). So
//! [`Session`](crate::schema::Session) /
//! [`SessionBranch`](crate::schema::SessionBranch) / per-branch
//! [`BranchMetadata`](super::super::metadata::BranchMetadata) are
//! visible mid-transaction exactly the way they are post-commit.
//!
//! # Non-composable
//!
//! [`TransactionQuery`] intentionally has no `.with(...)` chain — a
//! transaction's view is the branch plus its own pending changes,
//! not a session you can layer more sources onto. To compose more
//! sources, commit the transaction first and use
//! [`Branch::query`](crate::Branch::query) (which gives full
//! `.with(...)` / `.join(...)` composition on the session).

use std::collections::HashSet;

use async_trait::async_trait;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    ArtifactSelector, ArtifactStream, Changes, DialogArtifactsError, Select, SortKey,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use dialog_query::source::SelectRules;

use crate::Branch;
use crate::RemoteSite;
use crate::layer::{filter_tombstones, merge_grouped, tombstones_from};
use crate::repository::branch::QueryLayer;
use crate::repository::branch::select_from_branch;

/// A non-composable query handle returned by
/// [`Transaction::query`](crate::repository::branch::Transaction::query).
///
/// Holds an immutable snapshot of the transaction's pending changes
/// plus a reference to the branch. The transaction itself remains
/// open and committable.
///
/// See module docs for tombstone semantics.
pub struct TransactionQuery<'a> {
    branch: &'a Branch,
    changes: Changes,
}

impl<'a> TransactionQuery<'a> {
    pub(crate) fn new(branch: &'a Branch, changes: &Changes) -> Self {
        Self {
            branch,
            changes: changes.clone(),
        }
    }

    /// Stage a query against this transaction's view. Call
    /// [`perform`](TransactionSelectQuery::perform) to execute.
    pub fn select<Q: Application>(self, query: Q) -> TransactionSelectQuery<'a, Q> {
        TransactionSelectQuery {
            branch: self.branch,
            changes: self.changes,
            query,
        }
    }
}

/// A staged query on a [`TransactionQuery`].
pub struct TransactionSelectQuery<'a, Q> {
    branch: &'a Branch,
    changes: Changes,
    query: Q,
}

impl<'a, Q: Application> TransactionSelectQuery<'a, Q> {
    /// Execute the query, returning a stream of results.
    ///
    /// Mirrors [`SelectQuery::perform`](crate::SelectQuery::perform):
    /// resolves the operator's identity via [`Identify`], builds the
    /// per-query overlay (pending transaction changes + auto-injected
    /// schema metadata) through
    /// [`QueryLayer::overlay`](crate::QueryLayer::overlay), lifts any
    /// retracts in it into tombstones, and unions the branch stream
    /// (tombstone-filtered) with the overlay. The schema-metadata
    /// pass is what keeps `Session` / `SessionBranch` /
    /// `BranchMetadata` visible mid-transaction.
    pub fn perform<Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let TransactionSelectQuery {
            branch,
            changes,
            query,
        } = self;
        async_stream::try_stream! {
            let operator = Identify
                .perform(env)
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("identify: {e}")))?;

            // Route through the same QueryLayer overlay path
            // Branch::query() uses, so schema-injected metadata
            // (Session, SessionBranch, per-branch BranchMetadata)
            // surfaces alongside the transaction's pending changes.
            // `with(changes)` preserves Assert/Replace/Retract
            // polarity via `Statement for Changes`, so the user's
            // retracts stay retracts and lift into tombstones below.
            let overlay = QueryLayer::from(branch)
                .with(changes)
                .overlay(&operator);
            let tombstones = tombstones_from(&overlay);

            let trans_env = TransactionEnv {
                branch,
                changes: overlay,
                tombstones,
                env,
            };
            let results = Box::pin(query.perform(&trans_env));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// The runtime env serving a [`TransactionSelectQuery`].
///
/// Unions the (tombstone-filtered) branch stream with the pending
/// [`Changes`] overlay via `Provider<Select> for Changes`. Built per
/// `.perform(env)` so the env reference is never captured on the
/// outer [`TransactionQuery`].
pub(crate) struct TransactionEnv<'a, Env> {
    branch: &'a Branch,
    changes: Changes,
    tombstones: HashSet<SortKey>,
    env: &'a Env,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<'a, Env> Provider<Select<'a>> for TransactionEnv<'a, Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        // Branch stream — filtered by tombstones from the pending
        // retracts. Tombstones touch only the branch source; the
        // overlay's facts (below) pass through unfiltered so a
        // `retract(x).assert(x)` pattern surfaces `x` correctly.
        let raw = select_from_branch(self.branch, self.env, input.clone()).await?;
        let filtered_branch = filter_tombstones(raw, self.tombstones.clone());

        // Pending changes overlay — Changes itself is a Provider<Select>.
        let overlay = Provider::<Select<'a>>::execute(&self.changes, input).await?;

        Ok(merge_grouped(vec![filtered_branch, overlay]))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env: ConditionalSync> Provider<SelectRules> for TransactionEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        // Surfaces only the implicit per-descriptor rule each
        // `ConceptDescriptor` carries.
        Ok(ConceptRules::new(&input))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use crate::schema;
    use crate::schema::DidExt as _;
    use dialog_artifacts::Entity;
    use dialog_query::query::Output;
    use dialog_query::{Concept, Query, Term, the};

    mod people {
        /// `test/name` attribute used by the Person concept tests.
        #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("test")]
        pub struct Name(
            /// The person's name string.
            pub String,
        );
    }

    /// A simple concept used to exercise transaction-query semantics.
    #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Person {
        /// The person entity.
        pub this: Entity,
        /// Their `test/name` attribute.
        pub name: people::Name,
    }

    #[dialog_common::test]
    async fn it_surfaces_pending_asserts_through_transaction_query() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        let tx = branch.transaction().assert(Person {
            this: alice.clone(),
            name: people::Name("Alice".into()),
        });

        let results: Vec<Person> = tx
            .query()
            .select(Query::<Person> {
                this: alice.clone().into(),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].this, alice);
        assert_eq!(results[0].name.0, "Alice");
        Ok(())
    }

    #[dialog_common::test]
    async fn it_tombstones_pending_retracts_through_transaction_query() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        branch
            .transaction()
            .assert(Person {
                this: alice.clone(),
                name: people::Name("Alice".into()),
            })
            .assert(Person {
                this: bob.clone(),
                name: people::Name("Bob".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let tx = branch
            .transaction()
            .retract(the!("test/name").of(alice.clone()).is("Alice".to_string()));

        let names: Vec<String> = tx
            .query()
            .select(Query::<Person> {
                this: Term::var("this"),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|p| p.name.0)
            .collect();
        assert_eq!(names, vec!["Bob".to_string()]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_keeps_value_when_retract_is_followed_by_assert() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        branch
            .transaction()
            .assert(Person {
                this: alice.clone(),
                name: people::Name("Alice".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let stmt = the!("test/name").of(alice.clone()).is("Alice".to_string());
        let tx = branch.transaction().retract(stmt.clone()).assert(stmt);

        let names: Vec<String> = tx
            .query()
            .select(Query::<Person> {
                this: Term::var("this"),
                name: Term::var("name"),
            })
            .perform(&operator)
            .try_vec()
            .await?
            .into_iter()
            .map(|p| p.name.0)
            .collect();
        assert_eq!(names, vec!["Alice".to_string()]);
        Ok(())
    }

    /// `Transaction::query()` must surface the same auto-injected
    /// session metadata that `Branch::query()` does. The txn view is
    /// "branch + pending changes" — schema-shaped facts the branch
    /// auto-materializes (`Session`, `SessionBranch`, …) must pass
    /// through unchanged, even when the transaction is empty.
    ///
    /// Counterpart to `repository::tests::it_auto_includes_session_facts`.
    #[dialog_common::test]
    async fn it_auto_includes_session_facts_in_transaction_query() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let from_branch: Vec<schema::Session> = branch
            .query()
            .select(Query::<schema::Session> {
                this: schema::Session::entity().into(),
                profile: Term::var("profile"),
                operator: Term::var("operator"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(from_branch.len(), 1, "Branch::query() must see the Session");

        let from_txn: Vec<schema::Session> = branch
            .transaction()
            .query()
            .select(Query::<schema::Session> {
                this: schema::Session::entity().into(),
                profile: Term::var("profile"),
                operator: Term::var("operator"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(
            from_txn, from_branch,
            "Transaction::query() must see the same Session row as Branch::query()"
        );
        assert_eq!(from_txn[0].profile.0, profile.did().this());
        Ok(())
    }

    /// The schema-metadata overlay used by `Transaction::query()`
    /// must stay isolated to the query path: it must never leak into
    /// the transaction's committable `Changes`. So a transaction that
    /// only queries (no `.assert` / `.retract`) and then commits must
    /// land zero metadata facts on the branch tree.
    #[dialog_common::test]
    async fn it_does_not_leak_session_metadata_into_commits() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        // Run a query that sees Session via the txn-query overlay,
        // then commit the (otherwise empty) transaction.
        let tx = branch.transaction();
        let seen: Vec<schema::Session> = tx
            .query()
            .select(Query::<schema::Session> {
                this: schema::Session::entity().into(),
                profile: Term::var("profile"),
                operator: Term::var("operator"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(seen.len(), 1, "txn-query must see Session");
        tx.commit().perform(&operator).await?;

        // After commit, the branch tree must not contain any
        // `dialog.session/*` facts — those are auto-materialized at
        // query time, not persisted. Confirm by raw attribute lookup
        // through the branch's underlying claim stream.
        use dialog_query::AttributeQuery;
        use dialog_query::attribute::The;
        let profile_facts: Vec<dialog_query::Claim> = branch
            .query()
            .select(AttributeQuery::from(
                Term::<The>::from(the!("dialog.session/profile"))
                    .of(Term::<Entity>::var("e"))
                    .is(Term::<Entity>::var("v")),
            ))
            .perform(&operator)
            .try_vec()
            .await?;
        // `branch.query()` re-injects metadata at query time, so this
        // returns one row — but it's coming from the *overlay*, not
        // the persisted tree. Persisted metadata would mean two rows
        // (one from tree, one from overlay) post commit, since the
        // overlay isn't deduplicated against the tree.
        assert_eq!(
            profile_facts.len(),
            1,
            "metadata must come from the query-time overlay only, \
             not be persisted into the branch tree on commit; \
             saw {profile_facts:?}"
        );
        Ok(())
    }

    /// `dialog.session/branch` is cardinality-many and one row per
    /// branch in scope is asserted by the metadata pass. The
    /// transaction-query path must reproduce the full set.
    ///
    /// Counterpart to
    /// `repository::tests::it_auto_includes_session_branch_attribute_per_branch_in_scope`.
    #[dialog_common::test]
    async fn it_auto_includes_session_branch_in_transaction_query() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let main = repo.branch("main").open().perform(&operator).await?;

        let origin = schema::Origin::new(profile.did(), main.of().clone());
        let main_branch = schema::Branch::new(&origin, "main");

        let session_entity = schema::Session::entity();
        let rows: Vec<schema::SessionBranch> = main
            .transaction()
            .query()
            .select(Query::<schema::SessionBranch> {
                this: session_entity.into(),
                branch: Term::var("branch"),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        let got: Vec<Entity> = rows.into_iter().map(|r| r.branch.0).collect();
        assert_eq!(
            got,
            vec![main_branch.this],
            "Transaction::query() must see the SessionBranch row for every in-scope branch"
        );
        Ok(())
    }
}
