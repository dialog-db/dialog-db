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
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use dialog_query::source::SelectRules;

use crate::Branch;
use crate::RemoteSite;
use crate::layer::{filter_tombstones, merge_grouped, tombstones_from};
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
    tombstones: HashSet<SortKey>,
}

impl<'a> TransactionQuery<'a> {
    pub(crate) fn new(branch: &'a Branch, changes: &Changes) -> Self {
        Self {
            branch,
            changes: changes.clone(),
            tombstones: tombstones_from(changes),
        }
    }

    /// Stage a query against this transaction's view. Call
    /// [`perform`](TransactionSelectQuery::perform) to execute.
    pub fn select<Q: Application>(self, query: Q) -> TransactionSelectQuery<'a, Q> {
        TransactionSelectQuery {
            branch: self.branch,
            changes: self.changes,
            tombstones: self.tombstones,
            query,
        }
    }
}

/// A staged query on a [`TransactionQuery`].
pub struct TransactionSelectQuery<'a, Q> {
    branch: &'a Branch,
    changes: Changes,
    tombstones: HashSet<SortKey>,
    query: Q,
}

impl<'a, Q: Application> TransactionSelectQuery<'a, Q> {
    /// Execute the query against an env that unions the branch
    /// (tombstone-filtered) with the transaction's pending changes.
    pub fn perform<Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let TransactionSelectQuery {
            branch,
            changes,
            tombstones,
            query,
        } = self;
        async_stream::try_stream! {
            let trans_env = TransactionEnv {
                branch,
                changes,
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
}
