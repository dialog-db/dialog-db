//! Querying a transaction's uncommitted writes.
//!
//! [`Transaction::query`](crate::repository::branch::Transaction::query)
//! and [`VolatileTransaction::query`](crate::layer::VolatileTransaction::query)
//! both return a [`TransactionQuery`] that surfaces the transaction's
//! pending writes against the underlying source — "as-if committed"
//! semantics so a caller can run normal queries mid-transaction.
//!
//! # Pending asserts and retracts
//!
//! Pending [`Change::Assert`](dialog_artifacts::Change) and
//! [`Change::Replace`](dialog_artifacts::Change) entries materialize into
//! a fresh [`VolatileLayer`] at perform time and union with the source.
//! Pending [`Change::Retract`](dialog_artifacts::Change) entries become
//! *tombstones* keyed by [`sort_key`](crate::layer::sort_key) — the same
//! `(the, of, value_type, value_reference)` identity the prolly tree
//! uses — and filter matching facts out of the source's stream before
//! the merge.
//!
//! # Tombstone scope: source-only
//!
//! Tombstones suppress facts only in the source the transaction is
//! committing into (the branch for [`Transaction`], the layer for
//! [`VolatileTransaction`]). They do not affect the materialized pending
//! layer, so `tx.retract(X).assert(X)` correctly shows `X` (it's in the
//! materialized layer; the source's `X`, if any, is tombstoned).
//!
//! # Non-composable
//!
//! [`TransactionQuery`] intentionally has no `.with(...)` chain — a
//! transaction's view is the source plus pending changes, not a session
//! you can layer more sources onto. To compose more sources, commit the
//! transaction first and use [`Branch::query`](crate::Branch::query) or
//! [`VolatileLayer::query`](crate::layer::VolatileLayer::query).

use std::collections::HashSet;
use std::pin::Pin;

use async_trait::async_trait;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, Changes, DialogArtifactsError, Instruction, Select,
    Update,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use dialog_query::rule::deductive::DeductiveRule;
use dialog_query::source::SelectRules;
use futures_util::StreamExt;
use futures_util::stream;

use crate::Branch;
use crate::RemoteSite;
use crate::layer::{VolatileLayer, merge_grouped, sort_key};
use crate::repository::branch::select_from_branch;

/// Tombstone identity for a pending retraction.
///
/// Matches the prolly tree's per-key sort order:
/// `(the_bytes, of_bytes, value_type_byte, value_reference_hash)`.
/// Cause is intentionally excluded — a retraction means "this
/// `(the, of, is)` should not appear", regardless of which causal
/// history produced it in the source.
type Tombstone = (Vec<u8>, Vec<u8>, u8, [u8; 32]);

/// The source a [`TransactionQuery`] reads from.
///
/// Branches go through [`select_from_branch`] (which handles remote
/// upstream resolution); volatile layers go through their own
/// [`Provider<Select>`].
pub(crate) enum Source<'a> {
    /// A persistent branch.
    Branch(&'a Branch),
    /// A volatile in-memory layer.
    Volatile(&'a VolatileLayer),
}

/// Materialized pending state for a [`TransactionQuery`].
///
/// `changes` and `rules` are cloned/copied off the transaction so the
/// transaction remains intact (`query` takes `&self`) and the user can
/// still `.commit()` it after running queries.
struct Pending {
    changes: Changes,
    tombstones: HashSet<Tombstone>,
    rules: Vec<DeductiveRule>,
}

impl Pending {
    fn from_branch_transaction(changes: &Changes) -> Self {
        Self {
            changes: changes.clone(),
            tombstones: collect_tombstones(changes),
            rules: Vec::new(),
        }
    }

    fn from_volatile_transaction(changes: &Changes, rules: &[DeductiveRule]) -> Self {
        Self {
            changes: changes.clone(),
            tombstones: collect_tombstones(changes),
            rules: rules.to_vec(),
        }
    }
}

/// Walk a [`Changes`] snapshot and build the tombstone set from its
/// retract entries. Asserts and Replaces don't tombstone — they
/// contribute positive facts through the materialized pending layer.
fn collect_tombstones(changes: &Changes) -> HashSet<Tombstone> {
    let mut tombstones = HashSet::new();
    for instruction in changes.clone().into_instructions() {
        if let Instruction::Retract(artifact) = instruction {
            tombstones.insert(sort_key(&artifact));
        }
    }
    tombstones
}

/// A non-composable query handle returned by
/// [`Transaction::query`](crate::repository::branch::Transaction::query)
/// or [`VolatileTransaction::query`](crate::layer::VolatileTransaction::query).
///
/// See module docs for tombstone semantics and the "source-only"
/// filtering rationale.
pub struct TransactionQuery<'a> {
    source: Source<'a>,
    pending: Pending,
}

impl<'a> TransactionQuery<'a> {
    pub(crate) fn for_branch(branch: &'a Branch, changes: &Changes) -> Self {
        Self {
            source: Source::Branch(branch),
            pending: Pending::from_branch_transaction(changes),
        }
    }

    pub(crate) fn for_volatile(
        layer: &'a VolatileLayer,
        changes: &Changes,
        rules: &[DeductiveRule],
    ) -> Self {
        Self {
            source: Source::Volatile(layer),
            pending: Pending::from_volatile_transaction(changes, rules),
        }
    }

    /// Stage a query against this transaction's view. Call
    /// [`perform`](TransactionSelectQuery::perform) to execute.
    pub fn select<Q: Application>(self, query: Q) -> TransactionSelectQuery<'a, Q> {
        TransactionSelectQuery {
            source: self.source,
            pending: self.pending,
            query,
        }
    }
}

/// A staged query on a [`TransactionQuery`].
pub struct TransactionSelectQuery<'a, Q> {
    source: Source<'a>,
    pending: Pending,
    query: Q,
}

impl<'a, Q: Application> TransactionSelectQuery<'a, Q> {
    /// Execute the query. Materializes pending writes into a fresh
    /// [`VolatileLayer`], then runs the query against an env that
    /// unions the source (tombstone-filtered) with that layer.
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
            source,
            pending,
            query,
        } = self;
        async_stream::try_stream! {
            // Materialize pending changes into a fresh VolatileLayer.
            // We use the public transaction API rather than poking at
            // tree internals directly so this stays robust if the
            // underlying storage changes.
            let pending_layer = VolatileLayer::new();
            {
                let mut tx = pending_layer.transaction();
                // Walk the cloned Changes and replay each instruction
                // into the scratch layer's transaction. Asserts/Replaces
                // contribute positive entries; Retracts apply on top so
                // patterns like `assert(X).retract(X)` collapse to nothing
                // in the materialized layer just as they would on commit.
                for instruction in pending.changes.into_instructions() {
                    match instruction {
                        Instruction::Assert(a) => {
                            Update::associate(&mut tx, a.the, a.of, a.is);
                        }
                        Instruction::Replace(a) => {
                            Update::associate_unique(&mut tx, a.the, a.of, a.is);
                        }
                        Instruction::Retract(a) => {
                            Update::dissociate(&mut tx, a.the, a.of, a.is);
                        }
                    }
                }
                for rule in pending.rules {
                    tx = tx.register(rule).map_err(|e| DialogArtifactsError::Storage(
                        format!("rule registration failed: {e}")
                    ))?;
                }
                tx.commit().await?;
            }

            let trans_env = TransactionEnv {
                source,
                pending_layer,
                tombstones: pending.tombstones,
                env,
            };

            let results = Box::pin(query.perform(&trans_env));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// The runtime environment serving a [`TransactionSelectQuery`].
///
/// Provides the union of the (tombstone-filtered) source stream and
/// the materialized pending layer to the query engine. Built per
/// `.perform(env)` so the environment reference is never captured on
/// the [`TransactionQuery`] itself.
pub(crate) struct TransactionEnv<'a, Env> {
    source: Source<'a>,
    pending_layer: VolatileLayer,
    tombstones: HashSet<Tombstone>,
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
        let source_stream = match &self.source {
            Source::Branch(b) => select_from_branch(b, self.env, input.clone()).await?,
            Source::Volatile(l) => Provider::<Select<'a>>::execute(*l, input.clone()).await?,
        };
        // Filter the source's stream — and only the source's — by
        // tombstones. The pending layer's facts are never filtered
        // (the user's most recent intent wins, even if they retracted
        // the same fact earlier in the transaction).
        let filtered_source = filter_by_tombstones(source_stream, self.tombstones.clone());
        let pending_stream =
            Provider::<Select<'a>>::execute(&self.pending_layer, input).await?;
        Ok(merge_grouped(vec![filtered_source, pending_stream]))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env: ConditionalSync> Provider<SelectRules> for TransactionEnv<'_, Env> {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        // Branches don't (yet) carry rules of their own; volatile layers do.
        let mut acquired = match &self.source {
            Source::Branch(_) => ConceptRules::new(&input),
            Source::Volatile(l) => Provider::<SelectRules>::execute(*l, input.clone()).await?,
        };
        let pending_rules =
            Provider::<SelectRules>::execute(&self.pending_layer, input).await?;
        acquired.extend(&pending_rules);
        Ok(acquired)
    }
}

/// Wrap an artifact stream in a filter that drops any item whose
/// [`sort_key`] matches an entry in `tombstones`.
fn filter_by_tombstones<'a>(
    inner: ArtifactStream<'a>,
    tombstones: HashSet<Tombstone>,
) -> ArtifactStream<'a> {
    if tombstones.is_empty() {
        return inner;
    }
    Box::pin(stream::unfold(
        (Pin::from(inner), tombstones),
        |(mut inner, tombstones)| async move {
            loop {
                match inner.next().await {
                    None => return None,
                    Some(Err(e)) => return Some((Err::<Artifact, _>(e), (inner, tombstones))),
                    Some(Ok(artifact)) => {
                        if tombstones.contains(&sort_key(&artifact)) {
                            continue;
                        }
                        return Some((Ok(artifact), (inner, tombstones)));
                    }
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use crate::layer::VolatileLayer;
    use dialog_artifacts::Entity;
    use dialog_query::query::Output;
    use dialog_query::{Concept, Query, Term, the};

    mod people {
        #[derive(dialog_query::Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("test")]
        pub struct Name(pub String);
    }

    #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Person {
        this: Entity,
        name: people::Name,
    }

    /// `tx.query()` surfaces pending asserts as if they were committed.
    #[dialog_common::test]
    async fn branch_transaction_query_sees_pending_asserts() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        // Pending assert: not committed yet — but visible through tx.query().
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

        assert_eq!(results.len(), 1, "pending assert must be visible");
        assert_eq!(results[0].this, alice);
        assert_eq!(results[0].name.0, "Alice");
        Ok(())
    }

    /// `tx.query()` tombstones pending retracts so the branch's matching
    /// facts disappear from the result — even though they're still in
    /// the branch's persistent tree.
    #[dialog_common::test]
    async fn branch_transaction_query_tombstones_pending_retracts() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;

        // Commit two people to the branch.
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

        // Open a new transaction that retracts Alice's name.
        let tx = branch.transaction().retract(
            the!("test/name")
                .of(alice.clone())
                .is("Alice".to_string()),
        );

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

        assert_eq!(
            names,
            vec!["Bob".to_string()],
            "Alice must be tombstoned through tx.query() even though the branch \
             still holds her name; got {names:?}"
        );

        // And the branch itself is untouched until commit.
        let committed_names: Vec<String> = branch
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
        let mut sorted = committed_names.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec!["Alice".to_string(), "Bob".to_string()],
            "branch.query() must still see both names — tx is not committed; got {committed_names:?}"
        );
        Ok(())
    }

    /// `tx.retract(X).assert(X)` shows X because the materialized layer
    /// has X (Retract is a no-op on a fresh tree, Assert adds it) AND
    /// any X in the branch is tombstoned but the layer's X passes the
    /// filter (tombstones only apply to the source stream).
    #[dialog_common::test]
    async fn branch_transaction_query_retract_then_assert_keeps_value() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let alice: Entity = "id:alice".parse()?;
        // Commit Alice to the branch.
        branch
            .transaction()
            .assert(Person {
                this: alice.clone(),
                name: people::Name("Alice".into()),
            })
            .commit()
            .perform(&operator)
            .await?;

        let stmt = the!("test/name")
            .of(alice.clone())
            .is("Alice".to_string());
        // Retract Alice then re-assert: the net pending effect is that
        // Alice should be visible.
        let tx = branch
            .transaction()
            .retract(stmt.clone())
            .assert(stmt);

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
        assert_eq!(
            names,
            vec!["Alice".to_string()],
            "retract followed by assert must surface the value via the materialized \
             pending layer (tombstones do not filter it); got {names:?}"
        );
        Ok(())
    }

    /// `volatile_layer.transaction().query()` works on volatile sources too.
    #[dialog_common::test]
    async fn volatile_transaction_query_sees_pending_state() -> anyhow::Result<()> {
        let (operator, _profile) = test_operator_with_profile().await;

        let layer = VolatileLayer::new();
        // Commit one person to the layer.
        let alice: Entity = "id:alice".parse()?;
        layer
            .transaction()
            .assert(Person {
                this: alice.clone(),
                name: people::Name("Alice".into()),
            })
            .commit()
            .await?;

        let bob: Entity = "id:bob".parse()?;
        // Open a transaction that adds Bob and retracts Alice.
        let tx = layer
            .transaction()
            .assert(Person {
                this: bob.clone(),
                name: people::Name("Bob".into()),
            })
            .retract(
                the!("test/name")
                    .of(alice.clone())
                    .is("Alice".to_string()),
            );

        let mut names: Vec<String> = tx
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
        names.sort();
        assert_eq!(
            names,
            vec!["Bob".to_string()],
            "volatile tx.query() must add Bob (pending assert) and drop Alice \
             (pending retract tombstone); got {names:?}"
        );
        Ok(())
    }
}
